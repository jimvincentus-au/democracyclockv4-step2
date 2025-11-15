# getpopinfo_v4.py
from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- V4 infra (shared) ----
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

HARVESTER_ID = "popinfo"  # --only popinfo
SOURCE_NAME  = "Popular Information"

# Substack archive endpoint for Popular Information
API_URL = "https://popular.info/api/v1/archive"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ---------------------------
# Helpers
# ---------------------------

def _iso_date_from_any(obj: Dict[str, Any]) -> Optional[date]:
    """
    Accept common Substack-ish fields; prefer post_date if present.
    """
    candidates = [
        obj.get("post_date"),
        obj.get("published_at"),
        obj.get("created_at"),
        obj.get("date"),
    ]
    for s in candidates:
        if not s:
            continue
        try:
            return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
        except Exception:
            continue
    return None

def _title_of(p: Dict[str, Any]) -> str:
    return (p.get("title") or p.get("social_title") or "").strip()

def _url_of(p: Dict[str, Any]) -> str:
    return (p.get("canonical_url") or p.get("url") or "").strip()

def _posts_from_json(payload: Any) -> List[Dict[str, Any]]:
    """
    Some archives return a list; others wrap items in {"items":[...]}.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return payload["items"]  # type: ignore[return-value]
    return []

def _make_retry_session(timeout: int) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(BROWSER_HEADERS)

    # attach default timeout via wrapper
    orig_request = s.request
    def _with_timeout(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return orig_request(method, url, **kwargs)
    s.request = _with_timeout  # type: ignore[assignment]
    return s

def _print_titles_and_dates(posts: Iterable[Dict[str, Any]], logger) -> None:
    for p in posts or []:
        title = _title_of(p)
        dstr = (p.get("post_date") or p.get("published_at") or p.get("created_at") or p.get("date") or "").strip()
        dshow = dstr[:19] if dstr else ""
        logger.debug("%s | %s", dshow, title)

# ---------------------------
# COPY-mode discovery (Meidas V4 flow, Pop Info filter)
# ---------------------------

def _discover_copy_mode(
    start_iso: str,
    end_iso: str,
    *,
    pages: int,   # safety ceiling
    per: int,
    timeout: int,
    logger
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Newest→older via Substack archive API, offset paging.
    Keep posts whose post_date (or ISO fallback) is within [start .. end].
    Early stop only if this page contains items older than start AND we found no in-window items on this page.
    """
    try:
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    s = _make_retry_session(timeout)

    all_seen:  List[Dict[str, Any]] = []
    matches:   List[Dict[str, Any]] = []
    audit:     List[Dict[str, Any]] = []

    seen_ids:  set[str] = set()   # stabilize snapshot de-dupe
    seen_keys: set[str] = set()   # stabilize match de-dupe

    max_pages = max(1, int(pages))
    per = min(50, max(1, int(per)))  # Substack caps at 50

    logger.info(
        "Starting %s archive fetch: window %s → %s (max_pages=%d, per_page=%d)",
        SOURCE_NAME, start_d, end_d, max_pages, per
    )

    stagnation = 0
    STAGNATION_LIMIT = 3

    page_idx = 0
    while page_idx < max_pages:
        offset = page_idx * per
        params = {"sort": "new", "offset": offset, "limit": per}
        logger.info("REQUEST page=%d: GET %s params=%s", page_idx + 1, API_URL, params)

        try:
            r = s.get(API_URL, params=params)
            logger.info("FETCHED page=%d → %s", page_idx + 1, r.url)
            logger.debug("HTTP status=%s bytes=%s", r.status_code, len(r.content))
        except requests.RequestException as e:
            logger.warning("Request error on page %d: %s", page_idx + 1, e)
            break

        if r.status_code != 200:
            logger.warning("Non-200 from archive on page %d (status=%s). Stopping.", page_idx + 1, r.status_code)
            break

        try:
            payload = r.json()
        except Exception as e:
            logger.warning("JSON parse error on page %d: %s", page_idx + 1, e)
            break

        posts = _posts_from_json(payload)
        if not isinstance(posts, list):
            logger.warning(
                "Unexpected JSON shape on page %d; stopping. top_keys=%s",
                page_idx + 1, list(payload.keys())[:10] if isinstance(payload, dict) else type(payload)
            )
            break

        if not posts:
            logger.info("Empty page at %d; stopping.", page_idx + 1)
            break

        # Normalize + de-dup within this page
        page_posts: List[Dict[str, Any]] = []
        for idx, p in enumerate(posts):
            pid = (
                str(p.get("canonical_url") or p.get("url") or "").strip()
                or f"{_iso_date_from_any(p) or ''}|{_title_of(p)}"
                or f"{page_idx}:{idx}"
            )
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            page_posts.append(p)

        new_this_page = len(page_posts)
        all_seen.extend(page_posts)

        # Page summary / bounds
        dates_on_page: List[date] = []
        kept_on_page = 0
        older_seen = False
        in_range_found = False

        for p in page_posts:
            used_d = _iso_date_from_any(p)
            if used_d:
                dates_on_page.append(used_d)

        earliest = min(dates_on_page).isoformat() if dates_on_page else ""
        latest   = max(dates_on_page).isoformat() if dates_on_page else ""
        logger.info(
            "Page %d: total=%d new_unique=%d date_range=[%s .. %s]",
            page_idx + 1, len(posts), new_this_page, earliest or "?", latest or "?"
        )

        if new_this_page == 0:
            stagnation += 1
            logger.debug("Stagnation %d/%d: page %d yielded 0 new uniques",
                         stagnation, STAGNATION_LIMIT, page_idx + 1)
            if stagnation >= STAGNATION_LIMIT:
                logger.warning("Stopping due to repeated stagnation (no new uniques).")
                break
        else:
            stagnation = 0

        # Per-item decisions (Pop Info: date-only gating, no title rules)
        for p in page_posts:
            title = _title_of(p)
            url   = _url_of(p)
            used_d = _iso_date_from_any(p)

            audit.append({
                "page": page_idx + 1,
                "title": title,
                "url": url,
                "date_used": used_d.isoformat() if used_d else "",
            })

            if not used_d:
                logger.debug("SKIPT(reason=no_date) title=%r url=%s", title, url)
                continue

            if used_d < start_d:
                older_seen = True

            if start_d <= used_d <= end_d:
                in_range_found = True
                key = str(p.get("id") or url or title)
                if key not in seen_keys:
                    seen_keys.add(key)
                    q = dict(p)
                    q["_matched_date"] = used_d.isoformat()
                    matches.append(q)
                    kept_on_page += 1
                    logger.debug("KEPT title=%r date=%s url=%s", title, used_d.isoformat(), url)
                else:
                    logger.debug("SKIPT(reason=dup_key) title=%r date=%s url=%s", title, used_d.isoformat(), url)
            else:
                reason = "older_than_start" if used_d < start_d else "after_end"
                logger.debug("SKIPT(reason=%s) title=%r date=%s url=%s",
                             reason, title, used_d.isoformat(), url)

        logger.info(
            "Page %d decisions: kept=%d, in_window=%s, saw_older=%s",
            page_idx + 1, kept_on_page, in_range_found, older_seen
        )

        # Early stop: we’ve paged past start and this page had no in-window items
        if older_seen and not in_range_found:
            logger.info("Early stop at page %d (older-than-start and no in-window hits).", page_idx + 1)
            break

        page_idx += 1  # next page

    logger.info(
        "Archive fetch complete. total_unique_seen=%d in_window_kept=%d",
        len(all_seen), len(matches)
    )

    _print_titles_and_dates(all_seen, logger)
    return matches, all_seen, audit

# ---------------------------
# Transform to V4 entity schema
# ---------------------------

def _to_entity_v4(p: Dict[str, Any]) -> Dict[str, Any]:
    title = _title_of(p)
    url   = _url_of(p)
    iso_d = (p.get("_matched_date") or _iso_date_from_any(p))
    post_date = iso_d if isinstance(iso_d, str) else (iso_d.isoformat() if iso_d else "")

    return {
        "source": SOURCE_NAME,
        "doc_type": "news_article",
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date or "",
        "raw_line": f"[popinfo] {title} ({post_date or ''})",
    }

# ---------------------------
# Public entry (V4 standard)
# ---------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Copy-mode harvester for Popular Information (Substack).
      RAW:      snapshot of ALL parsed archive items (pre-window)
      FILTERED: windowed, de-duplicated list of posts (date-only gating)
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    # Paging defaults (safe upper ceiling)
    PAGES_CAP = 2000
    PER_PAGE  = 50
    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering %s (COPY mode): archive=%s", SOURCE_NAME, API_URL)

    matches, all_seen, audit_rows = _discover_copy_mode(
        start_iso=start, end_iso=end, pages=PAGES_CAP, per=PER_PAGE, timeout=TIMEOUT_S, logger=logger
    )

    # Transform matches → V4 entities
    entities = [_to_entity_v4(p) for p in matches]

    # RAW write — include full snapshot (pre-window) with audit
    raw_payload = {
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "archive_url": API_URL,
        "parsed_total": len(all_seen),
        "audit": audit_rows,
        "items_snapshot": [
            {
                "url": _url_of(it),
                "title": _title_of(it),
                "post_date": (it.get("_matched_date") or it.get("post_date") or it.get("published_at") or "")[:10],
                "doc_type": "news_article",
                "raw_line": f"[popinfo_raw] {_title_of(it)}",
            }
            for it in all_seen
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED write — stable de-dup by canonical_url
    seen = set()
    deduped: List[Dict[str, Any]] = []
    dupes = 0
    for e in entities:
        k = e.get("canonical_url") or e.get("url") or ""
        if not k or k in seen:
            dupes += 1
            logger.debug("Dedupe: SKIPT duplicate canonical=%r", k)
            continue
        seen.add(k)
        deduped.append(e)

    win_stats = {
        "inside": len(entities),
        "outside": 0,
        "nodate": 0,
        "no_title": 0,
        "no_url": 0,
        "dupes": dupes,
    }

    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | dupes=%d",
        start, end, len(all_seen), len(entities), len(deduped), dupes
    )

    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "news_article",
        "window": {"start": start, "end": end},
        "count": len(deduped),
        "entities": deduped,
        "window_stats": win_stats,
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(deduped))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(deduped),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }

# ---------------------------
# Optional direct CLI
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — Popular Information harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    log = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        log_path=None,
        session=None,
    )
    log.info("Summary: %s", json.dumps(meta, ensure_ascii=False))