# getzeteo_v4.py
from __future__ import annotations

import re
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- V4 infra (names consistent with your other harvesters) ----
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

HARVESTER_ID = "zeteo"

# Zeteo Substack-style archive API
API_URL = "https://zeteo.com/api/v1/archive"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# Helpers (kept in parity with getmeidas_v4 where reasonable)
# ---------------------------------------------------------------------------

def _iso_date_from_any(obj: Dict[str, Any]) -> Optional[date]:
    """
    Accept common timestamp fields; slice YYYY-MM-DD.
    We EXCLUSIVELY use post_date (or published_at/created_at fallback) for windowing.
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
    # Substack-like archives surface canonical_url and/or url
    return (p.get("canonical_url") or p.get("url") or "").strip()

def _posts_from_json(payload: Any) -> List[Dict[str, Any]]:
    # Some APIs return a list; others wrap in {"items":[...]}
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

# ---------------------------------------------------------------------------
# Discovery (COPY mode from Meidas): newest→older, early-stop rule
# Here: use ONLY post_date (no title date) and filter on “This Week in Democracy”
# ---------------------------------------------------------------------------

def _discover_copy_mode(
    start_iso: str,
    end_iso: str,
    *,
    pages: int,          # safety ceiling
    per: int,
    timeout: int,
    logger
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Page newest→older. Keep posts in [start..end] whose title contains 'This Week in Democracy' (case-insensitive).
    Windowing uses ONLY post_date/published_at/created_at (no title-based dates).
    Early-stop: when this page contains any post older than start, *and* we found no in-window items on this page.
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

    seen_ids:  set[str] = set()   # de-dup for snapshot
    seen_keys: set[str] = set()   # de-dup for matches

    max_pages = max(1, int(pages))
    per = min(50, max(1, int(per)))  # Substack caps at 50

    logger.info(
        "Starting Zeteo archive fetch: window %s → %s (max_pages=%d, per_page=%d)",
        start_d, end_d, max_pages, per
    )

    page_idx = 0
    while page_idx < max_pages:
        offset = page_idx * per
        params = {"sort": "new", "offset": offset, "limit": per}
        logger.info("REQUEST page=%d: GET %s params=%s", page_idx + 1, API_URL, params)

        try:
            r = s.get(API_URL, params=params)
            logger.info("FETCHED page=%d → %s", page_idx + 1, r.url)
            status_ok = (r.status_code == 200)
            logger.debug("HTTP status=%s bytes=%s", r.status_code, len(r.content))
            if not status_ok:
                logger.warning("Non-200 from archive on page %d (status=%s). Stopping.", page_idx + 1, r.status_code)
                break
            try:
                payload = r.json()
            except Exception as e:
                logger.warning("JSON parse error on page %d: %s", page_idx + 1, e)
                break
        except requests.RequestException as e:
            logger.warning("Request error on page %d: %s", page_idx + 1, e)
            break

        posts = _posts_from_json(payload)
        logger.info("Page %d returned %d posts", page_idx + 1, len(posts))

        if not isinstance(posts, list):
            logger.warning("Unexpected JSON shape on page %d; stopping. top_keys=%s",
                           page_idx + 1, list(payload.keys())[:10] if isinstance(payload, dict) else type(payload))
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

        # Page summary & control flags
        dates_on_page: List[date] = []
        kept_on_page = 0
        older_seen = False
        in_range_found = False

        # Summarize dates for logging
        for p in page_posts:
            used_d = _iso_date_from_any(p)
            if used_d:
                dates_on_page.append(used_d)

        earliest = min(dates_on_page).isoformat() if dates_on_page else ""
        latest   = max(dates_on_page).isoformat() if dates_on_page else ""
        twid_count = sum(1 for p in page_posts if "this week in democracy" in _title_of(p).lower())

        logger.info(
            "Page %d: total=%d new_unique=%d twid_titles=%d date_range=[%s .. %s]",
            page_idx + 1, len(posts), new_this_page, twid_count, earliest or "?", latest or "?"
        )

        # Per-item decisions (windowing with USED DATE = post_date/published_at/created_at only)
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
                if "this week in democracy" in (title or "").lower():
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
                    logger.debug("SKIPT(reason=not_twid) title=%r date=%s url=%s", title, used_d.isoformat(), url)
            else:
                reason = "older_than_start" if used_d < start_d else "after_end"
                logger.debug("SKIPT(reason=%s) title=%r date=%s url=%s", reason, title, used_d.isoformat(), url)

        logger.info(
            "Page %d decisions: kept=%d, in_window=%s, saw_older=%s",
            page_idx + 1, kept_on_page, in_range_found, older_seen
        )

        # Early stop: once page is entirely older than start and had no in-window hits
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

# ---------------------------------------------------------------------------
# Transform to V4 entity schema
# ---------------------------------------------------------------------------

def _to_entity_v4(p: Dict[str, Any]) -> Dict[str, Any]:
    title = _title_of(p)
    url = _url_of(p)
    # Use the date we matched on; else fallback again to post_date/published_at/created_at
    d = None
    if isinstance(p.get("_matched_date"), str):
        try:
            d = datetime.strptime(p["_matched_date"], "%Y-%m-%d").date()
        except Exception:
            d = None
    if not d:
        d = _iso_date_from_any(p)
    post_date = d.isoformat() if d else ""

    return {
        "source": "Zeteo",
        "doc_type": "news_article",
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": "",         # no transcript/JW asset handling for Zeteo TWID posts
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date,
        "raw_line": f"[zeteo] {title} ({post_date})",
    }

# ---------------------------------------------------------------------------
# Public entry (V4 standard)
# ---------------------------------------------------------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Step-1 harvester (COPY-mode from Meidas):
      RAW:      snapshot of ALL parsed archive items (pre-window)
      FILTERED: windowed, de-duplicated list of TWID posts (entities)
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    # Same defaults you used successfully with Meidas
    PAGES_CAP = 2000
    PER_PAGE  = 50
    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Zeteo (COPY mode): archive=%s", API_URL)

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
                "raw_line": f"[zeteo_raw] {_title_of(it)}",
            }
            for it in all_seen
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED write — entities that passed the window gating (stable de-dup by canonical_url)
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
# Optional direct CLI (matches other V4 modules)
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — Zeteo harvester")
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
    log.info("Summary: %s", meta)   