# getmeidas_v4.py
from __future__ import annotations

import re
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- V4 infra ----
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

HARVESTER_ID = "meidas"
API_URL = "https://www.meidasplus.com/api/v1/archive"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# V3-equivalent helpers (unchanged behavior)
# ---------------------------------------------------------------------------

def _date_from_title(title: str) -> Optional[date]:
    s = (title or "").strip()
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', s)
    if not m:
        return None
    mm, dd, yy = m.groups()
    try:
        mm_i, dd_i = int(mm), int(dd)
        yy_i = int(yy)
        if yy_i < 100:  # assume 20xx
            yy_i += 2000
        return date(yy_i, mm_i, dd_i)
    except Exception:
        return None

def _iso_date_from_any(obj: Dict[str, Any]) -> Optional[date]:
    candidates = [obj.get("post_date"), obj.get("published_at"), obj.get("created_at"), obj.get("date")]
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
    V3 accepted either a list or {"items":[...]}.
    Be a bit more defensive: accept common shapes seen in the wild.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "posts", "data"):
            v = payload.get(key)
            if isinstance(v, list):
                return v  # type: ignore[return-value]
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

def _end_date_from_weeks(start_d: date, weeks: int) -> date:
    days_to_friday = (4 - start_d.weekday()) % 7  # Mon=0
    first_week_end = start_d + timedelta(days=days_to_friday)
    if weeks <= 1:
        return first_week_end
    return first_week_end + timedelta(days=7 * (weeks - 1))

# ---------------------------------------------------------------------------
# V3 COPY-mode discovery (offset/limit paging, newest→older, Bulletin-only)
# ---------------------------------------------------------------------------

def _discover(
    start_iso: str,
    end_iso: str,
    *,
    pages: int,          # max pages safety ceiling
    per: int,
    timeout: int,
    logger
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    from datetime import timezone
    try:
        # Window boundaries as local dates (America/New_York)
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    # --- helpers (scoped to this function to keep drop-in simple) ---
    try:
        from zoneinfo import ZoneInfo  # py>=3.9
        TZ_NY = ZoneInfo("America/New_York")
    except Exception:
        TZ_NY = None  # Fallback: treat timestamps as naive/UTC if zoneinfo absent

    def _parse_published_local_date(p: Dict[str, Any]) -> Optional[date]:
        """Pick the authoritative timestamp (published), parse to aware dt, convert to NY, return .date()."""
        s = p.get("published_at") or p.get("post_date") or p.get("created_at") or p.get("date")
        if not s:
            return None
        ss = str(s).strip()
        try:
            # Handle common 'Z' UTC suffix
            if ss.endswith("Z"):
                dt_utc = datetime.fromisoformat(ss.replace("Z", "+00:00"))
            else:
                # If it already has offset, fromisoformat handles it; else assume UTC
                if ("+" in ss[10:] or "-" in ss[10:]) and "T" in ss:
                    dt_utc = datetime.fromisoformat(ss)
                else:
                    # Bare "YYYY-MM-DD" or naive "YYYY-MM-DDTHH:MM:SS"
                    if "T" in ss:
                        dt_utc = datetime.fromisoformat(ss)
                    else:
                        # date-only: treat as midnight UTC
                        dt_utc = datetime.fromisoformat(ss + "T00:00:00")
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
            # Convert to America/New_York if available
            if TZ_NY is not None:
                dt_local = dt_utc.astimezone(TZ_NY)
            else:
                # Best-effort fallback: use UTC as "local"
                dt_local = dt_utc
            return dt_local.date()
        except Exception:
            return None

    def _is_series_shift(title: str) -> bool:
        t = (title or "").strip().lower()
        return t.startswith("today in politics") or t.startswith("this weekend in politics")

    # ---------------------------------------------------------------

    s = _make_retry_session(timeout)

    all_seen:  List[Dict[str, Any]] = []
    matches:   List[Dict[str, Any]] = []
    audit:     List[Dict[str, Any]] = []

    seen_ids:  set[str] = set()   # de-dup for snapshot
    seen_keys: set[str] = set()   # de-dup for matches

    max_pages = max(1, int(pages))
    per = min(50, max(1, int(per)))  # API caps at 50

    logger.info(
        "Starting Meidas archive fetch: window %s → %s (max_pages=%d, per_page=%d)",
        start_d, end_d, max_pages, per
    )

    page_idx = 0
    while page_idx < max_pages:
        offset = page_idx * per
        params = {"sort": "new", "offset": offset, "limit": per}
        logger.info("REQUEST page=%d: GET %s params=%s", page_idx + 1, API_URL, params)

        try:
            r = s.get(API_URL, params=params)
        except requests.RequestException as e:
            logger.warning("Request error on page %d: %s", page_idx + 1, e)
            break

        logger.info("FETCHED page=%d → %s", page_idx + 1, getattr(r, "url", "(no url)"))
        logger.debug("HTTP status=%s bytes=%s", r.status_code, len(r.content))

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

        logger.info("Page %d returned %d posts", page_idx + 1, len(page_posts))
        all_seen.extend(page_posts)

        # Page summary (based on PUBLISHED LOCAL date only)
        dates_on_page: List[date] = []
        bulletins_on_page = 0
        kept_on_page = 0
        in_range_found = False

        for p in page_posts:
            title = _title_of(p) or ""
            if "bulletin" in title.lower():
                bulletins_on_page += 1
            d_pub = _parse_published_local_date(p)
            if d_pub:
                dates_on_page.append(d_pub)

        earliest = min(dates_on_page).isoformat() if dates_on_page else ""
        latest   = max(dates_on_page).isoformat() if dates_on_page else ""
        logger.info(
            "Page %d: total=%d new_unique=%d bulletin_titles=%d date_range=[%s .. %s]",
            page_idx + 1, len(posts), len(page_posts), bulletins_on_page, earliest or "?", latest or "?"
        )

        # Per-item decisions (windowing uses ONLY published_local date)
        for p in page_posts:
            title = _title_of(p)
            url   = _url_of(p)
            d_pub = _parse_published_local_date(p)
            if d_pub is None:
                audit.append({
                    "page": page_idx + 1,
                    "title": title,
                    "url": url,
                    "published_local": "",
                    "anchor_date": "",
                    "series_shift": False,
                    "decision": "SKIPT(no_published_date)"
                })
                logger.debug("SKIPT(reason=no_published_date) title=%r url=%s", title, url)
                continue

            # Anchor/labeling: apply -1 day for the two series; windowing still uses d_pub
            series = _is_series_shift(title)
            anchor = d_pub - timedelta(days=1) if series else d_pub
            if series:
                logger.debug("Applied series-date adjustment (–1 day) for %r → anchor=%s (from published=%s)",
                             title, anchor.isoformat(), d_pub.isoformat())

            # Audit row
            audit.append({
                "page": page_idx + 1,
                "title": title,
                "url": url,
                "published_local": d_pub.isoformat(),
                "anchor_date": anchor.isoformat(),
                "series_shift": bool(series),
                "decision": ""
            })

            if start_d <= d_pub <= end_d:
                in_range_found = True

                title_l = (title or "").lower()
                is_bulletin = "bulletin" in title_l
                is_news_update = "news update" in title_l

                if is_bulletin or is_news_update:
                    key = str(p.get("id") or url or title)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        q = dict(p)
                        # Keep the anchor date we present downstream (windowing used published date)
                        q["_matched_date"] = anchor.isoformat()
                        matches.append(q)
                        kept_on_page += 1
                        audit[-1]["decision"] = "KEPT"
                        logger.debug(
                            "KEPT title=%r published=%s anchor=%s url=%s (bulletin=%s news_update=%s)",
                            title, d_pub.isoformat(), anchor.isoformat(), url, is_bulletin, is_news_update
                        )
                    else:
                        audit[-1]["decision"] = "SKIPT(dup_key)"
                        logger.debug("SKIPT(reason=dup_key) title=%r published=%s url=%s",
                                     title, d_pub.isoformat(), url)
                else:
                    audit[-1]["decision"] = "SKIPT(not_bulletin_or_newsupdate)"
                    logger.debug("SKIPT(reason=not_bulletin_or_newsupdate) title=%r published=%s url=%s",
                                 title, d_pub.isoformat(), url)
            else:
                reason = "older_than_start" if d_pub < start_d else "after_end"
                audit[-1]["decision"] = f"SKIPT({reason})"
                logger.debug("SKIPT(reason=%s) title=%r published=%s url=%s",
                             reason, title, d_pub.isoformat(), url)

        logger.info(
            "Page %d decisions: kept=%d, in_window=%s",
            page_idx + 1, kept_on_page, in_range_found
        )

        # ✅ Correct early-stop:
        # Only stop when the ENTIRE page is older than start (max date < start).
        page_max = max(dates_on_page) if dates_on_page else None
        if page_max is not None and page_max < start_d and not in_range_found:
            logger.info(
                "Early stop: page %d is entirely older than start (page_max=%s < start=%s) and no in-window hits.",
                page_idx + 1, page_max.isoformat(), start_d.isoformat()
            )
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
    iso = (p.get("_matched_date") or (_date_from_title(title) or _iso_date_from_any(p)))
    post_date = iso if isinstance(iso, str) else (iso.isoformat() if iso else "")

    return {
        "source": "MeidasTouch",
        "doc_type": "news_article",
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date or "",
        "raw_line": f"[meidas] {title} ({post_date or ''})",
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
    Step-1 harvester (COPY-mode from V3):
      RAW:      snapshot of ALL parsed archive items (pre-window)
      FILTERED: windowed, de-duplicated list of Bulletin posts (entities)
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    # Match V3 behavior: offset/limit pagination, early stop, bulletin-only
    PAGES_CAP = 80     # your V3 default; raise if you truly need deeper scan
    PER_PAGE  = 50     # API max
    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Meidas (COPY mode via API): %s", API_URL)

    matches, all_seen, audit_rows = _discover(
        start_iso=start, end_iso=end, pages=PAGES_CAP, per=PER_PAGE, timeout=TIMEOUT_S, logger=logger
    )

    entities = [_to_entity_v4(p) for p in matches]

    # RAW payload
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
                "raw_line": f"[meidas_raw] {_title_of(it)}",
            }
            for it in all_seen
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED payload (stable de-dup by canonical_url)
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
    p = argparse.ArgumentParser(description="Democracy Clock V4 — MeidasTouch harvester")
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