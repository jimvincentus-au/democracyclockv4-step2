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

HARVESTER_ID = "tracker"
API_URL = "https://trumptyrannytracker.substack.com/api/v1/archive"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# V3-equivalent helpers (UNCHANGED — COPY MODE)
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
        if yy_i < 100:
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
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "posts", "data"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
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
    s.request = _with_timeout
    return s

def _print_titles_and_dates(posts: Iterable[Dict[str, Any]], logger) -> None:
    for p in posts or []:
        title = _title_of(p)
        dstr = (p.get("post_date") or p.get("published_at") or p.get("created_at") or p.get("date") or "").strip()
        dshow = dstr[:19] if dstr else ""
        logger.debug("%s | %s", dshow, title)

def _end_date_from_weeks(start_d: date, weeks: int) -> date:
    days_to_friday = (4 - start_d.weekday()) % 7
    first_week_end = start_d + timedelta(days=days_to_friday)
    if weeks <= 1:
        return first_week_end
    return first_week_end + timedelta(days=7 * (weeks - 1))

# ---------------------------------------------------------------------------
# V3 COPY-mode discovery (offset/limit paging, newest→older, KEEP-ALL-IN-WINDOW)
# ---------------------------------------------------------------------------

def _discover(
    start_iso: str,
    end_iso: str,
    *,
    pages: int,
    per: int,
    timeout: int,
    logger,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    from datetime import timezone
    try:
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    try:
        from zoneinfo import ZoneInfo
        TZ_NY = ZoneInfo("America/New_York")
    except Exception:
        TZ_NY = None

    def _parse_published_local_date(p: Dict[str, Any]) -> Optional[date]:
        s = p.get("published_at") or p.get("post_date") or p.get("created_at") or p.get("date")
        if not s:
            return None
        ss = str(s).strip()
        try:
            if ss.endswith("Z"):
                dt_utc = datetime.fromisoformat(ss.replace("Z", "+00:00"))
            else:
                if ("+" in ss[10:] or "-" in ss[10:]) and "T" in ss:
                    dt_utc = datetime.fromisoformat(ss)
                else:
                    if "T" in ss:
                        dt_utc = datetime.fromisoformat(ss)
                    else:
                        dt_utc = datetime.fromisoformat(ss + "T00:00:00")
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
            if TZ_NY is not None:
                dt_local = dt_utc.astimezone(TZ_NY)
            else:
                dt_local = dt_utc
            return dt_local.date()
        except Exception:
            return None

    s = _make_retry_session(timeout)

    all_seen: List[Dict[str, Any]] = []
    matches:  List[Dict[str, Any]] = []
    audit:    List[Dict[str, Any]] = []

    seen_ids: set[str] = set()
    seen_keys: set[str] = set()

    max_pages = max(1, int(pages))
    per = min(50, max(1, int(per)))

    logger.info(
        "Starting Trump Tyranny Tracker archive fetch: window %s → %s (max_pages=%d, per_page=%d)",
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
        if not posts:
            logger.info("Empty page at %d; stopping.", page_idx + 1)
            break

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

        all_seen.extend(page_posts)

        for p in page_posts:
            title = _title_of(p)
            url   = _url_of(p)
            d_pub = _parse_published_local_date(p)
            if d_pub is None:
                continue

            if start_d <= d_pub <= end_d:
                key = str(p.get("id") or url or title)
                if key not in seen_keys:
                    seen_keys.add(key)
                    q = dict(p)
                    q["_matched_date"] = d_pub.isoformat()
                    matches.append(q)

        page_idx += 1

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
    iso = p.get("_matched_date") or (_date_from_title(title) or _iso_date_from_any(p))
    post_date = iso if isinstance(iso, str) else (iso.isoformat() if iso else "")

    return {
        "source": "tracker",
        "doc_type": "news_article",
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date or "",
        "raw_line": f"[tracker] {title} ({post_date or ''})",
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
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    PAGES_CAP = 80
    PER_PAGE  = 50
    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Trump Tyranny Tracker (COPY mode via API): %s", API_URL)

    matches, all_seen, audit_rows = _discover(
        start_iso=start,
        end_iso=end,
        pages=PAGES_CAP,
        per=PER_PAGE,
        timeout=TIMEOUT_S,
        logger=logger,
    )

    entities = [_to_entity_v4(p) for p in matches]

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
                "raw_line": f"[tracker_raw] {_title_of(it)}",
            }
            for it in all_seen
        ],
    }
    write_json(raw_path, raw_payload)

    seen = set()
    deduped: List[Dict[str, Any]] = []
    for e in entities:
        k = e.get("canonical_url") or e.get("url") or ""
        if not k or k in seen:
            continue
        seen.add(k)
        deduped.append(e)

    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "news_article",
        "window": {"start": start, "end": end},
        "count": len(deduped),
        "entities": deduped,
        "window_stats": {
            "inside": len(entities),
            "outside": 0,
            "nodate": 0,
            "no_title": 0,
            "no_url": 0,
            "dupes": len(entities) - len(deduped),
        },
    }
    write_json(filtered_path, filtered_payload)

    return {
        "source": HARVESTER_ID,
        "entity_count": len(deduped),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — Trump Tyranny Tracker harvester")
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