"""
The Bulwark Step 2 harvester v4

Strategy (modeled on getmeidas_v4):
- Discover posts via The Bulwark's Substack-style archive API:
    https://www.thebulwark.com/api/v1/archive
- Paginate via ?sort=new&offset=N&limit=M (Substack cap is 50/page)
- Filter to requested date window using post_date
- AUDIENCE FILTER (Bulwark-specific): exclude posts where audience == "only_paid"
  because their body content is paywalled. We harvest free + email-gated
  content only.
- De-dupe by canonical_url
- Write raw + filtered artifacts using the V4 contract

The Bulwark is a center-right anti-Trump publication that migrated from
Substack to its own domain (www.thebulwark.com) but retained the Substack
backend, so the archive API is identical in shape to meidas / zeteo.
"""

from __future__ import annotations

import re
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

HARVESTER_ID = "bulwark"
SOURCE_DISPLAY = "The Bulwark"
__all__ = ["run_harvester"]

API_URL = "https://www.thebulwark.com/api/v1/archive"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _iso_date_from_any(obj: Dict[str, Any]) -> Optional[date]:
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


def _audience_of(p: Dict[str, Any]) -> str:
    """Substack 'audience' field. Common values: 'everyone', 'only_paid', 'founding'."""
    return (p.get("audience") or "").strip().lower()


def _is_paid_only(p: Dict[str, Any]) -> bool:
    return _audience_of(p) == "only_paid"


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
        total=6, connect=3, read=3,
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


def _discover(
    start_iso: str,
    end_iso: str,
    *,
    pages: int,
    per: int,
    timeout: int,
    logger,
):
    """Paginate the Bulwark archive newest→older, filter by window + audience."""
    try:
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_iso, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    s = _make_retry_session(timeout)
    all_seen: List[Dict[str, Any]] = []
    matches: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []

    seen_ids: set = set()
    max_pages = max(1, int(pages))
    per = min(50, max(1, int(per)))

    logger.info(
        "Starting Bulwark archive fetch: window %s → %s (max_pages=%d, per_page=%d)",
        start_d, end_d, max_pages, per,
    )

    # We page newest-first. Once the page's MAX post_date is older than start_d,
    # we can stop early — no chance of finding more in-window posts.
    page_idx = 0
    offset = 0
    all_older_than_window_pages = 0
    while page_idx < max_pages:
        params = {"sort": "new", "offset": offset, "limit": per}
        logger.info("REQUEST page=%d: GET %s params=%s", page_idx + 1, API_URL, params)

        try:
            r = s.get(API_URL, params=params)
        except requests.RequestException as e:
            logger.warning("Request error on page %d: %s", page_idx + 1, e)
            break

        if r.status_code != 200:
            logger.warning("Non-200 from archive on page %d (status=%s). Stopping.",
                           page_idx + 1, r.status_code)
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

        logger.info("Page %d returned %d posts", page_idx + 1, len(page_posts))
        all_seen.extend(page_posts)

        page_dates: List[date] = []
        page_in_window = 0
        page_paid_filtered = 0

        for p in page_posts:
            title = _title_of(p)
            url = _url_of(p)
            d_pub = _iso_date_from_any(p)
            audience = _audience_of(p)

            if d_pub is None:
                audit.append({
                    "page": page_idx + 1, "title": title, "url": url,
                    "published": "", "audience": audience,
                    "decision": "SKIPT(no_date)",
                })
                continue

            page_dates.append(d_pub)

            in_range = start_d <= d_pub <= end_d
            paid = _is_paid_only(p)

            decision_parts = []
            if not in_range:
                decision_parts.append("out_of_window")
            if paid:
                decision_parts.append("paid_only")

            audit.append({
                "page": page_idx + 1, "title": title, "url": url,
                "published": d_pub.isoformat(), "audience": audience,
                "decision": ",".join(decision_parts) if decision_parts else "KEEP",
            })

            if not in_range:
                continue
            if paid:
                page_paid_filtered += 1
                logger.debug("Filtered (audience=only_paid): %s", title)
                continue

            page_in_window += 1
            matches.append({
                "source_key": HARVESTER_ID,
                "source": SOURCE_DISPLAY,
                "doc_type": "news_article",
                "title": title,
                "url": url,
                "canonical_url": url,
                "summary_url": "",
                "summary": (p.get("subtitle") or p.get("description") or "").strip(),
                "summary_origin": "subtitle",
                "summary_timestamp": "",
                "post_date": d_pub.isoformat(),
                "raw_line": f"[archive p={page_idx+1}] {url}",
                "section": "news",
                "audience": audience,
            })

        page_max_date = max(page_dates).isoformat() if page_dates else "?"
        page_min_date = min(page_dates).isoformat() if page_dates else "?"
        logger.info(
            "Page %d: new_unique=%d in_window=%d paid_filtered=%d date_range=[%s..%s]",
            page_idx + 1, len(page_posts), page_in_window, page_paid_filtered,
            page_min_date, page_max_date,
        )

        # Early termination: if the entire page is older than start_d, stop.
        if page_dates and max(page_dates) < start_d:
            all_older_than_window_pages += 1
            if all_older_than_window_pages >= 1:
                logger.info("Page max date %s older than window start %s; stopping.",
                            max(page_dates).isoformat(), start_d.isoformat())
                break

        # Advance by the number of posts the API actually returned, not by
        # `per`. A short page (e.g. 23 < 50) does NOT mean end-of-data for the
        # Substack archive API — advancing by `per` would skip offsets
        # [len(posts), per) and silently drop in-window posts.
        offset += len(posts)
        page_idx += 1

    return all_seen, matches, audit


def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
    pages: int = 200,
    per: int = 50,
    timeout: int = 30,
) -> Dict[str, Any]:
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    logger.info("Harvesting Bulwark %s → %s", start, end)

    all_seen, matches, audit = _discover(
        start, end,
        pages=pages, per=per, timeout=timeout,
        logger=logger,
    )

    # Stable dedupe on canonical_url within the in-window match set.
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for m in matches:
        k = m.get("canonical_url") or m.get("url")
        if not k or k in seen:
            continue
        seen.add(k)
        deduped.append(m)

    logger.info(
        "Bulwark: %d seen, %d in-window (after audience filter), %d after dedupe",
        len(all_seen), len(matches), len(deduped),
    )

    raw_payload = {
        "schema": "raw.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "parsed_total": len(all_seen),
        "items_snapshot": all_seen,
        "audit": {"per_item": audit},
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    filtered_payload = {
        "schema": "filtered.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "item_type": "news_article",
        "items_count": len(deduped),
        "items": deduped,
        "entity_type": "news_article",
        "count": len(deduped),
        "entities": deduped,
        "audience_filter": "exclude_only_paid",
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


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock V4 — The Bulwark harvester (Substack API)")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    p.add_argument("--pages", type=int, default=200, help="max pages to fetch (safety ceiling)")
    p.add_argument("--per", type=int, default=50, help="per-page item count (Substack cap 50)")
    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    args = p.parse_args()

    if not args.start or not args.end:
        p.error("--start and --end are required")

    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        pages=args.pages,
        per=args.per,
        timeout=args.timeout,
    )
    print(meta)
