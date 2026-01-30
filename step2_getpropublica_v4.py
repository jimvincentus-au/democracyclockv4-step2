# step2_getpropublica_v4.py
from __future__ import annotations

import re
import json
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- V4 infra ----
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

def _normalize_iso_date(s: str) -> str:
    if re.fullmatch(r"\d{8}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s

# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

HARVESTER_ID = "propublica"

RSS_FEEDS = [
    "https://www.propublica.org/feeds/propublica/main",
    "https://www.propublica.org/feeds/propublica/investigations",
    "https://www.propublica.org/feeds/propublica/politics",
]

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/rss+xml",
}

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

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
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(BROWSER_HEADERS)

    orig = s.request
    def _with_timeout(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return orig(method, url, **kwargs)
    s.request = _with_timeout  # type: ignore

    return s

def _print_titles_and_dates(rows: Iterable[Dict[str, Any]], logger) -> None:
    for r in rows or []:
        title = (r.get("title") or "").strip()
        dstr = (r.get("post_date") or "").strip()
        dshow = dstr[:19] if dstr else ""
        logger.debug("%s | %s", dshow, title)

# ---------------------------------------------------------------------------
# Discovery (RSS-first)
# ---------------------------------------------------------------------------

def _discover(
    start_iso: str,
    end_iso: str,
    *,
    timeout: int,
    logger,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:

    try:
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end_iso, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    s = _make_retry_session(timeout)

    matches: List[Dict[str, Any]] = []
    all_seen: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for feed in RSS_FEEDS:
        logger.info("Fetching ProPublica RSS: %s", feed)
        try:
            r = s.get(feed)
        except requests.RequestException as e:
            logger.warning("RSS fetch failed: %s", e)
            continue

        soup = BeautifulSoup(r.content, "xml")
        items = soup.find_all("item")
        logger.info("Feed returned %d items", len(items))

        kept_on_feed = 0
        in_window_found = False

        for it in items:
            title = (it.title.text or "").strip() if it.title else ""
            url = (it.link.text or "").strip() if it.link else ""
            pub = (it.pubDate.text or "").strip() if it.pubDate else ""

            try:
                d_pub = datetime.strptime(pub[:16], "%a, %d %b %Y").date()
                pub_iso = d_pub.isoformat()
            except Exception:
                pub_iso = ""
            decision = None

            if not url or url in seen_urls:
                decision = "SKIPT(dup_or_no_url)"
            elif not pub_iso:
                decision = "SKIPT(no_parseable_date)"
            else:
                if not (start_d <= d_pub <= end_d):
                    decision = "SKIPT(out_of_window)"
                else:
                    decision = "KEPT"
                    in_window_found = True

            if url not in seen_urls:
                all_seen.append({
                    "title": title,
                    "url": url,
                    "post_date": pub_iso,
                })
                seen_urls.add(url)

            audit.append({
                "feed": feed,
                "title": title,
                "url": url,
                "published": pub_iso,
                "decision": decision,
            })

            if decision == "KEPT":
                matches.append({
                    "title": title,
                    "url": url,
                    "post_date": pub_iso,
                })
                kept_on_feed += 1

        logger.info("Feed decisions: kept=%d, in_window=%s", kept_on_feed, in_window_found)

    logger.info("Archive fetch complete. total_unique_seen=%d in_window_kept=%d", len(all_seen), len(matches))
    _print_titles_and_dates(all_seen, logger)

    return matches, all_seen, audit

# ---------------------------------------------------------------------------
# Article fetch & parse
# ---------------------------------------------------------------------------

def _fetch_article(url: str, *, timeout: int, logger) -> Optional[str]:
    s = _make_retry_session(timeout)
    try:
        r = s.get(url)
    except requests.RequestException as e:
        logger.warning("Fetch failed: %s", e)
        return None
    if r.status_code != 200:
        logger.warning("Non-200 for %s (%s)", url, r.status_code)
        return None
    return r.text

def _parse_article_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    article = soup.find("article")
    if not article:
        return ""

    # Remove junk
    for tag in article.find_all(["aside", "figure", "script", "style"]):
        tag.decompose()

    paragraphs = []
    for p in article.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt:
            paragraphs.append(txt)

    return "\n\n".join(paragraphs)

# ---------------------------------------------------------------------------
# Transform to V4 entity
# ---------------------------------------------------------------------------

def _to_entity_v4(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": "ProPublica",
        "doc_type": "investigation",
        "title": p["title"],
        "url": p["url"],
        "canonical_url": p["url"],
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": p.get("post_date") or "",
        "raw_line": f"[propublica] {p['title']} ({p.get('post_date') or ''})",
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

    start = _normalize_iso_date(start)
    end   = _normalize_iso_date(end)

    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(
        artifacts, HARVESTER_ID, start, end
    )

    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering ProPublica (RSS mode): %s", " | ".join(RSS_FEEDS))

    matches, all_seen, audit_rows = _discover(
        start_iso=start,
        end_iso=end,
        timeout=TIMEOUT_S,
        logger=logger,
    )

    entities = [_to_entity_v4(p) for p in matches]

    raw_payload = {
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "archive_url": "https://www.propublica.org",
        "parsed_total": len(all_seen),
        "audit": audit_rows,
        "items_snapshot": [
            {
                "url": (it.get("url") or ""),
                "title": (it.get("title") or ""),
                "post_date": (it.get("post_date") or "")[:10],
                "doc_type": "investigation",
                "raw_line": f"[propublica_raw] {(it.get('title') or '')}",
            }
            for it in all_seen
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

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
        "entity_type": "investigation",
        "window": {"start": start, "end": end},
        "count": len(deduped),
        "entities": deduped,
        "window_stats": win_stats,
    }
    write_json(filtered_path, filtered_payload)
    logger.info(
        "Wrote filtered entities: %s (count=%d)",
        filtered_path, len(deduped)
    )

    return {
        "source": HARVESTER_ID,
        "entity_count": len(deduped),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }

# ---------------------------------------------------------------------------
# Optional CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock V4 — ProPublica harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
    )
    log = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    log.info("Summary: %s", meta)
    print(meta)