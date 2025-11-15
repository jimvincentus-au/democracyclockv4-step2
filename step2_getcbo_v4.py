from __future__ import annotations

"""
Democracy Clock V4 — CBO harvester (COPY mode)

Scope:
- Pull Congressional Budget Office (CBO) publications in a date window.
- Prefer the (undocumented but stable) CBO search JSON endpoint.
- HTML search fallback when JSON blocks (e.g., 403/JS wall).
- Emit raw snapshot (policy-controlled) and filtered entities.

Notes:
- We NEVER fall back to RSS (too limited).
- Adds browser-like headers to reduce 403s.
- DEBUG logs include copy-pasteable request URLs and response heads.
"""

import os
import re
import json
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests

from bs4 import BeautifulSoup

# Public entrypoints
__all__ = ["run_harvester"]

# -------------------------
# V4 helpers & config
# -------------------------
try:
    from config_v4 import ARTIFACTS_ROOT
except Exception:
    ARTIFACTS_ROOT = Path("./artifacts")

try:
    from step2_helper_v4 import (
        setup_logger,
        build_session,
        create_artifact_paths,
        write_json,
        normalize_ws,
    )
except Exception as e:
    raise RuntimeError("helper_v4.py is required in V4") from e


HARVESTER_ID = "cbo"

# Base host + endpoints (allow override for testing)
CBO_BASE = os.getenv("CBO_BASE", "https://www.cbo.gov").rstrip("/")
CBO_SEARCH_API = os.getenv("CBO_SEARCH_API", f"{CBO_BASE}/api/search")
CBO_SEARCH_HTML = f"{CBO_BASE}/search"

# Default filters
# Comma-separated topic ids to include; defaults to Budget(2) + Economy(6)
CBO_TOPIC_IDS = [t.strip() for t in os.getenv("CBO_TOPIC_IDS", "2,6").split(",") if t.strip()]
# Content types to include; defaults to reports only
CBO_CONTENT_TYPES = [t.strip() for t in os.getenv("CBO_CONTENT_TYPES", "reports").split(",") if t.strip()]

# Browser-like default headers (reduces 403s / JS walls)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
    "Referer": "https://www.cbo.gov/search",
    "Accept-Encoding": "gzip, deflate, br",
}

# -------------------------
# Raw write policy
# -------------------------
def _raw_policy() -> str:
    p = (os.getenv("DC_WRITE_RAW") or "").strip().lower()
    if p in {"never", "false", "0", "no"}:
        return "never"
    if p in {"auto", "smart", "smart-if-errors"}:
        return "auto"
    return "always"

def _should_write_raw(ok: bool) -> bool:
    pol = _raw_policy()
    if pol == "always":
        return True
    if pol == "never":
        return False
    # "auto" / "smart": write raw only when something went wrong
    return not ok

# -------------------------
# Utilities
# -------------------------
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def _to_date(s: str | None) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        if _ISO_DATE_RE.match(s):
            return datetime.strptime(s, "%Y-%m-%d").date()
        try:
            return datetime.strptime(s, "%B %d, %Y").date()
        except Exception:
            return datetime.strptime(s, "%b %d, %Y").date()
    except Exception:
        return None

def _is_in_window(d: date | None, start: date, end: date) -> bool:
    return bool(d and start <= d <= end)

def _guess_doc_type(url: str, item_type: str | None) -> str:
    u = (url or "").lower()
    t = (item_type or "").lower()
    if "/cost-estimates/" in u or t == "cost estimate":
        return "cost_estimate"
    if "/faqs/" in u or t == "faq":
        return "faq"
    if "/testimony/" in u or t == "testimony":
        return "testimony"
    if "/report/" in u or t == "report":
        return "report"
    if "/blog/" in u or t == "blog post":
        return "blog"
    return "publication"

def _abs(url: str) -> str:
    return url if (url or "").startswith("http") else urljoin(CBO_BASE + "/", (url or "").lstrip("/"))

def _entity_from_cbo_item(item: Dict[str, Any], *, summary: str = "") -> Optional[Dict[str, Any]]:
    """
    Map a CBO API/HTML item to our entity format.
    Expect keys like: title, url, publication_date (or date), type
    """
    title = normalize_ws(item.get("title") or "")
    url = _abs((item.get("url") or item.get("web_url") or item.get("link") or "").strip())
    if not (title and url):
        return None

    raw_date = item.get("publication_date") or item.get("date") or item.get("pubDate") or ""
    d = _to_date(raw_date)
    post_date = d.isoformat() if d else ""

    doc_type = _guess_doc_type(url, item.get("type"))
    raw_line = normalize_ws(f"{post_date} — {title}")

    return {
        "source": "CBO",
        "doc_type": doc_type,
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": url,
        "summary": summary or "",
        "summary_origin": "cbo_detail" if summary else "",
        "summary_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z" if summary else "",
        "post_date": post_date,
        "raw_line": raw_line,
        # pre-tagging for Step 2
        "topic_hint": "Economic & Regulatory Power",
        "source_label": "Congressional Budget Office",
    }

def _build_search_params(year: int, topics: List[str], types: List[str], page: int) -> Dict[str, Any]:
    params: Dict[str, Any] = {"search_api_fulltext": ""}
    for i, t in enumerate(topics):
        params[f"field_publication_topics[{i}]"] = t
    params["field_display_date[0]"] = str(year)
    for i, tp in enumerate(types):
        params[f"type[{i}]"] = tp
    if page > 0:
        params["page"] = str(page)
    return params

# -------------------------
# Fetchers (with DEBUG URLs)
# -------------------------
def _send_prepared(session: requests.Session, method: str, url: str, *, params=None, headers=None, timeout=30, logger=None) -> requests.Response:
    merged_headers = dict(session.headers)
    if headers:
        merged_headers.update(headers)
    req = requests.Request(method=method, url=url, params=params, headers=merged_headers)
    prepped = session.prepare_request(req)
    # Build a copy-pasteable curl command for debugging
    curl_parts = ["curl", "-i", "-sS", "-X", method.upper(), f"'{prepped.url}'"]
    # Include headers explicitly
    for hk, hv in (prepped.headers or {}).items():
        # Skip headers that curl will add implicitly to keep it readable
        if hk.lower() in {"content-length"}:
            continue
        curl_parts.append(f"-H '{hk}: {hv}'")
    curl_cmd = " ".join(curl_parts)
    if logger:
        logger.debug("HTTP CMD: %s", curl_cmd)
        logger.debug("GET URL: %s", prepped.url)
    resp = session.send(prepped, timeout=timeout)
    if logger:
        text = resp.text or ""
        if resp.status_code >= 400:
            # On failure, log the ENTIRE response body (no truncation)
            logger.debug("Response ERROR: status=%s chars=%s body=%r", resp.status_code, len(text), text)
        else:
            # On success, keep a short preview to avoid log bloat
            head = text[:300]
            logger.debug("Response: status=%s chars=%s head=%r", resp.status_code, len(text), head)
    return resp

def _fetch_cbo_search_json(session: requests.Session, q: str, offset: int, limit: int, logger) -> Dict[str, Any]:
    params = {
        "query": q,
        "sort": "publication_date",
        "order": "desc",
        "format": "json",
        "limit": str(limit),
        "offset": str(offset),
    }
    resp = _send_prepared(
        session,
        "GET",
        CBO_SEARCH_API,
        params=params,
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": CBO_SEARCH_HTML,
        },
        logger=logger,
    )
    if resp.status_code != 200:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}

def _iter_cbo_items_json(session: requests.Session, logger) -> Iterable[Dict[str, Any]]:
    """Yield items from the JSON search endpoint. Stop when a short page appears."""
    offset = 0
    limit = 100
    while True:
        data = _fetch_cbo_search_json(session, q="", offset=offset, limit=limit, logger=logger)
        items = (data.get("items") or data.get("results") or [])
        logger.debug("JSON page: offset=%d limit=%d items=%d", offset, limit, len(items))
        if not items:
            break
        for it in items:
            yield {
                "title": it.get("title") or it.get("headline"),
                "url": it.get("url") or it.get("web_url"),
                "publication_date": it.get("publication_date") or it.get("date"),
                "type": it.get("type") or it.get("content_type"),
            }
        if len(items) < limit:
            break
        offset += limit

def _iter_cbo_items_html(session: requests.Session, years: List[int], topics: List[str], types: List[str], logger) -> Iterable[Dict[str, Any]]:
    """Yield items by scraping cbo.gov/search result pages for given years/topics/types."""
    logger.debug("HTML base URL: %s", CBO_SEARCH_HTML)
    for year in years:
        page = 0
        while True:
            params = _build_search_params(year, topics, types, page)
            resp = _send_prepared(
                session,
                "GET",
                CBO_SEARCH_HTML,
                params=params,
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7", "Referer": CBO_SEARCH_HTML},
                logger=logger,
            )
            if resp.status_code != 200 or not resp.text:
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            ol = soup.select_one("#block-cbo-cbo-system-main div.view-content div > ol")
            items = [] if not ol else ol.select("li.views-row")
            logger.debug("HTML year=%s page=%s items=%s", year, page, len(items))
            if not items:
                break
            for li in items:
                a = li.select_one(".views-field-title a, h3.field-content a")
                title = (a.get_text(strip=True) if a else "").strip()
                href = a.get("href") if a else ""
                tnode = li.select_one("time[datetime]")
                dt = (tnode.get("datetime") or (tnode.get_text(strip=True) if tnode else "")).strip()
                kind = (li.select_one(".views-field-type .field-content") or li.select_one(".views-field.views-field-type .field-content"))
                itype = kind.get_text(strip=True) if kind else ""
                yield {
                    "title": title,
                    "url": _abs(href),
                    "publication_date": dt,
                    "type": itype,
                }
            page += 1

def _fetch_detail_summary(session: requests.Session, url: str, logger) -> str:
    """Fetch the detail page and extract the two-paragraph summary if present."""
    try:
        resp = _send_prepared(session, "GET", url, logger=logger)
        if resp.status_code != 200 or not resp.text:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        # Summary container (top content area)
        # Example selector: '#content-panel > article > div > p'
        paras = soup.select("#content-panel article div > p")
        texts = [normalize_ws(p.get_text(" ", strip=True)) for p in paras if p.get_text(strip=True)]
        if not texts:
            return ""
        # take first 2 reasonably-sized paragraphs
        pieces = []
        for t in texts:
            pieces.append(t)
            if len(" ".join(pieces)) > 600 or len(pieces) >= 2:
                break
        return " ".join(pieces).strip()
    except Exception:
        return ""

def _iter_cbo_items(session: requests.Session, start: date = None, end: date = None, logger=None) -> Iterable[Dict[str, Any]]:
    yielded = False
    for it in _iter_cbo_items_json(session, logger):
        yielded = True
        yield it
    if yielded:
        return
    # HTML fallback by year/topic/type
    y_start = (start or date.today()).year
    y_end = (end or y_start).year if isinstance(end, date) else y_start
    years = list(range(min(y_start, y_end), max(y_start, y_end) + 1))
    for it in _iter_cbo_items_html(session, years, CBO_TOPIC_IDS, CBO_CONTENT_TYPES, logger):
        yield it

# -------------------------
# Public entry point
# -------------------------
def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Step-1 harvester for CBO:
      - Reads CBO publications via JSON search (preferred) or HTML search (fallback).
      - Filters strictly by publication date in [start, end] (inclusive).
      - Writes RAW (policy-controlled) and FILTERED entities JSON.
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)
    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    # Parse window
    s_date = datetime.strptime(start[:10], "%Y-%m-%d").date()
    e_date = datetime.strptime(end[:10], "%Y-%m-%d").date()
    sess = session or build_session()
    sess.headers.update(DEFAULT_HEADERS)

    try:
        priming = sess.get(CBO_BASE, timeout=15)
        logger.debug("Priming GET %s -> %s; cookies now: %s", CBO_BASE, priming.status_code, {c.name: c.value for c in sess.cookies})
    except Exception as e:
        logger.debug("Priming GET failed: %r", e)
    # Many Drupal sites expect has_js=1
    try:
        sess.cookies.set("has_js", "1", domain="www.cbo.gov")
        logger.debug("Set cookie has_js=1 for domain www.cbo.gov")
    except Exception as e:
        logger.debug("Setting has_js cookie failed: %r", e)

    logger.debug(
        "CBO config: BASE=%s SEARCH_API=%s HTML_SEARCH=%s topics=%s types=%s",
        CBO_BASE, CBO_SEARCH_API, CBO_SEARCH_HTML, CBO_TOPIC_IDS, CBO_CONTENT_TYPES
    )
    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering CBO publications (JSON preferred; HTML fallback).")
    logger.debug("CBO window: start=%s end=%s", start, end)

    snapshot: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for it in _iter_cbo_items(sess, s_date, e_date, logger):
        snap_row = {
            "title": it.get("title"),
            "url": it.get("url"),
            "publication_date": it.get("publication_date") or it.get("pubDate"),
            "type": it.get("type"),
        }
        snapshot.append(snap_row)

        d = _to_date(snap_row.get("publication_date"))
        if not _is_in_window(d, s_date, e_date):
            continue

        # Try to fetch a short summary from the detail page
        detail_url = _abs(snap_row.get("url") or "")
        summary = _fetch_detail_summary(sess, detail_url, logger) if detail_url else ""

        ent = _entity_from_cbo_item(snap_row, summary=summary)
        if not ent:
            continue

        ukey = (ent["canonical_url"] or ent["url"]).strip().lower()
        if ukey in seen_urls:
            continue
        seen_urls.add(ukey)
        kept.append(ent)

    ok = True
    # RAW write (policy-controlled)
    if _should_write_raw(ok):
        raw_payload = {
            "source": HARVESTER_ID,
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "schema": "dc.v4.raw",
            "window": {"start": start, "end": end},
            "api_scope": {
                "search_api": CBO_SEARCH_API,
                "html_search": CBO_SEARCH_HTML,
                "topics": CBO_TOPIC_IDS,
                "types": CBO_CONTENT_TYPES,
            },
            "parsed_total": len(snapshot),
            "items_snapshot": snapshot,
        }
        write_json(raw_path, raw_payload)
        logger.info("Wrote raw JSON: %s", raw_path)
    else:
        logger.info("RAW write skipped by policy (DC_WRITE_RAW=%s)", _raw_policy())

    # FILTERED write
    filtered_payload = {
        "source": HARVESTER_ID,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "schema": "dc.v4.filtered",
        "entity_type": "cbo_publication",
        "window": {"start": start, "end": end},
        "count": len(kept),
        "entities": kept,
        "window_stats": {
            "total_seen": len(snapshot),
            "kept_in_window": len(kept),
        },
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(kept))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(kept),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }

# ---------------------------
# Direct CLI (optional)
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — CBO harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    logger = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        log_path=None,
        session=None,
    )
    logger.info("Summary: %s", meta)