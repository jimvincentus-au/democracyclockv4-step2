# getreuters_v4.py
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from step2_helper_v4 import setup_logger

# Playwright (optional, gated via env flag)
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except Exception:
    _PLAYWRIGHT_AVAILABLE = False

# -----------------------------
# Module configuration
# -----------------------------
BASE = "https://www.reuters.com"
SEARCH = f"{BASE}/site-search/"
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# 20 results per page; Reuters paginates via ?offset=20,40,...
PAGE_SIZE = 20
MAX_PAGES = 5  # safety cap; we stop earlier if we run out

# Categories to drop (per discussion: World / Asia Pacific, and a few noisy ones)
EXCLUDE_CATEGORIES = {
    "World",
    "Asia Pacific",
    "Sports",
}

# Optional headless browser switch (default ON)
USE_PLAYWRIGHT = os.getenv("DC_USE_PLAYWRIGHT", "1").lower() in {"1", "true", "yes", "on"}

# Queries
PROTEST_QUERY = "protests or demonstration"
ECON_QUERY = "inflation or jobs or unemployment or tariffs"

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"

# -----------------------------
# Data model
# -----------------------------
@dataclass
class ReutersItem:
    source: str
    title: str
    canonical_url: str
    post_date: str  # ISO 8601
    category: Optional[str]
    summary: Optional[str]

    @property
    def id(self) -> str:
        return self.canonical_url



# -----------------------------
# Playwright client (optional)
# -----------------------------

class PWClient:
    def __init__(self, logger: logging.Logger):
        if not _PLAYWRIGHT_AVAILABLE:
            raise RuntimeError("Playwright not available")
        self._logger = logger
        self._pl = sync_playwright().start()
        self._browser = self._pl.chromium.launch(headless=True, args=["--no-sandbox"])
        self._context = self._browser.new_context(
            user_agent=os.getenv("DC_HTTP_UA", DEFAULT_UA),
            locale="en-US",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        self._page = self._context.new_page()

    def prime(self, base_url: str):
        self._logger.debug("PW prime GET %s", base_url)
        resp = self._page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
        status = resp.status if resp else None
        body = self._page.content()
        if status != 200:
            headers_dump = "\n".join(f"{k}: {v}" for k, v in (resp.headers().items() if resp else []))
            self._logger.debug(
                "PW prime non-200: status=%s\n=== RESPONSE HEADERS ===\n%s\n=== BEGIN BODY ===\n%s\n=== END BODY ===",
                status, headers_dump, body
            )
        else:
            self._logger.debug("PW prime OK: status=200 head=%r", body[:400])
        try:
            self._context.add_cookies([{
                "name": "has_js",
                "value": "1",
                "domain": "www.reuters.com",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "Lax",
            }])
            self._logger.debug("PW set cookie has_js=1 for domain www.reuters.com")
        except Exception as e:
            self._logger.debug("PW unable to set has_js cookie: %r", e)

    def get(self, url: str) -> Optional[str]:
        self._logger.debug("PW GET URL: %s", url)
        resp = self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
        status = resp.status if resp else None
        body = self._page.content()
        if status != 200:
            headers_dump = "\n".join(f"{k}: {v}" for k, v in (resp.headers().items() if resp else []))
            self._logger.debug(
                "Response ERROR (PW): status=%s\n=== RESPONSE HEADERS ===\n%s\n=== BEGIN BODY ===\n%s\n=== END BODY ===",
                status, headers_dump, body
            )
            return None
        self._logger.debug("Response (PW): status=200 len=%d head=%r", len(body), body[:400])
        return body

    def close(self):
        try:
            self._context.close()
        except Exception:
            pass
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._pl.stop()
        except Exception:
            pass

# -----------------------------
# Helpers
# -----------------------------

def _ua_headers() -> Dict[str, str]:
    return {
        "User-Agent": os.getenv("DC_HTTP_UA", DEFAULT_UA),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Upgrade-Insecure-Requests": "1",
        "Referer": BASE,
    }

def _prime_session(session: requests.Session, logger: logging.Logger) -> None:
    """Warm up cookies (DataDome etc.) by visiting the homepage and setting has_js.
    We also log any cookies we receive so we can debug the wall.
    """
    try:
        logger.debug("Priming GET %s", BASE)
        r = session.get(BASE, headers=_ua_headers(), timeout=15)
        # Always dump full on non-200 to see interstitials
        if r.status_code != 200:
            _debug_show_response(logger, r, full=True)
        else:
            _debug_show_response(logger, r, full=False)
        # Many JS firewalls look for a simple JS presence cookie
        try:
            session.cookies.set("has_js", "1", domain="www.reuters.com")
            logger.debug("Set cookie has_js=1 for domain www.reuters.com")
        except Exception as e:
            logger.debug("Unable to set has_js cookie: %r", e)
        # Log the current cookie jar
        try:
            cookies_dump = "; ".join(f"{c.name}={c.value}" for c in session.cookies)
        except Exception:
            try:
                cookies_dump = "; ".join(f"{k}={v}" for k, v in session.cookies.get_dict().items())
            except Exception:
                cookies_dump = "<cookies-unavailable>"
        logger.debug("Cookies after priming: %s", cookies_dump)
    except Exception as e:
        logger.debug("Prime failed: %r", e)


def _iso_date(dt_str: str) -> str:
    # Reuters uses Zulu timestamps like 2025-10-28T10:10:39Z
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).isoformat()
    except Exception:
        return dt_str


def _in_window(iso: str, start: date, end: date) -> bool:
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00")).date()
        return start <= d <= end
    except Exception:
        return False


def _debug_show_response(logger: logging.Logger, resp: requests.Response, *, full: bool = False) -> None:
    """
    Log a response. When full=True (used on non-200), dump full body and headers.
    """
    try:
        body = resp.text or ""
    except Exception:
        body = "<no-text-available>"

    if full:
        # Full dump for troubleshooting (captcha walls, 401/403 interstitials, etc.)
        try:
            headers_dump = "\n".join(f"{k}: {v}" for k, v in resp.headers.items())
        except Exception:
            headers_dump = "<headers-unavailable>"

        try:
            cookies_dump = "; ".join(f"{c.name}={c.value}" for c in resp.cookies)
        except Exception:
            # Fallback if resp.cookies is not iterable as expected
            try:
                cookies_dump = "; ".join(f"{k}={v}" for k, v in resp.cookies.get_dict().items())
            except Exception:
                cookies_dump = "<cookies-unavailable>"

        logger.debug(
            "Response ERROR: status=%s reason=%s len=%s\n"
            "=== RESPONSE HEADERS ===\n%s\n"
            "=== SET-COOKIES ===\n%s\n"
            "=== BEGIN BODY ===\n%s\n=== END BODY ===",
            resp.status_code,
            getattr(resp, "reason", ""),
            len(body),
            headers_dump,
            cookies_dump,
            body,
        )
    else:
        # Lightweight preview on 200s
        logger.debug(
            "Response: status=%s len=%s head=%r",
            resp.status_code,
            len(body),
            body[:400],
        )


def _parse_search_results(html: str, logger: logging.Logger) -> List[ReutersItem]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[ReutersItem] = []
    for li in soup.select('li[data-testid="StoryCard"]'):
        # Title & URL
        a = li.select_one('a[data-testid="TitleLink"]')
        if not a or not a.get("href"):
            continue
        url = a.get("href")
        if url.startswith("/"):
            url = BASE + url
        title_el = a.select_one('[data-testid="TitleHeading"]') or a
        title = title_el.get_text(strip=True)

        # Category (kicker)
        cat_el = li.select_one('a[data-testid="KickerLink"]')
        category = cat_el.get_text(strip=True) if cat_el else None

        # Datetime
        t = li.select_one('time[data-testid="DateLineText"]')
        post_iso = _iso_date(t.get("datetime")) if t and t.get("datetime") else None
        if not post_iso:
            # Try to parse fallback text but usually not needed
            continue

        # Short summary (not guaranteed present in search cards)
        summary_el = li.select_one('p')
        summary = summary_el.get_text(strip=True) if summary_el else None

        items.append(
            ReutersItem(
                source="reuters",
                title=title,
                canonical_url=url,
                post_date=post_iso,
                category=category,
                summary=summary,
            )
        )
    logger.debug("Parsed %d story cards", len(items))
    return items


def _fetch_page(session: requests.Session, params: Dict[str, str], logger: logging.Logger, pw: Optional[PWClient] = None) -> Optional[str]:
    # Build full URL for debug copy-paste
    try:
        from urllib.parse import urlencode
        full_url = f"{SEARCH}?{urlencode(params, doseq=True)}"
    except Exception:
        full_url = SEARCH

    # Preferred path: Playwright if enabled and available
    if USE_PLAYWRIGHT and pw is not None:
        return pw.get(full_url)

    logger.debug("GET URL: %s", full_url)
    resp = session.get(SEARCH, params=params, headers=_ua_headers(), timeout=20, allow_redirects=True)

    if resp.status_code != 200:
        _debug_show_response(logger, resp, full=True)
        logger.debug("Non-200 from Reuters (%s) at %s", resp.status_code, full_url)
        return None

    _debug_show_response(logger, resp, full=False)
    return resp.text

def _fetch_url(session: requests.Session, url: str, params: Optional[Dict[str, str]], logger: logging.Logger, pw: Optional[PWClient] = None) -> Optional[str]:
    try:
        from urllib.parse import urlencode
        full_url = f"{url}?{urlencode(params or {}, doseq=True)}" if params else url
    except Exception:
        full_url = url

    # Preferred path: Playwright if enabled and available
    if USE_PLAYWRIGHT and pw is not None:
        return pw.get(full_url)

    logger.debug("GET URL: %s", full_url)
    resp = session.get(url, params=params, headers=_ua_headers(), timeout=20, allow_redirects=True)
    if resp.status_code != 200:
        _debug_show_response(logger, resp, full=True)
        logger.debug("Non-200 from GET (%s) at %s", resp.status_code, full_url)
        return None
    _debug_show_response(logger, resp, full=False)
    return resp.text


def _search_topic(
    session: requests.Session,
    query: str,
    start: date,
    end: date,
    logger: logging.Logger,
    pw: Optional[PWClient] = None,
) -> List[ReutersItem]:
    kept: List[ReutersItem] = []
    for page in range(MAX_PAGES):
        params = {
            "query": query,
            "date": "past_year",
            "offset": str(page * PAGE_SIZE) if page else None,
        }
        # drop None to keep the URL clean
        params = {k: v for k, v in params.items() if v is not None}
        html = _fetch_page(session, params, logger, pw=pw)
        if not html:
            break
        page_items = _parse_search_results(html, logger)
        if not page_items:
            break
        # filter in-window
        page_items = [it for it in page_items if _in_window(it.post_date, start, end)]
        # category filter (drop excluded)
        filtered = [it for it in page_items if (it.category or "").strip() not in EXCLUDE_CATEGORIES]
        logger.debug(
            "Page %d: parsed=%d in_window=%d kept_after_category=%d",
            page,
            len(page_items),
            len([it for it in page_items]),
            len(filtered),
        )
        kept.extend(filtered)
        # soft stop: if oldest items on page are before start, likely next pages are older
        if page_items and all(not _in_window(it.post_date, start, end) for it in page_items):
            break
    return kept

def _search_via_google_news(
    session: requests.Session,
    query: str,
    start: date,
    end: date,
    logger: logging.Logger,
    pw: Optional[PWClient] = None,
) -> List[ReutersItem]:
    """Fallback using Google News RSS constrained to site:reuters.com.
    Note: Category filtering is not available in this path.
    """
    from email.utils import parsedate_to_datetime
    items: List[ReutersItem] = []
    # Google News query: enforce site:reuters.com and reuse our OR logic
    q = f"{query} site:reuters.com"
    params = {
        "q": q,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    xml = _fetch_url(session, GOOGLE_NEWS_RSS, params, logger, pw=pw)
    if not xml:
        return items
    soup = BeautifulSoup(xml, "xml")
    for it in soup.select("item"):
        title = it.title.get_text(strip=True) if it.title else None
        link = it.link.get_text(strip=True) if it.link else None
        pub = it.pubDate.get_text(strip=True) if it.pubDate else None
        if not (title and link and pub):
            continue
        try:
            pub_iso = parsedate_to_datetime(pub).isoformat()
        except Exception:
            pub_iso = pub
        if not _in_window(pub_iso, start, end):
            continue
        # Prefer Reuters links; Google sometimes wraps links via news.google.com. Try to expand if url= param exists.
        try:
            if link.startswith("https://news.google.") and "url=" in link:
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(link).query)
                link = qs.get("url", [link])[0]
        except Exception:
            pass
        items.append(
            ReutersItem(
                source="reuters",
                title=title,
                canonical_url=link,
                post_date=pub_iso,
                category=None,
                summary=None,
            )
        )
    logger.debug("Google News fallback produced %d items in window", len(items))
    return items


# -----------------------------
# Public entry point used by driver
# -----------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session: Optional[requests.Session] = None,
    **_: Any,
) -> Dict[str, Any]:
    """
    Harvest Reuters search results for protest/civil-action and economic topics.
    Returns meta with paths and counts and writes JSON artifacts.
    """
    # Logger/session bootstrap to match driver expectations
    logger = setup_logger("dc.reuters", level=level, logfile=log_path)
    sess = session or requests.Session()
    _prime_session(sess, logger)

    # Optional Playwright bootstrap
    pw: Optional[PWClient] = None
    if USE_PLAYWRIGHT:
        if not _PLAYWRIGHT_AVAILABLE:
            logger.warning("DC_USE_PLAYWRIGHT enabled but Playwright not installed. Falling back to requests.")
        else:
            try:
                pw = PWClient(logger)
                pw.prime(BASE)
            except Exception as e:
                logger.warning("Failed to start Playwright. Falling back to requests. err=%r", e)
                pw = None

    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()

    logger.info("Discovering Reuters items (site-search HTML)")
    logger.debug(
        "Reuters config: SEARCH=%s PAGE_SIZE=%s MAX_PAGES=%s exclude=%s",
        SEARCH,
        PAGE_SIZE,
        MAX_PAGES,
        sorted(EXCLUDE_CATEGORIES),
    )

    protests = _search_topic(sess, PROTEST_QUERY, start_d, end_d, logger, pw=pw)
    economics = _search_topic(sess, ECON_QUERY, start_d, end_d, logger, pw=pw)

    # If Reuters blocked our HTML fetch (DataDome 401/403), try Google News RSS fallback
    if not protests and not economics:
        logger.warning("Reuters site-search blocked (no items). FALLBACK: Google News RSS (site:reuters.com)")
        protests = _search_via_google_news(sess, PROTEST_QUERY, start_d, end_d, logger, pw=pw)
        economics = _search_via_google_news(sess, ECON_QUERY, start_d, end_d, logger, pw=pw)

    # de-duplicate by URL
    by_url: Dict[str, ReutersItem] = {}
    for it in protests + economics:
        by_url[it.canonical_url] = it
    items = list(by_url.values())
    items.sort(key=lambda x: x.post_date, reverse=True)

    # Prepare artifacts
    artifacts = Path(artifacts_root)
    artifacts.mkdir(parents=True, exist_ok=True)
    json_dir = artifacts / "json"
    json_dir.mkdir(parents=True, exist_ok=True)

    raw_path = json_dir / f"reuters_raw_{start}_{end}.json"
    filtered_path = json_dir / f"reuters_filtered_{start}_{end}.json"

    raw_snapshot = {
        "source": "reuters",
        "start": start,
        "end": end,
        "protest_query": PROTEST_QUERY,
        "economic_query": ECON_QUERY,
        "fallback": "google_news_rss" if not any([protests, economics]) else None,
        "exclude_categories": sorted(EXCLUDE_CATEGORIES),
        "count": len(items),
        "items": [asdict(it) for it in items],
    }

    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(raw_snapshot, f, ensure_ascii=False, indent=2)
    logger.info("Wrote raw JSON: %s", raw_path)

    filtered_entities = [
        {
            "source": it.source,
            "title": it.title,
            "canonical_url": it.canonical_url,
            "post_date": it.post_date,
            "category": it.category,
            "summary": it.summary,
        }
        for it in items
    ]

    with filtered_path.open("w", encoding="utf-8") as f:
        json.dump({"entities": filtered_entities}, f, ensure_ascii=False, indent=2)
    logger.info(
        "Wrote filtered entities: %s (count=%d)", filtered_path, len(filtered_entities)
    )

    # Ensure Playwright resources are closed
    if pw is not None:
        try:
            pw.close()
        except Exception:
            pass

    return {
        "source": "reuters",
        "entity_count": len(filtered_entities),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": log_path,
    }


# Optional minimal CLI for quick ad-hoc tests
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--artifacts-root", default="artifacts")
    ap.add_argument("--level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.level.upper(), logging.INFO))

    with requests.Session() as s:
        meta = run_harvester(
            start=args.start,
            end=args.end,
            artifacts_root=args.artifacts_root,
            level=args.level,
            log_path=None,
            session=s,
        )
        print(json.dumps(meta, indent=2))