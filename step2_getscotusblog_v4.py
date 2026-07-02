"""
SCOTUSblog Step 2 Scraper v4

V4 strategy:
- Support ordinary date-window calls: --start YYYY-MM-DD --end YYYY-MM-DD
- Support Democracy Clock week-window calls: --week 1 --weeks 75
- Discover SCOTUSblog article cards from topic pages such as /topics/merits-cases/
- Page through the site's frontend listing endpoint for historical backfill
- Enrich API/listing items by fetching article pages when the listing omits dates
- Filter to the requested date window and de-dupe by canonical_url
- Write raw + filtered artifacts using the V4 contract

Notes:
- SCOTUSblog currently serves topic archives through a Next.js frontend.
- The frontend listing endpoint is not an official public API, so this harvester is defensive.
- This source is best treated as commentary / event-discovery rather than canonical legal-record data.
"""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# --- V4 infrastructure imports ---
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    build_session,
    create_artifact_paths,
    write_json,
    within_window,
)

VERSION = "v4"
HARVESTER_ID = "scotusblog"
__all__ = ["run_harvester"]

SCOTUSBLOG_BASE = "https://www.scotusblog.com"
DEMOCRACY_CLOCK_WEEK1_START = date(2025, 1, 20)

# Topic pages to harvest. Add/remove topics here without changing the output contract.
SCOTUSBLOG_TOPICS = {
    "merits-cases": "/topics/merits-cases/",
    "court-news": "/topics/court-news/",
    "court-analysis": "/topics/court-analysis/",
    "emergency-appeals-and-applications": "/topics/emergency-appeals-and-applications/",
}

SCOTUSBLOG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
}

DATE_FORMATS = (
    "%b %d, %Y",      # Jun 30, 2026
    "%B %d, %Y",     # June 30, 2026
    "%Y-%m-%d",      # 2026-06-30
)

ARTICLE_EXCLUDE_PREFIXES = (
    "topics/",
    "topic/",
    "author/",
    "authors/",
    "category/",
    "tag/",
    "cases/",
    "case-files/",
    "statistics/",
    "calendar/",
    "justices/",
    "about/",
    "newsletters/",
    "podcasts/",
    "account/",
    "search",
    "wp-content/",
    "_next/",
)


def _clean_text(value: str) -> str:
    """Normalize whitespace and HTML entities."""
    if not value:
        return ""
    return " ".join(html.unescape(value).split())


def _parse_date_to_iso(value: str) -> str:
    """Convert common SCOTUSblog date strings to YYYY-MM-DD."""
    value = _clean_text(value)
    if not value:
        return ""

    iso_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", value)
    if iso_match:
        return iso_match.group(1)

    value_no_ordinals = re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", value, flags=re.I)

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value_no_ordinals, fmt).date().isoformat()
        except ValueError:
            continue

    return ""


def _title_from_url(url: str) -> str:
    """Fallback title when only a URL is available."""
    if not url:
        return ""
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-", " ").replace("_", " ")
    slug = " ".join(slug.split())
    return slug.capitalize()


def _is_internal_scotusblog_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in {"", "www.scotusblog.com", "scotusblog.com"}


def _is_probable_article_url(url: str) -> bool:
    """Reject known site chrome, author pages, topic pages, and other non-articles."""
    if not url or not _is_internal_scotusblog_url(url):
        return False

    parsed_path = urlparse(url).path.strip("/")
    if not parsed_path:
        return False

    if parsed_path.startswith(ARTICLE_EXCLUDE_PREFIXES):
        return False

    # Old and new SCOTUSblog article URLs may be either /YYYY/MM/slug/ or /slug/.
    # Reject obvious files and assets.
    if "." in parsed_path.rsplit("/", 1)[-1]:
        return False

    return True


def _extract_listing_endpoint(page_html: str, topic_slug: str) -> str:
    """Return the likely Next.js listing endpoint for a topic."""
    explicit = re.search(r"(/api/listings/topic/[-a-z0-9_]+)", page_html)
    if explicit:
        return explicit.group(1)
    return f"/api/listings/topic/{topic_slug}"


def _extract_initial_offset(page_html: str, default: int = 12) -> int:
    """Find the first pagination offset if embedded in the rendered app state."""
    patterns = [
        r'"initialOffset"\s*:\s*(\d+)',
        r"initialOffset\s*[:=]\s*(\d+)",
        r'"offset"\s*:\s*(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, page_html)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
    return default


def _extract_date_from_text(value: str) -> str:
    """Find a publish-date-looking string inside arbitrary text."""
    if not value:
        return ""

    iso = _parse_date_to_iso(value)
    if iso:
        return iso

    date_match = re.search(
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? \d{1,2}, 20\d{2}\b",
        value,
        flags=re.I,
    )
    if date_match:
        return _parse_date_to_iso(date_match.group(0))

    return ""


def _iter_strings(value: Any) -> Iterable[str]:
    """Yield all strings found inside a nested JSON-like structure."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _iter_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_strings(child)


def _extract_date_from_json_obj(obj: Dict[str, Any]) -> str:
    """Search common keys first, then all nested strings, for a usable date."""
    likely_keys = (
        "date",
        "publishedAt",
        "published_at",
        "publishedDate",
        "published_date",
        "publicationDate",
        "publication_date",
        "post_date",
        "createdAt",
        "created_at",
        "_createdAt",
        "updatedAt",
        "modifiedAt",
    )
    for key in likely_keys:
        if key in obj:
            iso = _extract_date_from_text(str(obj.get(key) or ""))
            if iso:
                return iso

    for s in _iter_strings(obj):
        iso = _extract_date_from_text(s)
        if iso:
            return iso

    return ""


def _extract_article_date_from_html(page_html: str) -> str:
    """Parse an article publish date from an article page HTML."""
    soup = BeautifulSoup(page_html, "html.parser")

    meta_selectors = [
        ("property", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "article:published_time"),
        ("name", "date"),
        ("name", "publish_date"),
        ("name", "parsely-pub-date"),
    ]
    for attr_name, attr_value in meta_selectors:
        for tag in soup.find_all("meta"):
            tag_value = tag.get(attr_name)
            if isinstance(tag_value, list):
                matched = attr_value in [str(v) for v in tag_value]
            else:
                matched = str(tag_value or "") == attr_value
            if not matched:
                continue

            content_value = tag.get("content")
            if not content_value:
                continue
            if isinstance(content_value, list):
                content_text = " ".join(str(v) for v in content_value)
            else:
                content_text = str(content_value)

            iso = _extract_date_from_text(content_text)
            if iso:
                return iso

    time_tag = soup.find("time")
    if time_tag:
        iso = _extract_date_from_text(str(time_tag.get("datetime") or time_tag.get_text(" ", strip=True)))
        if iso:
            return iso

    ld_json_tags = soup.find_all("script", attrs={"type": "application/ld+json"})
    for tag in ld_json_tags:
        raw = tag.string or tag.get_text(" ", strip=True)
        iso = _extract_date_from_text(raw)
        if iso:
            return iso

    return _extract_date_from_text(soup.get_text(" ", strip=True)[:5000])


def _fetch_article_date(session: requests.Session, url: str, logger: logging.Logger) -> str:
    """Fetch an article page and parse its date. Used for historical listing objects."""
    try:
        resp = session.get(url, headers=SCOTUSBLOG_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as ex:
        logger.warning("Article date enrichment failed for %s: %s", url, ex)
        return ""

    return _extract_article_date_from_html(resp.text)


def _snapshot_from_card(article, topic_slug: str, topic_url: str) -> Optional[Dict[str, Any]]:
    """Parse one rendered article card from a SCOTUSblog topic archive."""
    title_link = None

    for heading in article.find_all(["h2", "h3"]):
        candidate = heading.find("a", href=True)
        if candidate and candidate.get_text(strip=True):
            title_link = candidate
            break

    if title_link is None:
        for candidate in article.find_all("a", href=True):
            href = candidate.get("href") or ""
            text = candidate.get_text(" ", strip=True)
            if text and _is_probable_article_url(urljoin(SCOTUSBLOG_BASE, href)):
                title_link = candidate
                break

    if title_link is None:
        return None

    href = title_link.get("href") or ""
    canonical_url = urljoin(SCOTUSBLOG_BASE, href)
    if not _is_probable_article_url(canonical_url):
        return None

    title = _clean_text(title_link.get_text(" ", strip=True)) or _title_from_url(canonical_url)
    full_text = _clean_text(article.get_text(" ", strip=True))

    author = ""
    author_link = article.find("a", href=re.compile(r"/author/"))
    if author_link:
        author = _clean_text(author_link.get_text(" ", strip=True))

    post_date = _extract_date_from_text(full_text)

    label = ""
    for span in article.find_all("span"):
        span_text = _clean_text(span.get_text(" ", strip=True))
        if span_text and span_text.lower() in {
            "opinion analysis",
            "analysis",
            "news",
            "court news",
            "case preview",
            "argument analysis",
            "breaking news",
            "symposium",
            "editorial",
        }:
            label = span_text
            break

    return {
        "source_key": HARVESTER_ID,
        "source": "SCOTUSblog",
        "doc_type": "news_article",
        "title": title,
        "url": canonical_url,
        "canonical_url": canonical_url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date,
        "raw_line": f"[topic:{topic_slug}] {canonical_url}",
        "section": topic_slug,
        "topic": topic_slug,
        "label": label,
        "author": author,
        "discovery_url": topic_url,
    }


def _parse_topic_html(page_html: str, topic_slug: str, topic_url: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Parse rendered article cards from a topic page."""
    soup = BeautifulSoup(page_html, "html.parser")
    items: List[Dict[str, Any]] = []

    for article in soup.find_all("article"):
        snapshot = _snapshot_from_card(article, topic_slug, topic_url)
        if snapshot:
            items.append(snapshot)

    logger.info("Parsed %d rendered article cards from topic '%s'", len(items), topic_slug)
    return items


def _iter_json_objects(value: Any) -> Iterable[Dict[str, Any]]:
    """Yield plausible article/listing dicts from an unknown JSON response shape."""
    if isinstance(value, dict):
        keys = set(value.keys())
        if (
            {"title", "slug"} & keys
            or {"title", "url"} & keys
            or {"headline", "url"} & keys
            or {"name", "url"} & keys
            or {"slug", "publishedAt"} & keys
            or {"slug", "date"} & keys
        ):
            yield value

        for child in value.values():
            yield from _iter_json_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_objects(child)


def _extract_author_from_json(obj: Dict[str, Any]) -> str:
    author_value = obj.get("author") or obj.get("byline") or obj.get("authors") or ""
    if isinstance(author_value, str):
        return _clean_text(author_value)
    if isinstance(author_value, dict):
        return _clean_text(str(author_value.get("name") or author_value.get("title") or ""))
    if isinstance(author_value, list):
        author_names = []
        for a in author_value:
            if isinstance(a, str):
                author_names.append(_clean_text(a))
            elif isinstance(a, dict):
                name = _clean_text(str(a.get("name") or a.get("title") or ""))
                if name:
                    author_names.append(name)
        return ", ".join(author_names)
    return ""


def _snapshot_from_json(
    obj: Dict[str, Any],
    topic_slug: str,
    discovery_url: str,
    session: requests.Session,
    logger: logging.Logger,
    enrich_dates: bool = True,
) -> Optional[Dict[str, Any]]:
    """Normalize one article-ish object returned by the SCOTUSblog listing endpoint."""
    title_value = obj.get("title") or obj.get("headline") or obj.get("name") or ""
    if isinstance(title_value, dict):
        title_value = title_value.get("rendered") or title_value.get("plain") or title_value.get("text") or ""
    title = _clean_text(str(title_value))

    url_value = obj.get("url") or obj.get("href") or obj.get("link") or obj.get("canonical_url") or obj.get("canonicalUrl") or ""
    slug_value = obj.get("slug") or obj.get("path") or ""

    if url_value:
        canonical_url = urljoin(SCOTUSBLOG_BASE, str(url_value))
    elif slug_value:
        slug = str(slug_value).strip("/")
        canonical_url = urljoin(SCOTUSBLOG_BASE, f"/{slug}/")
    else:
        return None

    if not _is_probable_article_url(canonical_url):
        return None

    if not title:
        title = _title_from_url(canonical_url)

    post_date = _extract_date_from_json_obj(obj)
    date_origin = "listing"
    if not post_date and enrich_dates:
        post_date = _fetch_article_date(session, canonical_url, logger)
        date_origin = "article_page" if post_date else ""

    if not post_date:
        return None

    label = _clean_text(str(obj.get("label") or obj.get("category") or obj.get("section") or ""))
    author = _extract_author_from_json(obj)

    return {
        "source_key": HARVESTER_ID,
        "source": "SCOTUSblog",
        "doc_type": "news_article",
        "title": title,
        "url": canonical_url,
        "canonical_url": canonical_url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date,
        "raw_line": f"[api-topic:{topic_slug}] {canonical_url}",
        "section": topic_slug,
        "topic": topic_slug,
        "label": label,
        "author": author,
        "date_origin": date_origin,
        "discovery_url": discovery_url,
    }


def _fetch_listing_page(
    session: requests.Session,
    endpoint_url: str,
    topic_slug: str,
    offset: int,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], int, str]:
    """Try likely SCOTUSblog frontend pagination query shapes for one offset."""
    query_shapes = [
        {"offset": offset},
        {"offset": offset, "limit": 12},
        {"skip": offset},
        {"page": (offset // 12) + 1},
    ]

    for params in query_shapes:
        try:
            resp = session.get(endpoint_url, headers=SCOTUSBLOG_HEADERS, params=params, timeout=30)
            if resp.status_code in {400, 404, 405}:
                logger.debug("Listing endpoint rejected params=%s status=%s", params, resp.status_code)
                continue
            resp.raise_for_status()
        except Exception as ex:
            logger.debug("Listing endpoint fetch failed params=%s: %s", params, ex)
            continue

        content_type = resp.headers.get("content-type", "")
        if "json" not in content_type.lower():
            text = resp.text.strip()
            if not text.startswith(("{", "[")):
                logger.debug("Listing endpoint returned non-JSON params=%s content-type=%r", params, content_type)
                continue

        try:
            payload = resp.json()
        except json.JSONDecodeError as ex:
            logger.debug("Listing endpoint JSON parse failed params=%s: %s", params, ex)
            continue

        candidates = list(_iter_json_objects(payload))
        items: List[Dict[str, Any]] = []
        for obj in candidates:
            snapshot = _snapshot_from_json(obj, topic_slug, resp.url, session, logger, enrich_dates=True)
            if snapshot:
                items.append(snapshot)

        if candidates or items:
            dated_values = sorted({it.get("post_date", "") for it in items if it.get("post_date")})
            date_span = f"{dated_values[0]} → {dated_values[-1]}" if dated_values else "no dated items"
            logger.info(
                "Listing endpoint topic='%s' params=%s candidates=%d dated_items=%d date_span=%s",
                topic_slug,
                params,
                len(candidates),
                len(items),
                date_span,
            )
            if candidates and not items:
                logger.warning(
                    "Listing endpoint returned %d candidates but 0 usable dated article items for topic='%s' params=%s",
                    len(candidates),
                    topic_slug,
                    params,
                )
            return items, len(candidates), resp.url

        logger.debug("Listing endpoint returned JSON but no article candidates params=%s", params)

    return [], 0, endpoint_url


def _all_dated_items_older_than(items: List[Dict[str, Any]], start_iso: str) -> bool:
    if not items:
        return False
    dated = [it for it in items if it.get("post_date")]
    return bool(dated) and all(str(it["post_date"]) < start_iso for it in dated)


def _discover_topic(
    session: requests.Session,
    topic_slug: str,
    topic_path: str,
    logger: logging.Logger,
    start_iso: str,
    max_api_pages: int = 120,
) -> List[Dict[str, Any]]:
    """Discover SCOTUSblog article snapshots for one topic."""
    topic_url = urljoin(SCOTUSBLOG_BASE, topic_path)
    logger.info("SCOTUSblog v4: fetching topic '%s': %s", topic_slug, topic_url)

    try:
        resp = session.get(topic_url, headers=SCOTUSBLOG_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as ex:
        logger.error("Failed to fetch SCOTUSblog topic page: %s (%s)", topic_url, ex)
        return []

    page_html = resp.text
    items = _parse_topic_html(page_html, topic_slug, topic_url, logger)

    endpoint_path = _extract_listing_endpoint(page_html, topic_slug)
    endpoint_url = urljoin(SCOTUSBLOG_BASE, endpoint_path)
    initial_offset = _extract_initial_offset(page_html, default=max(len(items), 12))

    logger.info(
        "Topic '%s' listing endpoint candidate=%s initial_offset=%d max_api_pages=%d",
        topic_slug,
        endpoint_url,
        initial_offset,
        max_api_pages,
    )

    seen_api_urls = set()
    for existing in items:
        key = existing.get("canonical_url") or existing.get("url") or ""
        if key:
            seen_api_urls.add(key)

    api_pages_attempted = 0
    api_candidates_seen = 0
    api_items_seen = 0
    api_new_items_seen = 0

    offset = initial_offset
    consecutive_no_new_pages = 0

    for _page_num in range(max_api_pages):
        page_num = _page_num + 1
        api_pages_attempted += 1
        logger.info(
            "Topic '%s': API page %d/%d offset=%d",
            topic_slug,
            page_num,
            max_api_pages,
            offset,
        )
        api_items, candidate_count, used_url = _fetch_listing_page(session, endpoint_url, topic_slug, offset, logger)
        api_candidates_seen += candidate_count
        api_items_seen += len(api_items)
        if candidate_count == 0 and not api_items:
            logger.info(
                "Topic '%s': stopping pagination at offset=%d because endpoint returned no candidates/items",
                topic_slug,
                offset,
            )
            break

        new_items: List[Dict[str, Any]] = []
        for item in api_items:
            key = item.get("canonical_url") or item.get("url") or ""
            if key and key not in seen_api_urls:
                seen_api_urls.add(key)
                new_items.append(item)

        if new_items:
            items.extend(new_items)
            consecutive_no_new_pages = 0
            api_new_items_seen += len(new_items)
            dated_values = sorted({it.get("post_date", "") for it in new_items if it.get("post_date")})
            date_span = f"{dated_values[0]} → {dated_values[-1]}" if dated_values else "no dated new items"
            logger.info(
                "Topic '%s': accepted %d new API items at offset=%d date_span=%s total_topic_items=%d",
                topic_slug,
                len(new_items),
                offset,
                date_span,
                len(items),
            )
        else:
            consecutive_no_new_pages += 1
            logger.warning(
                "Topic '%s': API page produced no new URLs at offset=%d url=%s consecutive_no_new_pages=%d",
                topic_slug,
                offset,
                used_url,
                consecutive_no_new_pages,
            )
            if consecutive_no_new_pages >= 2:
                logger.warning(
                    "Topic '%s': stopping pagination after %d consecutive pages with no new URLs",
                    topic_slug,
                    consecutive_no_new_pages,
                )
                break

        # SCOTUSblog topic feeds appear reverse chronological. Once a full dated page
        # is older than the requested start, later pages are not needed for this run.
        if _all_dated_items_older_than(new_items, start_iso):
            oldest = min(str(it.get("post_date")) for it in new_items if it.get("post_date"))
            newest = max(str(it.get("post_date")) for it in new_items if it.get("post_date"))
            logger.info(
                "Topic '%s': stopping pagination because page date span %s → %s is older than start=%s",
                topic_slug,
                oldest,
                newest,
                start_iso,
            )
            break

        offset += max(len(new_items), candidate_count, 12)

    dated_topic_items = [it for it in items if it.get("post_date")]
    dated_values = sorted({it.get("post_date", "") for it in dated_topic_items if it.get("post_date")})
    date_span = f"{dated_values[0]} → {dated_values[-1]}" if dated_values else "no dated topic items"
    logger.info(
        "Topic '%s' complete: rendered_items=%d api_pages_attempted=%d api_candidates=%d api_items=%d api_new_items=%d total_items=%d date_span=%s",
        topic_slug,
        len(items) - api_new_items_seen,
        api_pages_attempted,
        api_candidates_seen,
        api_items_seen,
        api_new_items_seen,
        len(items),
        date_span,
    )
    return items


def _discover_scotusblog(
    session: requests.Session,
    logger: logging.Logger,
    start_iso: str,
    max_api_pages: int,
) -> List[Dict[str, Any]]:
    """Discover SCOTUSblog article snapshots across configured topics."""
    logger.info("SCOTUSblog v4: starting discovery across %d topics", len(SCOTUSBLOG_TOPICS))

    all_snapshots: List[Dict[str, Any]] = []
    for topic_slug, topic_path in SCOTUSBLOG_TOPICS.items():
        topic_items = _discover_topic(
            session=session,
            topic_slug=topic_slug,
            topic_path=topic_path,
            logger=logger,
            start_iso=start_iso,
            max_api_pages=max_api_pages,
        )
        all_snapshots.extend(topic_items)

    if not all_snapshots:
        logger.warning("No SCOTUSblog articles discovered.")
    else:
        logger.info("Total %d SCOTUSblog article snapshots discovered.", len(all_snapshots))

    return all_snapshots


def _filter_window_and_dedupe(
    snapshot_items: List[Dict[str, Any]],
    start_iso: str,
    end_iso: str,
    logger: logging.Logger,
):
    """Window filter + stable dedupe by canonical_url."""
    kept_pre_dedupe: List[Dict[str, Any]] = []
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_url": 0}

    for it in snapshot_items:
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        iso = (it.get("post_date") or "").strip()

        reason = None
        if not url:
            stats["no_url"] += 1
            reason = "no_url"
        elif not iso:
            stats["nodate"] += 1
            reason = "nodate"
        elif not within_window(iso, start_iso, end_iso):
            stats["outside"] += 1
            reason = "outside"

        if reason:
            logger.debug("Window: %s SKIP reason=%s | url=%r", iso or "''", reason, url)
            continue

        stats["inside"] += 1
        kept_pre_dedupe.append(it)

    seen = set()
    deduped: List[Dict[str, Any]] = []
    dups = 0
    for r in kept_pre_dedupe:
        k = r.get("canonical_url") or r.get("url") or ""
        if not k or k in seen:
            dups += 1
            continue
        seen.add(k)
        deduped.append(r)

    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | outside=%d nodate=%d no_url=%d dupes=%d",
        start_iso,
        end_iso,
        len(snapshot_items),
        len(kept_pre_dedupe),
        len(deduped),
        stats["outside"],
        stats["nodate"],
        stats["no_url"],
        dups,
    )
    return deduped, stats


def _default_max_api_pages(start: str, end: str) -> int:
    """Scale pagination depth for historical backfills."""
    try:
        start_d = datetime.strptime(start, "%Y-%m-%d").date()
        end_d = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        return 120

    window_days = max((end_d - start_d).days + 1, 1)
    # One page can expose dozens of items; this errs on the side of completeness.
    if window_days >= 365:
        return 160
    if window_days >= 180:
        return 100
    if window_days >= 60:
        return 60
    return 20


def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
    max_api_pages: Optional[int] = None,
) -> Dict[str, Any]:

    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    active_session = session or build_session()
    pages = max_api_pages if max_api_pages is not None else _default_max_api_pages(start, end)

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering SCOTUSblog (V4 historical topic/archive mode; max_api_pages=%d)", pages)

    snapshot_items = _discover_scotusblog(active_session, logger, start_iso=start, max_api_pages=pages)

    total_discovered = len(snapshot_items)
    total_in_window = sum(
        1
        for it in snapshot_items
        if it.get("post_date") and within_window(str(it["post_date"]), start, end)
    )
    logger.info(
        "Discovered %d article snapshots total; %d within window %s → %s",
        total_discovered,
        total_in_window,
        start,
        end,
    )
    if snapshot_items:
        dated_values = sorted({str(it.get("post_date")) for it in snapshot_items if it.get("post_date")})
        if dated_values:
            logger.info("Discovery date coverage: %s → %s", dated_values[0], dated_values[-1])
        else:
            logger.warning("Discovery produced snapshots but no dated items")

    filtered_items, win_stats = _filter_window_and_dedupe(snapshot_items, start, end, logger)

    raw_payload = {
        "schema": "raw.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "parsed_total": len(snapshot_items),
        "items_snapshot": snapshot_items,
        "audit": {
            "version": VERSION,
            "topics": SCOTUSBLOG_TOPICS,
            "base_url": SCOTUSBLOG_BASE,
            "max_api_pages": pages,
            "week1_start": DEMOCRACY_CLOCK_WEEK1_START.isoformat(),
        },
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    filtered_payload = {
        "schema": "filtered.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "item_type": "news_article",
        "items_count": len(filtered_items),
        "items": filtered_items,
        "entity_type": "news_article",
        "count": len(filtered_items),
        "entities": filtered_items,
        "window_stats": win_stats,
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(filtered_items))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(filtered_items),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


def _resolve_cli_window(args) -> Tuple[str, str]:
    """Resolve either --start/--end or Democracy Clock --week/--weeks."""
    if args.week is not None:
        weeks = args.weeks or 1
        if args.week < 1:
            raise SystemExit("--week must be >= 1")
        if weeks < 1:
            raise SystemExit("--weeks must be >= 1")
        start_date = DEMOCRACY_CLOCK_WEEK1_START + timedelta(days=(args.week - 1) * 7)
        end_date = start_date + timedelta(days=(weeks * 7) - 1)
        return start_date.isoformat(), end_date.isoformat()

    if not args.start or not args.end:
        raise SystemExit("Either --start/--end or --week [--weeks] is required")

    return args.start, args.end


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock V4 — SCOTUSblog harvester")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--week", type=int, help="Democracy Clock start week number; Week 1 starts 2025-01-20")
    p.add_argument("--weeks", type=int, default=1, help="number of weeks to harvest when --week is used")
    p.add_argument("--max-api-pages", type=int, default=None, help="maximum frontend listing pages to try per topic")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    start_arg, end_arg = _resolve_cli_window(args)

    meta = run_harvester(
        start=start_arg,
        end=end_arg,
        artifacts_root=args.artifacts,
        level=args.level,
        max_api_pages=args.max_api_pages,
    )
    print(meta)