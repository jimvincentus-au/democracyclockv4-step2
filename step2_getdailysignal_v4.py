"""
Daily Signal Step 2 harvester v4

Strategy (modeled on democracydocket v5):
- Discover article URLs via The Daily Signal's WordPress sitemap_index
- Restrict to article URLs (exclude /category/, /tag/, /author/, /feed/)
- PRIMARY post_date source: YYYY-MM-DD extracted from the URL path
  (/YYYY/MM/DD/slug/). The Daily Signal's sitemap <lastmod> reflects
  a recent CMS migration, not the original publish date, so URL-date
  is the reliable source.
- Fall back to <lastmod> only when the URL has no date pattern
- Skip chunks that don't intersect the requested window when chunk URLs
  carry dates (cheap heuristic — avoids extracting from off-window chunks)
- Filter to requested date window and de-dupe by canonical_url
- Write raw + filtered artifacts using the V4 contract

The Daily Signal is the Heritage Foundation's news arm: free, no paywall,
WordPress-based, sitemap-accessible back through site inception. Titles are
derived from URL slug per project convention.
"""

from __future__ import annotations

import logging
import re
import requests
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    build_session,
    create_artifact_paths,
    write_json,
    within_window,
)
from step2_sitemap_cache_v4 import (
    load_cache,
    save_cache,
    should_skip_chunk,
    update_chunk,
    cache_stats,
)

VERSION = "v4"
HARVESTER_ID = "dailysignal"
SOURCE_DISPLAY = "The Daily Signal"
__all__ = ["run_harvester"]

DS_SITEMAP_INDEX = "https://www.dailysignal.com/sitemap_index.xml"

# Sitemap families to keep (WordPress convention: post-sitemap-N.xml chunks).
# Adjust if the site's structure changes.
DS_KEEP_SITEMAP_PATTERNS = (
    "post-sitemap",
    "post_sitemap",
    "news-sitemap",
)

# URL path fragments that mark non-article URLs to skip.
DS_SKIP_PATH_FRAGMENTS = (
    "/category/", "/tag/", "/author/", "/feed/",
    "/page/", "/about", "/contact", "/privacy",
)

DS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
}


def title_from_slug(url: str) -> str:
    """Derive a readable title from the URL slug."""
    if not url:
        return ""
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-", " ").replace("_", " ")
    slug = " ".join(slug.split())
    return slug.capitalize()


# Matches /YYYY/MM/DD/ or /YYYY/MM/ in URL paths.
_URL_DATE_RE = re.compile(r"/(\d{4})/(\d{1,2})/(?:(\d{1,2})/)?")


def _date_from_url(url: str) -> str:
    """Extract YYYY-MM-DD from URL path. Returns '' if not found.

    Handles both /YYYY/MM/DD/slug/ and /YYYY/MM/slug/ patterns.
    For /YYYY/MM/ (no day), returns 'YYYY-MM-01' as a conservative
    in-month placeholder; window-filter logic will treat as best-effort.
    """
    if not url:
        return ""
    m = _URL_DATE_RE.search(url)
    if not m:
        return ""
    y, mo, d = m.group(1), m.group(2), m.group(3)
    try:
        yi, mi = int(y), int(mo)
        di = int(d) if d else 1
        # Validate
        date(yi, mi, di)
        return f"{yi:04d}-{mi:02d}-{di:02d}"
    except (ValueError, TypeError):
        return ""


def _is_article_url(url: str) -> bool:
    """True if the URL looks like an article (not a category page, etc.)."""
    if not url:
        return False
    low = url.lower()
    for skip in DS_SKIP_PATH_FRAGMENTS:
        if skip in low:
            return False
    # Daily Signal article URLs typically include /YYYY/MM/DD/slug/ or /YYYY/MM/slug/
    # Require at least one numeric path segment to filter out static pages.
    parts = [p for p in url.rstrip("/").split("/") if p]
    has_numeric = any(p.isdigit() and len(p) == 4 for p in parts)
    return has_numeric


def _chunk_overlaps_window(url_dates: List[str], start_iso: str, end_iso: str) -> bool:
    """True if any URL-derived date in the chunk falls within the window.

    Cheap optimization: once we've extracted dates from a chunk's URLs,
    if NONE of them is in the window, we know the chunk has nothing to
    contribute and can skip per-URL processing.
    """
    if not url_dates:
        return True  # No URL dates extracted; can't skip — process conservatively
    return any(start_iso <= d <= end_iso for d in url_dates if d)


def _discover_via_sitemaps(
    logger: logging.Logger,
    window: Optional[Tuple[str, str]] = None,
    cache: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Discover Daily Signal URLs via the sitemap index + post-sitemap chunks.

    If window=(start_iso, end_iso) is provided, chunks whose URL-derived
    date range does not intersect the window are skipped entirely.

    If cache is provided, the sitemap-chunk cache is consulted before each
    chunk fetch and updated after.
    """
    logger.info("Daily Signal: starting sitemap discovery at %s", DS_SITEMAP_INDEX)

    try:
        resp = requests.get(DS_SITEMAP_INDEX, headers=DS_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as ex:
        logger.error("Failed to fetch sitemap index: %s (%s)", DS_SITEMAP_INDEX, ex)
        return []

    try:
        root = ET.fromstring(resp.content)
    except Exception as ex:
        logger.error("Could not parse sitemap index XML: %s", ex)
        return []

    # Capture chunk URL + <lastmod> pairs from the index.
    sitemap_urls_with_lastmod: List[Tuple[str, Optional[str]]] = []
    for sitemap in root.findall(".//{*}sitemap"):
        loc_elem = sitemap.find("{*}loc")
        lastmod_elem = sitemap.find("{*}lastmod")
        loc_text = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
        lastmod_text = (
            lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else None
        )
        if loc_text:
            sitemap_urls_with_lastmod.append((loc_text, lastmod_text))

    selected: List[Tuple[str, Optional[str]]] = []
    for url, lastmod in sitemap_urls_with_lastmod:
        name = url.rstrip("/").split("/")[-1].lower()
        if any(p in name for p in DS_KEEP_SITEMAP_PATTERNS):
            selected.append((url, lastmod))

    if not selected:
        logger.warning("No post-sitemap chunks matched in sitemap index.")
        return []

    logger.info("Selected %d post-sitemap chunks", len(selected))

    return _process_selected_sitemaps(selected, logger, window=window, cache=cache)


def _process_selected_sitemaps(
    selected_with_lastmod: List[Tuple[str, Optional[str]]],
    logger: logging.Logger,
    window: Optional[Tuple[str, str]] = None,
    cache: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Process selected sitemap chunks. If window is provided as (start_iso, end_iso),
    skip chunks whose URL-derived dates do not intersect the window.

    If cache is provided, the cache is consulted to skip unchanged-and-out-of-window
    chunks BEFORE the HTTP fetch, and updated after each successful chunk fetch.
    """
    all_snapshots: List[Dict[str, Any]] = []
    cache_skip_count = 0

    for sitemap_url, chunk_index_lastmod in selected_with_lastmod:
        # Cache check: skip the HTTP fetch entirely when possible.
        if cache is not None:
            skip, reason = should_skip_chunk(sitemap_url, chunk_index_lastmod, cache, window)
            if skip:
                logger.info("Cache-skip %s: %s",
                            sitemap_url.rsplit("/", 1)[-1], reason)
                cache_skip_count += 1
                continue

        logger.info("Fetching sitemap: %s", sitemap_url)
        try:
            resp = requests.get(sitemap_url, headers=DS_HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as ex:
            logger.error("Failed to fetch sitemap: %s (%s)", sitemap_url, ex)
            continue

        try:
            sitemap_root = ET.fromstring(resp.content)
        except Exception as ex:
            logger.error("Could not parse sitemap XML for %s: %s", sitemap_url, ex)
            continue

        url_elems = sitemap_root.findall(".//{*}url")
        logger.info("Found %d URLs in %s", len(url_elems), sitemap_url)

        # Extract per-URL data once; reuse for cache update and snapshot build.
        chunk_url_dates: List[str] = []
        chunk_snapshots: List[Dict[str, Any]] = []

        for url_elem in url_elems:
            loc_elem = url_elem.find("{*}loc")
            lastmod_elem = url_elem.find("{*}lastmod")

            canonical_url = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
            lastmod_text = lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else ""

            if not canonical_url:
                continue

            url_date = _date_from_url(canonical_url)
            if url_date:
                chunk_url_dates.append(url_date)

            if not _is_article_url(canonical_url):
                logger.debug("Skipping non-article URL: %s", canonical_url)
                continue

            post_date = url_date or (lastmod_text[:10] if lastmod_text else "")
            date_origin = "url" if url_date else ("lastmod" if lastmod_text else "")
            title = title_from_slug(canonical_url)

            chunk_snapshots.append({
                "source_key": HARVESTER_ID,
                "source": SOURCE_DISPLAY,
                "doc_type": "news_article",
                "title": title,
                "url": canonical_url,
                "canonical_url": canonical_url,
                "summary_url": "",
                "summary": "",
                "summary_origin": "",
                "summary_timestamp": "",
                "post_date": post_date,
                "post_date_origin": date_origin,
                "raw_line": f"[sitemap:{sitemap_url.rsplit('/', 1)[-1]}] {canonical_url}",
                "section": "news",
            })

        # Update cache with this chunk's date range (only after a successful fetch).
        if cache is not None:
            update_chunk(
                cache,
                sitemap_url,
                chunk_index_lastmod,
                chunk_url_dates,
                url_count=len(url_elems),
            )

        # Apply window-overlap skip AFTER cache update so we still record the
        # chunk's date range even if we don't keep its URLs this run.
        if window is not None and chunk_url_dates:
            if not _chunk_overlaps_window(chunk_url_dates, window[0], window[1]):
                d_min = min(chunk_url_dates)
                d_max = max(chunk_url_dates)
                logger.info(
                    "Discarding %s URLs from %s (chunk URL-date range %s..%s does not overlap window %s..%s)",
                    len(chunk_snapshots), sitemap_url.rsplit("/", 1)[-1], d_min, d_max, window[0], window[1],
                )
                continue

        all_snapshots.extend(chunk_snapshots)

    if cache is not None and cache_skip_count:
        logger.info("Cache saved %d chunk fetches this run.", cache_skip_count)

    logger.info("Total %d candidate URLs discovered via sitemaps.", len(all_snapshots))
    return all_snapshots


def _filter_window_and_dedupe(
    snapshot_items: List[Dict[str, Any]],
    start_iso: str,
    end_iso: str,
    logger: logging.Logger,
):
    """Window filter + stable dedupe by canonical_url. Mirrors democracydocket v5."""
    kept_pre_dedupe: List[Dict[str, Any]] = []
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_url": 0}

    for it in snapshot_items:
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        iso = (it.get("post_date") or "").strip()

        if not url:
            stats["no_url"] += 1
            continue
        if not iso:
            stats["nodate"] += 1
            continue
        if not within_window(iso, start_iso, end_iso):
            stats["outside"] += 1
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
        start_iso, end_iso,
        len(snapshot_items), len(kept_pre_dedupe), len(deduped),
        stats["outside"], stats["nodate"], stats["no_url"], dups,
    )
    return deduped, stats


def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
    use_cache: bool = True,
) -> Dict[str, Any]:

    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    _ = session or build_session()

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Daily Signal articles (sitemap mode; chunk-skip on URL-date)")

    cache = load_cache(artifacts, HARVESTER_ID) if use_cache else None
    if cache is not None:
        stats = cache_stats(cache)
        logger.info("Sitemap cache loaded: %d chunks cached, overall date range %s..%s",
                    stats["chunk_count"], stats["overall_date_min"], stats["overall_date_max"])

    snapshot_items = _discover_via_sitemaps(logger, window=(start, end), cache=cache)

    if cache is not None:
        save_cache(artifacts, HARVESTER_ID, cache)
        logger.info("Sitemap cache saved.")
    total_in_window = sum(
        1 for it in snapshot_items
        if it.get("post_date") and within_window(str(it["post_date"]), start, end)
    )
    logger.info(
        "Discovered %d URLs total; %d within window %s → %s",
        len(snapshot_items), total_in_window, start, end,
    )

    filtered_items, win_stats = _filter_window_and_dedupe(snapshot_items, start, end, logger)

    raw_payload = {
        "schema": "raw.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "parsed_total": len(snapshot_items),
        "items_snapshot": snapshot_items,
        "audit": {},
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


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock V4 — Daily Signal harvester (SITEMAP)")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    p.add_argument("--no-cache", action="store_true",
                   help="bypass sitemap cache (force fetch all chunks)")
    args = p.parse_args()

    if not args.start or not args.end:
        p.error("--start and --end are required")

    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        use_cache=not args.no_cache,
    )
    print(meta)
