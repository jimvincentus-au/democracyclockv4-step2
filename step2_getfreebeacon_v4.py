"""
Washington Free Beacon Step 2 harvester v4

Strategy (modeled on democracydocket v5 / dailysignal):
- Discover article URLs via Free Beacon's WordPress sitemap_index
- Restrict to substantive section URLs; exclude non-article paths
- Use <lastmod> as post_date (YYYY-MM-DD)
- Filter to requested date window and de-dupe by canonical_url

The Washington Free Beacon is a free, conservative-oriented opposition-research
news outlet. WordPress-based; archive accessible. URL section patterns vary;
we accept any URL with a section fragment from the allowlist below.
"""

from __future__ import annotations

import logging
import requests
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
HARVESTER_ID = "freebeacon"
SOURCE_DISPLAY = "Washington Free Beacon"
__all__ = ["run_harvester"]

FB_SITEMAP_INDEX = "https://freebeacon.com/sitemap_index.xml"

FB_KEEP_SITEMAP_PATTERNS = (
    "post-sitemap",
    "post_sitemap",
    "news-sitemap",
)

# Preferred section path fragments. Free Beacon has many sections; this list
# captures the federal-action-bearing ones. Adjust if their structure changes.
FB_PREFERRED_SECTIONS = (
    "/politics/",
    "/national-security/",
    "/courts/",
    "/media/",
    "/campus/",
    "/economy/",
    "/elections/",
    "/biden-administration/",
    "/trump-administration/",
    "/government/",
    "/issues/",
    "/democrats/",
    "/republicans/",
)

FB_SKIP_PATH_FRAGMENTS = (
    "/tag/", "/author/", "/category/", "/feed/", "/page/",
    "/about", "/contact", "/privacy", "/subscribe",
)

FB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
}


def title_from_slug(url: str) -> str:
    if not url:
        return ""
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-", " ").replace("_", " ")
    slug = " ".join(slug.split())
    return slug.capitalize()


def _section_of(url: str) -> str:
    low = url.lower()
    for frag in FB_PREFERRED_SECTIONS:
        if frag in low:
            return frag.strip("/")
    return "news"


def _is_article_url(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    for skip in FB_SKIP_PATH_FRAGMENTS:
        if skip in low:
            return False
    if not any(frag in low for frag in FB_PREFERRED_SECTIONS):
        return False
    return True


def _chunk_sort_index(url: str) -> int:
    """Chronological position of a Free Beacon sub-sitemap, for reverse-scan
    ordering. The numbered post-sitemaps are chronological: 'post-sitemap.xml'
    is oldest (position 1), 'post-sitemapN.xml' increases with recency
    (verified: all 79 chunks are monotonic by article date). Any non-numbered
    sub-sitemap (e.g. a news-sitemap, should the index ever add one) is treated
    as most-recent so the reverse-scan visits it first."""
    name = url.rsplit("/", 1)[-1].split("?")[0]
    if name.startswith("post-sitemap") and name.endswith(".xml"):
        mid = name[len("post-sitemap"):-len(".xml")]
        if mid == "":
            return 1
        if mid.isdigit():
            return int(mid)
    return 10 ** 9  # unknown/special chunk -> treat as newest


def _discover_via_sitemaps(
    logger: logging.Logger,
    cache: Optional[Dict[str, Any]] = None,
    window: Optional[Tuple[str, str]] = None,
) -> List[Dict[str, Any]]:
    logger.info("Washington Free Beacon: starting sitemap discovery at %s", FB_SITEMAP_INDEX)

    try:
        resp = requests.get(FB_SITEMAP_INDEX, headers=FB_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as ex:
        logger.error("Failed to fetch sitemap index: %s (%s)", FB_SITEMAP_INDEX, ex)
        return []

    try:
        root = ET.fromstring(resp.content)
    except Exception as ex:
        logger.error("Could not parse sitemap index XML: %s", ex)
        return []

    # Capture (loc, lastmod) tuples from the sitemap index for cache decisions.
    sitemap_urls_with_lastmod: List[Tuple[str, Optional[str]]] = []
    for sitemap in root.findall(".//{*}sitemap"):
        loc_elem = sitemap.find("{*}loc")
        lastmod_elem = sitemap.find("{*}lastmod")
        loc_text = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
        lastmod_text = lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else None
        if loc_text:
            sitemap_urls_with_lastmod.append((loc_text, lastmod_text))

    selected_with_lastmod: List[Tuple[str, Optional[str]]] = []
    for url, lastmod in sitemap_urls_with_lastmod:
        name = url.rstrip("/").split("/")[-1].lower()
        if any(p in name for p in FB_KEEP_SITEMAP_PATTERNS):
            selected_with_lastmod.append((url, lastmod))

    if not selected_with_lastmod:
        logger.warning("No article-bearing sub-sitemaps matched. "
                       "If structure changed, adjust FB_KEEP_SITEMAP_PATTERNS.")
        return []

    logger.info("Selected %d sub-sitemaps", len(selected_with_lastmod))

    # Reverse-scan: walk the sub-sitemaps newest -> oldest. Free Beacon's
    # numbered post-sitemaps are chronological, so once we reach a chunk whose
    # content is entirely older than the requested window, every remaining
    # chunk is older still and we can stop. On a cold cache this turns a
    # ~79-fetch full scan into (in-window chunks + 1 boundary chunk).
    scan_order = sorted(
        selected_with_lastmod, key=lambda t: _chunk_sort_index(t[0]), reverse=True
    )

    all_snapshots: List[Dict[str, Any]] = []
    cache_skipped = 0
    cache_fetched = 0
    for sitemap_url, chunk_index_lastmod in scan_order:
        short_name = sitemap_url.rsplit("/", 1)[-1]
        if cache is not None:
            skip, reason = should_skip_chunk(sitemap_url, chunk_index_lastmod, cache, window)
            if skip:
                # Newest -> oldest scan: a chunk that ends before the window
                # means every remaining (older) chunk is out of window too,
                # so stop. A chunk that starts after the window is just
                # skip-and-continue.
                if reason.startswith("chunk_dates_end_"):
                    logger.info("Reverse-scan stop at %s: %s", short_name, reason)
                    cache_skipped += 1
                    break
                logger.info("Cache-skip %s: %s", short_name, reason)
                cache_skipped += 1
                continue
            else:
                logger.debug("Cache-fetch %s: %s", short_name, reason)

        logger.info("Fetching sitemap: %s", sitemap_url)
        try:
            resp = requests.get(sitemap_url, headers=FB_HEADERS, timeout=30)
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
        cache_fetched += 1

        chunk_url_dates: List[str] = []
        for url_elem in url_elems:
            loc_elem = url_elem.find("{*}loc")
            lastmod_elem = url_elem.find("{*}lastmod")

            canonical_url = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
            lastmod_text = lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else ""

            if not canonical_url:
                continue

            post_date = lastmod_text[:10] if lastmod_text else ""

            if not _is_article_url(canonical_url):
                logger.debug("Skipping non-article URL: %s", canonical_url)
                continue

            # Record the date for the chunk's range ONLY from article URLs.
            # Non-article URLs (category/tag/author index pages) carry a
            # <lastmod> reflecting recent site activity, not this chunk's
            # article content. Including them smears the cached date range —
            # e.g. a 2012 chunk showing date_max 2026 because a category page
            # listed in it was touched today.
            if post_date:
                chunk_url_dates.append(post_date)

            title = title_from_slug(canonical_url)
            section = _section_of(canonical_url)

            snapshot: Dict[str, Any] = {
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
                "raw_line": f"[sitemap:{sitemap_url.rsplit('/', 1)[-1]}] {canonical_url}",
                "section": section,
            }
            all_snapshots.append(snapshot)

        if cache is not None:
            update_chunk(
                cache,
                sitemap_url,
                chunk_index_lastmod,
                chunk_url_dates,
                url_count=len(url_elems),
            )

        # Cold-cache early termination: now that we've fetched this chunk and
        # know its real date span, stop if it is entirely older than the
        # window. We scan newest -> oldest over chronological chunks, so every
        # remaining chunk is older still.
        if window and chunk_url_dates:
            ws, _we = window
            chunk_dmax = max(chunk_url_dates)
            if chunk_dmax < ws:
                logger.info(
                    "Reverse-scan stop after %s: chunk date_max %s < window_start %s",
                    short_name, chunk_dmax, ws,
                )
                break

    if cache is not None:
        logger.info(
            "Sitemap cache: skipped=%d fetched=%d (of %d selected)",
            cache_skipped, cache_fetched, len(selected_with_lastmod),
        )

    logger.info("Total %d candidate URLs discovered.", len(all_snapshots))
    return all_snapshots


def _filter_window_and_dedupe(snapshot_items, start_iso, end_iso, logger):
    kept_pre_dedupe, stats = [], {"inside": 0, "outside": 0, "nodate": 0, "no_url": 0}
    for it in snapshot_items:
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        iso = (it.get("post_date") or "").strip()
        if not url:
            stats["no_url"] += 1; continue
        if not iso:
            stats["nodate"] += 1; continue
        if not within_window(iso, start_iso, end_iso):
            stats["outside"] += 1; continue
        stats["inside"] += 1
        kept_pre_dedupe.append(it)

    seen, deduped, dups = set(), [], 0
    for r in kept_pre_dedupe:
        k = r.get("canonical_url") or r.get("url") or ""
        if not k or k in seen:
            dups += 1; continue
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
    logger.info("Discovering Washington Free Beacon articles (sitemap mode)")

    cache = None
    if use_cache:
        cache = load_cache(artifacts, HARVESTER_ID)
        pre_stats = cache_stats(cache)
        logger.info(
            "Sitemap cache loaded: chunks=%d dated=%d range=%s..%s",
            pre_stats["chunk_count"], pre_stats["dated_chunk_count"],
            pre_stats["overall_date_min"], pre_stats["overall_date_max"],
        )
    else:
        logger.info("Sitemap cache disabled by flag.")

    snapshot_items = _discover_via_sitemaps(logger, cache=cache, window=(start, end))

    if cache is not None:
        try:
            save_cache(artifacts, HARVESTER_ID, cache)
            post_stats = cache_stats(cache)
            logger.info(
                "Sitemap cache saved: chunks=%d dated=%d range=%s..%s",
                post_stats["chunk_count"], post_stats["dated_chunk_count"],
                post_stats["overall_date_min"], post_stats["overall_date_max"],
            )
        except Exception as ex:
            logger.warning("Failed to save sitemap cache: %s", ex)

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
        "schema": "raw.v4", "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "parsed_total": len(snapshot_items),
        "items_snapshot": snapshot_items,
        "audit": {},
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    filtered_payload = {
        "schema": "filtered.v4", "source": HARVESTER_ID,
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

    p = argparse.ArgumentParser(description="Democracy Clock V4 — Washington Free Beacon harvester (SITEMAP)")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    p.add_argument("--no-cache", action="store_true",
                   help="disable persistent sitemap chunk cache for this run")
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
