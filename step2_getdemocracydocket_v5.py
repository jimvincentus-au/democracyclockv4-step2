"""
Democracy Docket Step 2 Scraper v5

V5 strategy:
- Discover article URLs via Democracy Docket sitemap index
- Restrict to allowed sections (news, analysis, opinion)
- Use <lastmod> as post_date (YYYY-MM-DD)
- Filter to requested date window and de-dupe by canonical_url
- Write raw + filtered artifacts using the V4 contract

Note: Per project decision, we do NOT fetch HTML to extract titles.
We derive a “good enough” title from the URL slug.
"""

from __future__ import annotations

import logging
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

# --- V4 infrastructure imports ---
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    build_session,
    create_artifact_paths,
    write_json,
    within_window,
)

VERSION = "v5"
HARVESTER_ID = "democracydocket"
__all__ = ["run_harvester"]

# SITEMAP discovery constants
DD_SITEMAP_INDEX = "https://www.democracydocket.com/sitemap_index.xml"

# Allowed sitemap families (NOTE: DD may place /analysis/ URLs inside other sitemaps)
DD_ALLOWED_SITEMAP_PREFIXES = {
    "news-sitemap": "news",
    "opinion-sitemap": "opinion",
    "analysis-sitemap": "analysis",
}

# Headers for requests to avoid 403 errors (browser-identifying)
DD_HEADERS = {
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


def _discover_via_sitemaps(logger: logging.Logger) -> List[Dict[str, Any]]:
    """Discover Democracy Docket URLs via sitemap index + allowed sitemap families."""
    logger.info("Democracy Docket v5: starting sitemap discovery")

    try:
        resp = requests.get(DD_SITEMAP_INDEX, headers=DD_HEADERS, timeout=30)
        resp.raise_for_status()
        logger.info("Fetched sitemap index: %s", DD_SITEMAP_INDEX)
    except Exception as ex:
        logger.error("Failed to fetch sitemap index: %s (%s)", DD_SITEMAP_INDEX, ex)
        return []

    try:
        root = ET.fromstring(resp.content)
    except Exception as ex:
        logger.error("Could not parse sitemap index XML: %s", ex)
        return []

    sitemap_urls: List[str] = []
    for sitemap in root.findall(".//{*}sitemap"):
        loc_elem = sitemap.find("{*}loc")
        loc_text = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
        if loc_text:
            sitemap_urls.append(loc_text)

    selected_sitemaps: List[tuple[str, str]] = []
    for url in sitemap_urls:
        name = url.rstrip("/").split("/")[-1]
        for prefix, section_name in DD_ALLOWED_SITEMAP_PREFIXES.items():
            if name.startswith(prefix):
                selected_sitemaps.append((url, section_name))
                break

    if not selected_sitemaps:
        logger.warning("No allowed sitemaps matched in sitemap index.")
        return []

    all_snapshots: List[Dict[str, Any]] = []

    for sitemap_url, sitemap_family in selected_sitemaps:
        logger.info("Fetching sitemap family '%s': %s", sitemap_family, sitemap_url)
        try:
            resp = requests.get(sitemap_url, headers=DD_HEADERS, timeout=30)
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
        logger.info("Found %d URLs in sitemap: %s", len(url_elems), sitemap_url)

        for url_elem in url_elems:
            loc_elem = url_elem.find("{*}loc")
            lastmod_elem = url_elem.find("{*}lastmod")

            canonical_url = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
            lastmod_text = lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else ""

            if not canonical_url:
                continue

            # Restrict to allowed sections only: /news/, /analysis/, /opinion/
            if not ("/news/" in canonical_url or "/analysis/" in canonical_url or "/opinion/" in canonical_url):
                logger.debug("Skipping URL due to section filtering: %s", canonical_url)
                continue

            post_date = lastmod_text[:10] if lastmod_text else ""

            # Derive effective section from URL path.
            if "/analysis/" in canonical_url:
                effective_section = "analysis"
            elif "/opinion/" in canonical_url:
                effective_section = "opinion"
            else:
                effective_section = "news"

            title = title_from_slug(canonical_url)

            snapshot: Dict[str, Any] = {
                # Stable source key and display name
                "source_key": HARVESTER_ID,
                "source": "Democracy Docket",
                "doc_type": "news_article",
                "title": title,
                "url": canonical_url,
                "canonical_url": canonical_url,
                "summary_url": "",
                "summary": "",
                "summary_origin": "",
                "summary_timestamp": "",
                "post_date": post_date,
                "raw_line": f"[sitemap:{sitemap_family}] {canonical_url}",
                "section": effective_section,
            }
            all_snapshots.append(snapshot)

    if not all_snapshots:
        logger.warning("No URLs discovered via sitemaps.")
    else:
        logger.info("Total %d URLs discovered via sitemaps.", len(all_snapshots))

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
            logger.debug("Window: %s SKIPT reason=%s | url=%r", iso or "''", reason, url)
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

    _ = session or build_session()

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Democracy Docket (V5 sitemap mode)")

    snapshot_items = _discover_via_sitemaps(logger)

    total_discovered = len(snapshot_items)
    total_in_window = sum(
        1
        for it in snapshot_items
        if it.get("post_date") and within_window(str(it["post_date"]), start, end)
    )
    logger.info(
        "Discovered %d URLs total; %d within window %s → %s",
        total_discovered,
        total_in_window,
        start,
        end,
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

    p = argparse.ArgumentParser(description="Democracy Clock V5 — Democracy Docket harvester (SITEMAP)")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    if not args.start or not args.end:
        p.error("--start and --end are required")

    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
    )
    print(meta)