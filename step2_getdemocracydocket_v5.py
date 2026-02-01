"""
Democracy Docket Step 2 Scraper v5
"""

import os
import sys
import logging
import requests
from datetime import datetime
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup

# --- V4 infrastructure imports ---
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    build_session,
    create_artifact_paths,
    write_json,
    normalize_ws,
    canonicalize_url,
    within_window,
)

# VERSION constant
VERSION = "v5"

HARVESTER_ID = "democracydocket"
__all__ = ["run_harvester"]


# SITEMAP discovery constants
DD_SITEMAP_INDEX = "https://www.democracydocket.com/sitemap_index.xml"
# We harvest the three green-check sitemap families.
# Note: Democracy Docket includes /analysis/ URLs inside news/opinion sitemaps.
DD_ALLOWED_SITEMAP_PREFIXES = {
    "news-sitemap": "news",
    "opinion-sitemap": "opinion",
    "alerts-sitemap": "alerts",
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
    if not url:
        return ""
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-", " ").replace("_", " ")
    slug = " ".join(slug.split())
    return slug.capitalize()


def _discover_via_sitemaps(logger) -> List[Dict[str, Any]]:
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

    # Extract sitemap URLs from index
    sitemap_urls: List[str] = []
    for sitemap in root.findall(".//{*}sitemap"):
        loc_elem = sitemap.find("{*}loc")
        loc_text = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
        if loc_text:
            sitemap_urls.append(loc_text)

    # Select only the allowed sitemap families (three green-check groups)
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
    for sitemap_url, section_name in selected_sitemaps:
        logger.info("Fetching sitemap for section '%s': %s", section_name, sitemap_url)
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

            # Normalize lastmod to YYYY-MM-DD if present
            post_date = lastmod_text[:10] if lastmod_text else ""

            # Derive an "effective section" from the URL path (DD mixes /analysis/ into news/opinion sitemaps)
            effective_section = section_name
            if "/analysis/" in canonical_url:
                effective_section = "analysis"
            elif "/news/" in canonical_url:
                effective_section = "news"
            elif "/opinion/" in canonical_url:
                effective_section = "opinion"
            elif "/alerts/" in canonical_url:
                effective_section = "alerts"

            title = title_from_slug(canonical_url)

            snapshot: Dict[str, Any] = {
                "source": "Democracy Docket",
                "doc_type": "news_article",
                "title": title,  # slug-derived title
                "url": canonical_url,
                "canonical_url": canonical_url,
                "summary_url": "",
                "summary": "",
                "summary_origin": "",
                "summary_timestamp": "",
                "post_date": post_date,  # YYYY-MM-DD
                "raw_line": f"[sitemap:{section_name}] {canonical_url}",
                "section": effective_section,
            }
            all_snapshots.append(snapshot)

    if not all_snapshots:
        logger.warning("No URLs discovered via sitemaps.")
    else:
        logger.info("Total %d URLs discovered via sitemaps.", len(all_snapshots))

    return all_snapshots


# ---- Window filter and dedupe (V4 logic) ----
def _filter_window_and_dedupe(snapshot_items: List[Dict[str, Any]], start_iso: str, end_iso: str, logger):
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
        start_iso, end_iso, len(snapshot_items), len(kept_pre_dedupe), len(deduped),
        stats["outside"], stats["nodate"], stats["no_url"], dups
    )
    return deduped, stats


# ---- Harvester entrypoint (V4 contract) ----
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

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Democracy Docket (V5 sitemap mode)")

    snapshot_items = _discover_via_sitemaps(logger)

    # Track and log date-window counts before HTML fetch
    total_discovered = len(snapshot_items)
    total_in_window = sum(
        1 for it in snapshot_items if it.get("post_date") and within_window(it["post_date"], start, end)
    )
    logger.info(
        "Discovered %d URLs total; %d within window %s → %s",
        total_discovered, total_in_window, start, end
    )

    # No HTML fetch for title extraction (removed per instructions)
    # If body-text extraction is present it would go here, but currently none.

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


# CLI: V4-compatible contract
if __name__ == "__main__":
    import argparse
    import json
    from collections import Counter

    p = argparse.ArgumentParser(description="Democracy Clock V5 — Democracy Docket harvester (SITEMAP)")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    p.add_argument("--discovery-only", action="store_true", help="discover sitemap URLs and print summary JSON; do not write artifacts")
    args = p.parse_args()

    logger = setup_logger(f"dc.{HARVESTER_ID}", args.level)

    if args.discovery_only:
        logger.info("=== Discovery-only mode: Starting sitemap discovery ===")
        snapshot_items = _discover_via_sitemaps(logger)
        total = len(snapshot_items)

        by_section = Counter()
        post_dates: List[str] = []
        items: List[Dict[str, Any]] = []

        for item in snapshot_items:
            section = (item.get("section") or "")
            by_section[section] += 1
            post_date = (item.get("post_date") or "")
            if post_date:
                post_dates.append(post_date)
            items.append({
                "url": item.get("url"),
                "section": section,
                "post_date": post_date,
            })

        earliest = min(post_dates) if post_dates else None
        latest = max(post_dates) if post_dates else None

        logger.info("Discovery-only: total URLs discovered: %d", total)
        logger.info("Discovery-only: count per section: %r", dict(by_section))
        logger.info("Discovery-only: earliest post_date: %r latest post_date: %r", earliest, latest)

        artifact = {
            "schema": "democracydocket.discovery.v5",
            "total": total,
            "by_section": dict(by_section),
            "date_range": {"earliest": earliest, "latest": latest},
            "items": items,
        }
        print(json.dumps(artifact, indent=2, sort_keys=True))
        logger.info("=== Discovery-only mode: Completed. Exiting. ===")
        raise SystemExit(0)

    # Normal mode: require start/end like V4
    if not args.start or not args.end:
        p.error("--start and --end are required unless --discovery-only is used")

    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
    )
    print(meta)