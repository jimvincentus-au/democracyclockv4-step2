"""
Washington Examiner Step 2 harvester v4

Strategy (modeled on democracydocket v5 / dailysignal):
- Discover article URLs via the Examiner's sitemap index
- Restrict to substantive news sections; exclude obvious non-article URLs
- Use <lastmod> as post_date (YYYY-MM-DD)
- Filter to requested date window and de-dupe by canonical_url

The Washington Examiner is a free, archive-accessible conservative DC news
outlet. Section convention varies across the site; we accept everything that
matches a news-shaped URL pattern and let the builder's LLM extractor filter
for federal-action relevance.
"""

from __future__ import annotations

import logging
import re
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
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
HARVESTER_ID = "examiner"
SOURCE_DISPLAY = "Washington Examiner"
__all__ = ["run_harvester"]

# Per Examiner robots.txt:
#   Sitemap: https://www.washingtonexaminer.com/sitemap.xml
#   Sitemap: https://www.washingtonexaminer.com/news-sitemap.xml
# sitemap.xml may be a sitemapindex (lists chunks) OR a flat urlset.
# news-sitemap.xml is the Google News sitemap (recent items only).
# We try the primary first; if it yields nothing in window, fall back to news.
WE_SITEMAP_PRIMARY = "https://www.washingtonexaminer.com/sitemap.xml"
WE_SITEMAP_NEWS = "https://www.washingtonexaminer.com/news-sitemap.xml"
WE_SITEMAP_INDEX = WE_SITEMAP_PRIMARY  # backward-compat alias

# Examiner does not appear to be WordPress; accept every chunk listed.
WE_KEEP_SITEMAP_PATTERNS = ()  # empty = accept all chunks

# Section path fragments preferred (kept). Only federal-governance news +
# policy. Opinion / restoring-america / gossip verticals are excluded so we
# don't spend LLM tokens building "events" from commentary. (Filter scoped
# from a weeks 68+69 harvest analysis, 2026-05-14.)
WE_PREFERRED_SECTIONS = (
    "/news/",
    "/policy/",
)

# Path fragments that mark non-article OR out-of-scope URLs to skip.
# Checked BEFORE the preferred-section allowlist. The /news/<subsection>/
# entries drop peripheral news verticals that aren't federal-governance
# events: campaigns (election horse-race), world, crime, business, the
# washington-secrets gossip column, entertainment, and partisan-scandal
# investigations. Bare /news/<id>/ articles are intentionally still kept.
WE_SKIP_PATH_FRAGMENTS = (
    "/tag/", "/author/", "/category/", "/feed/", "/page/",
    "/about", "/contact", "/privacy", "/subscribe", "/newsletters",
    "/news/campaigns/", "/news/world/", "/news/crime/",
    "/news/business/", "/news/washington-secrets/",
    "/news/entertainment/", "/news/investigations/",
)

WE_HEADERS = {
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
    """Return the first matching preferred-section fragment without slashes."""
    low = url.lower()
    for frag in WE_PREFERRED_SECTIONS:
        if frag in low:
            return frag.strip("/")
    return "news"


def _is_article_url(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    for skip in WE_SKIP_PATH_FRAGMENTS:
        if skip in low:
            return False
    # Allow if URL contains any preferred-section fragment.
    if not any(frag in low for frag in WE_PREFERRED_SECTIONS):
        return False
    return True


def _fetch_xml(url: str, logger: logging.Logger):
    """Fetch and parse an XML doc. Returns (root_element, raw_bytes) or (None, None)."""
    try:
        resp = requests.get(url, headers=WE_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as ex:
        logger.error("Failed to fetch %s (%s)", url, ex)
        return None, None
    try:
        return ET.fromstring(resp.content), resp.content
    except Exception as ex:
        logger.error("Could not parse XML for %s: %s", url, ex)
        return None, None


def _root_tag_local(root) -> str:
    """Return the local-name of the XML root tag (strips namespace)."""
    tag = root.tag if root is not None else ""
    if "}" in tag:
        tag = tag.split("}", 1)[1]
    return tag.lower()


def _date_from_examiner_chunk_url(url: str) -> Optional[str]:
    """Return YYYY-MM-DD if the URL is a dated Examiner daily sub-sitemap.

    Examiner's sitemap index lists ~8,800 daily chunks of the form
        https://www.washingtonexaminer.com/sitemap.xml?yyyy=2025&mm=01&dd=22
    We exploit that the publish date is encoded in the query string and
    pre-filter chunks against the requested window WITHOUT fetching them.
    Returns None if the URL is not dated (e.g., news-sitemap.xml, or a
    structural change at the source).
    """
    try:
        q = parse_qs(urlparse(url).query)
        y = int(q["yyyy"][0])
        m = int(q["mm"][0])
        d = int(q["dd"][0])
        # Cheap sanity check on field ranges.
        if not (1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31):
            return None
        return f"{y:04d}-{m:02d}-{d:02d}"
    except (KeyError, ValueError, IndexError):
        return None


def _extract_url_entries(
    root,
    source_sitemap: str,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], List[str], int]:
    """Extract <url> entries from a <urlset> root.

    Returns (snapshot_dicts, chunk_url_dates, total_url_count) where
    chunk_url_dates includes the YYYY-MM-DD strings of every <url> entry
    that has any date — even non-article URLs — so the sitemap cache can
    record an accurate date range for this chunk."""
    out: List[Dict[str, Any]] = []
    chunk_url_dates: List[str] = []
    url_elems = root.findall(".//{*}url")
    logger.info("Found %d URLs in %s", len(url_elems), source_sitemap)
    for url_elem in url_elems:
        loc_elem = url_elem.find("{*}loc")
        lastmod_elem = url_elem.find("{*}lastmod")

        # Google News sitemap publishes date inside <news:publication_date>
        news_date_elem = url_elem.find(".//{*}publication_date")

        canonical_url = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
        lastmod_text = lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else ""
        news_date_text = news_date_elem.text.strip() if (news_date_elem is not None and news_date_elem.text) else ""

        if not canonical_url:
            continue

        # News sitemap publication_date wins if present (it's the publish date);
        # otherwise fall back to <lastmod>.
        post_date = (news_date_text[:10] if news_date_text else lastmod_text[:10]) if (news_date_text or lastmod_text) else ""
        if post_date:
            chunk_url_dates.append(post_date)

        if not _is_article_url(canonical_url):
            logger.debug("Skipping non-article URL: %s", canonical_url)
            continue

        title = title_from_slug(canonical_url)
        section = _section_of(canonical_url)

        out.append({
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
            "raw_line": f"[sitemap:{source_sitemap.rsplit('/', 1)[-1]}] {canonical_url}",
            "section": section,
        })
    return out, chunk_url_dates, len(url_elems)


def _discover_from_sitemap_url(
    sitemap_url: str,
    logger: logging.Logger,
    cache: Optional[Dict[str, Any]] = None,
    window: Optional[Tuple[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Fetch a sitemap URL. Handle both <sitemapindex> (recurse into chunks)
    and <urlset> (extract URL entries directly).

    When a sitemap cache is provided, sub-chunks listed inside a
    <sitemapindex> are checked against the cache before fetching: chunks
    whose cached date range cannot overlap the requested window, and
    whose <lastmod> hasn't changed since last fetch, are skipped."""
    root, _ = _fetch_xml(sitemap_url, logger)
    if root is None:
        return []

    tag = _root_tag_local(root)
    logger.info("Sitemap %s root tag: <%s>", sitemap_url, tag)

    if tag == "urlset":
        entries, _dates, _count = _extract_url_entries(root, sitemap_url, logger)
        return entries

    if tag == "sitemapindex":
        sitemap_urls_with_lastmod: List[Tuple[str, Optional[str]]] = []
        for sitemap in root.findall(".//{*}sitemap"):
            loc_elem = sitemap.find("{*}loc")
            lastmod_elem = sitemap.find("{*}lastmod")
            loc_text = loc_elem.text.strip() if (loc_elem is not None and loc_elem.text) else ""
            lastmod_text = lastmod_elem.text.strip() if (lastmod_elem is not None and lastmod_elem.text) else None
            if loc_text:
                sitemap_urls_with_lastmod.append((loc_text, lastmod_text))

        # If a keep-pattern allowlist is configured, filter; otherwise accept all.
        if WE_KEEP_SITEMAP_PATTERNS:
            sitemap_urls_with_lastmod = [
                (u, lm) for (u, lm) in sitemap_urls_with_lastmod
                if any(p in u.rstrip("/").split("/")[-1].lower() for p in WE_KEEP_SITEMAP_PATTERNS)
            ]

        # URL-date pre-filter: Examiner publishes one sub-sitemap per day with
        # the date encoded in the URL query (yyyy=...&mm=...&dd=...). For dated
        # chunks outside the requested window we skip the fetch entirely — no
        # cache lookup needed. Undated chunks (e.g., news-sitemap.xml) pass
        # through to the normal cache + fetch path.
        url_date_skipped = 0
        if window:
            ws, we = window
            pre: List[Tuple[str, Optional[str]]] = []
            for u, lm in sitemap_urls_with_lastmod:
                chunk_iso = _date_from_examiner_chunk_url(u)
                if chunk_iso is None:
                    pre.append((u, lm))
                    continue
                if ws <= chunk_iso <= we:
                    pre.append((u, lm))
                else:
                    url_date_skipped += 1
            if url_date_skipped:
                logger.info(
                    "URL-date pre-filter: kept=%d skipped=%d (of %d sub-sitemaps) for window %s..%s",
                    len(pre), url_date_skipped, len(sitemap_urls_with_lastmod), ws, we,
                )
            sitemap_urls_with_lastmod = pre

        if not sitemap_urls_with_lastmod:
            logger.warning("Sitemap index at %s yielded zero sub-sitemaps to fetch.", sitemap_url)
            return []

        logger.info("Sitemap index %s lists %d sub-sitemaps (after URL-date pre-filter)",
                    sitemap_url, len(sitemap_urls_with_lastmod))

        all_snapshots: List[Dict[str, Any]] = []
        cache_skipped = 0
        cache_fetched = 0
        for sub_url, sub_lastmod in sitemap_urls_with_lastmod:
            if cache is not None:
                skip, reason = should_skip_chunk(sub_url, sub_lastmod, cache, window)
                if skip:
                    logger.info("Cache-skip %s: %s", sub_url.rsplit("/", 1)[-1], reason)
                    cache_skipped += 1
                    continue
                else:
                    logger.debug("Cache-fetch %s: %s", sub_url.rsplit("/", 1)[-1], reason)

            sub_root, _ = _fetch_xml(sub_url, logger)
            if sub_root is None:
                continue
            sub_tag = _root_tag_local(sub_root)
            if sub_tag == "urlset":
                entries, chunk_dates, chunk_count = _extract_url_entries(sub_root, sub_url, logger)
                all_snapshots.extend(entries)
                cache_fetched += 1
                if cache is not None:
                    update_chunk(cache, sub_url, sub_lastmod, chunk_dates, url_count=chunk_count)
            else:
                logger.warning("Unexpected root tag <%s> in %s; skipping.", sub_tag, sub_url)

        if cache is not None:
            logger.info(
                "Sitemap cache: skipped=%d fetched=%d (of %d sub-sitemaps)",
                cache_skipped, cache_fetched, len(sitemap_urls_with_lastmod),
            )
        return all_snapshots

    logger.warning("Unrecognized sitemap root tag <%s> at %s; treating as empty.", tag, sitemap_url)
    return []


def _discover_via_sitemaps(logger: logging.Logger,
                            start_iso: Optional[str] = None,
                            end_iso: Optional[str] = None,
                            cache: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Discover Examiner URLs via primary sitemap.xml; fall back to news-sitemap.xml
    if the primary yields nothing in the requested window."""
    logger.info("Washington Examiner: starting sitemap discovery at %s", WE_SITEMAP_PRIMARY)

    window: Optional[Tuple[str, str]] = None
    if start_iso and end_iso:
        window = (start_iso, end_iso)

    primary = _discover_from_sitemap_url(WE_SITEMAP_PRIMARY, logger, cache=cache, window=window)
    logger.info("Primary sitemap yielded %d candidates.", len(primary))

    if not primary or (start_iso and end_iso and not any(
        it.get("post_date") and within_window(it["post_date"], start_iso, end_iso)
        for it in primary
    )):
        logger.info("Primary sitemap empty or no in-window items; trying news sitemap %s",
                    WE_SITEMAP_NEWS)
        news = _discover_from_sitemap_url(WE_SITEMAP_NEWS, logger, cache=cache, window=window)
        logger.info("News sitemap yielded %d candidates.", len(news))
        # Merge primary + news; downstream dedupe handles duplicates.
        primary.extend(news)

    logger.info("Total %d candidate URLs discovered.", len(primary))
    return primary


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
    logger.info("Discovering Washington Examiner articles (sitemap mode)")

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

    snapshot_items = _discover_via_sitemaps(logger, start_iso=start, end_iso=end, cache=cache)

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

    p = argparse.ArgumentParser(description="Democracy Clock V4 — Washington Examiner harvester (SITEMAP)")
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
