# getdemocracydocket_v4.py
from __future__ import annotations

import email.utils as email_utils
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

# V4 infra
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    build_session,
    create_artifact_paths,
    http_get,
    write_json,
    normalize_ws,
    canonicalize_url,
    within_window,
)

HARVESTER_ID = "democracydocket"

__all__ = ["run_harvester"]

# COPY mode: crawl WordPress feeds (fast, stable, matches V3 behavior)
DD_BASE = "https://www.democracydocket.com"
DD_SECTIONS = {
    "news": f"{DD_BASE}/news/feed/",
    "updates": f"{DD_BASE}/updates/feed/",
    "analysis": f"{DD_BASE}/analysis/feed/",
}

# safety caps so we never loop forever on feeds
MAX_PAGES = 200
MAX_ITEMS_PER_PAGE_ALERT = 200  # warn if a feed page looks abnormally huge


def _iso_from_rfc822(pubdate_text: str) -> str:
    """
    Accepts either RFC 822 (RSS pubDate) or ISO-8601 (Atom) strings and returns
    date-only ISO (YYYY-MM-DD). Returns '' on failure.
    """
    if not pubdate_text:
        return ""
    txt = pubdate_text.strip()
    # 1) Try RFC 822 (typical RSS pubDate)
    try:
        dt = email_utils.parsedate_to_datetime(txt)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).date().isoformat()
    except Exception:
        pass
    # 2) Try ISO-8601 (typical Atom updated/published)
    try:
        t = txt.replace("Z", "+00:00")
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date().isoformat()
    except Exception:
        pass
    # 3) Fallback: bare YYYY-MM-DD inside the string
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", txt)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def _extract_feed_items(html: str, logger) -> List[Dict[str, str]]:
    """
    Parse a WordPress RSS/Atom feed page; return list of {title, link, iso, raw_date}.
    Uses BeautifulSoup 'xml' parser with safe accessors (no .findtext()).
    """
    out: List[Dict[str, str]] = []
    soup = BeautifulSoup(html, "xml")

    def _txt(tag, name: str) -> str:
        if not tag:
            return ""
        node = tag.find(name)
        return normalize_ws(node.get_text(strip=True)) if node else ""

    # RSS <item>
    items = soup.find_all("item")
    if items:
        for it in items:
            title = _txt(it, "title")
            # RSS often has a simple <link>http…</link>
            link_node = it.find("link")
            link = (link_node.get_text(strip=True) if link_node else "").strip()
            raw_date = (_txt(it, "pubDate") or _txt(it, "updated") or _txt(it, "published")).strip()
            iso = _iso_from_rfc822(raw_date)
            out.append({"title": title, "link": link, "iso": iso, "raw_date": raw_date})
        return out

    # Atom <entry>
    entries = soup.find_all("entry")
    for e in entries:
        title = _txt(e, "title")
        link_tag = e.find("link", href=True)
        link = (link_tag.get("href") or "").strip() if link_tag else ""
        raw_date = (_txt(e, "updated") or _txt(e, "published")).strip()
        iso = _iso_from_rfc822(raw_date)
        out.append({"title": title, "link": link, "iso": iso, "raw_date": raw_date})

    return out


def _discover_via_feeds_COPY_mode(session, start_iso: str, end_iso: str, logger):
    """
    COPY-mode discovery via section feeds (mirrors V3’s reliable approach):
      - For each section feed (/feed/), paginate with ?paged=N
      - Parse <item> (or <entry>) for title, link, and pubDate
      - Build snapshot entities (pre-window); window filter + dedupe handled later
    Returns: (snapshot_items, audit_rows)
    """
    snapshot: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    for section, feed_base in DD_SECTIONS.items():
        page = 1
        stagnation = 0
        last_page_url = ""
        logger.debug("DD FEED [%s] start feed crawl base=%s", section, feed_base)

        while page <= MAX_PAGES:
            feed_url = feed_base if page == 1 else (feed_base.rstrip("/") + f"/?paged={page}")
            status, html = http_get(session, feed_url, logger)
            logger.debug("DD FEED [%s] page=%d url=%s status=%s", section, page, feed_url, status)

            if status != 200 or not html:
                logger.debug("DD FEED [%s] page=%d stop: non-200 or empty html", section, page)
                break

            if feed_url == last_page_url:
                stagnation += 1
                logger.debug("DD FEED [%s] page=%d stagnation=%d (same URL repeated)", section, page, stagnation)
                if stagnation >= 3:
                    logger.debug("DD FEED [%s] stop: max stagnation reached", section)
                    break
            else:
                stagnation = 0
                last_page_url = feed_url

            items = _extract_feed_items(html, logger)
            logger.debug("DD FEED [%s] page=%d items_found=%d", section, page, len(items))
            if len(items) > MAX_ITEMS_PER_PAGE_ALERT:
                logger.debug("DD FEED [%s] page=%d WARNING unusually large feed page (%d items)", section, page, len(items))

            if not items:
                logger.debug("DD FEED [%s] page=%d stop: no items on page", section, page)
                break

            # Build snapshot items directly from feed; canonical_url = article link
            for idx, it in enumerate(items, 1):
                title = normalize_ws(it.get("title") or "")
                link = canonicalize_url(it.get("link") or "", base=DD_BASE)
                iso = (it.get("iso") or "").strip()
                raw_date = it.get("raw_date") or ""

                if not link:
                    logger.debug("DD FEED [%s] page=%d item#%d SKIPT reason=no_link title=%r", section, page, idx, title)
                    continue

                raw_line = f"[{section}] {title} ({iso or raw_date})"
                entity = {
                    "source": "Democracy Docket",
                    "doc_type": "news_article",
                    "title": title,
                    "url": link,                 # use permalink as both
                    "canonical_url": link,       # stable de-dupe key
                    "summary_url": "",           # no separate summary anchor on DD
                    "summary": "",               # enrichment later
                    "summary_origin": "",
                    "summary_timestamp": "",
                    "post_date": iso,            # date-only ISO (YYYY-MM-DD); may be ''
                    "raw_line": raw_line,
                }
                snapshot.append(entity)

                audit_rows.append({
                    "section": section,
                    "page": page,
                    "title": title,
                    "link": link,
                    "post_date": iso,
                    "raw_date": raw_date,
                    "status": "parsed",
                })

                logger.debug(
                    "DD FEED [%s] page=%d item#%d title=%r iso=%s link=%s",
                    section, page, idx, title, iso, link
                )

            # Heuristic early stop: if *all* items on this page are clearly older than start,
            # next pages will only be older. We can stop crawling this section.
            older = 0
            dated = 0
            for it in items:
                iso = (it.get("iso") or "").strip()
                if iso:
                    dated += 1
                    if not within_window(iso, start_iso, end_iso) and iso < start_iso:
                        older += 1
            if dated and older == dated:
                logger.debug("DD FEED [%s] page=%d stop: all %d dated items older than window start=%s",
                             section, page, dated, start_iso)
                break

            page += 1

    logger.debug("DD FEED snapshot total items=%d", len(snapshot))
    return snapshot, audit_rows


def _filter_window_and_dedupe(snapshot_items: List[Dict[str, Any]], start_iso: str, end_iso: str, logger):
    """
    Window filter + stable dedupe by canonical_url with detailed DEBUG.
    """
    kept_pre_dedupe: List[Dict[str, Any]] = []
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_title": 0, "no_url": 0}

    for it in snapshot_items:
        title = (it.get("title") or "").strip()
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        iso = (it.get("post_date") or "").strip()

        reason = None
        if not title:
            stats["no_title"] += 1
            reason = "no_title"
        elif not url:
            stats["no_url"] += 1
            reason = "no_url"
        elif not iso:
            stats["nodate"] += 1
            reason = "nodate"
        elif not within_window(iso, start_iso, end_iso):
            stats["outside"] += 1
            reason = "outside"

        if reason:
            logger.debug("Window: %s SKIPT reason=%s | title=%r url=%r", iso or "''", reason, title, url)
            continue

        stats["inside"] += 1
        logger.debug("Window: %s KEPT | title=%r url=%r", iso, title, url)
        kept_pre_dedupe.append(it)

    # De-dup by canonical_url preserving order
    seen = set()
    deduped: List[Dict[str, Any]] = []
    dups = 0
    for r in kept_pre_dedupe:
        k = r.get("canonical_url") or r.get("url") or ""
        if not k or k in seen:
            dups += 1
            logger.debug("Dedupe: SKIPT duplicate canonical=%r", k)
            continue
        seen.add(k)
        deduped.append(r)

    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | outside=%d nodate=%d no_title=%d no_url=%d dupes=%d",
        start_iso, end_iso, len(snapshot_items), len(kept_pre_dedupe), len(deduped),
        stats["outside"], stats["nodate"], stats["no_title"], stats["no_url"], dups
    )
    return deduped, stats


def _fetch_article_body(url: str, session, logger) -> str:
    """
    Best-effort fetch of the Democracy Docket article HTML, stripped to readable text.
    We keep this here (harvester) so builders always receive body_text.
    """
    if not url:
        return ""
    try:
        status, html = http_get(session, url, logger)
        if status != 200 or not html:
            logger.debug("DD BODY: SKIPT %s → %s", url, status)
            return ""
        soup = BeautifulSoup(html, "html.parser")

        candidates = [
            soup.find("article"),
            soup.find("div", class_="entry-content"),
            soup.find("div", class_="post-content"),
        ]
        for c in candidates:
            if c:
                text = normalize_ws(c.get_text(" ", strip=True))
                if text:
                    return text[:12000]

        text = normalize_ws(soup.get_text(" ", strip=True))
        return text[:12000]
    except Exception as e:
        logger.debug("DD BODY: error fetching %s: %s", url, e)
        return ""


def run_harvester(start: str, end: str, artifacts_root: str | Path = ARTIFACTS_ROOT, level: str = "INFO", log_path: Optional[str] = None, session=None) -> Dict[str, Any]:
    """
    Step-1 harvester (COPY-mode via feeds):
      RAW:      snapshot of ALL parsed items (pre-window) across sections
      FILTERED: windowed, de-duplicated list of article entities
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Democracy Docket (COPY mode via feeds): sections=%s", ",".join(DD_SECTIONS.keys()))

    # Pre-window snapshot (COPY-mode via feeds)
    snapshot_items, audit_rows = _discover_via_feeds_COPY_mode(sess, start, end, logger)

    # Window + dedupe with loud DEBUG
    filtered_items, win_stats = _filter_window_and_dedupe(snapshot_items, start, end, logger)

    # Enrich filtered items with article body so builders/LLM have real text
    for it in filtered_items:
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        body = _fetch_article_body(url, sess, logger)
        it["body_text"] = body  # always present, may be ''

    # Per-section quick rollup (filtered)
    by_section: Dict[str, int] = {}
    for r in filtered_items:
        path = r.get("canonical_url", "")
        sec = "unknown"
        if "/news/" in path:
            sec = "news"
        elif "/updates/" in path:
            sec = "updates"
        elif "/analysis/" in path:
            sec = "analysis"
        by_section[sec] = by_section.get(sec, 0) + 1
    logger.info("Filtered counts by section: %s", by_section)

    # RAW write — full snapshot for audit (pre-window)
    raw_payload = {
        "schema": "raw.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "parsed_total": len(snapshot_items),
        "items_snapshot": snapshot_items,
        "sections": DD_SECTIONS,
        "audit": audit_rows,  # crawl trace
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED write — entities that passed window+dedupe
    filtered_payload = {
        "schema": "filtered.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "item_type": "news_article",
        "items_count": len(filtered_items),
        "items": filtered_items,
        # Back-compat fields
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


# Optional CLI hook (handy for isolated testing)
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — Democracy Docket harvester (COPY via feeds)")
    p.add_argument("--start", required=True, help="start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="end date (YYYY-MM-DD)")
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