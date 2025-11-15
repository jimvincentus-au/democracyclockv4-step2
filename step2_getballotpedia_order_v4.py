#!/usr/bin/env python3
# MODE: Copy
# ORIGIN: getballotpedia_orders_v3.py (parser parity)
# INTENT: Use V3 logic to collect in-window index items; no detail fetch; no LLM.

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re
from datetime import datetime, timezone, date

from bs4 import BeautifulSoup  # pip install beautifulsoup4

# Shared config
from config_v4 import (
    ARTIFACTS_ROOT,
    BP_BASE,
    BP_URL_2025,
    BP_URL_PREFIX_ALLOW,   # ("/Executive_Order:", "/Proclamation:", "/Presidential_Memorandum:")
)

# Shared helpers
from step2_helper_v4 import (
    build_session,
    http_get,
    write_json,
    create_artifact_paths,
    setup_logger,
    canonicalize_url,
    within_window,
    normalize_ws,
    extract_iso_from_text,
)

# Month anchors look like: <span class="mw-headline" id="September_2025">
MONTH_RE = re.compile(r"^(January|February|March|April|May|June|July|August|September|October|November|December)_(\d{4})$")
HARVESTER_ID = "ballotpedia_orders"

# V3 used visible H2 labels
SECTION_MAP: Dict[str, str] = {
    "Executive orders": "executive_order",
    "Proclamations": "proclamation",
    "Presidential memoranda": "memorandum",
}


def _filter_window_and_dedupe(items, start_iso, end_iso, logger):
    kept = []
    stats = {"inside": 0, "before": 0, "after": 0, "nodate": 0}
    s = date.fromisoformat(start_iso)
    e = date.fromisoformat(end_iso)

    for idx, it in enumerate(items, 1):
        url = it.get("url", "")
        title = (it.get("title") or "")[:140]
        iso = (it.get("post_date") or "").strip()

        if not iso:
            stats["nodate"] += 1
            kept.append(it)
            logger.debug(" [%03d] KEEP (nodate)  | %s | %r", idx, url, title)
            continue

        try:
            d = date.fromisoformat(iso)
        except ValueError:
            # handle '2025-9-5' etc.
            parts = iso.split("-")
            if len(parts) == 3:
                y, m, d2 = parts
                d = date(int(y), int(m), int(d2))
            else:
                stats["nodate"] += 1
                kept.append(it)
                continue

        if d < s:
            stats["before"] += 1
            logger.debug(" [%03d] SKIP (before) | %s | %s | %r", idx, url, iso, title)
            continue
        if d > e:
            stats["after"] += 1
            logger.debug(" [%03d] SKIP (after)  | %s | %s | %r", idx, url, iso, title)
            continue

        stats["inside"] += 1
        kept.append(it)
        logger.debug(" [%03d] KEEP (inside) | %s | %s | %r", idx, url, iso, title)

    # Stable de-dupe by canonical_url (or url)
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for it in kept:
        k = it.get("canonical_url") or it.get("url")
        if k and k not in seen:
            seen.add(k)
            deduped.append(it)

    logger.info(
        "Window results: inside=%d before=%d after=%d nodate=%d → kept=%d → deduped=%d",
        stats["inside"], stats["before"], stats["after"], stats["nodate"], len(kept), len(deduped)
    )
    return deduped, stats

# ────────────────────────────────────────────────────────────────────────────────
# V3-parity section parsing: H2 (by text) → next UL → direct LI/A
# ────────────────────────────────────────────────────────────────────────────────

def _parse_section_v3(soup: BeautifulSoup, header_text: str, dtype: str, logger) -> List[Dict[str, Any]]:
    """
    COPY mode, robust traversal:
      - Find <span id=…> → parent <h2>
      - Collect ALL descendant <ul> nodes up to (but not including) the next <h2>
      - For each <li><a>, keep Ballotpedia doc-page prefixes
      - Pull parenthetical Month D, YYYY from the LI text
    """
    out: List[Dict[str, Any]] = []

    # Map V3 labels to exact span ids
    header_to_span_id = {
        "Executive orders": "Executive_orders_issued_by_Trump",
        "Presidential memoranda": "Memoranda_issued_by_Trump",
        "Proclamations": "Proclamations_issued_by_Trump",
    }
    span_id = header_to_span_id.get(header_text)
    span = soup.find("span", id=span_id) if span_id else None
    h2 = span.find_parent("h2") if span else None

    if not h2:
        logger.debug("[%s] H2 not found (span id %r)", header_text, span_id)
        return out

    # Slice the DOM: gather siblings after this H2 up to the next H2,
    # then search ALL descendant ULs within that slice.
    section_nodes: List[Any] = []
    sib = h2.next_sibling
    while sib is not None and getattr(sib, "name", None) != "h2":
        section_nodes.append(sib)
        sib = sib.next_sibling

    # Collect ALL ULs under the section slice (descendants, not just direct siblings)
    ul_nodes: List[Any] = []
    for node in section_nodes:
        # Some nodes are NavigableString; guard with getattr
        find_all = getattr(node, "find_all", None)
        if callable(find_all):
            ul_nodes.extend(node.find_all("ul"))

    logger.debug("[%s] Descendant ULs under section: %d", header_text, len(ul_nodes))

    allowed = BP_URL_PREFIX_ALLOW  # tuple of Ballotpedia doc prefixes

    total_li = 0
    kept_li = 0
    for ul in ul_nodes:
        lis = ul.find_all("li")  # allow nested <li> — some months wrap more deeply
        total_li += len(lis)
        for li in lis:
            a = li.find("a", href=True)
            if not a:
                continue
            href = (a["href"] or "").strip()
            if not any(href.startswith(p) for p in allowed):
                continue

            title = normalize_ws(a.get_text(" ", strip=True))
            raw_line = normalize_ws(li.get_text(" ", strip=True))
            iso = extract_iso_from_text(raw_line)  # parenthetical → ISO or ''

            url = canonicalize_url(href, base=BP_BASE)

            out.append({
                "source": "Ballotpedia",
                "doc_type": dtype,        # executive_order | proclamation | memorandum
                "title": title,
                "url": url,
                "canonical_url": url,
                "post_date": iso,         # '' if none
                "raw_line": raw_line,
            })
            kept_li += 1
            logger.debug("[%s] LI kept: %r | iso=%r | href=%r", header_text, title, iso, href)

    logger.debug("[%s] Parsed kept %d of %d <li>", header_text, kept_li, total_li)
    return out


def _discover_index_items_v3(session, start_iso: str, end_iso: str, logger):
    """
    UPDATED (2025-11): Ballotpedia changed the 2025 page to a month-based layout.

    We now:
      • scan ALL <h3> under #mw-content-text (top-level and inside collapsible blocks),
      • keep only those with <span.mw-headline id="<Month>_2025">,
      • find the month list as the FIRST <ul> after the <h3> (or inside an immediate wrapper <div>),
      • extract <li><a> items whose href starts with BP_URL_PREFIX_ALLOW.
    """
    logger.info("Discovering Ballotpedia index (month layout): %s", BP_URL_2025)
    status, html = http_get(session, BP_URL_2025, logger)
    if status != 200 or not html:
        logger.error("Index fetch failed (status=%s). Aborting discovery.", status)
        return [], []

    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("div", id="mw-content-text")
    if not root:
        logger.error("Could not find #mw-content-text container; page structure changed.")
        return [], []

    snapshot_items: List[Dict[str, Any]] = []
    allowed_prefixes = BP_URL_PREFIX_ALLOW

    def _first_month_ul_after(h3):
        """
        Return the first <ul> that belongs to this month block.
        Handles an intervening 'mobile-columns' or similar wrapper.
        """
        for sib in h3.next_siblings:
            # Skip whitespace / NavigableString
            name = getattr(sib, "name", None)
            if not name:
                continue
            if name == "ul":
                return sib
            if name == "div":
                # Some pages insert a columns wrapper between <h3> and <ul>
                ul = sib.find("ul", recursive=False) or sib.find("ul")
                if ul:
                    return ul
            # Stop if we hit the next month/section header
            if name in ("h2", "h3"):
                break
        return None

    def _collect_from_month_h3(h3) -> None:
        span = h3.find("span", class_="mw-headline")
        if not span:
            return
        hid = (span.get("id") or "").strip()
        if not MONTH_RE.match(hid):
            return

        ul = _first_month_ul_after(h3)
        if not ul:
            logger.debug("[month=%s] No UL found after H3", hid)
            return

        lis = ul.find_all("li", recursive=False) or ul.find_all("li")
        logger.debug("[month=%s] found %d <li>", hid, len(lis))

        for li in lis:
            a = li.find("a", href=True)
            if not a:
                continue
            href = a["href"].strip()
            if not any(href.startswith(p) for p in allowed_prefixes):
                # Not an Exec Order / Proclamation / Memorandum page
                continue

            title = normalize_ws(a.get_text(" ", strip=True))
            raw_line = normalize_ws(li.get_text(" ", strip=True))
            iso = extract_iso_from_text(raw_line)  # "(September 30, 2025)" → "2025-09-30"
            url = canonicalize_url(href, base=BP_BASE)

            if href.startswith("/Executive_Order:"):
                doc_type = "executive_order"
            elif href.startswith("/Presidential_Memorandum:"):
                doc_type = "memorandum"
            elif href.startswith("/Proclamation:"):
                doc_type = "proclamation"
            else:
                doc_type = "order"

            snapshot_items.append({
                "source": "Ballotpedia",
                "doc_type": doc_type,
                "title": title,
                "url": url,
                "canonical_url": url,
                "post_date": iso,
                "raw_line": raw_line,
            })
            logger.debug("[month=%s] COLLECT: %r | %r | %s", hid, title, iso, href)

    # 1) Month headers anywhere under #mw-content-text (covers visible & collapsible)
    for h3 in root.find_all("h3"):
        _collect_from_month_h3(h3)

    # ---- Window filter + stable de-duplication (reuse your existing logic) ----
    logger.debug("Beginning date filter (month layout): %s → %s (items=%d)", start_iso, end_iso, len(snapshot_items))

    prelim: List[Dict[str, Any]] = []
    outside, nodate = 0, 0
    for r in snapshot_items:
        iso = (r.get("post_date") or "").strip()
        if not iso:
            nodate += 1
            logger.debug("[FILTER] SKIP(no_date) title=%r raw=%r", r.get("title", ""), r.get("raw_line", ""))
            continue
        if not within_window(iso, start_iso, end_iso):
            outside += 1
            logger.debug("[FILTER] SKIP(outside_window) date=%s title=%r", iso, r.get("title", ""))
            continue
        prelim.append(r)

    # de-dupe by canonical_url
    windowed_items: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for r in prelim:
        k = r.get("canonical_url") or r.get("url") or ""
        if not k or k in seen_urls:
            continue
        seen_urls.add(k)
        windowed_items.append(r)

    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | outside=%d nodate=%d",
        start_iso, end_iso, len(snapshot_items), len(prelim), len(windowed_items), outside, nodate
    )

    by_type: Dict[str, int] = {}
    for r in windowed_items:
        t = r.get("doc_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
    logger.info("Filtered counts by type: %s", by_type)

    return windowed_items, snapshot_items


# ────────────────────────────────────────────────────────────────────────────────
# Public entrypoint (used by getweekevents_v4.py)
# ────────────────────────────────────────────────────────────────────────────────

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Step-1 harvester: V3-equivalent index collection.
      RAW:      snapshot of ALL parsed items (pre-window) by section
      FILTERED: windowed, de-duplicated list of items (index-level)
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)

    now_iso = datetime.now(timezone.utc).isoformat()

    # Parse everything from the index (pre-window snapshot) using COPY-mode discovery.
    # NOTE: _discover_index_items_v3 returns (deduped_windowed, snapshot_items).
    windowed_items, snapshot_items = _discover_index_items_v3(sess, start, end, logger)

    # Re-run the windowing with explicit, loud DEBUG so we can see exactly
    # how each item's date compares to the requested window (COPY-mode filter).
    filtered_items, win_stats = _filter_window_and_dedupe(snapshot_items, start, end, logger)

    # ---- Ensure new fields exist (non-destructive augmentation) ----
    # Orders/Memos/Proclamations pages don't have per-item summaries; keep these empty for now.
    # Also guarantee canonical_url exists (fallback to url).
    for coll in (snapshot_items, filtered_items):
        for it in coll:
            it.setdefault("canonical_url", it.get("url", "") or "")
            it.setdefault("summary_url", "")
            it.setdefault("summary", "")
            it.setdefault("summary_origin", "")
            it.setdefault("summary_timestamp", "")

    # Per-doctype rollup for filtered
    by_type: Dict[str, int] = {}
    for r in filtered_items:
        t = r.get("doc_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
    logger.info("Filtered counts by type: %s", by_type)

    # RAW write — include full snapshot for audit (pre-window)
    raw_payload = {
        "meta": {"generated_at": now_iso, "schema": "canonical_raw_v1"},
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "index_url": BP_URL_2025,
        "parsed_total": len(snapshot_items),
        "items_snapshot": [
            {
                "url": it.get("url", ""),
                "canonical_url": it.get("canonical_url", "") or it.get("url", ""),
                "title": it.get("title", ""),
                "post_date": it.get("post_date", ""),
                "doc_type": it.get("doc_type", ""),
                "summary_url": it.get("summary_url", ""),
                "summary": it.get("summary", ""),
                "summary_origin": it.get("summary_origin", ""),
                "summary_timestamp": it.get("summary_timestamp", ""),
                "raw_line": (it.get("raw_line", "") or "")[:500],
            }
            for it in snapshot_items
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED write — index entities that passed window+dedupe
    filtered_payload = {
        "meta": {"generated_at": now_iso, "schema": "canonical_filtered_v1"},
        "source": HARVESTER_ID,
        "entity_type": "index_list_item",
        "window": {"start": start, "end": end},
        "count": len(filtered_items),
        "entities": filtered_items,
        "window_stats": win_stats,  # helps debug before/after/nodate
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


# ────────────────────────────────────────────────────────────────────────────────
# Optional CLI for direct testing
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, json as _json
    ap = argparse.ArgumentParser(description="Ballotpedia Orders/Procs/Memos harvester (V3-parity, Step-1)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--artifacts-root", default=str(ARTIFACTS_ROOT))
    ap.add_argument("--level", default="INFO")
    args = ap.parse_args()

    logger = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(args.start, args.end, args.artifacts_root, level=args.level)
    print(_json.dumps(meta, indent=2))