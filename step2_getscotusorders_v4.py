

"""
Supreme Court Orders Step 2 Scraper v4

Purpose:
- Harvest official Supreme Court order lists from supremecourt.gov
- Support ordinary date-window calls: --start YYYY-MM-DD --end YYYY-MM-DD
- Support Democracy Clock week-window calls: --week 1 --weeks 75
- Write raw + filtered artifacts using the V4 Step 2 contract

Source:
- Official Supreme Court orders index pages:
  https://www.supremecourt.gov/orders/ordersofthecourt/24
  https://www.supremecourt.gov/orders/ordersofthecourt/25

Notes:
- This is the canonical official-orders layer. Use SCOTUSblog separately as Tier B commentary/discovery.
- The Court publishes order lists as linked PDFs. This harvester records order-list metadata and PDF URLs; it does not OCR or parse individual docket entries inside the PDFs.
"""

from __future__ import annotations

import html
import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

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
HARVESTER_ID = "scotusorders"
SOURCE_NAME = "Supreme Court of the United States"
__all__ = ["run_harvester"]

SCOTUS_BASE = "https://www.supremecourt.gov"
ORDERS_PATH = "/orders/ordersofthecourt/{term}"
DEMOCRACY_CLOCK_WEEK1_START = date(2025, 1, 20)

SCOTUS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DATE_FORMATS = (
    "%m/%d/%y",      # 6/27/25
    "%m/%d/%Y",     # 6/27/2025
    "%b %d, %Y",    # Jun 27, 2025
    "%B %d, %Y",    # June 27, 2025
    "%Y-%m-%d",     # 2025-06-27
)

ORDER_TYPE_PATTERNS = [
    ("orders_of_the_court", re.compile(r"\border(s)?\b", re.I)),
    ("miscellaneous_order", re.compile(r"\bmiscellaneous\b", re.I)),
    ("list_of_orders", re.compile(r"\blist\b", re.I)),
    ("journal", re.compile(r"\bjournal\b", re.I)),
]


def _clean_text(value: str) -> str:
    """Normalize whitespace and HTML entities."""
    if not value:
        return ""
    return " ".join(html.unescape(value).replace("\xa0", " ").split())



def _parse_date_to_iso(value: str) -> str:
    """Convert Supreme Court date strings to YYYY-MM-DD."""
    value = _clean_text(value)
    if not value:
        return ""

    iso_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", value)
    if iso_match:
        return iso_match.group(1)

    value = re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", value, flags=re.I)

    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(value, fmt).date()
            return parsed.isoformat()
        except ValueError:
            continue

    embedded = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", value)
    if embedded:
        return _parse_date_to_iso(embedded.group(0))

    month_date = re.search(
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? \d{1,2}, 20\d{2}\b",
        value,
        flags=re.I,
    )
    if month_date:
        return _parse_date_to_iso(month_date.group(0))

    return ""


# --- New function to parse order date from PDF URL ---
def _parse_order_date_from_url(pdf_url: str) -> str:
    """Parse SCOTUS order dates encoded in courtorders PDF filenames.

    Examples:
    - /orders/courtorders/012725zr_5h26.pdf  -> 2025-01-27
    - /orders/courtorders/063026zor_3f14.pdf -> 2026-06-30
    - /orders/courtorders/091225.zr_7l48.pdf -> 2025-09-12
    """
    filename = pdf_url.rsplit("/", 1)[-1]
    match = re.match(r"^(\d{2})(\d{2})(\d{2})", filename)
    if not match:
        return ""

    month_s, day_s, year_s = match.groups()
    year = 2000 + int(year_s)
    month = int(month_s)
    day = int(day_s)

    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return ""


def _term_years_for_window(start: str, end: str) -> List[str]:
    """Return SCOTUS term suffixes needed for a date window.

    Supreme Court terms start in October. A date in Jan-Sep belongs to the previous
    term. A date in Oct-Dec belongs to that calendar year's term.
    """
    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end, "%Y-%m-%d").date()

    terms = set()
    probe = start_d
    while probe <= end_d:
        term_year = probe.year if probe.month >= 10 else probe.year - 1
        terms.add(f"{term_year % 100:02d}")
        probe += timedelta(days=31)

    for d in (start_d, end_d):
        term_year = d.year if d.month >= 10 else d.year - 1
        terms.add(f"{term_year % 100:02d}")

    return sorted(terms)


def _first_nonempty(values: Iterable[str]) -> str:
    for value in values:
        clean = _clean_text(value)
        if clean:
            return clean
    return ""


def _extract_pdf_links_from_row(row, page_url: str) -> List[Tuple[str, str]]:
    """Return PDF links from a table row as (label, absolute_url)."""
    links: List[Tuple[str, str]] = []
    for a in row.find_all("a", href=True):
        href_attr = a.get("href")
        if isinstance(href_attr, list):
            href = str(href_attr[0]) if href_attr else ""
        else:
            href = str(href_attr or "")
        if ".pdf" not in href.lower():
            continue
        absolute = urljoin(page_url, href)
        if "/orders/courtorders/" not in absolute:
            continue
        label = _clean_text(a.get_text(" ", strip=True))
        links.append((label, absolute))
    return links


def _guess_order_type(text: str, pdf_url: str) -> str:
    """Classify the order-list item at document level."""
    combined = f"{text} {pdf_url}"
    lower = combined.lower()

    if "misc" in lower:
        return "miscellaneous_order"
    if "order" in lower or "ord" in lower:
        return "orders_of_the_court"
    if "journal" in lower:
        return "journal"

    for order_type, pattern in ORDER_TYPE_PATTERNS:
        if pattern.search(combined):
            return order_type

    return "order_list"


def _guess_docket_numbers(text: str) -> List[str]:
    """Extract docket-like numbers if visible on the index page."""
    patterns = [
        r"\b\d{2}-\d{1,6}\b",
        r"\b\d{1,2}[A-Z][A-Z]?\d{1,4}\b",
        r"\bOrig\.\s*\d+\b",
    ]
    found: List[str] = []
    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.I):
            value = _clean_text(m.group(0))
            if value not in found:
                found.append(value)
    return found


def _make_title(post_date: str, order_type: str, label: str) -> str:
    """Construct a stable document-level title."""
    friendly_type = {
        "orders_of_the_court": "Orders of the Court",
        "miscellaneous_order": "Miscellaneous Order",
        "list_of_orders": "Order List",
        "journal": "Journal",
        "order_list": "Order List",
    }.get(order_type, "Order List")

    if label and label.lower() not in {"pdf", "orders", "order", "order list"}:
        if post_date:
            return f"{friendly_type}: {label} ({post_date})"
        return f"{friendly_type}: {label}"

    if post_date:
        return f"{friendly_type} ({post_date})"
    return friendly_type


def _snapshot_from_table_row(row, term: str, page_url: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Normalize one official orders table row.

    A row may contain more than one linked PDF. Keep one Step 2 entity per PDF because
    the linked document is the canonical official record.
    """
    cells = [_clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
    row_text = _clean_text(row.get_text(" ", strip=True))

    if not cells or not row_text:
        return []

    pdf_links = _extract_pdf_links_from_row(row, page_url)
    if not pdf_links:
        return []

    post_date = _first_nonempty(_parse_date_to_iso(cell) for cell in cells)
    if not post_date:
        post_date = _parse_date_to_iso(row_text)

    # Removed skip if no post_date. Instead, allow fallback to PDF URL parsing per item.

    docket_numbers = _guess_docket_numbers(row_text)
    snapshots: List[Dict[str, Any]] = []

    for label, pdf_url in pdf_links:
        item_date = post_date or _parse_order_date_from_url(pdf_url)
        if not item_date:
            logger.warning("Skipping orders PDF without parseable date: %s", pdf_url)
            continue
        order_type = _guess_order_type(f"{row_text} {label}", pdf_url)
        # title = _make_title(post_date, order_type, label)  # <-- remove this line per instructions

        snapshots.append({
            "source_key": HARVESTER_ID,
            "source": SOURCE_NAME,
            "doc_type": "supreme_court_order",
            "title": _make_title(item_date, order_type, label),
            "url": pdf_url,
            "canonical_url": pdf_url,
            "summary_url": "",
            "summary": "",
            "summary_origin": "",
            "summary_timestamp": "",
            "post_date": item_date,
            "raw_line": f"[orders:{term}] {pdf_url}",
            "section": "orders_of_the_court",
            "term": term,
            "order_type": order_type,
            "order_date": item_date,
            "docket_numbers": docket_numbers,
            "pdf_url": pdf_url,
            "pdf_links": [
                {"label": item_label, "url": item_url}
                for item_label, item_url in pdf_links
            ],
            "row_text": row_text,
            "discovery_url": page_url,
        })

    return snapshots


def _parse_orders_page(page_html: str, term: str, page_url: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Parse one official orders index page."""
    soup = BeautifulSoup(page_html, "html.parser")
    items: List[Dict[str, Any]] = []

    rows = soup.find_all("tr")
    logger.info("Term %s orders page has %d table rows", term, len(rows))

    for row in rows:
        snapshots = _snapshot_from_table_row(row, term, page_url, logger)
        items.extend(snapshots)

    if not items:
        logger.warning("No table-row order items parsed for term %s; trying direct PDF-link fallback", term)
        for a in soup.find_all("a", href=True):
            href_attr = a.get("href")
            if isinstance(href_attr, list):
                href = str(href_attr[0]) if href_attr else ""
            else:
                href = str(href_attr or "")
            if ".pdf" not in href.lower():
                continue
            pdf_url = urljoin(page_url, href)
            if "/orders/courtorders/" not in pdf_url:
                continue
            label = _clean_text(a.get_text(" ", strip=True)) or pdf_url.rsplit("/", 1)[-1]
            surrounding_text = _clean_text(a.parent.get_text(" ", strip=True) if a.parent else label)
            post_date = _parse_date_to_iso(surrounding_text) or _parse_order_date_from_url(pdf_url)
            if not post_date:
                logger.warning("Skipping direct PDF fallback without date: %s", pdf_url)
                continue
            order_type = _guess_order_type(f"{surrounding_text} {label}", pdf_url)
            title = _make_title(post_date, order_type, label)
            items.append({
                "source_key": HARVESTER_ID,
                "source": SOURCE_NAME,
                "doc_type": "supreme_court_order",
                "title": title,
                "url": pdf_url,
                "canonical_url": pdf_url,
                "summary_url": "",
                "summary": "",
                "summary_origin": "",
                "summary_timestamp": "",
                "post_date": post_date,
                "raw_line": f"[orders:{term}] {pdf_url}",
                "section": "orders_of_the_court",
                "term": term,
                "order_type": order_type,
                "order_date": post_date,
                "docket_numbers": _guess_docket_numbers(surrounding_text),
                "pdf_url": pdf_url,
                "pdf_links": [{"label": label, "url": pdf_url}],
                "row_text": surrounding_text,
                "discovery_url": page_url,
            })

    dated_values = sorted({it.get("post_date", "") for it in items if it.get("post_date")})
    date_span = f"{dated_values[0]} → {dated_values[-1]}" if dated_values else "no dated items"
    logger.info("Parsed %d order-list items for term %s date_span=%s", len(items), term, date_span)
    return items


def _fetch_term(term: str, session: requests.Session, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Fetch and parse one SCOTUS term orders page."""
    page_url = urljoin(SCOTUS_BASE, ORDERS_PATH.format(term=term))
    logger.info("Fetching SCOTUS orders term %s: %s", term, page_url)

    try:
        resp = session.get(page_url, headers=SCOTUS_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as ex:
        logger.error("Failed to fetch SCOTUS orders term %s: %s", term, ex)
        return []

    return _parse_orders_page(resp.text, term, page_url, logger)


def _discover_scotus_orders(
    session: requests.Session,
    logger: logging.Logger,
    start: str,
    end: str,
) -> List[Dict[str, Any]]:
    """Discover official SCOTUS order-list PDFs across required term years."""
    terms = _term_years_for_window(start, end)
    logger.info("SCOTUS orders discovery terms for %s → %s: %s", start, end, ", ".join(terms))

    all_items: List[Dict[str, Any]] = []
    for term in terms:
        all_items.extend(_fetch_term(term, session, logger))

    logger.info("Total %d SCOTUS order snapshots discovered before filtering", len(all_items))
    return all_items


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

    active_session = session or build_session()

    logger.info("Session ready. Harvesting SCOTUS orders %s → %s", start, end)

    snapshot_items = _discover_scotus_orders(active_session, logger, start, end)

    total_discovered = len(snapshot_items)
    total_in_window = sum(
        1
        for it in snapshot_items
        if it.get("post_date") and within_window(str(it["post_date"]), start, end)
    )
    logger.info(
        "Discovered %d SCOTUS order snapshots total; %d within window %s → %s",
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
            "source_name": SOURCE_NAME,
            "base_url": SCOTUS_BASE,
            "orders_path": ORDERS_PATH,
            "terms": _term_years_for_window(start, end),
            "week1_start": DEMOCRACY_CLOCK_WEEK1_START.isoformat(),
        },
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    filtered_payload = {
        "schema": "filtered.v4",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "item_type": "supreme_court_order",
        "items_count": len(filtered_items),
        "items": filtered_items,
        "entity_type": "supreme_court_order",
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

    p = argparse.ArgumentParser(description="Democracy Clock V4 — official SCOTUS orders harvester")
    p.add_argument("--start", help="start date (YYYY-MM-DD)")
    p.add_argument("--end", help="end date (YYYY-MM-DD)")
    p.add_argument("--week", type=int, help="Democracy Clock start week number; Week 1 starts 2025-01-20")
    p.add_argument("--weeks", type=int, default=1, help="number of weeks to harvest when --week is used")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    start_arg, end_arg = _resolve_cli_window(args)

    meta = run_harvester(
        start=start_arg,
        end=end_arg,
        artifacts_root=args.artifacts,
        level=args.level,
    )
    print(meta)