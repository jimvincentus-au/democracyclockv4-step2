# getballotpedia_shadow_v4.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import re

# Project helpers / config (COPY mode: use existing helpers; no reinvention)
from config_v4 import ARTIFACTS_ROOT  # required by getweekevents_v4
import config_v4
from step2_helper_v4 import (
    setup_logger,
    build_session,
    http_get,
    write_json,
    create_artifact_paths,
    normalize_ws,
    within_window,
    canonicalize_url,
)

HARVESTER_ID = "ballotpedia_shadow"

# Fallback URL (V3 exact page) if config_v4 doesn't supply one
BP_SHADOW_URL_FALLBACK = (
    "https://ballotpedia.org/"
    "Supreme_Court_emergency_orders_related_to_the_Trump_administration,_2025"
)

# Caption substrings to positively identify the correct data-table
CAPTION_NEEDLES = [
    "Decided emergency docket applications",
    "second Trump administration",
]

DATE_MDY_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")

def _resolve_shadow_url(logger) -> str:
    # Prefer BP_SHADOW_URL (if provided), else DEFAULT_SHADOW_URL, else fallback (V3)
    url = getattr(config_v4, "BP_SHADOW_URL", None)
    if url:
        logger.debug("Shadow URL: using BP_SHADOW_URL=%s", url)
        return url
    url = getattr(config_v4, "DEFAULT_SHADOW_URL", None)
    if url:
        logger.debug("Shadow URL: using DEFAULT_SHADOW_URL=%s", url)
        return url
    logger.debug("Shadow URL: using FALLBACK (V3)=%s", BP_SHADOW_URL_FALLBACK)
    return BP_SHADOW_URL_FALLBACK

def _mdy_to_iso(s: str) -> str:
    """
    Convert 'MM/DD/YYYY' → 'YYYY-MM-DD'. Return '' if not parseable.
    """
    m = DATE_MDY_RE.match(s or "")
    if not m:
        return ""
    mm, dd, yyyy = m.groups()
    try:
        mm_i = int(mm)
        dd_i = int(dd)
        yyyy_i = int(yyyy)
        return f"{yyyy_i:04d}-{mm_i:02d}-{dd_i:02d}"
    except Exception:
        return ""

def _find_shadow_table(soup: BeautifulSoup, logger) -> Optional[Tuple[Any, List[str]]]:
    """
    Find the target <table> by inspecting <caption> text for our needles,
    then return (table, header_texts). If not found, return None.
    """
    candidates = soup.find_all("table")
    logger.debug("Shadow: scanning %d table(s) to find decisions grid", len(candidates))

    for idx, tbl in enumerate(candidates):
        # Try to get caption text
        cap = tbl.find("caption")
        cap_text = normalize_ws(cap.get_text(" ", strip=True)) if cap else ""
        # Soft match on caption
        if cap_text and all(x.lower() in cap_text.lower() for x in CAPTION_NEEDLES):
            # Pull header cells
            thead = tbl.find("thead")
            headers = []
            if thead:
                for th in thead.find_all("th"):
                    headers.append(normalize_ws(th.get_text(" ", strip=True)))
            logger.debug(
                "Shadow: MATCH table #%d (caption=%r) | headers=%s",
                idx, cap_text, headers or "[]"
            )
            return tbl, headers
        else:
            logger.debug(
                "Shadow: skip table #%d (caption=%r) — missing needles=%s",
                idx, cap_text, CAPTION_NEEDLES
            )

    logger.debug("Shadow: no suitable table discovered (no caption with needles).")
    return None

def _discover_shadow_rows(session, url: str, logger) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    COPY-mode discovery:
      - Fetch Ballotpedia shadow docket page
      - Find the 'Decided emergency docket applications ... second Trump administration' data table
      - For each <tr> in <tbody>:
          columns expected (by position):
            0: Name (contains on-page anchor <a href="#...">)
            1: Lower court
            2: Applicant
            3: Application date (MM/DD/YYYY)
            4: Docket number (external SCOTUS link)
            5: Decision date (MM/DD/YYYY)  ← we use this as 'post_date'
            6: Granted or denied?
            7: Dissents
        Build a full snapshot (pre-window). Filtering/dedupe is handled later.
    Returns: (snapshot_items, debug_rows)
      snapshot_items: list of dicts with fields the JSON writer expects
      debug_rows:     list with richer raw info (used only for RAW audit dump)
    """
    status, html = http_get(session, url, logger)
    if status != 200 or not html:
        logger.error("Failed to fetch shadow docket page (status=%s).", status)
        return [], []

    soup = BeautifulSoup(html, "html.parser")

    found = _find_shadow_table(soup, logger)
    if not found:
        logger.error("Shadow: decisions table not found.")
        return [], []

    table, headers = found
    tbody = table.find("tbody")
    if not tbody:
        logger.error("Shadow: table had no <tbody>.")
        return [], []

    rows = tbody.find_all("tr")
    logger.debug("Shadow: tbody rows discovered=%d", len(rows))

    snapshot: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    for i, tr in enumerate(rows, start=1):
        tds = tr.find_all("td")
        # Require at least 6 columns (we need 0,4,5)
        if len(tds) < 6:
            raw_line = normalize_ws(tr.get_text(" ", strip=True))
            logger.debug("Shadow: row #%d SKIPT reason=too_few_columns cols=%d raw=%r", i, len(tds), raw_line[:200])
            audit_rows.append({
                "row_index": i,
                "reason": "too_few_columns",
                "raw_line": raw_line
            })
            continue

        # Extract main pieces
        name_td = tds[0]
        docket_td = tds[4]
        decision_td = tds[5]

        # Title (case name) from first column
        a_name = name_td.find("a", href=True)
        title = normalize_ws(a_name.get_text(" ", strip=True)) if a_name else normalize_ws(name_td.get_text(" ", strip=True))

        # URL & canonical_url — prefer the on-page anchor if present
        frag = ""
        if a_name and (a_name.get("href") or "").startswith("#"):
            frag = a_name["href"].strip()
        # Construct canonical as page + fragment (if any)
        canonical = url + (frag if frag.startswith("#") else "")

        # Decision date → ISO
        post_iso = _mdy_to_iso(normalize_ws(decision_td.get_text(" ", strip=True)))

        # --- ADD: derive summary_url from the first link in the Name cell (td[0]) ---
        summary_url = ""
        a = tds[0].find("a", href=True)
        if a:
            href = (a.get("href") or "").strip()
            if href.startswith("#"):
                # local anchor on the same page
                summary_url = url + href  # 'url' is the page URL you're already using
            elif href:
                summary_url = canonicalize_url(href, base=url)

        # A docket link may exist (external); keep it inside raw_line for auditing
        docket_link = ""
        a_docket = docket_td.find("a", href=True)
        if a_docket:
            docket_link = a_docket["href"].strip()

        # Build raw_line (compact per-row text)
        raw_line = normalize_ws(tr.get_text(" ", strip=True))

        # Minimal entity record
        entity = {
            "source": "Ballotpedia",
            "doc_type": "shadow_docket",
            "title": title,
            "url": url,
            "canonical_url": canonical,
            "summary_url": summary_url,
            "summary": "",
            "summary_origin": "",
            "summary_timestamp": "",
            "post_date": post_iso,
            "raw_line": raw_line,
        }

        snapshot.append(entity)

        # richer audit record
        audit_rows.append({
            "row_index": i,
            "title": title,
            "canonical_url": canonical,
            "decision_date_raw": normalize_ws(decision_td.get_text(" ", strip=True)),
            "post_date_iso": post_iso,
            "docket_link": docket_link,
            "raw_line": raw_line,
        })

        logger.debug(
            "Shadow: row #%d FOUND title=%r decision_raw=%r post_iso=%r url=%r",
            i, title, decision_td.get_text(" ", strip=True), post_iso, canonical
        )

    return snapshot, audit_rows

def _window_and_dedupe(items: List[Dict[str, Any]], start_iso: str, end_iso: str, logger) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Apply date window (by 'post_date'), drop items with no title/url/date,
    then dedupe by canonical_url preserving first occurrence.
    For each item, emit KEPT/SKIPT with reason.
    """
    prelim: List[Dict[str, Any]] = []
    counts = {"outside": 0, "nodate": 0, "no_title": 0, "no_url": 0}

    logger.debug("Shadow: beginning date filter %s → %s (items=%d)", start_iso, end_iso, len(items))
    for idx, r in enumerate(items, start=1):
        title = (r.get("title") or "").strip()
        url = (r.get("canonical_url") or r.get("url") or "").strip()
        iso = (r.get("post_date") or "").strip()

        if not title:
            counts["no_title"] += 1
            logger.debug("Shadow: row #%d SKIPT reason=no_title", idx)
            continue
        if not url:
            counts["no_url"] += 1
            logger.debug("Shadow: row #%d SKIPT reason=no_url title=%r", idx, title)
            continue
        if not iso:
            counts["nodate"] += 1
            logger.debug("Shadow: row #%d SKIPT reason=no_date title=%r url=%r", idx, title, url)
            continue
        if not within_window(iso, start_iso, end_iso):
            counts["outside"] += 1
            logger.debug("Shadow: row #%d SKIPT reason=outside_window iso=%s title=%r url=%r", idx, iso, title, url)
            continue

        prelim.append(r)
        logger.debug("Shadow: row #%d KEPT iso=%s title=%r url=%r", idx, iso, title, url)

    # Dedupe by canonical_url preserving order
    windowed: List[Dict[str, Any]] = []
    seen: set = set()
    for r in prelim:
        k = r.get("canonical_url") or r.get("url") or ""
        if not k or k in seen:
            continue
        seen.add(k)
        windowed.append(r)

    return windowed, counts

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Ballotpedia Shadow Docket — COPY mode:
      RAW:      snapshot of ALL parsed table rows (pre-window)
      FILTERED: windowed, de-duplicated list of items (entity-level)
    Output entity format:
      {
        "source": "Ballotpedia",
        "doc_type": "shadow_docket",
        "title": "...",
        "url": "https://...#Fragment",
        "canonical_url": "https://...#Fragment",
        "post_date": "YYYY-MM-DD",
        "raw_line": "..."
      }
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)

    url = _resolve_shadow_url(logger)
    logger.info("Discovering Ballotpedia Shadow Docket (COPY mode): %s", url)

    snapshot_items, audit_rows = _discover_shadow_rows(sess, url, logger)

    # RAW write — include full snapshot & audit rows
    raw_payload = {
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "index_url": url,
        "parsed_total": len(snapshot_items),
        "rows_audit": audit_rows,
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # Window + dedupe
    windowed_items, counts = _window_and_dedupe(snapshot_items, start, end, logger)

    kept_after_filter = len(snapshot_items) - (counts["outside"] + counts["nodate"] + counts["no_title"] + counts["no_url"])
    kept_after_dedup = len(windowed_items)
    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | outside=%d nodate=%d no_title=%d no_url=%d",
        start, end, len(snapshot_items), kept_after_filter, kept_after_dedup,
        counts["outside"], counts["nodate"], counts["no_title"], counts["no_url"]
    )

    # FILTERED write
    filtered_payload = {
        "meta": {
            "source": HARVESTER_ID,
            "entity_type": "index_list_item",
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
        "window": {"start": start, "end": end},
        "count": len(windowed_items),
        "items": windowed_items,
    }
    write_json(filtered_path, filtered_payload)
    logger.info(
        "Wrote filtered entities: %s (count=%d)",
        filtered_path, len(windowed_items)
    )

    return {
        "source": HARVESTER_ID,
        "entity_count": len(windowed_items),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }