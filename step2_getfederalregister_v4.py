# getfederalregister_v4.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from datetime import datetime

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

HARVESTER_ID = "federalregister"

__all__ = ["run_harvester"]

# Federal Register API (no key required)
FR_API_BASE = "https://www.federalregister.gov/api/v1/documents.json"


# Democracy-affecting agencies and allowed types for Tier A
TIER_A_FILTERS: list[tuple[str, list[str]]] = [
    ("department-of-justice", ["RULE", "NOTICE"]),
    ("federal-bureau-of-investigation", ["NOTICE"]),
    ("drug-enforcement-administration", ["RULE", "NOTICE"]),

    ("department-of-homeland-security", ["RULE", "NOTICE"]),
    ("customs-and-border-protection", ["RULE", "NOTICE"]),
    ("immigration-and-customs-enforcement", ["RULE", "NOTICE"]),
    ("transportation-security-administration", ["RULE", "NOTICE"]),

    ("federal-election-commission", ["RULE", "NOTICE"]),
    ("election-assistance-commission", ["NOTICE"]),
    ("census-bureau", ["NOTICE"]),
    ("office-of-management-and-budget", ["NOTICE"]),

    ("department-of-education", ["RULE", "NOTICE"]),
    ("department-of-health-and-human-services", ["RULE", "NOTICE"]),
    ("department-of-housing-and-urban-development", ["RULE", "NOTICE"]),
    ("equal-employment-opportunity-commission", ["RULE", "NOTICE"]),

    ("national-archives-and-records-administration", ["RULE", "NOTICE"]),
    ("office-of-government-ethics", ["RULE", "NOTICE"]),
    ("general-services-administration", ["RULE", "NOTICE"]),
    ("federal-communications-commission", ["RULE", "NOTICE"]),

    ("department-of-labor", ["RULE", "NOTICE"]),
    ("occupational-safety-and-health-administration", ["RULE", "NOTICE"]),
    ("national-labor-relations-board", ["RULE", "NOTICE"]),

    ("environmental-protection-agency", ["RULE", "NOTICE"]),
    ("centers-for-disease-control-and-prevention", ["NOTICE"]),
    ("food-and-drug-administration", ["RULE", "NOTICE"]),
]

# EXACT slugs the API expects for presidential sub-types
FR_PRES_SUBTYPES = ["executive_order", "proclamation", "memorandum", "presidential_order"]

# Keep payloads compact but useful
FR_FIELDS = [
    "title",
    "html_url",
    "publication_date",
    "type",
    "presidential_document_type",
    "agency_names",
    "document_number",
]

PER_PAGE = 1000  # FR max is 1000

def _log_http_cmd(session, url: str, logger):
    """
    Emit a copy-pasteable curl command for debugging.
    """
    ua = ""
    accept = ""
    try:
        ua = session.headers.get("User-Agent", "")
        accept = session.headers.get("Accept", "")
    except Exception:
        pass
    parts = [f"curl -i -sS -X GET '{url}'"]
    if ua:
        parts.append(f"-H 'User-Agent: {ua}'")
    if accept:
        parts.append(f"-H 'Accept: {accept}'")
    logger.debug("HTTP CMD: %s", " ".join(parts))

def _fr_build_params(start_iso: str, end_iso: str, page: int) -> List[Tuple[str, str]]:
    """
    Build FR params as a list of (key, value) pairs so repeated keys (fields[], conditions[...][])
    are preserved exactly as FR expects.
    """
    params: List[Tuple[str, str]] = [
        ("per_page", str(PER_PAGE)),
        ("order", "newest"),
        ("page", str(page)),
        ("conditions[type][]", "PRESDOCU"),
        ("conditions[publication_date][gte]", start_iso),
        ("conditions[publication_date][lte]", end_iso),
    ]
    # presidential sub-type filters — arrayed
    for sub in FR_PRES_SUBTYPES:
        params.append(("conditions[presidential_document_type][]", sub))
    # explicit fields to reduce payload
    for f in FR_FIELDS:
        params.append(("fields[]", f))
    return params


def _fetch_fr_page(session, start_iso: str, end_iso: str, page: int, logger):
    """
    Fetch one page of Federal Register Presidential Documents between start_iso and end_iso.

    - Corrects use of `presidential_document_type` (as a condition, not a field)
    - Adds all known subtypes: executive_order, memorandum, proclamation, presidential_order
    - Adds full pre- and post-encoding debug output
    """
    params = [
        ("per_page", "1000"),
        ("order", "newest"),
        ("page", str(page)),

        # Filter for Presidential Documents
        ("conditions[type][]", "PRESDOCU"),

        # Date range
        ("conditions[publication_date][gte]", start_iso),
        ("conditions[publication_date][lte]", end_iso),

        # Presidential document subtypes
        ("conditions[presidential_document_type][]", "executive_order"),
        ("conditions[presidential_document_type][]", "memorandum"),
        ("conditions[presidential_document_type][]", "proclamation"),
        ("conditions[presidential_document_type][]", "presidential_order"),

        # Fields to return
        ("fields[]", "title"),
        ("fields[]", "html_url"),
        ("fields[]", "publication_date"),
        ("fields[]", "type"),
        ("fields[]", "agency_names"),
        ("fields[]", "document_number"),
        ("fields[]", "presidential_document_type"),
    ]

    # Log the unencoded query parameters
    unencoded_qs = "&".join(f"{k}={v}" for k, v in params)
    logger.debug("FR PRE-ENCODE: %s?%s", FR_API_BASE, unencoded_qs)

    # Encode for actual transmission
    qs = urlencode(params, doseq=True)
    url = f"{FR_API_BASE}?{qs}"

    # Log the encoded final URL
    logger.debug("FR ENCODED URL: %s", url)

    # Perform the HTTP GET request
    _log_http_cmd(session, url, logger)
    status, text = http_get(session, url, logger)
    logger.debug("FR RESPONSE: status=%s length=%s", status, len(text or ""))
    if status != 200:
        logger.debug("FR ERROR BODY (status=%s): %s", status, text)
    if status != 200 or not text:
        return status, None

    try:
        data = json.loads(text)
        logger.debug("FR PARSE OK page=%s total_results=%s", page, len(data.get("results", [])))
        return status, data
    except Exception as e:
        logger.error("FR JSON decode error (page=%s): %s", page, e)
        return 599, None


def _discover_presidential_docs(session, start_iso: str, end_iso: str, logger):
    """
    COPY-style discovery:
      - Pull Presidential Documents in the window (publication_date gte/lte)
      - Limit to sub-types executive_order, proclamation, memorandum via server-side filters
      - Build pre-window snapshot (we still keep window filter + dedupe pass for consistency + stats)
    """
    snapshot: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    # First page to discover total_pages
    status, data = _fetch_fr_page(session, start_iso, end_iso, page=1, logger=logger)
    if status != 200 or not data:
        logger.error("FR: initial fetch failed (status=%s).", status)
        return snapshot, audit_rows

    total_pages = int(data.get("total_pages", 0) or 0)
    results = data.get("results", []) or []

    logger.debug("FR: total_pages=%s", total_pages)

    # Process page 1
    for doc in results:
        _append_doc_to_snapshot(doc, snapshot, audit_rows, logger)

    # Process remaining pages (if any)
    for page in range(2, total_pages + 1):
        status, pdata = _fetch_fr_page(session, start_iso, end_iso, page=page, logger=logger)
        if status != 200 or not pdata:
            logger.debug("FR: stop on page=%s due to non-200/empty.", page)
            break
        for doc in (pdata.get("results", []) or []):
            _append_doc_to_snapshot(doc, snapshot, audit_rows, logger)

    logger.debug("FR snapshot total items=%d", len(snapshot))
    return snapshot, audit_rows

def _discover_tier_a_actions(session, start_iso: str, end_iso: str, logger):
    """
    Tier A: final, enforceable agency RULE/NOTICE actions (no PRORULE).
    Mirrors Tier 0 structure and returns (snapshot_items, audit_rows).
    """
    logger.info("Discovering Federal Register Tier A: agency RULE/NOTICE actions")

    snapshot: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    fields = [
        "title",
        "html_url",
        "publication_date",
        "type",
        "agency_names",
        "document_number",
    ]

    for agency_slug, allowed_types in TIER_A_FILTERS:
        page = 1
        while True:
            params = [
                ("per_page", "1000"),
                ("order", "newest"),
                ("page", str(page)),
                ("conditions[publication_date][gte]", start_iso),
                ("conditions[publication_date][lte]", end_iso),
                ("conditions[agencies][]", agency_slug),
            ]
            for t in allowed_types:
                params.append(("conditions[type][]", t))
            for f in fields:
                params.append(("fields[]", f))

            logger.debug("FR PARAMS (unencoded): %s", params)
            qs = urlencode(params, doseq=True)
            url = f"{FR_API_BASE}?{qs}"
            logger.debug("FR GET url=%s", url)
            _log_http_cmd(session, url, logger)
            status, text = http_get(session, url, logger)
            if status != 200:
                logger.debug("FR TierA ERROR BODY (status=%s): %s", status, text)
            if status != 200 or not text:
                logger.debug("FR TierA stop agency=%s page=%d (status=%s)", agency_slug, page, status)
                break

            try:
                data = json.loads(text)
            except Exception as e:
                logger.debug("FR TierA JSON decode fail agency=%s page=%d error=%s", agency_slug, page, e)
                break

            results = data.get("results", []) or []
            total_pages = int(data.get("total_pages", 0) or 0)
            logger.debug("FR TierA agency=%s page=%d/%d items=%d", agency_slug, page, total_pages, len(results))
            if not results:
                break

            for r in results:
                title = normalize_ws((r.get("title") or "").strip())
                url_item = (r.get("html_url") or "").strip()
                pub = (r.get("publication_date") or "").strip()
                rtype = (r.get("type") or "").strip().upper()

                # 1) DEA Decision-and-Order exclusion (noise)
                if agency_slug == "drug-enforcement-administration" and "decision and order" in title.lower():
                    audit_rows.append({
                        "agency": agency_slug,
                        "type": rtype,
                        "title": title,
                        "url": url_item,
                        "publication_date": pub,
                        "status": "SKIPT",
                        "reason": "dea_decision_and_order_excluded",
                    })
                    continue

                # 2) Keep only allowed types (RULE/NOTICE per agency’s allowlist)
                if rtype not in allowed_types:
                    audit_rows.append({
                        "agency": agency_slug,
                        "type": rtype,
                        "title": title,
                        "url": url_item,
                        "publication_date": pub,
                        "status": "SKIPT",
                        "reason": "not_in_allowed_types",
                    })
                    continue

                # Standard audit label; keep style consistent with other harvesters
                raw_line = f"[{agency_slug}:{rtype}] {title} ({pub})"

                entity = {
                    "source": "Federal Register",
                    "doc_type": "agency_action",
                    "title": title,
                    "url": url_item,
                    "canonical_url": url_item,
                    "summary_url": "",
                    "summary": "",
                    "summary_origin": "",
                    "summary_timestamp": "",
                    "post_date": pub,
                    "raw_line": raw_line,
                }
                snapshot.append(entity)

                audit_rows.append({
                    "agency": agency_slug,
                    "type": rtype,
                    "title": title,
                    "url": url_item,
                    "publication_date": pub,
                    "status": "parsed",
                })

            page += 1
            if total_pages and page > total_pages:
                break

    logger.debug("FR TierA snapshot total=%d", len(snapshot))
    return snapshot, audit_rows

def _append_doc_to_snapshot(doc: Dict[str, Any], snapshot: List[Dict[str, Any]],
                            audit_rows: List[Dict[str, Any]], logger):
    """
    Convert an FR result row into our entity record (pre-window snapshot).
    Emit a discovery-line so we can confirm we're seeing everything.
    """
    title = normalize_ws(doc.get("title") or "")
    html_url = canonicalize_url(doc.get("html_url") or "", base="https://www.federalregister.gov/")
    pub_date = (doc.get("publication_date") or "").strip()
    doctype = (doc.get("type") or "").strip()  # should be PRESDOCU
    sub = (doc.get("presidential_document_type") or "").strip()
    doc_num = (doc.get("document_number") or "").strip()
    agencies = doc.get("agency_names") or []

    raw_line = f"=== {pub_date} — {title}"

    # Each returned doc is a candidate; window filtering happens later
    entity = {
        "source": "Federal Register",
        "doc_type": "presidential_document",
        "title": title,
        "url": html_url,
        "canonical_url": html_url,
        "summary_url": "",           # none inherent; enrichment may add later
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": pub_date,       # publication_date
        "raw_line": raw_line,
        # FR extras (kept in entity; harmless for downstream)
        "fr_type": doctype,
        "fr_subtype": sub,
        "fr_document_number": doc_num,
        "fr_agencies": agencies,
    }
    snapshot.append(entity)

    audit_rows.append({
        "title": title,
        "url": html_url,
        "publication_date": pub_date,
        "type": doctype,
        "subtype": sub,
        "document_number": doc_num,
        "agencies": agencies,
        "status": "parsed",
    })

    logger.debug("FR DISCOVERED: %s", raw_line)


def _filter_window_and_dedupe(snapshot_items: List[Dict[str, Any]],
                              start_iso: str, end_iso: str, logger):
    """
    Window filter + stable dedupe by canonical_url with detailed DEBUG.
    Even though server-side filters are applied, we still run this for consistency
    and to get the KEPT/SKIPT audit.
    """
    kept_pre_dedupe: List[Dict[str, Any]] = []
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_title": 0, "no_url": 0, "bad_subtype": 0}

    for it in snapshot_items:
        title = (it.get("title") or "").strip()
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        iso = (it.get("post_date") or "").strip()
        subtype = (it.get("fr_subtype") or "").strip().lower().replace(" ", "_")

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
        elif subtype and (subtype not in FR_PRES_SUBTYPES):
            stats["bad_subtype"] += 1
            reason = f"unexpected_subtype={subtype}"
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
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | "
        "outside=%d nodate=%d no_title=%d no_url=%d bad_subtype=%d dupes=%d",
        start_iso, end_iso,
        len(snapshot_items), len(kept_pre_dedupe), len(deduped),
        stats["outside"], stats["nodate"], stats["no_title"], stats["no_url"],
        stats["bad_subtype"], dups
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
    """
    Federal Register harvester (COPY-mode + scoped merge):
      - Scope via FR_SCOPE env var: 'tier_a' | 'presdocs' | 'both' (default).
      - If 'both': Tier A collected first; PresDocs appended after (preserved order).
      - Writes RAW (pre-window) + FILTERED (window + dedupe).
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)

    # === NEW: scope switch, defaults to 'both' ===
    scope = (os.getenv("FR_SCOPE") or "both").strip().lower()
    if scope not in ("tier_a", "presdocs", "both"):
        scope = "both"
    logger.info("Discovering Federal Register: scope=%s", scope)

    # We keep your existing data shapes: snapshot_items + audit_rows
    full_snapshot: List[Dict[str, Any]] = []
    full_audit: List[Dict[str, Any]] = []

    # Run Tier A first (if selected)
    if scope in ("tier_a", "both"):
        logger.info("→ FR Tier A (RULE, NOTICE; excluding PRORULE)")
        tier_snapshot, tier_audit = _discover_tier_a_actions(sess, start, end, logger)
        full_snapshot.extend(tier_snapshot)
        # annotate audit rows so we can tell which channel produced them
        for r in tier_audit:
            r["channel"] = "tier_a"
        full_audit.extend(tier_audit)

    # Then PresDocs (if selected), appended after Tier A
    if scope in ("presdocs", "both"):
        logger.info("→ FR Presidential Documents (executive_order, memorandum, proclamation)")
        pd_snapshot, pd_audit = _discover_presidential_docs(sess, start, end, logger)
        full_snapshot.extend(pd_snapshot)
        for r in pd_audit:
            r["channel"] = "presdocs"
        full_audit.extend(pd_audit)

    logger.debug("FR snapshot merged total=%d", len(full_snapshot))

    now_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Reuse your existing window+dedupe machinery (unchanged)
    filtered_items, win_stats = _filter_window_and_dedupe(full_snapshot, start, end, logger)

    # RAW write — include full snapshot + audit (UNCHANGED SHAPE + added 'scope')
    raw_payload = {
        "generated_at": now_utc,
        "schema": "dc.v4.raw",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "scope": scope,  # ← NEW: lets downstream know how this was run
        "parsed_total": len(full_snapshot),
        "audit": full_audit,  # keep your audit; rows now include channel
        "items_snapshot": [
            {
                "url": it.get("url", ""),
                "title": it.get("title", ""),
                "post_date": it.get("post_date", ""),
                "doc_type": it.get("doc_type", ""),
                "raw_line": (it.get("raw_line", "") or "")[:500],
                "summary_url": it.get("summary_url", ""),
                "summary": it.get("summary", ""),
                "summary_origin": it.get("summary_origin", ""),
                "summary_timestamp": it.get("summary_timestamp", ""),
                "fr_type": it.get("fr_type", ""),
                "fr_subtype": it.get("fr_subtype", ""),
                "fr_document_number": it.get("fr_document_number", ""),
                "fr_agencies": it.get("fr_agencies", []),
            }
            for it in full_snapshot
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED write — unchanged, fed by the merged list
    filtered_payload = {
        "generated_at": now_utc,
        "schema": "dc.v4.filtered",
        "source": HARVESTER_ID,
        "entity_type": "federal_register_document",
        "window": {"start": start, "end": end},
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

# ---------------------------
# Direct CLI (optional)
# ---------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock V4 — Federal Register harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="Log level (e.g., INFO or DEBUG)")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="Artifacts root directory")
    p.add_argument(
        "--scope",
        choices=["tier_a", "presdocs", "both"],
        default="both",
        help="Which subset to harvest (Tier A only, PresDocs only, or both; default=both)",
    )

    args = p.parse_args()

    # Set FR_SCOPE env var so internal logic uses same value
    os.environ["FR_SCOPE"] = args.scope

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