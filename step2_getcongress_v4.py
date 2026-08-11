# getcongress_v4.py
from __future__ import annotations

import os
import sys
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from datetime import datetime

# Ceremonial/naming/coin/CGM heuristics — intentionally conservative
_CEREMONIAL_PATTERNS = [
    r"\bto designate the (?:facility|building|post office|postal facility)\b",
    r"\bdesignat(?:e|ing|ion) (?:of )?(?:the )?(?:post office|postal facility|building|facility)\b",
    r"\brename(?:s|d|) (?:the )?(?:post office|postal facility|building|facility)\b",
    r"\b(?:congressional )?gold medal\b",
    r"\bcommemorative coin\b",
    r"\bcoin(?:age)? act\b",
    r"\bname\s+(?:the|a)\s+(?:post office|building|facility)\b",
    r"\bredesignat(?:e|ing|ion)\b",
    # NEW: catch VA namings
    r"\bto name the (?:department of veterans affairs|va)\b",
    r"\bva (?:clinic|medical center|facility)\b",
]
_CEREMONIAL_RE = re.compile("|".join(_CEREMONIAL_PATTERNS), re.IGNORECASE)

def _looks_ceremonial(bill: Dict[str, Any]) -> bool:
    """
    Fast, no-extra-API-call screen for ceremonial bills like post office namings,
    commemorations, coins, and CGM. Uses title + latestAction text.
    """
    title = (bill.get("shortTitle") or bill.get("title") or bill.get("titleWithoutNumber") or "").strip()
    la_txt = ((bill.get("latestAction") or {}).get("text") or "").strip()
    blob = f"{title} || {la_txt}"
    return bool(_CEREMONIAL_RE.search(blob))

# Treat “weekly roundups / week-in-review” posts as non-events
_WEEKLY_ROLLUP_TITLE_RE = re.compile(
    r"""(?ix)
    \b(
        weekly\s*(wrap(?:-?up)?|round(?:-?up)?|review)      # "Weekly Wrap/Wrap-up/Roundup/Review"
      | week\s+in\s+review                                  # "Week in Review"
      | this\s+week\s+(in|with)\s+congress                  # "This Week in Congress"
      | week\s+of\s+[A-Z][a-z]+\s+\d{1,2}(?:,\s*\d{4})?     # "Week of October 21, 2025"
    )\b
    """,
)

def _is_weekly_rollup(title: str, url: str = "") -> bool:
    """
    Returns True for weekly roll-up/roundup/review posts that summarize prior actions.
    We skip these to avoid duplicative/non-discrete event entries.
    """
    t = (title or "").strip()
    u = (url or "").strip()
    if not t and not u:
        return False
    if _WEEKLY_ROLLUP_TITLE_RE.search(t):
        return True
    # URL hints (belt-and-suspenders)
    if re.search(r"/weekly[-_](wrap|round[-_]?up|review)/?", u, re.I):
        return True
    if re.search(r"/this[-_]?week[-_](in|with)[-_]?congress", u, re.I):
        return True
    if re.search(r"/week[-_]?in[-_]?review", u, re.I):
        return True
    return False

# add near the top (below imports)
def _get_congress_api_key() -> str:
    # Prefer env so you can change it per-shell/process
    k = os.getenv("CONGRESS_GOV_API_KEY", "").strip()
    if k:
        return k
    try:
        # use config_v4 fallback if defined
        from config_v4 import CONGRESS_API_KEY as CFG_KEY
        return (CFG_KEY or "").strip()
    except Exception:
        return ""

# --- V4 helpers & config (use only existing names; provide safe fallbacks) ---
try:
    from config_v4 import ARTIFACTS_ROOT  # artifacts root directory (Path or str)
except Exception:
    ARTIFACTS_ROOT = Path("./artifacts")

try:
    # If your config defines these, we’ll use them; otherwise we fall back below
    from config_v4 import CONGRESS_BASE  # e.g., "https://api.congress.gov/v3"
except Exception:
    CONGRESS_BASE = "https://api.congress.gov/v3"

try:
    from config_v4 import CONGRESS_API_KEY  # optional; else from env
except Exception:
    CONGRESS_API_KEY = os.getenv("CONGRESS_GOV_API_KEY", "").strip()

try:
    from config_v4 import CONGRESS_NUMBER  # optional convenience default
except Exception:
    CONGRESS_NUMBER = 119

try:
    from step2_helper_v4 import (
        setup_logger,
        build_session,
        create_artifact_paths,
        write_json,
        normalize_ws,
    )
except Exception as e:
    raise RuntimeError("helper_v4.py is required in V4") from e


HARVESTER_ID = "congress"

# Terminal action codes/texts we’ll recognize
_TERMINAL_CODES = {
    # common Congress.gov latestAction.actionCode values for final outcomes
    "BecamePublicLaw",           # signed / became law
    "PresidentSigned",           # sometimes present
    "Vetoed",
    "VetoOverridden",
    "VetoSustained",
    "PocketVetoed",
}

# Some feeds lack reliable actionCode; keep strong text heuristics too
_TERMINAL_TEXT_NEEDLES = [
    "became public law",
    "became law",
    "public law",
    "vetoed",
    "veto overridden",
    "veto sustained",
    "pocket veto",
]

from datetime import datetime, time, timezone

def _as_utc_datetime_str(date_str: str, end_of_day: bool) -> str:
    """
    Convert 'YYYY-MM-DD' or ISO date to 'YYYY-MM-DDTHH:MM:SSZ'.
    If end_of_day=True -> 23:59:59Z, else -> 00:00:00Z.
    """
    # accept either 'YYYY-MM-DD' or already-ISO-ish; normalize to date first
    try:
        # strict date
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except Exception:
        # last resort: let datetime parse, then take its date()
        d = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()

    t = time(23, 59, 59) if end_of_day else time(0, 0, 0)
    dt = datetime.combine(d, t, tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------
# API plumbing (COPY-style)
# ---------------------------
# change signature and body
def _api_params(from_dt: str, to_dt: str, limit: int, offset: int, api_key: str) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "format": "json",
        "limit": str(limit),
        "offset": str(offset),
        "fromDateTime": from_dt,
        "toDateTime": to_dt,
    }
    if api_key:
        params["api_key"] = api_key
    return params


def _walk_bill_pages(
    session: requests.Session,
    congress: int,
    from_dt: str,
    to_dt: str,
    logger,
    page_limit: int = 250,
    api_key: str = "",
) -> Iterable[Dict[str, Any]]:
    """
    Iterate Congress.gov bills for a specific Congress and update window.
    Stops when a page returns fewer than `limit` items.
    """
    base_url = f"{CONGRESS_BASE.rstrip('/')}/bill/{congress}"
    offset = 0
    seen = 0                         # <-- restore initialization

    while True:
        params = _api_params(from_dt, to_dt, page_limit, offset, api_key)
        resp = session.get(base_url, params=params, timeout=30)
        status = resp.status_code

        # C-3 fix (2026-08-09): a non-200 response (especially the HTTP 403
        # Congress.gov returns without a valid api_key) was previously ignored —
        # data.get("bills", []) became [], the loop broke, and the harvester
        # "succeeded" with zero bills, silently dropping every public law/veto.
        # Fail loudly instead. With the H-1 best-effort orchestrator this fails
        # only the congress source, not the whole weekly run.
        if status != 200:
            snippet = (resp.text or "")[:300]
            hint = " (missing or invalid CONGRESS_GOV_API_KEY?)" if status in (401, 403) else ""
            msg = f"Congress.gov returned HTTP {status} for {resp.url}{hint}: {snippet}"
            logger.error(msg)
            raise RuntimeError(msg)

        try:
            data = resp.json()
        except Exception:
            msg = f"Congress.gov returned HTTP 200 with unparseable JSON at {resp.url}"
            logger.error(msg)
            raise RuntimeError(msg)

        bills = data.get("bills", []) or []
        logger.debug(
            "GET %s offset=%s status=%s -> page_count=%s",
            resp.url, offset, status, len(bills)
        )

        for b in bills:
            seen += 1
            yield b

        # Congress.gov typically paginates until a short page
        if len(bills) < page_limit:
            break

        offset += page_limit

    logger.debug("Total bills seen across pages: %d", seen)


def _normalize_action_date(raw_date: str) -> str:
    """Normalize a Congress.gov actionDate ('YYYY-MM-DD' or ISO datetime) to YYYY-MM-DD."""
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return ""
    try:
        if "T" in raw_date:
            return datetime.fromisoformat(raw_date.replace("Z", "+00:00")).date().isoformat()
        return datetime.strptime(raw_date[:10], "%Y-%m-%d").date().isoformat()
    except Exception:
        return raw_date[:10]


def _walk_law_pages(
    session: requests.Session,
    congress: int,
    law_type: str,
    logger,
    page_limit: int = 250,
    api_key: str = "",
) -> Iterable[Dict[str, Any]]:
    """
    Iterate ENACTED laws for a Congress via /law/{congress}/{lawType}
    (lawType: 'pub' = public laws, 'priv' = private laws).

    Why not /bill windowed on fromDateTime/toDateTime? Because that endpoint filters
    on `updateDate`, which the Congress.gov docs state explicitly "is NOT a date
    corresponding to the legislative action date" — it bumps on any administrative
    touch. Windowing on it floods the result with thousands of referral-stage bills
    (2,405 in the wk34-45 run) and does NOT reliably surface enacted laws — the
    shutdown-ending public law was missed entirely. The /law endpoint returns only
    bills that became law (a few hundred per two-year Congress) and takes no date
    params, so we page through all and filter client-side by the enactment date
    (latestAction.actionDate). Same fail-loud handling as the bill walker.
    """
    base_url = f"{CONGRESS_BASE.rstrip('/')}/law/{congress}/{law_type}"
    offset = 0
    seen = 0
    while True:
        params: Dict[str, Any] = {"format": "json", "limit": str(page_limit), "offset": str(offset)}
        if api_key:
            params["api_key"] = api_key
        resp = session.get(base_url, params=params, timeout=30)
        status = resp.status_code
        if status != 200:
            snippet = (resp.text or "")[:300]
            hint = " (missing or invalid CONGRESS_GOV_API_KEY?)" if status in (401, 403) else ""
            msg = f"Congress.gov returned HTTP {status} for {resp.url}{hint}: {snippet}"
            logger.error(msg)
            raise RuntimeError(msg)
        try:
            data = resp.json()
        except Exception:
            msg = f"Congress.gov returned HTTP 200 with unparseable JSON at {resp.url}"
            logger.error(msg)
            raise RuntimeError(msg)

        laws = data.get("laws", []) or []
        logger.debug("GET %s offset=%s status=%s -> page_count=%s", resp.url, offset, status, len(laws))
        for law in laws:
            seen += 1
            yield law

        if len(laws) < page_limit:
            break
        offset += page_limit

    logger.debug("Total %s laws seen across pages: %d", law_type, seen)


# ---------------------------
# Filtering & mapping
# ---------------------------
def _is_terminal_bill(bill: Dict[str, Any]) -> Tuple[bool, str, str]:
    """
    Decide if this bill reached a terminal outcome.
    Returns (is_terminal, tag, decision_date_iso)
      - tag is one of: 'became_law', 'vetoed', 'override', 'sustained', 'pocket_veto'
      - date is the latestAction date (YYYY-MM-DD) when terminal occurred (if available)
    """
    latest = bill.get("latestAction") or {}
    code = (latest.get("actionCode") or "").strip()
    text = (latest.get("text") or "").strip().lower()

    # Prefer explicit code, then text needles
    tag = ""
    if code in _TERMINAL_CODES:
        if code in {"BecamePublicLaw", "PresidentSigned"}:
            tag = "became_law"
        elif code == "Vetoed":
            tag = "vetoed"
        elif code == "VetoOverridden":
            tag = "override"
        elif code == "VetoSustained":
            tag = "sustained"
        elif code == "PocketVetoed":
            tag = "pocket_veto"
    else:
        # Heuristic on text
        t = text
        if "pocket veto" in t:
            tag = "pocket_veto"
        elif "veto sustained" in t:
            tag = "sustained"
        elif "veto overridden" in t or "override" in t:
            tag = "override"
        elif "veto" in t:
            tag = "vetoed"
        elif ("became public law" in t) or ("became law" in t) or ("public law" in t):
            tag = "became_law"

    if not tag:
        return False, "", ""

    # Extract a decision date if possible
    # latest.actionDate may be 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SSZ'
    raw_date = (latest.get("actionDate") or "").strip()
    decision_date_iso = ""
    if raw_date:
        try:
            # tolerate both date-only and datetime
            if "T" in raw_date:
                dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                decision_date_iso = dt.date().isoformat()
            else:
                decision_date_iso = datetime.strptime(raw_date, "%Y-%m-%d").date().isoformat()
        except Exception:
            decision_date_iso = raw_date  # fall back

    return True, tag, decision_date_iso


def _bill_title(bill: Dict[str, Any]) -> str:
    """Pick a reasonable title string from Congress.gov bill object."""
    title = bill.get("title") or bill.get("titleWithoutNumber") or ""
    if not title:
        # Construct a minimal label like "H.R.1234 — <shortTitle>"
        number = bill.get("number") or ""
        bill_type = bill.get("type") or ""
        short = bill.get("shortTitle") or ""
        title = normalize_ws(f"{bill_type}.{number} — {short}".strip(" —"))
    return normalize_ws(title)


def _bill_urls(bill: Dict[str, Any]) -> Tuple[str, str]:
    """
    Return (entity_url, canonical_url) for the bill.
    Use Congress.gov HTML page for canonical, plus API 'url' if present.
    """
    # HTML canonical on congress.gov (not the API) has the stable human URL
    # Example: https://www.congress.gov/bill/119th-congress/house-bill/1234
    congress = bill.get("congress")
    bill_type = (bill.get("type") or "").lower()  # hr, s, etc., sometimes present
    number = bill.get("number")

    # Congress.gov sometimes uses long form paths; if type missing, fall back to API-supplied 'url'
    canonical = bill.get("congressDotGovUrl") or bill.get("url") or ""
    if not canonical and (congress and bill_type and number):
        # Try best-effort construction
        house_or_senate = "house-bill" if bill_type.startswith("h") else "senate-bill"
        canonical = f"https://www.congress.gov/bill/{congress}th-congress/{house_or_senate}/{number}"

    entity_url = bill.get("url") or canonical  # API URL if present, else HTML
    return entity_url, canonical


def _bill_to_entity(bill: Dict[str, Any], decision_date_iso: str) -> Dict[str, Any]:
    """Map a Congress.gov bill to our V4 entity JSON record."""
    title = _bill_title(bill)
    entity_url, canonical = _bill_urls(bill)

    latest = bill.get("latestAction") or {}
    latest_text = normalize_ws(latest.get("text") or "")
    raw_line = normalize_ws(f"{latest_text} ({decision_date_iso})")

    entity = {
        "source": "Congress.gov",
        "doc_type": "bill",
        "title": title,
        # URL fields
        "url": canonical or entity_url,          # prefer human canonical
        "canonical_url": canonical or entity_url,
        "entity_url": entity_url,                # API record URL if provided; else same
        # Summary fields (left empty for Congress; we don’t have on-page narrative)
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        # Dates & audit
        # Prefer the bill's latestAction.actionDate (true last-action date),
        # but fall back to the terminal decision_date_iso if missing.
        "post_date": (
            (bill.get("latestAction") or {}).get("actionDate") or decision_date_iso
        ),
        "raw_line": raw_line,                    # for audit/debug
    }
    return entity


# ---------------------------
# Public entry point
# ---------------------------
def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
    congress: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Step-1 harvester for Congress.gov ENACTED LAWS (COPY mode):
      - Queries /law/{congress}/{pub,priv} (all enacted laws for the Congress)
      - Filters client-side to laws ENACTED within [start, end]
        (latestAction.actionDate = the "Became Public Law" date)
      - Drops ceremonial laws (post-office namings, coins, CGM, VA facility renames)
      - Writes RAW snapshot and FILTERED entity list

    Rationale: the prior /bill window query filtered on `updateDate` (any
    administrative touch), which per Congress.gov docs is NOT the action date. It
    returned thousands of referral-stage bills and zero enacted laws, silently
    missing every public law (incl. the shutdown-ending CR). The /law endpoint is
    the authoritative, low-noise source for terminal "became law" outcomes.

    NOTE: veto/override/pocket-veto outcomes are not laws and are out of scope here
    (rare, and heavily covered by the news sources). Add via bill actions if needed.
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)
    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    api_key = _get_congress_api_key()
    if not api_key:
        # C-3 fix (2026-08-09): Congress.gov requires a key; a keyless request
        # returns HTTP 403, which _walk_bill_pages now raises on. Surface the
        # likely cause up front at error level so the failure is self-explaining.
        logger.error(
            "No Congress.gov API key found. Set CONGRESS_GOV_API_KEY (env) or "
            "CONGRESS_API_KEY (config_v4). Keyless requests return HTTP 403 and "
            "will fail this source with zero bills harvested."
        )

    # Congress scope
    congress_num = congress or CONGRESS_NUMBER

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info(
        "Discovering Congress.gov enacted laws (pub+priv) | Congress=%s | enacted-in-window %s → %s",
        congress_num, start, end
    )

    # Walk enacted laws (public + private); keep those enacted in-window.
    snapshot: List[Dict[str, Any]] = []
    kept_entities: List[Dict[str, Any]] = []
    seen_by_type: Dict[str, int] = {}

    for law_type in ("pub", "priv"):
        n_type = 0
        for law in _walk_law_pages(sess, congress_num, law_type, logger, api_key=api_key):
            n_type += 1
            latest = law.get("latestAction") or {}
            # Every /law item is enacted; the enactment date is latestAction.actionDate
            # (text e.g. "Became Public Law No: 119-37.").
            decision_date = _normalize_action_date(latest.get("actionDate") or "")
            law_no = ""
            _laws = law.get("laws") or []
            if _laws and isinstance(_laws[0], dict):
                law_no = _laws[0].get("number") or ""

            snapshot.append({
                "congress": law.get("congress"),
                "number": law.get("number"),
                "type": law.get("type"),
                "lawType": law_type,
                "lawNumber": law_no,
                "title": _bill_title(law),
                "latestAction": latest,
                "url": law.get("url"),
            })

            # Drop ceremonial laws (post-office namings, coins, CGM, VA renames).
            if _looks_ceremonial(law):
                continue

            # -------- HARD WINDOW FILTER on the ENACTMENT date --------
            if not decision_date:
                logger.debug("Congress law drop: missing actionDate → %r", _bill_title(law)[:140])
                continue
            if not (start <= decision_date <= end):
                continue
            # ----------------------------------------------------------

            kept_entities.append(_bill_to_entity(law, decision_date_iso=decision_date))
        seen_by_type[law_type] = n_type

    logger.info(
        "Laws scanned: pub=%s priv=%s | enacted-in-window kept=%d",
        seen_by_type.get("pub", 0), seen_by_type.get("priv", 0), len(kept_entities)
    )

    # ---- RAW write (pre-filter snapshot) ----
    raw_payload = {
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "api_scope": {
            "base": CONGRESS_BASE,
            "congress": congress_num,
            "endpoint": "/law/{congress}/{pub,priv}",
            "filter": "latestAction.actionDate within window",
            "limit": 250,
        },
        "parsed_total": len(snapshot),
        "laws_scanned_by_type": seen_by_type,
        "items_snapshot": snapshot,
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # ---- FILTERED write (terminal-only entities) ----
    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "congress_bill",
        "window": {"start": start, "end": end},
        "count": len(kept_entities),
        "entities": kept_entities,
        "window_stats": {
            "total_seen": len(snapshot),
            "kept_terminal": len(kept_entities),
        },
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(kept_entities))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(kept_entities),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


# ---------------------------
# Direct CLI (optional)
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — Congress.gov harvester")
    p.add_argument("--start", required=True, help="fromDateTime (YYYY-MM-DD or ISO)")
    p.add_argument("--end", required=True, help="toDateTime (YYYY-MM-DD or ISO)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    p.add_argument("--congress", type=int, default=CONGRESS_NUMBER, help="Congress number (e.g., 119)")
    args = p.parse_args()

    logger = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        log_path=None,
        session=None,
        congress=args.congress,
    )
    logger.info("Summary: %s", meta)