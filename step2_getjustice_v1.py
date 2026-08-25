# step2_getjustice_v1.py — Department of Justice statement-act harvester (V4 contract)
"""
DOJ harvester — OFFICIAL ACTOR RECORDS with LEGAL-STAGE classification.

THE SIGNAL-TO-NOISE PROBLEM, AND THE FILTER THAT SOLVES IT
    Measured 2026-08-25: DOJ publishes ~292 press releases per week, and **96% are
    US-Attorney-office criminal prosecutions** ("Gainesville Felon Pleads Guilty to
    Illegal Possession of Firearm"). Harvesting unfiltered would flood the archive
    with routine local crime.

    A component allowlist reduces this to ~8/week of genuinely relevant material —
    OLC opinions, election monitoring, civil-rights investigations, Attorney General
    actions. The allowlist is therefore not an optimisation; it is what makes this
    source usable at all.

EVIDENTIARY TREATMENT (Event Trust Contract v0.3 §4.5)
    Official actor record. A release documents that DOJ announced something, in those
    words, on that date. It does not establish the truth of any allegation within it.
    DOJ is simultaneously prosecutor, litigant, legal adviser, policymaker and public
    narrator of its own conduct.

        "DOJ announced a grand jury returned an indictment charging X"  -> documented
        "X committed the crimes described in the indictment"            -> NOT documented

    Constraints on every entity: never_corroborates, proposition_scope =
    own_acts_and_statements.

LEGAL STAGE — the field that prevents charge-to-conviction compression
    A DOJ announcement can describe an investigation, allegation, charge, plea,
    conviction, sentence, settlement or court ruling. These are six different
    evidentiary states and must not be flattened.

    `legal_stage` is SED-045's judicial chain applied to enforcement:
        filing -> order entered -> stay/injunction -> effective relief ->
        compliance -> final disposition

    DOJ prints a presumption-of-innocence line in most releases. That protection is
    enforced STRUCTURALLY here, by typing the stage — never by copying the disclaimer
    into prose, which would be a prose-only constraint (Ruling A).

SURFACE
    Official JSON API: https://www.justice.gov/api/v1/press_releases.json
    Zero-based pagination, max 50/page, throttles above ~4 req/s. Identity is the
    `uuid`, not the URL: DOJ has restructured its site and moved pre-2025-01-20
    material into archives, so URLs are not stable identifiers.

    NOTE the API returns DUPLICATE items across pages; dedupe by uuid is required.

OUTPUT (standard V4 contract)
    {artifacts}/json/justice_raw_{start}_{end}.json
    {artifacts}/json/justice_filtered_{start}_{end}.json
"""
from __future__ import annotations

import datetime as _dt
import html as _html
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
    normalize_ws,
    canonicalize_url,
    within_window,
)

HARVESTER_ID = "justice"

__all__ = ["run_harvester"]

API = "https://www.justice.gov/api/v1/press_releases.json"
PAGE_SIZE = 50            # API maximum
MAX_PAGES = 200           # runaway guard
REQ_PAUSE = 0.3           # stay well under the ~4 req/s throttle
UA = "Mozilla/5.0 (compatible; democracy-clock-harvester)"

# ── component allowlist ──────────────────────────────────────────────────────
# Policy-bearing components only. Everything else — every USAO, plus FBI/ATF/DEA
# field announcements — is routine criminal enforcement and out of scope.
# Measured effect: ~292/week -> ~8/week.
ALLOW_COMPONENTS = {
    "Office of the Attorney General",
    "Office of the Deputy Attorney General",
    "Office of the Associate Attorney General",
    "Office of Public Affairs",
    "Office of Legal Counsel",
    "Office of Legal Policy",
    "Office of the Solicitor General",
    "Office of the Pardon Attorney",
    "Civil Rights Division",
    "National Security Division",
    "Antitrust Division",
    "Civil Division",
    "Environment and Natural Resources Division",
    "Justice Management Division",
    "Executive Office for Immigration Review",
    "Office of Justice Programs",
}
# Any component matching these is disqualifying even if another allowed component
# is also attached — a USAO co-tag means it is a local prosecution announcement.
DENY_PATTERNS = (r"^USAO", r"U\.S\. Attorney", r"United States Attorney")

# ── legal-stage vocabulary (SED-045 judicial chain, enforcement refinement) ───
# Ordered: the FIRST match wins, so later/stronger stages are tested before
# earlier ones where phrasing overlaps ("pleaded guilty" before "charged").
_STAGES: List[Tuple[str, str]] = [
    (r"\bsentenc(ed|ing)\b|\bordered to (?:pay|serve)\b|\bprison term\b", "sentence_imposed"),
    (r"\bconvicted\b|\bfound guilty\b|\bjury (?:convicts|verdict)\b",     "conviction_after_trial"),
    (r"\bpleads? guilty\b|\bpleaded guilty\b|\bplea agreement\b",         "plea_entered"),
    (r"\bconsent decree\b.*\bentered\b|\bcourt enter(?:ed|s)\b",          "consent_decree_entered"),
    (r"\bproposed consent decree\b|\bproposed (?:final )?judgment\b",     "consent_decree_proposed"),
    (r"\bsettle(?:s|d|ment)\b|\bagrees? to pay\b|\bresolve[sd]? allegations\b|"
     r"\bsecures? (?:an? )?agreement\b|\breach(?:es|ed) (?:an? )?agreement\b", "settlement_executed"),
    (r"\bindict(?:ed|ment)\b|\bgrand jury\b",                             "indictment_returned"),
    (r"\bcriminal information\b|\binformation (?:was )?filed\b",          "information_filed"),
    (r"\barrested\b|\btaken into custody\b",                              "arrest_made"),
    (r"\bfiles? (?:a )?(?:civil )?(?:complaint|lawsuit|suit)\b|\bsues?\b|\blawsuit against\b",
     "complaint_filed"),
    (r"\bappeal(?:s|ed)\b|\bnotice of appeal\b",                          "appeal_filed"),
    (r"\bdismiss(?:ed|es|al)\b|\bdrops? charges\b",                       "case_dismissed"),
    (r"\bjudgment\b|\bcourt rule[sd]\b|\bjudge (?:rules?|orders?)\b",     "judgment_entered"),
    (r"\bopens? an investigation\b|\binvestigat(?:ion|ing) into\b|\breview of\b",
     "investigation_announced"),
]


def _classify_stage(title: str, teaser: str) -> Optional[str]:
    """Return the legal stage this announcement describes, or None if not enforcement."""
    t = f"{title} {teaser}".lower()
    for pat, stage in _STAGES:
        if re.search(pat, t):
            return stage
    return None


def _strip(s: str) -> str:
    return normalize_ws(_html.unescape(re.sub(r"<[^>]+>", " ", s or "")))


def _components(rec: Dict[str, Any]) -> List[str]:
    c = rec.get("component") or []
    if isinstance(c, list):
        return [x.get("name", "") for x in c if isinstance(x, dict)]
    return []


def _in_scope(names: List[str]) -> bool:
    if any(re.search(p, n) for n in names for p in DENY_PATTERNS):
        return False
    return any(n in ALLOW_COMPONENTS for n in names)


def _iso_from_epoch(v: Any) -> str:
    try:
        return _dt.datetime.fromtimestamp(int(str(v).strip())).date().isoformat()
    except Exception:
        return ""


def _fetch_day(day: _dt.date, page: int, logger) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Fetch one calendar day of releases.

    WHY BY DAY, NOT BY PAGING BACK. The API sorts newest-first, so reaching an early
    2025 window by pagination costs ~450+ pages (~10 minutes) and silently stops at
    whatever page cap is set — returning zero events for a week it simply never
    reached. Querying `parameters[date]` instead costs ONE request per day and works
    identically for a window last week or eighteen months ago.
    """
    epoch = int(_dt.datetime(day.year, day.month, day.day,
                             tzinfo=_dt.timezone.utc).timestamp())
    q = urllib.parse.urlencode({"parameters[date]": epoch,
                                "pagesize": PAGE_SIZE, "page": page})
    req = urllib.request.Request(f"{API}?{q}", headers={"User-Agent": UA,
                                                        "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return 200, (json.load(r).get("results") or [])
    except Exception as e:
        logger.warning("DOJ: %s page %s fetch failed: %s", day, page, str(e)[:70])
        return 0, []


def _day_range(start: str, end: str) -> List[_dt.date]:
    """
    Days to query, padded one either side.

    The API's date field is an epoch whose day boundary does not align exactly with
    the requested local date — a probe for 2025-02-18 returned items dated 02-17 —
    so the range is padded and the real window filter is applied afterwards from each
    item's own date. Padding is cheap (two requests) and removes an off-by-one.
    """
    s = _dt.date.fromisoformat(start) - _dt.timedelta(days=1)
    e = _dt.date.fromisoformat(end) + _dt.timedelta(days=1)
    out, d = [], s
    while d <= e:
        out.append(d)
        d += _dt.timedelta(days=1)
    return out


def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Harvest policy-bearing DOJ press releases for [start, end].

    The API is date-sorted DESC, so paging stops once a whole page falls before the
    window. Set DOJ_ALL_COMPONENTS=1 to bypass the allowlist (diagnostic only — this
    admits the ~96% US-Attorney criminal-prosecution volume).
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)
    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    allow_all = os.getenv("DOJ_ALL_COMPONENTS") == "1"
    if allow_all:
        logger.warning("DOJ_ALL_COMPONENTS=1 — component allowlist BYPASSED (diagnostic)")
    logger.info("Session ready. Harvesting %s → %s", start, end)

    snapshot: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []
    seen_uuid: set = set()
    pages_read = dupes = 0

    days = _day_range(start, end)
    logger.info("DOJ: querying %d day(s) %s .. %s (one request per day, not paged back)",
                len(days), days[0], days[-1])

    for di, day in enumerate(days, 1):
        day_items = 0
        for page in range(0, 20):          # within-day paging; DOJ runs ~40/day
            status, results = _fetch_day(day, page, logger)
            audit.append({"day": day.isoformat(), "page": page,
                          "status": status, "returned": len(results)})
            if status != 200 or not results:
                break
            pages_read += 1
            day_items += len(results)

            for rec in results:
                uuid = (rec.get("uuid") or "").strip()
                # Identity is the uuid, not the URL: DOJ restructured its site and
                # the API repeats items across pages.
                if uuid and uuid in seen_uuid:
                    dupes += 1
                    continue
                if uuid:
                    seen_uuid.add(uuid)

                title = _strip(rec.get("title"))
                url = canonicalize_url((rec.get("url") or "").strip(),
                                       base="https://www.justice.gov/")
                post_date = _iso_from_epoch(rec.get("date")) or _iso_from_epoch(rec.get("created"))
                teaser = _strip(rec.get("teaser"))
                names = _components(rec)
                if not title or not url:
                    continue

                in_scope = True if allow_all else _in_scope(names)
                stage = _classify_stage(title, teaser)

                snapshot.append({
                    "source": "Department of Justice",
                    "doc_type": "doj_press_release",
                    "title": title,
                    "url": url,
                    "canonical_url": url,
                    "summary_url": "",
                    "summary": "",
                    "summary_origin": "",
                    "summary_timestamp": "",
                    "post_date": post_date,
                    "raw_line": f"=== {post_date} — {title}",
                    "doj_uuid": uuid,
                    "doj_components": names,
                    "doj_teaser_verbatim": teaser,
                    "doj_changed": _iso_from_epoch(rec.get("changed")),
                    "legal_stage": stage,
                    "allegations_present": stage in {"indictment_returned", "information_filed",
                                                     "complaint_filed", "arrest_made",
                                                     "investigation_announced"},
                    "in_scope": in_scope,
                    "speech_act_verb": "announced",
                    "trust_basis": "official_actor_record",
                    "never_corroborates": True,
                    "proposition_scope": "own_acts_and_statements",
                })
            if len(results) < PAGE_SIZE:
                break
            time.sleep(REQ_PAUSE)

        # Progress every day: a silent multi-minute run is indistinguishable from a hung one.
        kept_so_far = sum(1 for i in snapshot if i.get("in_scope"))
        logger.info("DOJ: [%d/%d] %s — %d release(s), %d in-scope so far",
                    di, len(days), day, day_items, kept_so_far)
        time.sleep(REQ_PAUSE)

    logger.info("DOJ: %d request(s), %d unique items, %d duplicates skipped",
                pages_read, len(snapshot), dupes)

    # window + scope filter
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_title": 0,
             "dupe_url": 0, "out_of_scope": 0}
    kept: List[Dict[str, Any]] = []
    seen_url: set = set()
    for it in snapshot:
        iso = it.get("post_date") or ""
        if not it.get("title"):
            stats["no_title"] += 1; continue
        if not iso:
            stats["nodate"] += 1; continue
        if not within_window(iso, start, end):
            stats["outside"] += 1; continue
        if not it.get("in_scope"):
            stats["out_of_scope"] += 1
            logger.debug("Scope: SKIPT %s | components=%s", it["title"][:60], it["doj_components"])
            continue
        if it["canonical_url"] in seen_url:
            stats["dupe_url"] += 1; continue
        seen_url.add(it["canonical_url"])
        stats["inside"] += 1
        kept.append(it)

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    write_json(raw_path, {
        "generated_at": now_utc, "schema": "dc.v4.raw", "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "pages_read": pages_read, "duplicates_skipped": dupes,
        "component_allowlist_bypassed": allow_all,
        "parsed_total": len(snapshot), "audit": audit,
        "items_snapshot": [
            {"url": i["url"], "title": i["title"], "post_date": i["post_date"],
             "doj_uuid": i["doj_uuid"], "doj_components": i["doj_components"],
             "legal_stage": i["legal_stage"], "in_scope": i["in_scope"]}
            for i in snapshot
        ],
    })
    logger.info("Wrote raw JSON: %s", raw_path)

    write_json(filtered_path, {
        "generated_at": now_utc, "schema": "dc.v4.filtered", "source": HARVESTER_ID,
        "entity_type": "doj_statement_act",
        "window": {"start": start, "end": end},
        "count": len(kept), "entities": kept, "window_stats": stats,
        "source_constraints": {"trust_basis": "official_actor_record",
                               "never_corroborates": True,
                               "proposition_scope": "own_acts_and_statements"},
        "ruling": "Official actor record (Event Trust Contract v0.3 §4.5). Documents that "
                  "DOJ announced this, in those words, on that date. Does not establish the "
                  "truth of any allegation. legal_stage prevents a charge being compressed "
                  "into a conviction; allegations_present marks unadjudicated claims "
                  "structurally rather than by disclaimer.",
        "component_allowlist": sorted(ALLOW_COMPONENTS),
    })
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(kept))

    return {"source": HARVESTER_ID, "entity_count": len(kept),
            "entities_path": str(filtered_path), "raw_path": str(raw_path),
            "log_path": str(log_path or "")}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock — DOJ statement-act harvester")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT))
    p.add_argument("--level", default="INFO")
    p.add_argument("--log", default=None)
    a = p.parse_args()
    print(run_harvester(start=a.start, end=a.end, artifacts_root=a.artifacts,
                        level=a.level, log_path=a.log))
