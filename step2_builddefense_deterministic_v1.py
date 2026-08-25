#!/usr/bin/env python3
"""
Deterministic Defense Press Products event builder — ZERO LLM calls.

Companion to step2_getdefense_v1.py. Same reasoning as the White House and Federal
Register deterministic builders: the document IS the event, and its identity (official
title, date, canonical URL) is already in the harvested filtered file.

THE SPEECH-ACT WRAPPER
    Department press titles routinely assert outcomes and figures:
        "Department of War Announces a $750 Million Investment as Part of a $1.55 Bil…"
        "DOW Awards a Nearly $7 Billion Oracle Agreement to Accelerate the Arsenal of Freedom"
    Those are announced ceilings and conditional commitments, not obligated money, and
    not established outcomes. So every event is emitted as

        actor  = "The Department of War"
        action = <verb from the harvest sub-type> "<verbatim text, in quotes>"

    The verb comes from the channel/sub-type, never from the title's framing, and the
    quoted text is never paraphrased. USAspending is the check on any announced figure.

CASUALTY IDENTIFICATIONS — the one documented exception
    Author ruling 2026-08-25: casualties are kept, and the STANDFIRST carries the event.
    Their titles are uninformative by design ("DOW Identifies Army Casualties" — who,
    where, how many?), while the standfirst is specific and authoritative:
        "Army Staff Sgt. Angel S. Rampersad was killed in action supporting overseas
         operations in Jordan, the Department of War has confirmed."
    The department is the authoritative record for its own personnel, so for this
    sub-type the quoted text is the standfirst rather than the title. The wrapper is
    still applied — it remains an official actor record, not an independent finding.

OFFICER NOMINATIONS are excluded upstream by the getter (out of scope, ruled
2026-08-25) and never reach this builder.

INPUT   {artifacts}/json/defense_filtered_{start}_{end}.json
OUTPUT  {artifacts}/eventjson/defense_events_{start}_{end}.json
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ARTIFACTS = BASE_DIR / "artifacts"

SOURCE_KEY = "defense"
BUILDER_VERSION = "defense_deterministic_v1"
ACTOR = "The Department of War"

# Sub-types whose event text comes from the standfirst rather than the title.
STANDFIRST_SUBTYPES = {"casualty_identification"}

# Sub-type -> writer category (must be in CATEGORY_ORDER, step2_writeweekevents_v4.py).
CATEGORY_BY_SUBTYPE = {
    "casualty_identification": "Civil–Military Relations & State Violence",
    "contract_announcement":   "Economic & Regulatory Power",
    "publication":             "Transparency & Records",
}
DEFAULT_CATEGORY = "Civil–Military Relations & State Violence"

# Title routing, applied only where the sub-type does not already decide the
# category. Without it every defence press product lands in one bucket, which
# loses recoverable signal: a $7bn software agreement, a Guantánamo hearing
# invitation and a foreign-institutions watchlist are not the same kind of thing.
#
# This is a Step-2 grouping hint ONLY. Step 7 re-categorises into its own five
# canonical categories regardless, so a miss here is cheap — which is why the
# patterns are deliberately conservative and fall through to the default.
_CATEGORY_PATTERNS: List[tuple] = [
    # money: procurement, loans, equity, agreements with named dollar figures
    (r"\$[\d,.]+\s*(?:million|billion|trillion|m\b|b\b)|conditional loan|loan commitment|"
     r"equity investment|framework agreement|\bcontract\b|\bawards?\b|procurement|"
     r"office of strategic capital|investment in",
     "Economic & Regulatory Power"),
    # Allied engagement FIRST. "Joint Military Commission" is a bilateral defence
    # body and must not be captured by the war-crimes "military commission" pattern
    # below — the two share a phrase and mean entirely different things.
    (r"joint military commission|joint (?:defense )?cooperation committee|joint statement|"
     r"\breadout\b|defense cooperation|bilateral|\bnato\b|joint declaration|counterpart",
     "International Relations"),
    # courts and war-crimes military commissions
    (r"\bpre-?trial\b|\bhearing\b|(?<!joint )military commission|\bv\.\s|\bcourt\b|"
     r"\bindict|\bplea\b|united states v",
     "Judicial Developments"),
    # disclosure, records releases, reports, watchlists
    (r"\breleases? (?:its|the|updated)\b|annual report|declassif|\bfoia\b|"
     r"publishes|updated list|\barchive\b|transparency",
     "Transparency & Records"),
    # nominations/appointments that survive scope filtering
    (r"\bconfirmation\b|sworn in|assumes? (?:the )?(?:role|duties)|appointment of",
     "Appointments & Patronage"),
]


def _route_category(title: str, subtype: str) -> str:
    """Sub-type wins where defined; otherwise route on the title; else default."""
    if subtype in CATEGORY_BY_SUBTYPE:
        return CATEGORY_BY_SUBTYPE[subtype]
    t = (title or "").lower()
    for pat, cat in _CATEGORY_PATTERNS:
        if re.search(pat, t):
            return cat
    return DEFAULT_CATEGORY

GENERIC_VERB = "published a document titled"
_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

logger = logging.getLogger("dc.build.defense")


def _iso_or_none(s: str) -> Optional[str]:
    s = (s or "").strip()[:10]
    return s if _ISO.match(s) else None


def _quote(s: str) -> str:
    return '"' + (s or "").strip().replace('"', "'") + '"'


def _event_from_entity(e: Dict[str, Any]) -> Dict[str, Any]:
    title = (e.get("title") or "").strip()
    url = (e.get("canonical_url") or e.get("url") or "").strip()
    source_date = _iso_or_none(e.get("post_date") or "") or ""
    subtype = (e.get("def_subtype") or e.get("def_instrument_type") or "").strip()
    standfirst = (e.get("def_standfirst_verbatim") or "").strip()
    verb = (e.get("speech_act_verb") or GENERIC_VERB).strip()

    # Casualties: the standfirst is the event; the title is not informative.
    # Everything else: the title is the thing published.
    if subtype in STANDFIRST_SUBTYPES and standfirst:
        quoted, text_source = standfirst, "standfirst"
    else:
        quoted, text_source = title, "title"

    action = f"{verb} {_quote(quoted)}"

    return {
        "source_date": source_date,
        "occurred_on": source_date,
        "title": title,
        "url": url,
        # Empty by design: the standfirst is the department's own account of its
        # announcement, retained verbatim below but never rendered as our prose.
        "summary": "",
        "why_relevant": "",
        "category": _route_category(title, subtype),
        "sources": [url] if url else [],
        "tags": [SOURCE_KEY, subtype] if subtype else [SOURCE_KEY],
        "attacks": [],
        "source": SOURCE_KEY,
        # act_key inputs — explicit, never inferred downstream
        "actor": ACTOR,
        "action": action,
        "jurisdiction": "federal",
        "federal_nexus": "Official Department of War channel; documents the act of publication only.",
        # High confidence THAT IT WAS PUBLISHED — never in the content of the claim.
        "confidence": "high",
        "basis": "official_actor_record",
        # provenance + constraints carried from the harvest
        "def_channel": e.get("def_channel", ""),
        "def_subtype": subtype,
        "def_standfirst_verbatim": standfirst,
        "def_byline": e.get("def_byline", ""),
        "event_text_source": text_source,
        "observed_domain": e.get("observed_domain", ""),
        "trust_basis": e.get("trust_basis", "official_actor_record"),
        "never_corroborates": e.get("never_corroborates", True),
        "proposition_scope": e.get("proposition_scope", "own_acts_and_statements"),
        "builder_version": BUILDER_VERSION,
    }


def build(artifacts: Path, start: str, end: str, limit: Optional[int] = None) -> Dict[str, Any]:
    in_path = artifacts / "json" / f"{SOURCE_KEY}_filtered_{start}_{end}.json"
    if not in_path.exists():
        logger.error("Filtered input not found: %s", in_path)
        return {"source": SOURCE_KEY, "count": 0, "events_path": None, "input_json": str(in_path)}

    payload = json.loads(in_path.read_text(encoding="utf-8"))
    entities: List[Dict[str, Any]] = payload.get("entities") or []
    if limit:
        entities = entities[:limit]
    logger.info("Loaded %d entities from %s", len(entities), in_path.name)

    events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []
    for i, e in enumerate(entities):
        title = (e.get("title") or "").strip()
        url = (e.get("canonical_url") or e.get("url") or "").strip()
        if not title or not url:
            noncompliant.append({"idx": i, "url": url, "reason": "missing_title_or_url"})
            continue
        sub = (e.get("def_subtype") or "").strip()
        if sub in STANDFIRST_SUBTYPES and not (e.get("def_standfirst_verbatim") or "").strip():
            # A casualty with no standfirst would yield an uninformative event;
            # record it rather than emitting a hollow one.
            noncompliant.append({"idx": i, "url": url, "reason": "casualty_without_standfirst"})
            continue
        events.append(_event_from_entity(e))

    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    out_path = artifacts / "eventjson" / f"{SOURCE_KEY}_events_{start}_{end}.json"
    out_path.write_text(json.dumps({
        "source": SOURCE_KEY,
        "window": {"start": start, "end": end},
        "builder": BUILDER_VERSION,
        "collector": "defense_press_products",
        "llm_calls": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_constraints": {
            "trust_basis": "official_actor_record",
            "never_corroborates": True,
            "proposition_scope": "own_acts_and_statements",
        },
        "ruling": "Official actor record. Documents that the department issued the "
                  "announcement, in those words, on that date. Does not establish the "
                  "truth of any operational, numerical or outcome claim within it; "
                  "announced contract figures are ceilings, not obligations. Never "
                  "corroborates. Casualty identifications carry the standfirst by "
                  "author ruling 2026-08-25.",
        "events": events,
        "noncompliant": noncompliant,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote %s (events=%d, noncompliant=%d, llm_calls=0)",
                out_path, len(events), len(noncompliant))
    return {"source": SOURCE_KEY, "count": len(events),
            "events_path": str(out_path), "input_json": str(in_path)}


def run_builder(
    *,
    source: str = SOURCE_KEY,
    start: str,
    end: str,
    artifacts_root: str | Path = DEFAULT_ARTIFACTS,
    level: str = "INFO",
    log_path: Optional[str] = None,
    limit: Optional[int] = None,
    ids: Optional[List[int]] = None,
    skip_existing: bool = False,
) -> Dict[str, Any]:
    """Orchestrator entry point (step2_buildweekevents_v4.BUILDER_SPECS)."""
    logging.basicConfig(level=getattr(logging, str(level).upper(), logging.INFO),
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if log_path:
        h = logging.FileHandler(log_path, encoding="utf-8")
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(h)

    artifacts = Path(artifacts_root)
    out_path = artifacts / "eventjson" / f"{SOURCE_KEY}_events_{start}_{end}.json"
    if skip_existing and out_path.exists():
        logger.info("skip_existing: %s already present", out_path)
        try:
            n = len(json.loads(out_path.read_text(encoding="utf-8")).get("events") or [])
        except Exception:
            n = 0
        return {"source": SOURCE_KEY, "count": n, "events_path": str(out_path),
                "input_json": str(artifacts / "json" / f"{SOURCE_KEY}_filtered_{start}_{end}.json")}

    return build(artifacts, start, end, limit=limit)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Deterministic Defense press-product event builder (no LLM)")
    ap.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    ap.add_argument("--artifacts", default=str(DEFAULT_ARTIFACTS), help="Artifacts root")
    ap.add_argument("--level", default="INFO", help="Logging level")
    ap.add_argument("--limit", type=int, default=None, help="Only build the first N entities")
    args = ap.parse_args()

    res = run_builder(start=args.start, end=args.end,
                      artifacts_root=args.artifacts, level=args.level, limit=args.limit)
    print(json.dumps(res, indent=2))
    return 0 if res.get("count") else 1


if __name__ == "__main__":
    raise SystemExit(main())
