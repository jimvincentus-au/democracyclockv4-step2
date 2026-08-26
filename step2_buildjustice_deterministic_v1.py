#!/usr/bin/env python3
"""
Deterministic DOJ statement-act event builder — ZERO LLM calls.

Companion to step2_getjustice_v1.py. Same reasoning as the White House, Defense and
Federal Register deterministic builders: the announcement IS the event, and its
identity (title, date, canonical URL, uuid) is already in the harvested filtered file.

THE SPEECH-ACT WRAPPER, AND WHY DOJ NEEDS IT MOST
    DOJ is simultaneously prosecutor, litigant, legal adviser, policymaker and the
    public narrator of its own conduct. Its release titles are written as findings:
        "Justice Department Finds Duke Law School Discriminates Based on Race"
        "Amazon Agrees to $2.25 Million Settlement…"
    Copying such a title into `action` would enter DOJ's characterisation as archive
    fact. Every event is therefore

        actor  = "The Department of Justice"
        action = announced "<verbatim title, quoted>"

    The verb never comes from the title, and the title is never paraphrased.

LEGAL STAGE — carried onto the event, not left in the harvest
    A DOJ announcement can describe an investigation, allegation, charge, plea,
    conviction, sentence, settlement or ruling. These are different evidentiary
    states and must never be flattened. `legal_stage` (SED-045's judicial chain,
    refined for enforcement) travels onto the event, and `allegations_present` marks
    unadjudicated claims STRUCTURALLY.

    DOJ prints a presumption-of-innocence line in most releases. That protection is
    enforced by the field, never by copying the disclaimer into prose — a prose-only
    constraint is a false promise (Ruling A).

        documented: "DOJ announced a grand jury returned an indictment charging X"
        NOT documented: "X committed the crimes described in the indictment"

INPUT   {artifacts}/json/justice_filtered_{start}_{end}.json
OUTPUT  {artifacts}/eventjson/justice_events_{start}_{end}.json
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

SOURCE_KEY = "justice"
BUILDER_VERSION = "justice_deterministic_v1"
ACTOR = "The Department of Justice"

# Stages that describe an unadjudicated allegation. Kept here as well as in the
# getter so the builder cannot emit an event whose allegation status is unset.
UNADJUDICATED = {"investigation_announced", "complaint_filed", "indictment_returned",
                 "information_filed", "arrest_made"}

# Component -> writer category (must be in CATEGORY_ORDER, step2_writeweekevents_v4.py).
# Tested in order; first match wins.
_COMPONENT_CATEGORY = [
    # Civil Rights Division spans voting, housing, education and disability — only
    # the Voting Section is an elections matter; a blanket mapping put a law-school
    # admissions case under Elections & Representation.
    ("Civil Rights - Voting",        "Elections & Representation"),
    ("Voting Section",               "Elections & Representation"),
    ("Civil Rights",                 "Civil Society & Protest"),
    ("Office of Legal Counsel",      "Executive Actions & Orders"),
    ("Office of the Pardon Attorney","Executive Actions & Orders"),
    ("Office of the Attorney General","Executive Actions & Orders"),
    ("Office of the Deputy Attorney General", "Executive Actions & Orders"),
    ("National Security Division",   "Law Enforcement & Surveillance"),
    ("Antitrust Division",           "Economic & Regulatory Power"),
    ("Environment and Natural Resources", "Economic & Regulatory Power"),
    ("Executive Office for Immigration Review", "Judicial Developments"),
    ("Office of Justice Programs",   "Economic & Regulatory Power"),
]
# Any enforcement stage that has reached a court outranks component routing.
_JUDICIAL_STAGES = {"complaint_filed", "indictment_returned", "information_filed",
                    "plea_entered", "conviction_after_trial", "sentence_imposed",
                    "judgment_entered", "case_dismissed", "appeal_filed",
                    "consent_decree_entered", "consent_decree_proposed"}
# Pre-court enforcement. Routed to law enforcement regardless of which component
# announced it: an arrest announced by the AG's office is still an arrest, not an
# executive action.
_ENFORCEMENT_STAGES = {"arrest_made", "investigation_announced"}
DEFAULT_CATEGORY = "Law Enforcement & Surveillance"

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
logger = logging.getLogger("dc.build.justice")


def _iso_or_none(s: str) -> Optional[str]:
    s = (s or "").strip()[:10]
    return s if _ISO.match(s) else None


def _quote(s: str) -> str:
    return '"' + (s or "").strip().replace('"', "'") + '"'


def _route_category(components: List[str], stage: Optional[str]) -> str:
    """Court-reaching enforcement is judicial; otherwise route by component."""
    if stage in _JUDICIAL_STAGES:
        return "Judicial Developments"
    if stage in _ENFORCEMENT_STAGES:
        return "Law Enforcement & Surveillance"
    for needle, cat in _COMPONENT_CATEGORY:
        if any(needle in c for c in components):
            return cat
    return DEFAULT_CATEGORY


_STAGE_PHRASE = {
    "investigation_announced": "an announced investigation",
    "complaint_filed": "a complaint filed",
    "indictment_returned": "an indictment returned",
    "information_filed": "an information filed",
    "arrest_made": "an arrest",
    "plea_entered": "a plea entered",
    "conviction_after_trial": "a conviction after trial",
    "sentence_imposed": "a sentence imposed",
    "judgment_entered": "a judgment entered",
    "case_dismissed": "a case dismissed",
    "appeal_filed": "an appeal filed",
    "consent_decree_entered": "a consent decree entered",
    "consent_decree_proposed": "a consent decree proposed",
}


# Longest verbatim extract carried into a summary. DOJ teasers run to a median of
# 339 characters; this bounds the tail without truncating most of them.
_VERBATIM_MAX = 420


def _verbatim_clause(text: str, attribution: str) -> str:
    """
    Append DOJ's OWN WORDS, quoted and attributed. Added 2026-08-26. Identical
    rationale to step2_builddefense_deterministic_v1._verbatim_clause.

    WHY THE QUOTATION MARKS ARE THE WHOLE POINT.
        This channel is the government's account of itself — `never_corroborates`
        is already True on every record. The teaser gives Step 3's trait sensor and
        Step 7's appendix selector something to judge, and both read the summary.
        It must never be mistakable for the chronicle's own assertion, so it is
        quoted and attributed inside the same sentence.

    THIS MATTERS MORE HERE THAN ANYWHERE ELSE IN THE PIPELINE.
        A DOJ teaser routinely describes conduct alleged against a named private
        individual. The stage sentence built below already marks unadjudicated
        allegations as unadjudicated; the quotation marks make the second, separate
        point that the account itself is the Department's. Both are required — one
        without the other would state an accusation in the chronicle's voice.

    No model is involved; the text is copied, bounded and quoted.
    """
    t = " ".join((text or "").split())
    if not t:
        return ""
    if len(t) > _VERBATIM_MAX:
        cut = t[:_VERBATIM_MAX]
        stop = max(cut.rfind(". "), cut.rfind("? "), cut.rfind("! "))
        if stop > _VERBATIM_MAX // 2:
            t = cut[:stop + 1]
        else:
            sp = cut.rfind(" ")
            t = (cut[:sp] if sp > 0 else cut) + " …"
    # Inner double quotes would break the outer quotation; single them. Curly
    # pairs must be included — DOJ teasers routinely open with one, which
    # otherwise renders as a doubled quote mark ("“Today the Department...).
    t = t.translate(str.maketrans({'"': "'", "“": "'", "”": "'"})).strip()
    return f' {attribution}: "{t}"'


def _act_summary(e: Dict[str, Any], source_date: str, stage: Optional[str],
                 comps: List[str]) -> str:
    """
    A TEMPLATED, ACT-DESCRIPTIVE summary. Added 2026-08-26. Same rationale as the
    White House and Defense builders: measured that day, an event with an empty
    `summary` is invisible to Step 3's trait sensor and Step 7's appendix selector,
    so all 551 DOJ events were harvested, built, written and then silently dropped.

    DOJ ALSO CARRIES THE LEGAL STAGE, AND THAT BELONGS HERE.
        The stage is the difference between an allegation and an adjudication, and
        it is the one thing a downstream reader most needs and is most likely to
        lose. Where the stage is unadjudicated this sentence says so explicitly —
        structurally, not as a disclaimer copied from DOJ's own boilerplate.

    Contains only fields already held: who announced, when, which component, and
    the recorded stage. It never describes, paraphrases or endorses the conduct
    alleged, and no model is involved.
    """
    when = source_date or "an unrecorded date"
    lead = f"{ACTOR} announced this on {when}"
    if comps:
        lead += f" through {comps[0]}"
    lead += "."

    out = lead
    if stage:
        phrase = _STAGE_PHRASE.get(stage, stage.replace("_", " "))
        out += f" The recorded stage is {phrase}."
        if stage in UNADJUDICATED:
            out += (" The allegations are unadjudicated; this documents the "
                    "announcement, not the truth of any allegation.")

    # The quotation goes LAST, after the unadjudicated warning, so no reader
    # meets the Department's account of the conduct before the caveat on it.
    out += _verbatim_clause(e.get("doj_teaser_verbatim"),
                            "The department's own account of it reads")
    return out


def _event_from_entity(e: Dict[str, Any]) -> Dict[str, Any]:
    title = (e.get("title") or "").strip()
    url = (e.get("canonical_url") or e.get("url") or "").strip()
    source_date = _iso_or_none(e.get("post_date") or "") or ""
    stage = e.get("legal_stage")
    comps = e.get("doj_components") or []
    verb = (e.get("speech_act_verb") or "announced").strip()

    # THE WRAPPER. actor+action describe the act of announcing; DOJ's own
    # characterisation is quoted, never asserted. The writer derives
    # act_key = normalize(actor)|normalize(action)|date from these.
    action = f"{verb} {_quote(title)}"

    return {
        "source_date": source_date,
        "occurred_on": source_date,
        "title": title,
        "url": url,
        # Templated act description (2026-08-26). The teaser remains DOJ's own
        # account and is still never rendered as our prose — this sentence describes
        # only the act of announcement plus the legal stage. See _act_summary.
        "summary": _act_summary(e, source_date, stage, comps),
        "why_relevant": "",
        "category": _route_category(comps, stage),
        "sources": [url] if url else [],
        "tags": [SOURCE_KEY] + ([stage] if stage else []),
        "attacks": [],
        "source": SOURCE_KEY,
        # act_key inputs — explicit, never inferred downstream
        "actor": ACTOR,
        "action": action,
        "jurisdiction": "federal",
        "federal_nexus": "Official DOJ channel; documents the act of announcement only.",
        # High confidence THAT IT WAS ANNOUNCED — never in the content of the claim.
        "confidence": "high",
        "basis": "official_actor_record",
        # legal-stage discipline
        "legal_stage": stage,
        "allegations_present": bool(stage in UNADJUDICATED),
        "certified_proposition": ("announcement_and_procedural_act" if stage
                                  else "announcement_and_content"),
        # provenance + constraints carried from the harvest
        "doj_uuid": e.get("doj_uuid", ""),
        "doj_components": comps,
        "doj_teaser_verbatim": e.get("doj_teaser_verbatim", ""),
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
        if not (e.get("title") or "").strip() or not (
                e.get("canonical_url") or e.get("url") or "").strip():
            noncompliant.append({"idx": i, "reason": "missing_title_or_url"})
            continue
        events.append(_event_from_entity(e))

    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    out_path = artifacts / "eventjson" / f"{SOURCE_KEY}_events_{start}_{end}.json"
    out_path.write_text(json.dumps({
        "source": SOURCE_KEY,
        "window": {"start": start, "end": end},
        "builder": BUILDER_VERSION,
        "llm_calls": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_constraints": {"trust_basis": "official_actor_record",
                               "never_corroborates": True,
                               "proposition_scope": "own_acts_and_statements"},
        "ruling": "Official actor record. Documents that DOJ announced this, in those "
                  "words, on that date. Does not establish the truth of any allegation. "
                  "legal_stage prevents a charge being compressed into a conviction; "
                  "allegations_present marks unadjudicated claims structurally rather "
                  "than by disclaimer. Never corroborates.",
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
    ap = argparse.ArgumentParser(description="Deterministic DOJ statement-act event builder (no LLM)")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--artifacts", default=str(DEFAULT_ARTIFACTS))
    ap.add_argument("--level", default="INFO")
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    res = run_builder(start=a.start, end=a.end, artifacts_root=a.artifacts,
                      level=a.level, limit=a.limit)
    print(json.dumps(res, indent=2))
    return 0 if res.get("count") else 1


if __name__ == "__main__":
    raise SystemExit(main())
