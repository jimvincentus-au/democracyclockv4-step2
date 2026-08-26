#!/usr/bin/env python3
"""
Deterministic White House STATEMENT-ACT event builder — ZERO LLM calls.

WHY DETERMINISTIC
    Same reasoning as step2_build_fr_presdocs_deterministic_v1.py: the document IS
    the event. A White House release, fact sheet, executive order or proclamation is
    one act, already identified by its official title, its date and its canonical URL
    in the harvested filtered file. Running an extraction model over it would add
    cost and non-determinism for nothing.

WHY THE SPEECH-ACT WRAPPER IS NOT OPTIONAL
    This is the difference between this builder and the Federal Register one, and it
    is the whole safety mechanism.

    For the Federal Register, the title IS the act: "Declaring a National Emergency
    at the Southern Border" describes what was done, so title -> action is safe.

    White House titles are written as accomplished facts about the world:
        "President Trump Delivers Largest Drop in Violent Crime in American History"
        "Manufacturing Jobs Flock to the U.S. Thanks to President Trump"
        "President Trump's Trade Agenda Is Rebuilding the American Auto Industry"
    Copying such a title into `action` would enter the administration's claim into the
    archive as fact. That is precisely what the withdrawn "TOXIC" ruling existed to
    prevent, and what the 2026-08-25 ruling permits harvesting only because the act,
    not the claim, is what gets recorded.

    So every event is emitted as:
        actor  = "The White House"
        action = <verb from the harvest channel> "<title verbatim, in quotes>"
    The verb comes from the channel (published a release titled / issued a fact sheet
    titled / announced an executive order titled). It NEVER comes from the title, and
    the title is quoted, never paraphrased.

WHAT THIS BUILDER DOES NOT DO
    - It does not assert that anything in the statement is true.
    - It does not write a summary. A summary of a propaganda release is that claim
      restated in our voice; the FR presdoc builder leaves summary empty for the same
      structural reason and these events likewise merge onto news-sourced events in
      Step 3 grouping, inheriting their prose.
    - It does not perform the Federal Register join. Matching an announced executive
      order or proclamation to its FR record (and flagging an FR-expected instrument
      that never appeared) is a separate deterministic step, so that the announcement
      stage and the published record remain distinguishable per SED-045.

═══════════════════════════════════════════════════════════════════════════════
VERSION HISTORY
═══════════════════════════════════════════════════════════════════════════════
v1 (2026-08-25)
    Every harvested item carried an instrument type, because the getter discovered
    each presidential action through its typed subcategory listing.

v2 (2026-08-26) — HANDLES THE UNTYPED PRESIDENTIAL ACTION
    getwhitehouse v2 discovers from the sitemap, whose permalink carries only the
    family (/presidential-actions/), not the instrument. Most such items are typed
    from the Federal Register; some cannot be, and arrive as
    wh_instrument_type="presidential_action" with wh_typed_by="unresolved".

    Under v1 that value fell through CATEGORY_BY_INSTRUMENT to DEFAULT_CATEGORY —
    "Information & Media Control". Measured on week 5 (2025-02-15..21), that would
    have filed EO 14219, the DOGE deregulatory order, as media control, because the
    White House and the Federal Register publish that instrument under two different
    official titles ("…Regulatory Initiative" vs "…Deregulatory Initiative") and the
    exact-title join therefore, correctly, refused to match it.

    A default that silently miscategorises the hardest cases is the worst possible
    default: the items least likely to be typed are the contested ones. v2 routes an
    untyped presidential action to "Executive Actions & Orders" — what is actually
    known about it — and carries wh_typed_by onto the event so a reader can tell an
    authoritatively typed instrument from an unresolved one.

    v2 also carries the Federal Register provenance the getter now resolves
    (executive_order_number, fr_document_number, fr_signing_date) onto the event, so
    the announcement stage and the published record stay linked without re-querying.

INPUT   {artifacts}/json/whitehouse_filtered_{start}_{end}.json      (from the getter)
OUTPUT  {artifacts}/eventjson/whitehouse_events_{start}_{end}.json
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ARTIFACTS = BASE_DIR / "artifacts"

SOURCE_KEY = "whitehouse"
BUILDER_VERSION = "whitehouse_deterministic_v2"

ACTOR = "The White House"

# Instrument -> writer category (must be a member of CATEGORY_ORDER in
# step2_writeweekevents_v4.py).
CATEGORY_BY_INSTRUMENT = {
    "executive_order": "Executive Actions & Orders",
    "proclamation":    "Executive Actions & Orders",
    "memorandum":      "Executive Actions & Orders",
    "nominations":     "Appointments & Patronage",
    "release":         "Information & Media Control",
    "fact_sheet":      "Information & Media Control",
    "statement":       "Information & Media Control",
    # An action the White House filed under /presidential-actions/ whose instrument
    # could not be established from the source taxonomy or the Federal Register.
    # The instrument is unknown; that it is an executive action is not.
    "presidential_action": "Executive Actions & Orders",
}
DEFAULT_CATEGORY = "Information & Media Control"

# Fallback verbs, used only if the getter did not stamp speech_act_verb (older
# artifact). Keeping them here means the wrapper can never silently degrade into
# emitting a bare title.
FALLBACK_VERB = {
    "executive_order": "announced an executive order titled",
    "proclamation":    "announced a proclamation titled",
    "memorandum":      "announced a presidential memorandum titled",
    "nominations":     "published a nominations announcement titled",
    "release":         "published a release titled",
    "fact_sheet":      "issued a fact sheet titled",
    "statement":       "issued a statement titled",
    # Deliberately vague: says only what is known — that a presidential action was
    # announced under this title on this date.
    "presidential_action": "announced a presidential action titled",
}
GENERIC_VERB = "published a document titled"

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

logger = logging.getLogger("dc.build.whitehouse")


def _iso_or_none(s: str) -> Optional[str]:
    s = (s or "").strip()[:10]
    return s if _ISO.match(s) else None


def _quote(title: str) -> str:
    """Quote the title verbatim, normalising any embedded quotes so the wrapper reads cleanly."""
    t = (title or "").strip().replace('"', "'")
    return f'"{t}"'


_CHANNEL_PHRASE = {
    "executive_orders": "its executive-orders channel",
    "proclamations": "its proclamations channel",
    "presidential_memoranda": "its presidential-memoranda channel",
    "nominations_appointments": "its nominations-and-appointments channel",
    "fact_sheets": "its fact-sheets channel",
    "releases": "its releases channel",
    "briefings_statements": "its briefings-and-statements channel",
}


def _act_summary(e: Dict[str, Any], source_date: str, instrument: str) -> str:
    """
    A TEMPLATED, ACT-DESCRIPTIVE summary. Added 2026-08-26.

    WHY THIS EXISTS, AND WHY IT IS NOT A SUMMARY OF THE CONTENT
        This builder deliberately left `summary` empty: summarising a release
        titled "President Trump Delivers Largest Drop in Violent Crime in American
        History" would restate that CLAIM in our voice. That reasoning stands and
        nothing here softens it — this sentence never touches what was asserted.

        But measured 2026-08-26, the empty field had a consequence nobody intended.
        Step 3's trait sensor and Step 7's appendix selector both judge an event
        from its content, so an event with no summary is invisible to both:

            source            n   summary  attacks  trait  selected
            federalregister  29        29        6      4        16
            whitehouse       27         0        0      0         0

        Federal Register events carry summaries and almost no attacks, and 55% of
        them are still selected. The summary — not the attack tags — is the gate.
        With this field blank, every official-channel event was harvested, built,
        written, trait-scored and then silently discarded at Step 7.

    WHAT IT MAY CONTAIN
        Only facts this builder already holds as fields: who published, on what
        date, through which channel, as what instrument, and — where the Federal
        Register join resolved — the instrument's citation of record. Every clause
        is about the ACT OF PUBLICATION. Nothing describes, paraphrases, endorses
        or evaluates the content, and no model is involved.
    """
    who = ACTOR
    when = source_date or "an unrecorded date"
    channel = _CHANNEL_PHRASE.get(e.get("wh_channel") or "", "its official channel")

    if instrument == "presidential_action":
        # Instrument unresolved — say exactly that rather than implying a type.
        lead = (f"{who} published this presidential action on {when} through "
                f"{channel}. Its instrument type could not be established from the "
                f"source's own taxonomy or the Federal Register.")
    else:
        readable = instrument.replace("_", " ")
        lead = f"{who} published this {readable} on {when} through {channel}."

    eo = e.get("executive_order_number")
    fr_date = (e.get("fr_publication_date") or "").strip()
    fr_sub = (e.get("fr_subtype") or "").strip()
    if eo:
        tail = (f" The Federal Register published it as Executive Order {eo}"
                + (f" on {fr_date}." if fr_date else "."))
    elif fr_sub and fr_date:
        tail = f" The Federal Register published it as a {fr_sub.lower()} on {fr_date}."
    else:
        tail = ""

    return lead + tail


def _event_from_entity(e: Dict[str, Any]) -> Dict[str, Any]:
    title = (e.get("title") or "").strip()
    url = (e.get("canonical_url") or e.get("url") or "").strip()
    source_date = _iso_or_none(e.get("post_date") or "") or ""
    instrument = (e.get("wh_instrument_type") or "").strip()

    verb = (e.get("speech_act_verb") or FALLBACK_VERB.get(instrument) or GENERIC_VERB).strip()

    # THE WRAPPER. actor+action describe the act of publishing; the claim is quoted.
    # The writer computes act_key = normalize(actor)|normalize(action)|date from these,
    # so this wording is also the event's cross-source identity.
    action = f"{verb} {_quote(title)}"

    return {
        "source_date": source_date,
        "occurred_on": source_date,
        "title": title,
        "url": url,
        # Empty by design — see module docstring.
        "summary": _act_summary(e, source_date, instrument),
        "why_relevant": "",
        "category": CATEGORY_BY_INSTRUMENT.get(instrument, DEFAULT_CATEGORY),
        "sources": [url] if url else [],
        "tags": [SOURCE_KEY, instrument] if instrument else [SOURCE_KEY],
        "attacks": [],
        "source": SOURCE_KEY,
        # act_key inputs — explicit, never inferred downstream
        "actor": ACTOR,
        "action": action,
        # This source documents its own conduct; it is never evidence about the world.
        "jurisdiction": "federal",
        "federal_nexus": "Official White House channel; documents the act of publication only.",
        "confidence": "high",          # high confidence THAT IT WAS PUBLISHED — not in its content
        "basis": "official_actor_record",
        # provenance + constraints carried from the harvest so nothing downstream
        # can lose them (a prose-only constraint is a false promise — Ruling A)
        "wh_channel": e.get("wh_channel", ""),
        "wh_instrument_type": instrument,
        # Which surface established the instrument: listing_subcategory (the site's
        # own taxonomy), federal_register (subtype via signing date + exact title),
        # url_family, or unresolved. Carried so a downstream reader can distinguish
        # an authoritatively typed instrument from one we declined to guess at.
        "wh_typed_by": e.get("wh_typed_by", ""),
        "fr_publication_expected": e.get("fr_publication_expected"),
        # Federal Register provenance, when the join resolved. Keeps the announcement
        # stage linked to the published record without re-querying (SED-045).
        "executive_order_number": e.get("executive_order_number"),
        "fr_document_number": e.get("fr_document_number", ""),
        # The FR's own subtype string. Carried alongside wh_instrument_type because
        # the two can disagree in principle, and a reader auditing a residual match
        # needs to see what the Federal Register actually called the document.
        "fr_subtype": e.get("fr_subtype", ""),
        "fr_signing_date": e.get("fr_signing_date", ""),
        "fr_publication_date": e.get("fr_publication_date", ""),
        "fr_url": e.get("fr_url", ""),
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
        events.append(_event_from_entity(e))

    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    out_path = artifacts / "eventjson" / f"{SOURCE_KEY}_events_{start}_{end}.json"
    out_path.write_text(json.dumps({
        "source": SOURCE_KEY,
        "window": {"start": start, "end": end},
        "builder": BUILDER_VERSION,
        "llm_calls": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_constraints": {
            "trust_basis": "official_actor_record",
            "never_corroborates": True,
            "proposition_scope": "own_acts_and_statements",
        },
        "ruling": "AUTHOR RULING 2026-08-25 — supersedes the 2026-08-13 TOXIC ruling. "
                  "Records the act of publication, never the truth of the content. "
                  "Never corroborates. Executive orders, proclamations and memoranda "
                  "are cited from the Federal Register.",
        "events": events,
        "noncompliant": noncompliant,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote %s (events=%d, noncompliant=%d, llm_calls=0)",
                out_path, len(events), len(noncompliant))
    return {
        "source": SOURCE_KEY,
        "count": len(events),
        "events_path": str(out_path),
        "input_json": str(in_path),
    }


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
        description="Deterministic White House statement-act event builder (no LLM)")
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
