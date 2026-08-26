#!/usr/bin/env python3
"""
White House builder v3 — EXTRACTOR MODE.

WHY THIS EXISTS (2026-08-26)
    v2 is a deterministic, zero-LLM builder. It was written that way to stop an
    extractor restating the administration's claims about itself as established
    fact -- a real risk, demonstrated the same day: run without a system prompt,
    the extractor asserted White House claims as fact.

    But bypassing the extractor also bypassed the Canonical Extraction Protocol,
    so these events arrived with `why_relevant` and `attacks` empty while all
    thirteen other sources had them populated at ~100%. Measured on week 5:
    whitehouse 27 events, why_relevant 0, attacks 0. They were then invisible to
    Step 3's trait sensor and Step 7's appendix selector, both of which judge from
    content. Harvested, built, written, silently dropped.

    Every other source -- including federalregister, which is also the government
    publishing its own acts -- goes through the extractor and comes out the same
    shape. The correct home for a source-specific handling rule is a PREFACE, not
    a bypass. PREFACE_WHITEHOUSE (step2_prompts_v4.py) carries the speech-act rule:

        THE ACT IS THE ISSUANCE. The claim is attributed, never adopted.

    plus ONE-ACT-PER-RECORD, so a promotional fact sheet still yields its event.

VALIDATED BEFORE THIS FILE WAS WRITTEN (14 week-5 records, 2026-08-26)
    • why_relevant  14/14   (was 0)
    • summary       14/14
    • attacks       14/14 present, 12 non-empty
    • zero records declined for being promotional
    • 9 fact sheets re-routed out of "Information & Media Control" into the domain
      of the act they promote -- the same correction hand-patched into v2 that
      morning, now produced by instruction rather than a lookup table.
    • Lambkin test passed: for "Fact Sheet: ... Reins in Government Overreach and
      Begins Deconstruction of the Administrative State" the extractor wrote
      "The White House published a fact sheet ... describing an executive order
      ... The order directs agency heads ... to review regulations". The framing
      is not adopted; the operative direction is described plainly.

WHAT v2 STILL OWNS
    Nothing in the harvest changes. step2_getwhitehouse_v2.py keeps sitemap-first
    discovery, three-tier instrument typing and Federal Register matching. v2
    remains on disk, unmodified, and is still the fallback if this path is
    unavailable.

    NOTE, measured 2026-08-26: the writer normalises every event to a fixed
    22-field shape and drops the rest. wh_instrument_type, executive_order_number,
    fr_subtype, fr_url, never_corroborates and proposition_scope do NOT survive
    into the Master Event Log today -- under v2 either. They are still emitted
    here so the value exists at the builder boundary, and so that a future schema
    change has something to carry, but nothing downstream reads them yet. That is
    a separate open question, not a regression introduced by this file.

INPUT   {artifacts}/json/whitehouse_filtered_{start}_{end}.json
OUTPUT  {artifacts}/eventjson/whitehouse_events_{start}_{end}.json
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_builder_helper_v4 import parse_llm_events_canonical
from step2_extractor_v4 import extract_events_from_text, extract_events_from_url
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt

# Identity is stamped from the harvest, not from the model -- see _stamp_identity.
# Reusing v2's own table rather than restating it keeps one definition of the verb.
from step2_buildwhitehouse_deterministic_v2 import (
    ACTOR as WH_ACTOR,
    FALLBACK_VERB,
    GENERIC_VERB,
    _quote,
)

SOURCE_KEY = "whitehouse"
BUILDER_VERSION = "whitehouse_extractor_v3"

logger = logging.getLogger("dc.build.whitehouse")


def _stamp_identity(ev: Dict[str, Any], e: Dict[str, Any]) -> None:
    """
    Overwrite the model's `actor` and `action` with the harvest's own.

    WHY (measured 2026-08-26, week 5, 13 events)
        `act_key` = normalize(actor)|normalize(action)|date is the ARCHIVE JOIN
        KEY (step_pre8_v1.py:63) and the pipeline ships a dedicated tool,
        act_key_reproducibility_compare_v1.py, to check it reproduces across runs.

        Left to the model, the canonical 2-5 word action lemma cannot distinguish
        two instruments signed the same day. Five of the thirteen week-5 events
        collapsed into two keys:

            trump|signed executive order|2025-02-19   x3
            trump|signed executive order|2025-02-18   x2

        Three unrelated fact sheets -- regulatory review, bureaucracy reduction,
        benefits eligibility -- became one act. The deterministic builder produced
        27 unique keys for 27 events because it embeds the verbatim title. An
        LLM-authored lemma is also not reproducible run to run, which the
        reproducibility tool exists to catch.

    THE SPLIT THIS ENFORCES
        Identity fields (actor, action) assert WHAT HAPPENED -- deterministic, so
        they can neither drift nor launder a claim. Assessment fields (summary,
        why_relevant, attacks, category) judge WHY IT MATTERS -- from the model,
        because no template can honestly produce them.

        This is the same distinction that put the speech-act rule in the preface
        rather than in a bypass. The emitted record is still the standard shape;
        nothing downstream learns that this channel exists.
    """
    title = (e.get("title") or "").strip()
    instrument = (e.get("wh_instrument_type") or "").strip()
    verb = (e.get("speech_act_verb") or FALLBACK_VERB.get(instrument) or GENERIC_VERB).strip()
    ev["actor"] = WH_ACTOR
    ev["action"] = f"{verb} {_quote(title)}"


def _synthetic_text(e: Dict[str, Any]) -> str:
    """
    Fallback input when the live page cannot be fetched.

    Deliberately thin: title + instrument type + Federal Register confirmation.
    The preface can still identify the act of issuance from this, and the
    protocol's own threshold decides whether that is enough. It is NOT padded to
    force an event through -- an extractor refusing a record it cannot read is
    the correct outcome, and measured on Defense the same day, exactly what
    happens when only a standfirst is available.
    """
    parts = [(e.get("title") or "").strip()]
    inst = (e.get("wh_instrument_type") or "").replace("_", " ").strip()
    chan = (e.get("wh_channel") or "").replace("_", " ").strip()
    if inst:
        parts.append(f"Instrument type: {inst}.")
    if chan:
        parts.append(f"Published through the {chan} channel.")
    if e.get("executive_order_number"):
        parts.append(f"Published in the Federal Register as Executive Order "
                     f"{e['executive_order_number']}"
                     + (f" on {e.get('fr_publication_date')}." if e.get("fr_publication_date") else "."))
    elif (e.get("fr_subtype") or "").strip():
        parts.append(f"Published in the Federal Register as a {e['fr_subtype'].lower()}"
                     + (f" on {e.get('fr_publication_date')}." if e.get("fr_publication_date") else "."))
    return "\n\n".join(p for p in parts if p)


def _provenance(e: Dict[str, Any]) -> Dict[str, Any]:
    """Harvest-side provenance, carried verbatim from the filtered entity."""
    return {
        "wh_channel": e.get("wh_channel", ""),
        "wh_instrument_type": e.get("wh_instrument_type", ""),
        "wh_typed_by": e.get("wh_typed_by", ""),
        "executive_order_number": e.get("executive_order_number", ""),
        "fr_document_number": e.get("fr_document_number", ""),
        "fr_subtype": e.get("fr_subtype", ""),
        "fr_signing_date": e.get("fr_signing_date", ""),
        "fr_publication_date": e.get("fr_publication_date", ""),
        "fr_url": e.get("fr_url", ""),
        "trust_basis": e.get("trust_basis", "official_actor_record"),
        "never_corroborates": e.get("never_corroborates", True),
        "proposition_scope": e.get("proposition_scope", "own_acts_and_statements"),
    }


def build(artifacts: Path, start: str, end: str, limit: Optional[int] = None,
          dry_run: bool = False) -> Dict[str, Any]:
    in_path = artifacts / "json" / f"{SOURCE_KEY}_filtered_{start}_{end}.json"
    if not in_path.exists():
        logger.error("Filtered input not found: %s", in_path)
        return {"source": SOURCE_KEY, "count": 0, "events_path": None,
                "input_json": str(in_path)}

    entities: List[Dict[str, Any]] = json.loads(in_path.read_text(encoding="utf-8")).get("entities") or []
    if limit:
        entities = entities[:limit]
    logger.info("Loaded %d entities from %s", len(entities), in_path.name)

    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    system_prompt = compose_system_prompt(SOURCE_KEY, include_attacks=True)
    (artifacts / "log" / f"{SOURCE_KEY}_prompt_{start}_{end}.txt").write_text(
        system_prompt, encoding="utf-8")
    logger.info("System prompt: %d chars (PREFACE_WHITEHOUSE + attacks + canonical)",
                len(system_prompt))

    if dry_run:
        logger.info("Dry run: %d entities would be extracted; no LLM calls made.", len(entities))
        return {"source": SOURCE_KEY, "count": 0, "events_path": None,
                "input_json": str(in_path), "dry_run": True}

    all_events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []
    llm_calls = 0

    for i, e in enumerate(entities, 1):
        title = (e.get("title") or "").strip()
        url = (e.get("canonical_url") or e.get("url") or "").strip()
        post_date = (e.get("post_date") or start)[:10]
        logger.info("[whitehouse] %d/%d %s", i, len(entities), title[:80])

        try:
            if url:
                raw = extract_events_from_url(
                    url, system_prompt=system_prompt, article_title=title,
                    article_date=post_date, source_hint=SOURCE_KEY,
                    artifacts_root=str(artifacts), idx=i,
                )
            else:
                raw = extract_events_from_text(
                    _synthetic_text(e), system_prompt=system_prompt,
                    article_title=title, article_date=post_date,
                    artifacts_root=str(artifacts), idx=i,
                )
            llm_calls += 1
        except Exception as exc:  # noqa: BLE001 -- one bad page must not end the run
            logger.exception("Extractor failed on idx=%d url=%s", i, url)
            noncompliant.append({"idx": i, "url": url, "reason": f"extractor_error: {exc}"})
            continue

        evs = parse_llm_events_canonical(raw, article_url=url, logger=logger)
        if not evs:
            # The record produced no canonical block. Usually the page could not
            # be read; occasionally the protocol's threshold was not met. Either
            # way it is recorded, never silently discarded.
            noncompliant.append({"idx": i, "url": url, "reason": "no_blocks",
                                 "title": title})
            logger.warning("No canonical block for idx=%d: %s", i, title[:80])
            continue

        prov = _provenance(e)
        for ev in evs:
            ev.setdefault("source", SOURCE_KEY)
            ev.setdefault("attacks", [])
            # Identity from the harvest, assessment from the model.
            _stamp_identity(ev, e)
            ev["tags"] = [SOURCE_KEY] + ([prov["wh_instrument_type"]] if prov["wh_instrument_type"] else [])
            ev["builder_version"] = BUILDER_VERSION
            ev.update(prov)
            if not (ev.get("url") or "").strip():
                ev["url"] = url
                ev.setdefault("sources", [url] if url else [])
            all_events.append(ev)

    out_path = artifacts / "eventjson" / f"{SOURCE_KEY}_events_{start}_{end}.json"
    out_path.write_text(json.dumps({
        "source": SOURCE_KEY,
        "window": {"start": start, "end": end},
        "builder_version": BUILDER_VERSION,
        "events": all_events,
        "noncompliant": noncompliant,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote %s (events=%d, noncompliant=%d, llm_calls=%d)",
                out_path, len(all_events), len(noncompliant), llm_calls)
    return {"source": SOURCE_KEY, "count": len(all_events),
            "events_path": str(out_path), "input_json": str(in_path),
            "noncompliant": len(noncompliant), "llm_calls": llm_calls}


def main() -> int:
    p = argparse.ArgumentParser(description="White House builder v3 (extractor mode)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--level", default="INFO")
    a = p.parse_args()
    setup_logger("dc.build.whitehouse", a.level)
    res = build(Path(a.artifacts), a.start, a.end, limit=a.limit, dry_run=a.dry_run)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
