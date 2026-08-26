#!/usr/bin/env python3
"""
Justice builder v2 — EXTRACTOR MODE.

Mirrors step2_buildwhitehouse_v3.py exactly; see that file's header for the full
rationale. In short: the deterministic v1 bypassed the Canonical Extraction
Protocol, so DOJ events arrived with `why_relevant` and `attacks` empty while all
thirteen other sources had them at ~100%, and were then invisible to Step 3's
trait sensor and Step 7's appendix selector. PREFACE_JUSTICE now carries the
source-specific rule inside the common path:

    THE ACT IS THE ANNOUNCEMENT. An announcement is not an adjudication.
    AN ALLEGATION IS NOT A FINDING.

IDENTITY IS STAMPED FROM THE HARVEST, NOT THE MODEL
    Same reason as White House: `act_key` = normalize(actor)|normalize(action)|
    date is the archive join key (step_pre8_v1.py:63), and the canonical 2-5 word
    action lemma cannot keep two same-day announcements apart. Measured on week 5
    for whitehouse, five of thirteen events collapsed into two keys. Identity
    fields (actor, action) are therefore deterministic; assessment fields
    (summary, why_relevant, attacks, category) come from the model.

NO PRE-EXTRACTION SCOPE FILTER
    A keyword filter was written and rejected on 2026-08-26. Best case it dropped
    43% -- but among the drops was "Federal Grand Jury Charges Southern Poverty
    Law Center for Wire Fraud", which is lexically identical to an ordinary fraud
    case and politically the opposite. The signal is who the defendant is, which
    is world knowledge, not vocabulary. The Canonical Protocol's own relevance
    test is the filter, and it is the same filter every other source gets.
    step2_justice_scope_v1.py remains on disk, unused, as the record of that.

INPUT   {artifacts}/json/justice_filtered_{start}_{end}.json
OUTPUT  {artifacts}/eventjson/justice_events_{start}_{end}.json
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

# One definition of actor/verb: reuse the deterministic builder's own.
from step2_buildjustice_deterministic_v1 import ACTOR as DOJ_ACTOR, UNADJUDICATED, _quote

SOURCE_KEY = "justice"
BUILDER_VERSION = "justice_extractor_v2"

logger = logging.getLogger("dc.build.justice")


def _stamp_identity(ev: Dict[str, Any], e: Dict[str, Any]) -> None:
    """Overwrite the model's actor/action with the harvest's. See module header."""
    title = (e.get("title") or "").strip()
    verb = (e.get("speech_act_verb") or "announced").strip()
    ev["actor"] = DOJ_ACTOR
    ev["action"] = f"{verb} {_quote(title)}"


def _provenance(e: Dict[str, Any]) -> Dict[str, Any]:
    stage = (e.get("legal_stage") or "").strip()
    return {
        "doj_uuid": e.get("doj_uuid", ""),
        "doj_components": e.get("doj_components") or [],
        "legal_stage": stage,
        "allegations_present": bool(stage in UNADJUDICATED),
        "trust_basis": e.get("trust_basis", "official_actor_record"),
        "never_corroborates": e.get("never_corroborates", True),
        "proposition_scope": e.get("proposition_scope", "own_acts_and_statements"),
    }


def _synthetic_text(e: Dict[str, Any]) -> str:
    """Fallback when the page cannot be fetched: title + teaser + recorded stage."""
    parts = [(e.get("title") or "").strip(), (e.get("doj_teaser_verbatim") or "").strip()]
    comps = e.get("doj_components") or []
    if comps:
        parts.append(f"Announcing component: {', '.join(str(c) for c in comps)}.")
    if (e.get("legal_stage") or "").strip():
        parts.append(f"Recorded legal stage: {e['legal_stage'].replace('_',' ')}.")
    return "\n\n".join(p for p in parts if p)


def build(artifacts: Path, start: str, end: str, limit: Optional[int] = None,
          dry_run: bool = False) -> Dict[str, Any]:
    in_path = artifacts / "json" / f"{SOURCE_KEY}_filtered_{start}_{end}.json"
    if not in_path.exists():
        logger.error("Filtered input not found: %s", in_path)
        return {"source": SOURCE_KEY, "count": 0, "events_path": None, "input_json": str(in_path)}

    entities: List[Dict[str, Any]] = json.loads(in_path.read_text(encoding="utf-8")).get("entities") or []
    if limit:
        entities = entities[:limit]
    logger.info("Loaded %d entities from %s", len(entities), in_path.name)

    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    system_prompt = compose_system_prompt(SOURCE_KEY, include_attacks=True)
    (artifacts / "log" / f"{SOURCE_KEY}_prompt_{start}_{end}.txt").write_text(system_prompt, encoding="utf-8")
    logger.info("System prompt: %d chars (PREFACE_JUSTICE + attacks + canonical)", len(system_prompt))

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
        logger.info("[justice] %d/%d %s", i, len(entities), title[:80])
        try:
            if url:
                raw = extract_events_from_url(
                    url, system_prompt=system_prompt, article_title=title,
                    article_date=post_date, source_hint=SOURCE_KEY,
                    artifacts_root=str(artifacts), idx=i)
            else:
                raw = extract_events_from_text(
                    _synthetic_text(e), system_prompt=system_prompt,
                    article_title=title, article_date=post_date,
                    artifacts_root=str(artifacts), idx=i)
            llm_calls += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Extractor failed on idx=%d url=%s", i, url)
            noncompliant.append({"idx": i, "url": url, "reason": f"extractor_error: {exc}"})
            continue

        evs = parse_llm_events_canonical(raw, article_url=url, logger=logger)
        if not evs:
            noncompliant.append({"idx": i, "url": url, "reason": "no_blocks", "title": title})
            logger.warning("No canonical block for idx=%d: %s", i, title[:80])
            continue

        prov = _provenance(e)
        for ev in evs:
            ev.setdefault("source", SOURCE_KEY)
            ev.setdefault("attacks", [])
            _stamp_identity(ev, e)
            ev["tags"] = [SOURCE_KEY] + ([prov["legal_stage"]] if prov["legal_stage"] else [])
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
    return {"source": SOURCE_KEY, "count": len(all_events), "events_path": str(out_path),
            "input_json": str(in_path), "noncompliant": len(noncompliant), "llm_calls": llm_calls}


def main() -> int:
    p = argparse.ArgumentParser(description="Justice builder v2 (extractor mode)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--level", default="INFO")
    a = p.parse_args()
    setup_logger("dc.build.justice", a.level)
    print(json.dumps(build(Path(a.artifacts), a.start, a.end, limit=a.limit, dry_run=a.dry_run), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
