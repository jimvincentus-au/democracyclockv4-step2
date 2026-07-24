#!/usr/bin/env python3
"""
Deterministic Federal Register *presidential-document* event builder (Option B).

WHY THIS EXISTS
    The standard FR builder (step2_buildfederalregister_v4.py) runs one OpenAI
    extraction call per entity. For presidential documents that is unnecessary and
    non-deterministic: an executive order / proclamation / memorandum *is* the event,
    and its authoritative identity (title, date, canonical FR URL) is already present
    in the harvested filtered file. This script maps each presidential-document entity
    straight to the canonical event shape with ZERO LLM calls.

    Summary is intentionally left empty: the FR API returns no `abstract` for
    presidential docs (confirmed 0/572), and these events are meant to merge onto the
    executive-action events already in the enriched appendix during Step 3 grouping —
    inheriting those events' existing prose. Whatever fails to merge is the (small)
    residual that can be LLM-backfilled later.

INPUT   {artifacts}/json/federalregister_filtered_{start}_{end}.json   (from the getter)
OUTPUT  {artifacts}/eventjson/federalregister_events_{start}_{end}.json (presidential only)

The output filename matches what rebuild-all discovers (`*_events_*.json`) and what
_source_from_filename() reads as source "federalregister". It contains ONLY the 572
presidential docs; agency (tier_a) FR events live in the pre-existing per-week files
and are recombined alongside this file by `step2_writeweekevents_v4.py --rebuild-all`.

Deterministic dating: source_date = signing_date (actual EO signing date) when the
getter captured it, else publication_date. This aligns FR records with news-sourced
events for grouping.
"""
from __future__ import annotations
import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ARTIFACTS = BASE_DIR / "artifacts"

SOURCE_KEY = "federalregister"
CATEGORY = "Executive Actions & Orders"          # CATEGORY_ORDER[0] in the writer
TZ_DEFAULT = "Australia/Brisbane"                 # matches step2_buildfederalregister_v4
PRESDOC_DOCTYPE = "presidential_document"

logger = logging.getLogger("fr_presdocs_deterministic")

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _iso_or_none(s: str) -> str | None:
    s = (s or "").strip()[:10]
    return s if _ISO.match(s) else None


def _event_from_entity(e: Dict[str, Any]) -> Dict[str, Any]:
    title = (e.get("title") or "").strip()
    url = (e.get("canonical_url") or e.get("url") or "").strip()
    signing = _iso_or_none(e.get("signing_date") or "")
    pub = _iso_or_none(e.get("post_date") or "")
    source_date = signing or pub or ""
    # summary stays empty by design (see module docstring); abstract confirmed empty for presdocs
    return {
        "source_date": source_date,
        "title": title,
        "url": url,
        "summary": "",
        "why_relevant": "",
        "category": CATEGORY,
        "sources": [url] if url else [],
        "tags": [SOURCE_KEY],
        "attacks": [],
        "source": SOURCE_KEY,
        # provenance extras (ignored by the normalizer, useful to the Archive)
        "fr_document_number": (e.get("fr_document_number") or "").strip(),
        "signing_date": signing or "",
        "publication_date": pub or "",
    }


def build(artifacts: Path, start: str, end: str) -> Dict[str, Any]:
    in_path = artifacts / "json" / f"{SOURCE_KEY}_filtered_{start}_{end}.json"
    if not in_path.exists():
        logger.error("Filtered input not found: %s", in_path)
        return {"count": 0, "events_path": None, "input_json": str(in_path)}

    payload = json.loads(in_path.read_text(encoding="utf-8"))
    entities = payload.get("entities") or []
    presdocs = [e for e in entities if e.get("doc_type") == PRESDOC_DOCTYPE]
    logger.info("Loaded %d entities; %d are presidential documents", len(entities), len(presdocs))

    events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []
    n_signing = n_pub_fallback = n_nodate = n_nourl = 0
    for e in presdocs:
        ev = _event_from_entity(e)
        if not ev["title"] or not ev["url"]:
            noncompliant.append({"title": ev["title"], "url": ev["url"], "reason": "missing_title_or_url"})
            if not ev["url"]:
                n_nourl += 1
            continue
        if ev["signing_date"]:
            n_signing += 1
        elif ev["publication_date"]:
            n_pub_fallback += 1
        if not ev["source_date"]:
            n_nodate += 1
        events.append(ev)

    out_dir = artifacts / "eventjson"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{SOURCE_KEY}_events_{start}_{end}.json"
    if out_path.exists():
        logger.warning("Overwriting existing %s", out_path.name)

    out_payload = {
        "source": SOURCE_KEY,
        "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
        "builder": "deterministic_presdocs_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "events": events,
        "noncompliant": noncompliant,
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote %s", out_path)
    logger.info(
        "SUMMARY | presdocs=%d events=%d noncompliant=%d | dated_by_signing=%d dated_by_publication=%d no_date=%d no_url=%d",
        len(presdocs), len(events), len(noncompliant), n_signing, n_pub_fallback, n_nodate, n_nourl,
    )
    return {"count": len(events), "events_path": str(out_path), "input_json": str(in_path)}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Deterministic FR presidential-document event builder (no LLM)")
    ap.add_argument("--start", default="2025-01-20", help="Window start YYYY-MM-DD (matches the filtered filename)")
    ap.add_argument("--end", default="2026-07-03", help="Window end YYYY-MM-DD (matches the filtered filename)")
    ap.add_argument("--artifacts-root", default=str(DEFAULT_ARTIFACTS), help="Artifacts root (contains json/ and eventjson/)")
    ap.add_argument("--level", default="INFO", help="Logging level")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)-7s %(message)s")
    res = build(Path(args.artifacts_root), args.start, args.end)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res.get("count") else 1


if __name__ == "__main__":
    sys.exit(main())
