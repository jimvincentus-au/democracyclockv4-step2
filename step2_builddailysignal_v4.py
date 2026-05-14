#!/usr/bin/env python3
# builddailysignal_v4.py — Daily Signal builder (COPY MODE from buildsubstack_v4.py)
#
# Differs from buildsubstack only in source name + default; the extraction pattern
# (fetch via canonical_url, run LLM extractor, parse canonical event blocks) is
# identical to non-HCR Substack sources because the input shape (URL + title +
# post_date) is identical from the harvester side.
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_url

import hashlib
from datetime import datetime, date, timedelta

SOURCE_DEFAULT = "dailysignal"
TZ_DEFAULT = "Australia/Brisbane"

# ------------------------------------------------------------
# Canonical block parser — shared across all builders
# (single-sourced in step2_builder_helper_v4)
# ------------------------------------------------------------

from step2_builder_helper_v4 import parse_llm_events_canonical as _parse_llm_events_canonical


# ------------------------------------------------------------
# Runner
# ------------------------------------------------------------

def run_builder(
    *,
    source: str = SOURCE_DEFAULT,
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    limit: Optional[int] = None,
    ids: Optional[List[int]] = None,
    skip_existing: bool = False,
) -> Dict[str, Any]:
    """Daily Signal builder: fetch each article via canonical_url, LLM-extract events."""

    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)
    # Propagate --level to the extractor's logger so DEBUG output is opt-in
    # (default INFO stays quiet; --level debug surfaces extractor traces).
    setup_logger("dc.extractor", level)

    p = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    if not p.exists():
        raise FileNotFoundError(p)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)
    items = payload["entities"] if isinstance(payload, dict) and "entities" in payload else payload
    logger.info("Loaded %d entities from %s", len(items), p)

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results, bundle = [], []
    for i, idx in enumerate(idxs, 1):
        ent = items[idx]
        title = ent.get("title", "")
        url = ent.get("canonical_url") or ent.get("url")
        post_date = ent.get("post_date", "")
        logger.info("[%s] item %d/%d", source, i, len(idxs))
        if not url:
            logger.warning("No URL for idx=%d", idx)
            continue
        try:
            text = extract_events_from_url(
                url,
                system_prompt=system_prompt,
                article_title=title,
                article_date=post_date,
                source_hint=source,
                artifacts_root=artifacts_root,
                idx=idx,
            )
        except Exception as e:
            logger.exception("Extractor failed on %s", url)
            text = f"(Extraction error: {e})"

        with open(artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt", "w", encoding="utf-8") as f:
            f.write(text)
        bundle.append({"idx": idx, "url": url, "chars": len(text)})
        results.append({"_idx": idx, "url": url, "events_text": text})

    with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    all_events, noncompliant = [], []
    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        if not raw:
            noncompliant.append({"idx": rec["_idx"], "reason": "empty"})
            continue
        evs = _parse_llm_events_canonical(raw, article_url=rec["url"], logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "reason": "no_blocks"})
        for ev in evs:
            ev.setdefault("source", source)
            if not ev.get("tags"):
                ev["tags"] = [source, "news_article"]
            if "attacks" not in ev or ev["attacks"] is None:
                ev["attacks"] = []
            all_events.append(ev)

    out_json = artifacts / "eventjson" / f"{source}_events_{start}_{end}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(
            {"source": source,
             "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
             "events": all_events,
             "noncompliant": noncompliant},
            f, ensure_ascii=False, indent=2,
        )
    logger.info("Wrote %s (events=%d)", out_json, len(all_events))
    return {"source": source, "events": len(all_events), "path": str(out_json)}


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Daily Signal builder")
    ap.add_argument("--source", default=SOURCE_DEFAULT, help="Source key (default: dailysignal)")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--level", default="INFO")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--ids", type=int, nargs="+")
    ap.add_argument("--skip-existing", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    run_builder(
        source=args.source,
        start=args.start,
        end=args.end,
        artifacts_root=ARTIFACTS_ROOT,
        level=args.level,
        limit=args.limit,
        ids=args.ids,
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.", flush=True)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        raise
