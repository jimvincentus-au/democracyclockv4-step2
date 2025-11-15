#!/usr/bin/env python3
"""
buildhcr_v4.py — Step-2 builder for Heather Cox Richardson (Letters from an American)

- Reads filtered JSON from gethcr_v4.py (or your unified harvester that writes `hcr_filtered_*.json`)
- Fetches article body (or transcript JSON for podcast posts)
- Calls extractor_v4 with the HCR-specific system prompt (with `include_attacks=True`)
- Parses canonical blocks and writes Step-2 structured JSON (only if any events parsed)
"""

from __future__ import annotations
import json
import logging
import os
import hashlib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT, TZ_DEFAULT
from step2_builder_helper_v4 import (
    _ensure_dirs,
    _load_filtered,
    _debug_write_json,
    _debug_write_text,
    _parse_llm_events_canonical,
    serialize_events_structured,
    _pick_indices,
)
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_url, extract_events_from_text

import requests


# ---------- minimal local helper for HCR podcast transcripts ----------
def _fetch_substack_transcript_text(transcript_url: str, timeout: int = 30) -> str:
    """
    Best-effort: fetch a Substack transcript JSON and flatten all 'text' fields.
    Returns '' on failure. (HCR podcast pages expose a signed .../transcription.json URL)
    """
    try:
        r = requests.get(transcript_url, timeout=timeout)
        if r.status_code != 200:
            return ""
        try:
            j = r.json()
        except Exception:
            return ""
        parts: List[str] = []
        def _collect(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, str) and k.lower() == "text":
                        v = v.strip()
                        if v:
                            parts.append(v)
                    else:
                        _collect(v)
            elif isinstance(obj, list):
                for it in obj:
                    _collect(it)
        _collect(j)
        return "\n".join(parts)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Builder entry
# ---------------------------------------------------------------------------

def run_builder(
    *,
    source: str,
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    limit: Optional[int] = None,
    ids: Optional[List[int]] = None,
    skip_existing: bool = False,
    aim_per_post: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build Step-2 structured events for HCR posts.
    - If `summary_url` looks like a Substack transcript JSON, use it (podcast posts).
    - Else, fetch via article URL.
    - Write events JSON only when events > 0 (skip writing empty files).
    """
    artifacts = Path(artifacts_root)
    _ensure_dirs(artifacts)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    logger.info("Loading filtered entities | source=%s window=%s→%s root=%s", source, start, end, artifacts_root)
    pack = _load_filtered(artifacts, source, start, end, logger)
    # Normalize filtered JSON shape:
    # - Canonical: {"generated_at":..., "window":..., "source":..., "items":[...]}
    # - Legacy variants: {"entities":[...]}, {"events":[...]}, or a bare list [...]
    items: List[dict] = []
    detected_shape = "unknown"
    path_hint = None

    if isinstance(pack, dict):
        path_hint = pack.get("path")
        data = pack.get("data", pack)  # some loaders wrap actual payload under "data"
        if isinstance(data, dict):
            if isinstance(data.get("items"), list):
                items = data.get("items") or []
                detected_shape = "dict.items"
            elif isinstance(data.get("entities"), list):
                items = data.get("entities") or []
                detected_shape = "dict.entities"
            elif isinstance(data.get("events"), list):
                items = data.get("events") or []
                detected_shape = "dict.events"
            else:
                # last resort: if there is exactly one list-like value, take it
                for k, v in data.items():
                    if isinstance(v, list):
                        items = v
                        detected_shape = f"dict.{k}"
                        break
        elif isinstance(data, list):
            items = data
            detected_shape = "list"
    elif isinstance(pack, list):
        items = pack
        detected_shape = "list(top)"

    logger.debug("Loaded filtered JSON | shape=%s count=%d from=%s", detected_shape, len(items), path_hint or "-")

    # Select subset if requested
    sel_idx = _pick_indices(len(items), ids=ids, limit=limit)
    logger.info("Selected %d of %d items (ids=%s, limit=%s).", len(sel_idx), len(items), "-" if not ids else ids, "-" if not limit else limit)

    # Prepare prompt
    system_prompt = compose_system_prompt("hcr", include_attacks=True)
    prompt_path = artifacts / "log" / f"hcr_prompt_{start}_{end}.txt"
    try:
        prompt_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_path.write_text(system_prompt, encoding="utf-8")
    except Exception:
        pass
    logger.info("System prompt length: %d chars", len(system_prompt))
    logger.debug("System prompt (first 1200 chars):\n%s", system_prompt[:1200])

    results: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []
    llm_bundle: List[Dict[str, Any]] = []

    model_name = os.getenv("OPENAI_MODEL_EVENTS", "gpt-4o-mini")

    for i, idx in enumerate(sel_idx, 1):
        rec = items[idx]
        title = (rec.get("title") or "").strip()
        url = rec.get("canonical_url") or rec.get("url") or ""
        post_date = rec.get("post_date") or ""
        summary_url = (rec.get("summary_url") or "").strip().lower()

        logger.info("LLM %s item %d/%d | idx=%d", source, i, len(sel_idx), idx)
        logger.debug("Item meta | title=%r url=%s post_date=%s", title, url, post_date)

        # Prefer transcript text for podcast-style posts if clearly provided
        use_transcript = (summary_url.endswith(".json") and ("transcript" in summary_url or "transcription" in summary_url))
        try:
            if use_transcript:
                # Fetch transcript JSON → text and run text extractor
                transcript_text = _fetch_substack_transcript_text(rec.get("summary_url", ""))
                # Always log the first 800 chars of what we feed
                logger.debug("\n\n===== HCR TRANSCRIPT TEXT (first 800 chars) =====\n %s \n===============================================\n", (transcript_text or "")[:800])

                events_text = extract_events_from_text(
                    transcript_text or "",
                    system_prompt=system_prompt,
                    meta={**rec, "_idx": idx},
                    article_title=title,
                    article_date=post_date,
                    model=model_name,
                    temperature=0.0,
                    max_tokens=9000,
                    idx=idx,
                )
            else:
                # Normal essay: fetch the page and run URL extractor
                events_text = extract_events_from_url(
                    url,
                    system_prompt=system_prompt,
                    article_title=title,
                    article_date=post_date,
                    model=model_name,
                    temperature=0.0,
                    max_tokens=9000,
                    idx=idx,
                )
        except Exception as e:
            logger.exception("Extractor error on item idx=%d %s", idx, title)
            events_text = f"(LLM extraction failed: {e})"

        # Per-item I/O debug
        per_item_in = {
            "meta": rec,
            "prompt_sha1": hashlib.sha1(system_prompt.encode("utf-8")).hexdigest(),
            "model": model_name,
            "post_date": post_date,
            "title": title,
            "url": url,
            "used_transcript": use_transcript,
        }
        per_item_in_path = artifacts / "log" / f"{source}_llm_in_idx{idx}_{start}_{end}.json"
        _debug_write_json(per_item_in_path, per_item_in)

        per_item_out_path = artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt"
        _debug_write_text(per_item_out_path, "LLM OUTPUT", events_text or "")

        # Bundle preview (trim)
        llm_bundle.append({"idx": idx, "input": {"meta": rec}, "output_preview": (events_text or "")[:4000]})
        results.append({
            "_idx": idx,
            "title": title,
            "url": url,
            "post_date": post_date,
            "events_text": events_text or "",
        })

    # Combined debug bundle
    bundle_path = artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.debug.json"
    _debug_write_json(bundle_path, {
        "source": source,
        "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
        "count_items": len(results),
        "exchanges": llm_bundle,
    })
    logger.info("Wrote consolidated LLM debug bundle: %s", bundle_path)

    # Parse canonical blocks into structured events
    all_events: List[Dict[str, Any]] = []
    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        if not raw:
            noncompliant.append({"idx": rec.get("_idx"), "reason": "empty_output"})
            continue
        evs = _parse_llm_events_canonical(raw, article_url=rec.get("url", ""), logger=logger)
        if not evs:
            noncompliant.append({"idx": rec.get("_idx"), "reason": "no_blocks_parsed"})
            continue
        for ev in evs:
            ev.setdefault("tags", [source])
            ev.setdefault("sources", [rec.get("url", "")])
            ev.setdefault("attacks", [])
            all_events.append(ev)
        if any("attacks" not in ev or not isinstance(ev.get("attacks"), list) for ev in evs):
            logger.warning("Parsed %d events but some lack 'attacks' list (idx=%s).", len(evs), rec.get("_idx"))

    logger.info("Parsed %d events from %d source items (noncompliant=%d).", len(all_events), len(results), len(noncompliant))

    # Write Step-2 only when there are events (skip empty-file write)
    if all_events:
        out_json = serialize_events_structured(
            source=source,
            start_date_iso=start,
            end_date_iso=end,
            tz=TZ_DEFAULT,
            events_in=all_events,
            artifacts_root=str(artifacts),
        )
        logger.info("Wrote Step-2 structured JSON: %s (events=%d)", out_json, len(all_events))
        return {
            "source": source,
            "count": len(all_events),
            "events_json": str(out_json),
            "input_json": path_hint,
            "noncompliant": noncompliant or None,
            "debug_bundle": str(bundle_path),
            "prompt_path": str(prompt_path),
        }
    else:
        logger.warning("No events parsed; skipping output file write.")
        return {
            "source": source,
            "count": 0,
            "events_json": None,
            "input_json": path_hint,
            "noncompliant": noncompliant or None,
            "debug_bundle": str(bundle_path),
            "prompt_path": str(prompt_path),
            "skipped": True,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse, os, hashlib
    p = argparse.ArgumentParser(description="Democracy Clock V4 — HCR builder")
    p.add_argument("--start", required=True)
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--weeks", type=int)
    grp.add_argument("--end")
    p.add_argument("--level", default="INFO")
    p.add_argument("--artifacts-root", default=str(ARTIFACTS_ROOT))
    p.add_argument("--limit", type=int, help="Limit number of records for testing")
    args = p.parse_args()

    from datetime import date, timedelta
    s = date.fromisoformat(args.start)
    e = s + timedelta(days=args.weeks * 7 - 1) if args.weeks else date.fromisoformat(args.end)

    log_file = Path(args.artifacts_root) / "log" / f"hcr_build_{args.start}_{e.isoformat()}.log"
    meta = run_builder(
        source="hcr",
        start=args.start,
        end=e.isoformat(),
        artifacts_root=args.artifacts_root,
        level=args.level,
        log_path=str(log_file),
        limit=args.limit,
    )
    logging.getLogger().info("Summary: %s", json.dumps(meta, indent=2))