#!/usr/bin/env python3
# buildballotpedia_shadow_v4.py — Ballotpedia Shadow-Docket builder (COPY MODE from build50501_v4.py)
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_url, extract_events_from_text

import hashlib
from datetime import datetime, date, timedelta

# ------------------------------------------------------------
# Helpers (same parser shape as 50501; shadow adds URL-less support)
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)

def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    """Parse canonical LLM output blocks."""
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    blocks: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if _HDR_RE.match(ln):
            if cur:
                blocks.append(cur)
            cur = [ln]
        elif cur:
            cur.append(ln)
    if cur:
        blocks.append(cur)

    events: List[Dict[str, Any]] = []
    for bidx, block in enumerate(blocks, 1):
        date_s = title = summary = src_line = cat = why = ""
        attacks_line = ""
        url = ""

        m = _HDR_RE.match(block[0])
        if m:
            date_s = m.group(1).strip()
            title = m.group(2).strip()

        for ln in block[1:]:
            if not ln.strip():
                continue
            if _SUM_RE.match(ln):
                summary = _SUM_RE.match(ln).group(1).strip()
                continue
            if _SRC_RE.match(ln):
                src_line = _SRC_RE.match(ln).group(1).strip()
                u = _URL_EX.search(src_line)
                url = u.group(0) if u else ""
                continue
            if _CAT_RE.match(ln):
                cat = _CAT_RE.match(ln).group(1).strip()
                continue
            if _WHY_RE.match(ln):
                why = _WHY_RE.match(ln).group(1).strip()
                continue
            if _ATK_RE.match(ln):
                attacks_line = _ATK_RE.match(ln).group(1).strip()
                continue

        if not url:
            url = article_url or ""
        sources = [url] if url else []
        if article_url and article_url not in sources:
            sources.append(article_url)

        if logger:
            if not summary:
                logger.warning("Block %d missing Summary", bidx)
            if not cat:
                logger.warning("Block %d missing Category", bidx)
            if not why:
                logger.warning("Block %d missing Why Relevant", bidx)

        attacks_list: List[str] = []
        if attacks_line:
            cleaned = attacks_line.strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1].strip()
            raw_parts = re.split(r"[;,]", cleaned)
            for part in raw_parts:
                h = part.strip().strip('"').strip("'")
                if not h:
                    continue
                h_norm = h.lower().replace(" ", "_")
                attacks_list.append(h_norm)

        events.append({
            "source_date": date_s,
            "title": title,
            "url": url,
            "summary": summary,
            "why_relevant": why,
            "category": cat,
            "sources": sources,
            "tags": [],
            "attacks": attacks_list,
        })
    return events


TZ_DEFAULT = "Australia/Brisbane"

def make_event_id(source: str, title: str, url: Optional[str], source_date: str) -> str:
    base = f"{source}|{title.strip()}|{(url or '').strip()}|{source_date}"
    return "sha1:" + hashlib.sha1(base.encode("utf-8")).hexdigest()

def compute_post_date_str(source_date_str: str) -> str:
    d = datetime.strptime(source_date_str, "%Y-%m-%d").date()
    return (d + timedelta(days=1)).isoformat()

def _coerce_iso_date(v: Any) -> Optional[str]:
    if isinstance(v, (datetime, date)):
        return v.isoformat() if isinstance(v, date) else v.date().isoformat()
    s = str(v or "").strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None

def _make_shadow_synthetic_text(item: Dict[str, Any]) -> str:
    """
    Some Ballotpedia shadow entries may not have an accessible URL.
    Build a short DC-style input so the extractor can still do its job.
    """
    title = item.get("title") or item.get("name") or "Shadow Docket Item"
    desc = item.get("description") or item.get("summary") or ""
    date_s = item.get("date") or item.get("decided") or item.get("source_date") or ""
    date_iso = _coerce_iso_date(date_s) or datetime.utcnow().date().isoformat()
    court = item.get("court") or item.get("body") or "Supreme Court / Federal"
    url = item.get("url") or item.get("link") or ""
    lines = [
        f"{date_iso} — {title}",
        f"Summary: {desc or f'Shadow-docket action recorded for {court}.'}",
        f"Source: {url or '(no url)'}",
        "Category: Courts / Judicial power",
        "Why Relevant: Shadow docket actions can shift rights and power without full briefing or transparency.",
    ]
    # attacks line optional; we omit in synthetic (parser tolerates absence)
    return "\n".join(lines)


# ------------------------------------------------------------
# Runner (COPY MODE structure)
# ------------------------------------------------------------

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
) -> Dict[str, Any]:
    """
    Ballotpedia Shadow-Docket builder in the same shape as build50501_v4.py.
    Single prompt comes from prompts_v4.compose_system_prompt(source).
    Falls back to a synthetic text block when an item lacks a fetchable URL.
    """
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    # Input payload naming: harvester writes "ballotpedia_shadow_*" while orchestrator passes source="shadow".
    candidates = [
        artifacts / "json" / f"{source}_filtered_{start}_{end}.json",
        artifacts / "json" / f"ballotpedia_shadow_filtered_{start}_{end}.json",
        artifacts / "json" / f"shadow_filtered_{start}_{end}.json",  # legacy fallback
    ]
    p = next((c for c in candidates if c.exists()), None)
    if not p:
        # Log what exists to aid debugging
        logger.error("Missing filtered JSON. Tried: %s", ", ".join(str(c) for c in candidates))
        raise FileNotFoundError(candidates[0])
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)
    # Coerce payload to a list of entities regardless of shape:
    # - canonical: {"entities":[...]} or {"items":[...]}
    # - dict keyed by ids: {"abc123":{...}, "def456":{...}}
    # - bare list: [...]
    base = None
    if isinstance(payload, dict):
        if isinstance(payload.get("entities"), list):
            base = payload.get("entities")
        elif isinstance(payload.get("items"), list):
            base = payload.get("items")
        else:
            # values of arbitrary keyed dict
            base = list(payload.values())
    elif isinstance(payload, list):
        base = payload
    else:
        base = []

    items = base or []
    logger.info("Loaded %d entities (coerced type=%s)", len(items), type(base).__name__)

    # Build positional index list; if --ids provided, treat them as 1-based positions
    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    # One canonical prompt, persisted for debug; shadow does not need the attacks preface
    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=False)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results, bundle = [], []
    for i, idx in enumerate(idxs, 1):
        ent = items[idx]
        title = ent.get("title") or ent.get("name") or f"shadow-item-{idx}"
        url = ent.get("canonical_url") or ent.get("url") or ent.get("link")
        post_date = ent.get("post_date") or ent.get("date") or ent.get("decided") or ent.get("source_date") or ""
        logger.info("[%s] item %d/%d", source, i, len(idxs))

        try:
            if url:
                text = extract_events_from_url(
                    url,
                    system_prompt=system_prompt,
                    article_title=title,
                    article_date=post_date,
                    source_hint=source,
                    artifacts_root=artifacts_root,
                    idx=idx,
                )
            else:
                synthetic = _make_shadow_synthetic_text(ent)
                text = extract_events_from_text(
                    synthetic,
                    system_prompt=system_prompt,
                )
        except Exception as e:
            logger.exception("Extractor failed on idx=%d", idx)
            text = f"(Extraction error: {e})"

        with open(artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt", "w", encoding="utf-8") as f:
            f.write(text)
        bundle.append({"idx": idx, "url": url or "(synthetic)", "chars": len(text)})
        results.append({"_idx": idx, "url": url or "", "events_text": text})

    with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    all_events, noncompliant = [], []
    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        if not raw or raw.startswith("(Extraction error:"):
            noncompliant.append({"idx": rec["_idx"], "url": rec.get("url"), "reason": "no_blocks"})
            continue
        evs = _parse_llm_events_canonical(raw, article_url=rec.get("url") or "", logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "reason": "no_blocks"})
        for ev in evs:
            ev.setdefault("source", source)
            # Shadow-specific tag in addition to the source key
            tags = ev.get("tags") or []
            if "shadow" not in {t.lower() for t in tags}:
                tags.append("shadow")
            ev["tags"] = tags
            # Keep the schema stable even if LLM omitted “attacks”
            ev.setdefault("attacks", [])
            # If LLM forgot a category, default to court power
            if not ev.get("category"):
                ev["category"] = "Courts / Judicial power"
            all_events.append(ev)

    out_json = artifacts / "eventjson" / f"{source}_events_{start}_{end}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "source": source,
                "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
                "events": all_events,
                "noncompliant": noncompliant,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    logger.info("Wrote %s (events=%d)", out_json, len(all_events))
    return {
        "source": source,
        "count": len(all_events),
        "events_path": str(out_json),
        "input_json": str(p),
    }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Ballotpedia Shadow-Docket builder (COPY MODE)")
    ap.add_argument("--source", required=False, default="ballotpedia_shadow")
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