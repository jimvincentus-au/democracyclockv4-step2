#!/usr/bin/env python3
# builddemocracydocket_v4.py — Democracy Docket builder (COPY MODE from build50501_v4.py)
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
# single source of truth for prompts — no builder-side assembly
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_text

import hashlib
from datetime import datetime, date, timedelta

# ------------------------------------------------------------
# Helpers (same canonical block parser as build50501_v4.py)
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)

def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    """Parse canonical LLM output (non-HCR)."""
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
                summary = _SUM_RE.match(ln).group(1).strip(); continue
            if _SRC_RE.match(ln):
                src_line = _SRC_RE.match(ln).group(1).strip()
                u = _URL_EX.search(src_line)
                url = u.group(0) if u else ""
                continue
            if _CAT_RE.match(ln):
                cat = _CAT_RE.match(ln).group(1).strip(); continue
            if _WHY_RE.match(ln):
                why = _WHY_RE.match(ln).group(1).strip(); continue
            if _ATK_RE.match(ln):
                attacks_line = _ATK_RE.match(ln).group(1).strip(); continue

        if not url:
            url = article_url or ""
        sources = [url] if url else []
        if article_url and article_url not in sources:
            sources.append(article_url)

        if logger:
            if not summary: logger.warning("Block %d missing Summary", bidx)
            if not cat: logger.warning("Block %d missing Category", bidx)
            if not why: logger.warning("Block %d missing Why Relevant", bidx)

        attacks_list: List[str] = []
        if attacks_line:
            cleaned = attacks_line.strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1].strip()
            for part in re.split(r"[;,]", cleaned):
                h = part.strip().strip('"').strip("'")
                if not h:
                    continue
                attacks_list.append(h.lower().replace(" ", "_"))

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


def _load_filtered_list(source: str, start: str, end: str, artifacts: Path) -> List[Dict[str, Any]]:
    """Accepts dict with entities/items/events or a bare list — returns a list."""
    p = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    if not p.exists():
        raise FileNotFoundError(p)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        data = payload.get("entities") or payload.get("items") or payload.get("events") or []
        return data if isinstance(data, list) else []
    return payload if isinstance(payload, list) else []


# ------------------------------------------------------------
# Runner
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
    Democracy Docket builder (COPY MODE):
    - Single prompt via compose_system_prompt(source, include_attacks=True)
    - Build synthetic text using DD's summary/content (lighter & more stable than live crawl)
    - Canonical parse identical to build50501_v4.py
    """
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    items = _load_filtered_list(source, start, end, artifacts)
    logger.info("Loaded %d entities from %s", len(items), artifacts / "json" / f"{source}_filtered_{start}_{end}.json")

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    # Single prompt (persist for debug)
    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results, bundle = [], []
    for i, idx in enumerate(idxs, 1):
        ent = items[idx]
        title = ent.get("title") or "(untitled)"
        url = ent.get("canonical_url") or ent.get("url") or ent.get("link") or ""
        date_s = (ent.get("date") or ent.get("post_date") or ent.get("published") or "")[:10]
        # democracydocket specifics — we pass text, not a live page
        summary = ent.get("summary") or ""
        content = ent.get("content") or ent.get("html") or ""
        # keep synthetic under control; the extractor is already tuned to Canonical Protocol
        synthetic_text = (
            f"{(date_s or start)[:10]} — {title}\n"
            f"Summary: {(summary or content).strip()[:2000]}\n"
            f"Source: {url or '(no url)'}\n"
            "Category: Voting rights / Courts / Democracy protection\n"
            "Why Relevant: Democracy Docket tracks litigation and actions that directly shape election rules and democratic access."
        )

        logger.info("[%s] item %d/%d", source, i, len(idxs))
        try:
            text = extract_events_from_text(
                synthetic_text,
                system_prompt=system_prompt,
                artifacts_root=str(artifacts),
                idx=idx,
            )
        except Exception as e:
            logger.exception("Extractor failed on idx=%d (%s)", idx, title)
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
            noncompliant.append({"idx": rec["_idx"], "url": rec["url"], "reason": "no_blocks"})
            continue
        evs = _parse_llm_events_canonical(raw, article_url=rec["url"], logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "reason": "no_blocks"})
        for ev in evs:
            ev.setdefault("source", source)
            ev.setdefault("tags", [source])
            ev.setdefault("attacks", [])
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
    return {"source": source, "events": len(all_events), "path": str(out_json)}


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Democracy Docket builder (COPY MODE)")
    ap.add_argument("--source", required=True, help="Use 'democracydocket'")
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