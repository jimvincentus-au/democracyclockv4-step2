#!/usr/bin/env python3
# buildguardian_v4.py — Guardian builder (COPY MODE from build50501_v4.py)
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

# ------------------------------------------------------------
# Parsers (same as 50501)
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)


def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    """Parse canonical Democracy Clock LLM output into event dicts."""
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
            m_sum = _SUM_RE.match(ln)
            if m_sum:
                summary = m_sum.group(1).strip()
                continue
            m_src = _SRC_RE.match(ln)
            if m_src:
                src_line = m_src.group(1).strip()
                u = _URL_EX.search(src_line)
                url = u.group(0) if u else ""
                continue
            m_cat = _CAT_RE.match(ln)
            if m_cat:
                cat = m_cat.group(1).strip()
                continue
            m_why = _WHY_RE.match(ln)
            if m_why:
                why = m_why.group(1).strip()
                continue
            m_atk = _ATK_RE.match(ln)
            if m_atk:
                attacks_line = m_atk.group(1).strip()
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
                attacks_list.append(h.lower().replace(" ", "_"))

        events.append(
            {
                "source_date": date_s,
                "title": title,
                "url": url,
                "summary": summary,
                "why_relevant": why,
                "category": cat,
                "sources": sources,
                "tags": [],
                "attacks": attacks_list,
            }
        )
    return events


# ------------------------------------------------------------
# Utilities (same as 50501)
# ------------------------------------------------------------

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


def _load_filtered_items(artifacts: Path, source: str, start: str, end: str) -> List[Dict[str, Any]]:
    """
    Guardian filtered JSON is usually {"window":..., "entities":[...]} but
    we tolerate a bare list too.
    """
    p = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    if not p.exists():
        # try a hard-coded 'guardian' in case source key differs (e.g. theguardian)
        alt = artifacts / "json" / f"guardian_filtered_{start}_{end}.json"
        if alt.exists():
            p = alt
    if not p.exists():
        raise FileNotFoundError(p)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict) and "entities" in payload and isinstance(payload["entities"], list):
        return payload["entities"]
    return payload if isinstance(payload, list) else []


# ------------------------------------------------------------
# Runner (COPY MODE)
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
    Guardian builder in COPY MODE: identical scaffolding to build50501_v4.py,
    but reading guardian_filtered_{start}_{end}.json and tagging events as 'guardian'.
    """
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    items = _load_filtered_items(artifacts, source, start, end)
    logger.info("Loaded %d Guardian entities", len(items))

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    # Single prompt for this window — include attacks to keep schema stable
    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results: List[Dict[str, Any]] = []
    bundle: List[Dict[str, Any]] = []

    for i, idx in enumerate(idxs, 1):
        ent = items[idx]
        title = ent.get("title") or ent.get("headline") or ""
        url = ent.get("canonical_url") or ent.get("url") or ""
        post_date = ent.get("post_date") or ent.get("published_at") or ent.get("date") or ""

        logger.info("[%s] item %d/%d — %s", source, i, len(idxs), (title or "<untitled>")[:140])

        if not url:
            logger.warning("No URL for Guardian idx=%d — skipping LLM call", idx)
            text = ""
        else:
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
                logger.exception("Extractor failed on Guardian URL %s", url)
                text = f"(Extraction error: {e})"

        if (level or "").upper() == "DEBUG":
            with open(artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt", "w", encoding="utf-8") as f:
                f.write(text)

        bundle.append({"idx": idx, "url": url, "chars": len(text)})
        results.append({"_idx": idx, "url": url, "events_text": text})

    if (level or "").upper() == "DEBUG":
        with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f, ensure_ascii=False, indent=2)

    # parse → canonical
    all_events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []

    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        if not raw or raw.startswith("(Extraction error:"):
            noncompliant.append({"idx": rec["_idx"], "url": rec.get("url") or "", "reason": "no_blocks"})
            continue

        evs = _parse_llm_events_canonical(raw, article_url=rec.get("url") or "", logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "url": rec.get("url") or "", "reason": "no_blocks"})

        for ev in evs:
            ev.setdefault("source", source)
            ev.setdefault("tags", [source, "news"])
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

    # Return keys aligned with orchestrator logging
    return {
        "source": source,
        "count": len(all_events),
        "events_path": str(out_json),
        "prompt_path": str(prompt_path),
    }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Guardian builder (COPY MODE)")
    ap.add_argument("--source", default="guardian", help="Source key (default: guardian)")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--level", default="INFO")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--ids", type=int, nargs="+")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--artifacts-root", default=str(ARTIFACTS_ROOT))
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    run_builder(
        source=args.source,
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts_root,
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