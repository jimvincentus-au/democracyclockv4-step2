#!/usr/bin/env python3
# buildballotpedia_orders_v4.py — Ballotpedia Orders builder (COPY MODE, aligned with build50501_v4.py)
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
# single source of truth for prompts
from step2_prompts_v4 import compose_system_prompt
# extractor entrypoints — we need both because some orders have no URL
from step2_extractor_v4 import extract_events_from_url, extract_events_from_text

from datetime import datetime, date, timedelta
import hashlib

# ------------------------------------------------------------
# Parsers (mirrors build50501_v4.py)
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)


def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    """Parse the canonical Democracy Clock LLM event format (with attacks)."""
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
# Utilities
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
    Ballotpedia Orders builder — COPY MODE.
    1. load filtered JSON for this window (ballotpedia_orders_filtered_...json)
    2. call shared extractor with the shared prompt from prompts_v4
    3. parse canonical blocks (with attacks)
    4. write artifacts/eventjson/orders_events_{start}_{end}.json
    """

    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    # Ballotpedia naming quirk
    filtered_path = artifacts / "json" / f"ballotpedia_{source}_filtered_{start}_{end}.json"
    if not filtered_path.exists():
        alt = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
        if alt.exists():
            filtered_path = alt
        else:
            raise FileNotFoundError(filtered_path)

    with open(filtered_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    items = payload["entities"] if isinstance(payload, dict) and "entities" in payload else payload
    logger.info("Loaded %d orders", len(items))

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    # one prompt per window — exact COPY MODE
    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results: List[Dict[str, Any]] = []
    bundle: List[Dict[str, Any]] = []

    for i, idx in enumerate(idxs, 1):
        ent = items[idx]
        order_title = ent.get("title") or ent.get("name") or ""
        order_url = ent.get("canonical_url") or ent.get("url") or ""
        order_date = ent.get("date") or ent.get("source_date") or ent.get("post_date") or ""
        logger.info("[%s] order %d/%d | %s", source, i, len(idxs), order_title or "<untitled>")

        try:
            if order_url:
                text = extract_events_from_url(
                    order_url,
                    system_prompt=system_prompt,
                    article_title=order_title,
                    article_date=order_date,
                    artifacts_root=artifacts_root,
                    idx=idx,
                )
            else:
                # orders sometimes are just structured text — build a synthetic post
                synthetic_lines = []
                if order_date:
                    synthetic_lines.append(f"Date: {order_date}")
                if order_title:
                    synthetic_lines.append(f"Title: {order_title}")
                desc = ent.get("description") or ent.get("text") or ent.get("body") or ""
                if desc:
                    synthetic_lines.append("")
                    synthetic_lines.append(desc)
                synthetic_text = "\n".join(synthetic_lines)
                text = extract_events_from_text(
                    synthetic_text,
                    system_prompt=system_prompt,
                    artifacts_root=artifacts_root,
                    idx=idx,
                )
        except Exception as e:
            logger.exception("Extractor failed on order %s", order_title)
            text = f"(Extraction error: {e})"

        # dump per-item LLM output
        with open(
            artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt",
            "w",
            encoding="utf-8",
        ) as f:
            f.write(text)

        bundle.append({"idx": idx, "title": order_title, "url": order_url, "chars": len(text)})
        results.append({"_idx": idx, "url": order_url, "events_text": text})

    # write bundle like other builders
    with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    # parse to canonical events
    all_events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []

    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        if not raw:
            noncompliant.append({"idx": rec["_idx"], "reason": "empty"})
            continue
        evs = _parse_llm_events_canonical(raw, article_url=rec["url"], logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "reason": "no_blocks_parsed"})
        for ev in evs:
            ev.setdefault("source", source)
            ev.setdefault("tags", [source, "order"])
            ev.setdefault("attacks", [])
            all_events.append(ev)

    out_json = artifacts / "eventjson" / f"{source}_events_{start}_{end}.json"
    if all_events:
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
    else:
        logger.info("Parsed 0 events (%d noncompliant). Skipping write.", len(noncompliant))

    return {
        "source": source,
        "events": len(all_events),
        "path": str(out_json) if all_events else None,
    }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Ballotpedia Orders builder")
    ap.add_argument("--source", default="orders", help="Source key (default: orders)")
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