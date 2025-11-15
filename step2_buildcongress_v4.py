#!/usr/bin/env python3
# buildcongress_v4.py — Congress builder (COPY MODE from build50501_v4.py, with Congress-specific text enrichment)
from __future__ import annotations

import argparse
import json
import re
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_text

import requests
from datetime import datetime, date, timedelta

# ------------------------------------------------------------
# Canonical block parser (same style as build50501_v4.py)
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)

def _parse_llm_events_canonical(text: str, *, article_url: str, logger: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
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


# ------------------------------------------------------------
# Small date helpers (kept for parity with model)
# ------------------------------------------------------------

TZ_DEFAULT = "Australia/Brisbane"

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
# Congress-specific enrichment
# ------------------------------------------------------------

def _fetch_congress_entity_json(entity_url: str, logger: logging.Logger) -> dict:
    """Optional: pull structured JSON once for enrichment (best effort)."""
    if not entity_url:
        return {}
    try:
        r = requests.get(entity_url, timeout=12)
        if r.status_code != 200:
            logger.debug("congress: entity fetch %s → %s", entity_url, r.status_code)
            return {}
        j = r.json()
        if isinstance(j, dict) and "bill" in j and isinstance(j["bill"], dict):
            return j["bill"]
        return j if isinstance(j, dict) else {}
    except Exception as e:
        logger.debug("congress: fetch failed %s: %s", entity_url, e)
        return {}

def _build_synthetic_text(rec: dict, enriched: dict) -> str:
    """Minimal neutral context for the LLM (no scraping)."""
    title = (rec.get("title") or "").strip()
    post_date = (rec.get("post_date") or "")[:10]
    raw_line = (rec.get("raw_line") or "").strip()

    parts: List[str] = [
        f"CONGRESS_RECORD_TITLE: {title}",
        f"ACTION_DATE: {post_date}",
        f"CONGRESS.GOV_STATUS_LINE: {raw_line}",
        "",
        "This record reflects a concrete congressional action or status on a bill/resolution. "
        "Extract democracy-affecting events using the Canonical Extraction Protocol. "
        "If this is a Public Law, treat it as an enactment. If it is a veto, passage, failure, nomination/appointment, "
        "or removal, record the exact action and who/what was affected.",
    ]

    if enriched:
        latest = enriched.get("latestAction") or enriched.get("latest_action") or {}
        actions = enriched.get("actions") or []
        summary = enriched.get("summary") or {}
        summary_text = summary.get("text") if isinstance(summary, dict) else (summary if isinstance(summary, str) else "")

        parts.append("")
        parts.append("ADDITIONAL_CONTEXT_FROM_CONGRESS_DOT_GOV:")

        if isinstance(latest, dict):
            la_date = latest.get("actionDate") or latest.get("date") or ""
            la_text = (latest.get("text") or latest.get("description") or "").strip()
            if la_text:
                parts.append(f"- LATEST_ACTION: {la_text} ({la_date})")

        if isinstance(actions, list) and actions:
            parts.append("- ACTIONS:")
            for act in actions[:5]:
                if not isinstance(act, dict):
                    continue
                adate = act.get("actionDate") or act.get("date") or ""
                atext = (act.get("text") or act.get("description") or "").strip()
                if atext:
                    parts.append(f"  • {adate}: {atext}")

        if summary_text:
            parts.append("- SUMMARY:")
            parts.append(summary_text.strip()[:2000])

    return "\n".join(parts).strip()



# ------------------------------------------------------------
# Salient non-bills filtering (no intermediate stages)
# ------------------------------------------------------------

_KEEP_SALIENT_KEYWORDS = re.compile(
    r"(impeach|impeachment|censur|expuls|ethic|remov\w+\s+(member|officer)|war\s+powers|rules?\s+(amend|change|adopt)|privileges\s+of\s+the\s+house)",
    re.IGNORECASE,
)

def _is_public_law(raw_line: str) -> bool:
    s = (raw_line or "").lower()
    return (
        "became public law" in s
        or "became law" in s
        or ("public law" in s and ("became" in s or "signed" in s))
    )

def _is_salient_nonbill(title: str, raw_line: str) -> bool:
    """Keep terminal, politically final resolutions (not mere passage)."""
    tl = (title or "").lower()
    rl = (raw_line or "").lower()
    # Terminal verbs for resolutions
    terminal = ("agreed to" in rl) or ("adopted" in rl) or ("expunged" in rl) or ("laid on table agreed" in rl)
    if not terminal:
        return False
    # Must also be substantively salient
    return bool(_KEEP_SALIENT_KEYWORDS.search(tl))

# ------------------------------------------------------------
# Runner (COPY MODE shape from build50501_v4.py)
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
    ids: Optional[List[int]] = None,      # usually unused for congress
    skip_existing: bool = False,          # unused here (no on-disk caching by item)
) -> Dict[str, Any]:
    """
    Congress builder in 50501 COPY MODE:
    - Single prompt from prompts_v4 (congress preface + canonical + attacks preface)
    - No scraping; we pass a small synthetic text (optionally enriched from entity_url JSON)
    - Canonical parsing identical to 50501
    """
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)
    debug_enabled = logger.isEnabledFor(logging.DEBUG)

    # Load filtered JSON with flexible shape coercion
    p = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    if not p.exists():
        raise FileNotFoundError(p)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, dict):
        items = payload.get("entities") or payload.get("items") or payload.get("events") or []
        if isinstance(items, dict):
            items = list(items.values())
    elif isinstance(payload, list):
        items = payload
    else:
        items = []
    if not isinstance(items, list):
        items = []

    logger.info("Loaded %d entities (coerced type=list)", len(items))

    # Minimal policy filter at build-time: keep Public Laws and salient non-bill terminal actions; drop intermediate stages
    before_ct = len(items)
    kept: List[Dict[str, Any]] = []
    for rec in items:
        raw_line = rec.get("raw_line", "")
        title = rec.get("title", "")
        if _is_public_law(raw_line) or _is_salient_nonbill(title, raw_line):
            kept.append(rec)
    if kept:
        items = kept
    logger.info("congress: kept %d of %d after salient-nonbill filter", len(items), before_ct)

    # Select subset
    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    # One system prompt (with attacks preface)
    system_prompt = compose_system_prompt("congress", include_attacks=True)
    if debug_enabled:
        prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
        with open(prompt_path, "w", encoding="utf-8") as f:
            f.write(system_prompt)

    # Iterate
    results, bundle, noncompliant = [], [], []
    for i, idx in enumerate(idxs, 1):
        rec = items[idx]
        title = rec.get("title", "")
        article_url = rec.get("summary_url") or rec.get("entity_url") or rec.get("canonical_url") or rec.get("url") or ""
        post_date = (rec.get("post_date") or "")[:10]

        # Optional JSON enrichment
        enriched = {}
        ent_url = rec.get("entity_url") or ""
        if ent_url.lower().endswith(".json"):
            enriched = _fetch_congress_entity_json(ent_url, logger)

        synthetic_text = _build_synthetic_text(rec, enriched)

        logger.info("[%s] item %d/%d", source, i, len(idxs))
        try:
            text = extract_events_from_text(
                synthetic_text,
                system_prompt=system_prompt,
                meta=rec,
                article_title=title,
                article_date=post_date,
                model="gpt-4o-mini",
                temperature=0.0,
                max_tokens=9000,
                idx=idx,
            )
        except Exception as e:
            logger.exception("Extractor failed on idx=%d (%s)", idx, title[:160])
            text = f"(Extraction error: {e})"

        # Per-item logs (match model)
        if debug_enabled:
            (artifacts / "log").mkdir(parents=True, exist_ok=True)
            with open(artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt", "w", encoding="utf-8") as f:
                f.write(text or "")
            bundle.append({"idx": idx, "url": article_url, "chars": len(text or "")})

        results.append({
            "_idx": idx,
            "url": article_url,
            "events_text": text or "",
        })

    # Consolidated bundle
    if debug_enabled and bundle:
        with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f, ensure_ascii=False, indent=2)

    # Parse canonical → structured events
    all_events: List[Dict[str, Any]] = []
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
            # tag both source and 'congress' for downstream grouping
            tags = ev.get("tags") or []
            if source not in tags:
                tags.append(source)
            if "congress" not in tags:
                tags.append("congress")
            ev["tags"] = tags
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
    logging.getLogger().info("Wrote %s (events=%d)", out_json, len(all_events))
    return {"source": source, "events": len(all_events), "path": str(out_json)}


# ------------------------------------------------------------
# CLI (parity with model)
# ------------------------------------------------------------

def _parse_args() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Congress builder (COPY MODE)")
    ap.add_argument("--source", required=True, help="Use 'congress'")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--level", default="INFO")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--ids", type=int, nargs="+")
    ap.add_argument("--skip-existing", action="store_true")
    return ap

def main() -> None:
    args = _parse_args().parse_args()
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