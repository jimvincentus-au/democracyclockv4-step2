#!/usr/bin/env python3
# buildecon_v4.py — Economic builder (COPY MODE from build50501_v4.py, with econ-specific tagging)
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, date, timedelta
import hashlib

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_url, extract_events_from_text

# ------------------------------------------------------------
# Canonical parsers (same core as build50501_v4.py)
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)  # keep attacks for schema stability


def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    """
    Parse the Democracy Clock canonical LLM format into event dicts.
    Supports optional Attacks: ... line.
    """
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
        date_s = title = summary = src_line = cat = why = attacks_line = ""
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
            cleaned = attacks_line.strip().strip("[]")
            for part in re.split(r"[;,]", cleaned):
                h = part.strip().strip('"').strip("'")
                if h:
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
# Econ-specific helpers (kept from original buildecon_v4.py)
# ------------------------------------------------------------

TZ_DEFAULT = "Australia/Brisbane"

# simple topic hints — we keep these because econ is the only one doing econ tagging
KEYWORD_TOPICS = [
    ("inflation", "inflation"),
    ("cpi", "inflation"),
    ("ppi", "inflation"),
    ("jobs", "labor"),
    ("employment", "labor"),
    ("unemployment", "labor"),
    ("wage", "labor"),
    ("payroll", "labor"),
    ("tariff", "trade"),
    ("trade", "trade"),
    ("imports", "trade"),
    ("exports", "trade"),
    ("deficit", "fiscal"),
    ("budget", "fiscal"),
    ("debt", "fiscal"),
    ("gdp", "growth"),
    ("productivity", "growth"),
    ("housing", "housing"),
    ("mortgage", "housing"),
    ("rent", "housing"),
    ("fed", "fed"),
    ("fomc", "fed"),
    ("rate", "rates"),
    ("interest", "rates"),
    ("manufacturing", "industry"),
    ("services", "industry"),
    ("retail", "consumption"),
    ("consumer", "consumption"),
    ("confidence", "sentiment"),
    ("pmi", "industry"),
]


def derive_topics(title: str, tags: List[str] | None) -> List[str]:
    title_l = (title or "").lower()
    topics = set(tags or [])
    for kw, topic in KEYWORD_TOPICS:
        if kw in title_l:
            topics.add(topic)
    return sorted({t.strip().lower() for t in topics if t})


def make_event_id(source: str, title: str, url: Optional[str], source_date: str) -> str:
    base = f"{source}|{title.strip()}|{(url or '').strip()}|{source_date}"
    return "sha1:" + hashlib.sha1(base.encode("utf-8")).hexdigest()


def _coerce_iso_date(v: Any) -> Optional[str]:
    if isinstance(v, (datetime, date)):
        return v.date().isoformat() if isinstance(v, datetime) else v.isoformat()
    s = str(v or "").strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _load_filtered_econ(artifacts: Path, start: str, end: str) -> List[Dict[str, Any]]:
    p = artifacts / "json" / f"econ_filtered_{start}_{end}.json"
    if not p.exists():
        raise FileNotFoundError(p)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        return payload.get("entities") or payload.get("items") or payload.get("events") or []
    return payload if isinstance(payload, list) else []


def _make_econ_synthetic_text(item: Dict[str, Any], *, start: str) -> str:
    """
    If we don't have an accessible URL we synthesize a DC-style block.
    """
    title = item.get("title") or "Economic item"
    desc = item.get("summary") or item.get("description") or item.get("text") or ""
    date_s = (item.get("published_at") or item.get("date") or item.get("source_date") or start)[:10]
    date_iso = _coerce_iso_date(date_s) or start
    url = item.get("url") or ""
    lines = [
        f"{date_iso} — {title}",
        f"Summary: {desc.strip()[:2000]}",
        f"Source: {url or '(no url)'}",
        "Category: Economic, fiscal, labor, or trade policy",
        "Why Relevant: Economic measures, market shocks, or fiscal decisions can translate directly into democratic stability or instability.",
    ]
    return "\n".join(lines)


# ------------------------------------------------------------
# Runner (COPY MODE)
# ------------------------------------------------------------

def run_builder(
    *,
    source: str = "econ",
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    limit: Optional[int] = None,
    ids: Optional[List[int]] = None,
    skip_existing: bool = False,
) -> Dict[str, Any]:
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    try:
        items = _load_filtered_econ(artifacts, start, end)
    except Exception as e:
        logger.exception("Failed to load filtered econ input")
        return {
            "source": source,
            "events": 0,
            "error": str(e),
            "input_path": f"artifacts/json/econ_filtered_{start}_{end}.json",
        }

    logger.info("Loaded %d econ items", len(items))
    if not items:
        return {"source": source, "events": 0, "path": None}

    # we stay in COPY MODE: one canonical prompt per window
    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    results: List[Dict[str, Any]] = []
    bundle: List[Dict[str, Any]] = []

    for i, idx in enumerate(idxs, 1):
        it = items[idx]
        title = it.get("title") or f"econ-{idx}"
        url = it.get("url") or ""
        published = (it.get("published_at") or it.get("date") or start)[:10]
        logger.info("[%s] item %d/%d — %s", source, i, len(idxs), title[:120])

        try:
            if url:
                # call the shared extractor on the real page
                text = extract_events_from_url(
                    url,
                    system_prompt=system_prompt,
                    article_title=title,
                    article_date=published,
                    artifacts_root=artifacts_root,
                    idx=idx,
                )
            else:
                synthetic = _make_econ_synthetic_text(it, start=start)
                text = extract_events_from_text(
                    synthetic,
                    system_prompt=system_prompt,
                    artifacts_root=artifacts_root,
                    idx=idx,
                )
        except Exception as e:
            logger.exception("Extractor failed on econ item %d (%s)", idx, title)
            text = f"(Extraction error: {e})"

        # persist raw LLM text
        with open(artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt", "w", encoding="utf-8") as f:
            f.write(text)

        bundle.append({"idx": idx, "title": title, "url": url, "chars": len(text)})
        results.append({"_idx": idx, "url": url, "events_text": text, "_item": it})

    # write LLM bundle
    with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    all_events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []

    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        item = rec.get("_item") or {}
        url = rec.get("url") or ""
        orig_title = item.get("title") or ""
        orig_tags = item.get("tags") or []
        topics = derive_topics(orig_title, orig_tags)

        if not raw or raw.startswith("(Extraction error:")):
            noncompliant.append({"idx": rec["_idx"], "url": url, "reason": "no_blocks"})
            continue

        evs = _parse_llm_events_canonical(raw, article_url=url, logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "url": url, "reason": "no_blocks"})

        for ev in evs:
            ev.setdefault("source", source)
            # keep standard tags AND econ-specific ones
            base_tags = set(ev.get("tags") or [])
            base_tags.add(source)
            for t in topics:
                base_tags.add(f"econ:{t}")
            ev["tags"] = sorted(base_tags)
            ev.setdefault("attacks", [])
            # add topic metadata for downstream
            ev["econ_topics"] = topics
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
        "events": len(all_events),
        "path": str(out_json),
        "noncompliant": noncompliant or None,
    }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Economic builder (COPY MODE)")
    ap.add_argument("--source", default="econ")
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