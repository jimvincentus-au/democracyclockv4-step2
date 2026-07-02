

#!/usr/bin/env python3
"""
step2_buildscotusblog_v4.py — SCOTUSblog builder for Democracy Clock V4

Purpose:
- Load filtered SCOTUSblog Step 2 entities from artifacts/json
- Build controlled synthetic text for each article/item
- Send that text through the canonical Step 2 LLM extractor
- Parse canonical LLM event blocks into structured event JSON

Design:
- SCOTUSblog is an article/commentary/discovery source, so this builder follows the
  Democracy Docket / 50501 LLM-builder pattern rather than the deterministic official
  record pattern used for official SCOTUS opinions/orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_text

# ------------------------------------------------------------
# Canonical LLM block parser
# ------------------------------------------------------------

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)

TZ_DEFAULT = "Australia/Brisbane"
DEFAULT_SOURCE = "scotusblog"


def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    """Parse canonical LLM output into event dictionaries."""
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

            summary_match = _SUM_RE.match(ln)
            if summary_match:
                summary = summary_match.group(1).strip()
                continue

            source_match = _SRC_RE.match(ln)
            if source_match:
                src_line = source_match.group(1).strip()
                u = _URL_EX.search(src_line)
                url = u.group(0) if u else ""
                continue

            category_match = _CAT_RE.match(ln)
            if category_match:
                cat = category_match.group(1).strip()
                continue

            why_match = _WHY_RE.match(ln)
            if why_match:
                why = why_match.group(1).strip()
                continue

            attacks_match = _ATK_RE.match(ln)
            if attacks_match:
                attacks_line = attacks_match.group(1).strip()
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
# Helpers
# ------------------------------------------------------------


def make_event_id(source: str, title: str, url: Optional[str], source_date: str) -> str:
    base = f"{source}|{title.strip()}|{(url or '').strip()}|{source_date}"
    return "sha1:" + hashlib.sha1(base.encode("utf-8")).hexdigest()


def compute_post_date_str(source_date_str: str) -> str:
    d = datetime.strptime(source_date_str, "%Y-%m-%d").date()
    return (d + timedelta(days=1)).isoformat()


def _coerce_iso_date(v: Any) -> Optional[str]:
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v or "").strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _load_filtered_list(source: str, start: str, end: str, artifacts: Path) -> List[Dict[str, Any]]:
    """Accept dict with entities/items/events or a bare list and return a list."""
    p = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    if not p.exists():
        raise FileNotFoundError(p)

    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, dict):
        data = payload.get("entities") or payload.get("items") or payload.get("events") or []
        return data if isinstance(data, list) else []

    return payload if isinstance(payload, list) else []


def _first_text_value(ent: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        value = ent.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _scotusblog_url(ent: Dict[str, Any]) -> str:
    return _first_text_value(ent, ["canonical_url", "url", "link", "source_url", "permalink"])


def _scotusblog_date(ent: Dict[str, Any], fallback: str) -> str:
    for key in ["post_date", "date", "published", "published_at", "source_date"]:
        iso = _coerce_iso_date(ent.get(key))
        if iso:
            return iso
    return fallback[:10]


def _article_body(ent: Dict[str, Any]) -> str:
    return _first_text_value(ent, [
        "summary",
        "dek",
        "excerpt",
        "description",
        "content",
        "body",
        "html",
        "text",
    ])


def _scotusblog_category(ent: Dict[str, Any]) -> str:
    raw = ent.get("topic") or ent.get("category") or ent.get("section") or ent.get("topics") or ""
    if isinstance(raw, list):
        pieces = [str(x).strip() for x in raw if str(x).strip()]
        if pieces:
            return "Courts / Supreme Court / " + ", ".join(pieces[:3])
    raw_s = str(raw or "").strip()
    if raw_s:
        return f"Courts / Supreme Court / {raw_s}"
    return "Courts / Supreme Court / Analysis"


def _build_synthetic_text(ent: Dict[str, Any], *, start: str) -> str:
    title = _first_text_value(ent, ["title", "headline", "name"]) or "(untitled SCOTUSblog item)"
    url = _scotusblog_url(ent) or "(no url)"
    date_s = _scotusblog_date(ent, start)
    category = _scotusblog_category(ent)
    body = _article_body(ent)

    if not body:
        body = title

    body = body.strip()[:4000]

    return (
        f"{date_s} — {title}\n"
        f"Summary: {body}\n"
        f"Source: {url}\n"
        f"Category: {category}\n"
        "Why Relevant: SCOTUSblog tracks Supreme Court merits cases, emergency applications, orders, "
        "opinions, and analysis that may affect executive power, civil rights, elections, agency authority, "
        "immigration, criminal justice, and democratic governance."
    )


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
    """Run the SCOTUSblog article-style builder."""
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    out_json = artifacts / "eventjson" / f"{source}_events_{start}_{end}.json"
    if skip_existing and out_json.exists():
        logger.info("Skipping existing output: %s", out_json)
        return {"source": source, "events": -1, "path": str(out_json), "skipped": True}

    filtered_path = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    items = _load_filtered_list(source, start, end, artifacts)
    logger.info("Loaded %d entities from %s", len(items), filtered_path)

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt(source, include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results: List[Dict[str, Any]] = []
    bundle: List[Dict[str, Any]] = []

    for i, idx in enumerate(idxs, 1):
        ent = items[idx]
        title = _first_text_value(ent, ["title", "headline", "name"]) or "(untitled SCOTUSblog item)"
        url = _scotusblog_url(ent)
        synthetic_text = _build_synthetic_text(ent, start=start)

        logger.info("[%s] item %d/%d idx=%d title=%s", source, i, len(idxs), idx, title[:120])
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

        llm_out_path = artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt"
        with open(llm_out_path, "w", encoding="utf-8") as f:
            f.write(text)

        bundle.append({"idx": idx, "url": url, "chars": len(text), "title": title})
        results.append({"_idx": idx, "url": url, "events_text": text, "input_title": title})

    bundle_path = artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json"
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    all_events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []

    for rec in results:
        raw = (rec.get("events_text") or "").strip()
        if not raw:
            noncompliant.append({"idx": rec["_idx"], "url": rec["url"], "reason": "no_blocks"})
            continue

        evs = _parse_llm_events_canonical(raw, article_url=rec["url"], logger=logger)
        if not evs:
            noncompliant.append({"idx": rec["_idx"], "url": rec["url"], "reason": "no_blocks"})
            continue

        for ev in evs:
            ev.setdefault("source", source)
            if not ev.get("source"):
                ev["source"] = source
            ev.setdefault("tags", [])
            if source not in ev["tags"]:
                ev["tags"].append(source)
            ev.setdefault("attacks", [])
            ev["event_id"] = make_event_id(source, ev.get("title", ""), ev.get("url"), ev.get("source_date", ""))
            all_events.append(ev)

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

    logger.info("Wrote %s (events=%d noncompliant=%d)", out_json, len(all_events), len(noncompliant))
    return {"source": source, "events": len(all_events), "path": str(out_json), "noncompliant": len(noncompliant)}


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — SCOTUSblog builder")
    ap.add_argument("--source", default=DEFAULT_SOURCE, help=f"Source name (default: {DEFAULT_SOURCE})")
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