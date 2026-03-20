#!/usr/bin/env python3
# step2_buildjustsecurity_v5.py — Just Security builder (COPY MODE from buildfederalregister_v4.py)
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt
from step2_extractor_v4 import extract_events_from_text

import hashlib
from datetime import datetime, date, timedelta

# ------------------------------------------------------------
# Helpers
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


def _clean(s: Any) -> str:
    return " ".join(str(s or "").split()).strip()


# ------------------------------------------------------------
# Deterministic helpers for case_update events
# ------------------------------------------------------------

def _norm_sources(url: str) -> List[str]:
    u = _clean(url)
    return [u] if u else []


def _norm_tags(source: str) -> List[str]:
    tags = [source]
    if "justsecurity" not in tags:
        tags.append("justsecurity")
    return tags


def _first_sentence(text: str) -> str:
    s = _clean(text)
    if not s:
        return ""
    m = re.search(r"(.+?[.?!])(?:\s|$)", s)
    if m:
        return m.group(1).strip()
    return s


def _truncate_title(text: str, limit: int = 140) -> str:
    s = _clean(text)
    if len(s) <= limit:
        return s
    cut = s[:limit].rstrip(" ,;:-")
    return cut


# Helper functions for improved case_update titles
def _safe_case_caption(raw_title: str) -> str:
    s = _clean(raw_title)
    return _truncate_title(s, limit=160) if s else "Just Security litigation update"


def _derive_case_update_title(raw_title: str, update_text: str) -> str:
    """
    Build a conservative procedural title for a case-update record.
    Prefer a short procedural label plus the case caption. Do not try to turn the
    first sentence of the update into the title; that is what created fragments like
    `The D.C.` and `Judge R.`.
    """
    caption = _safe_case_caption(raw_title)
    u = _clean(update_text)
    low = u.lower()

    patterns: List[tuple[str, str]] = [
        ("notice of appeal", "Notice of appeal filed"),
        ("appealed", "Appeal filed"),
        ("petition for a writ of certiorari", "Certiorari petition filed"),
        ("granted the petition for a writ of certiorari", "Certiorari granted"),
        ("granted certiorari", "Certiorari granted"),
        ("motion to stay", "Stay motion decided"),
        ("stay pending appeal", "Stay pending appeal decided"),
        ("summary judgment", "Summary judgment ruling"),
        ("preliminary injunction", "Preliminary injunction ruling"),
        ("temporary restraining order", "Temporary restraining order ruling"),
        ("motion to dismiss", "Motion to dismiss decided"),
        ("dismissed", "Dismissal entered"),
        ("consolidated", "Cases consolidated"),
        ("remand", "Remand ordered"),
        ("vacated", "Ruling vacated"),
        ("affirmed", "Ruling affirmed"),
        ("reversed", "Ruling reversed"),
        ("granted", "Motion granted"),
        ("denied", "Motion denied"),
    ]

    for needle, label in patterns:
        if needle in low:
            return _truncate_title(f"{label} — {caption}", limit=160)

    return _truncate_title(f"Case update — {caption}", limit=160)


def _build_case_update_event(source: str, it: Dict[str, Any], fallback_date: str) -> Dict[str, Any]:
    """
    Deterministic event builder for Just Security case-update rows.
    This bypasses the generic extractor so the output stays tightly anchored to the
    dated update text rather than drifting into broader case-history recap.
    """
    raw_title = _clean(it.get("title") or it.get("raw_title") or "Just Security litigation update")
    update_text = _clean(it.get("update_text") or "")
    source_date = (_clean(it.get("source_date") or it.get("post_date") or "") or fallback_date)[:10]
    url = _clean(it.get("url") or "")

    title = _derive_case_update_title(raw_title, update_text)
    summary = update_text or raw_title

    return {
        "source_date": source_date,
        "title": title,
        "url": url,
        "summary": summary,
        "why_relevant": "This litigation update changes the posture of an active legal challenge to Trump administration action.",
        "category": "Judicial Developments",
        "sources": _norm_sources(url),
        "tags": _norm_tags(source),
        "attacks": [],
        "source": source,
    }


def _load_filtered(artifacts: Path, source: str, start: str, end: str) -> List[Dict[str, Any]]:
    """
    Load filtered JSON with a tolerant shape:
    - canonical: {"entities":[...]} or {"items":[...]} or {"events":[...]}
    - bare list: [...]
    """
    p = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    if not p.exists() and source != "justsecurity":
        p2 = artifacts / "json" / f"justsecurity_filtered_{start}_{end}.json"
        if p2.exists():
            p = p2
    if not p.exists():
        raise FileNotFoundError(p)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        for k in ("entities", "items", "events"):
            if isinstance(payload.get(k), list):
                return payload[k]
        for v in payload.values():
            if isinstance(v, list):
                return v
        return []
    return payload if isinstance(payload, list) else []


def _make_js_synthetic_text(it: Dict[str, Any], fallback_date: str) -> str:
    """
    Build a small synthetic 'article' for the Just Security tracker item so the
    standard extractor/prompt path can produce canonical Step 2 events.

    IMPORTANT:
    - For `case_update` records, the update text is the primary source material.
      Case summary, if present, is clearly subordinate background and is omitted
      entirely when the update is already intelligible on its own.
    - For `case_filed` records, the case summary can carry more weight because the
      filing often needs brief context to be understandable.
    """
    title = _clean(it.get("title") or it.get("raw_title") or "Just Security litigation update")
    update_text = _clean(it.get("update_text") or "")
    case_summary = _clean(it.get("summary") or "")
    event_kind = _clean(it.get("event_kind") or "")
    court = _clean(it.get("court_name") or "")
    docket = _clean(it.get("docket") or "")
    jurisdiction = _clean(it.get("jurisdiction") or "")
    source_date = (_clean(it.get("source_date") or it.get("post_date") or "") or fallback_date)[:10]
    url = _clean(it.get("url") or "")

    lines = [f"{source_date} — {title}"]

    if event_kind:
        lines.append(f"Event Kind: {event_kind}")
    if court:
        lines.append(f"Court: {court}")
    if jurisdiction:
        lines.append(f"Jurisdiction: {jurisdiction}")
    if docket:
        lines.append(f"Docket: {docket}")

    if event_kind == "case_update":
        if update_text:
            lines.append(f"Update Summary: {update_text}")
            lines.append(f"Summary: {update_text}")
        else:
            lines.append("Summary: Litigation update recorded in the Just Security tracker.")

        # Only include subordinate background when we actually have no update text.
        if case_summary and not update_text:
            lines.append(f"Background: {case_summary}")

        lines.append("Instruction: Focus on the dated update. Use background only if required for comprehension.")

    else:
        # Filing records often need the case summary to be understandable.
        if case_summary:
            lines.append(f"Case Summary: {case_summary}")
        if update_text:
            lines.append(f"Update Summary: {update_text}")

        if update_text:
            lines.append(f"Summary: {update_text}")
        elif case_summary:
            lines.append(f"Summary: {case_summary}")
        else:
            lines.append("Summary: Just Security litigation tracker entry.")

        lines.append("Instruction: Focus on the operative filing event and use only the minimum context needed.")

    lines.append(f"Source: {url or '(no url)'}")
    lines.append("Category: Judicial Developments")
    lines.append(
        "Why Relevant: Litigation over Trump administration actions affects judicial review, executive power, rights enforcement, and the rule of law."
    )

    return "\n".join(lines)


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
    Just Security builder (COPY MODE): identical skeleton to buildfederalregister_v4.py,
    but always uses synthetic text built from the tracker row so the output stays
    in the standard builder/extractor/prompt pipeline.
    """
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    items = _load_filtered(artifacts, source, start, end)
    logger.info("Loaded %d entities", len(items))
    if not items:
        return {
            "source": source,
            "count": 0,
            "events_path": None,
            "input_json": str(artifacts / 'json' / f"{source}_filtered_{start}_{end}.json"),
        }

    idxs_all = list(range(len(items)))
    if ids:
        idxs = [i - 1 for i in ids if 1 <= i <= len(items)]
    else:
        idxs = idxs_all
    if limit:
        idxs = idxs[:limit]

    prompt_path = artifacts / "log" / f"{source}_prompt_{start}_{end}.txt"
    system_prompt = compose_system_prompt("justsecurity", include_attacks=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)

    results, bundle = [], []

    for i, idx in enumerate(idxs, 1):
        it = items[idx]
        title = _clean(it.get("title") or it.get("raw_title") or "")
        url = _clean(it.get("url") or "")
        source_date = (_clean(it.get("source_date") or it.get("post_date") or "") or start)[:10]

        logger.debug("[%s] item %d/%d", source, i, len(idxs))

        event_kind = _clean(it.get("event_kind") or "")

        if event_kind == "case_update":
            ev = _build_case_update_event(source, it, source_date or start)
            results.append({"_idx": idx, "url": url or "", "events": [ev]})
            bundle.append({"idx": idx, "url": url or "(deterministic)", "chars": 0, "mode": "deterministic_case_update"})
            logger.debug("[%s] idx=%d deterministic case_update url=%s", source, idx, url or "(deterministic)")
        else:
            try:
                synthetic = _make_js_synthetic_text(it, source_date or start)
                text = extract_events_from_text(
                    synthetic,
                    system_prompt=system_prompt,
                    artifacts_root=str(artifacts_root),
                    idx=idx,
                )
            except Exception as e:
                logger.exception("Extractor failed on %s idx=%d", source, idx)
                text = f"(Extraction error: {e})"

            with open(artifacts / "log" / f"{source}_llm_out_idx{idx}_{start}_{end}.txt", "w", encoding="utf-8") as f:
                f.write(text)
            bundle.append({"idx": idx, "url": url or "(synthetic)", "chars": len(text), "mode": "extractor"})
            results.append({"_idx": idx, "url": url or "", "events_text": text})
            logger.debug("[%s] idx=%d extractor_chars=%d url=%s", source, idx, len(text), url or "(synthetic)")

    with open(artifacts / "log" / f"{source}_llm_bundle_{start}_{end}.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    all_events, noncompliant = [], []
    items_with_events = 0
    items_zero_events = 0
    items_multi_events = 0

    for rec in results:
        idx = rec["_idx"]
        rec_url = rec.get("url") or ""

        if "events" in rec:
            evs = rec.get("events") or []
            logger.debug("[%s] idx=%d parsed_events=%d (deterministic)", source, idx, len(evs))
            if not evs:
                noncompliant.append({"idx": idx, "url": rec_url, "reason": "no_blocks"})
                items_zero_events += 1
                logger.warning("[%s] idx=%d deterministic path yielded zero events", source, idx)
                continue
        else:
            raw = (rec.get("events_text") or "").strip()

            if not raw:
                noncompliant.append({"idx": idx, "url": rec_url, "reason": "empty_output"})
                items_zero_events += 1
                logger.warning("[%s] idx=%d produced empty extractor output", source, idx)
                continue

            if raw.startswith("(Extraction error:"):
                noncompliant.append({"idx": idx, "url": rec_url, "reason": "extraction_error"})
                items_zero_events += 1
                logger.warning("[%s] idx=%d extractor error output", source, idx)
                continue

            evs = _parse_llm_events_canonical(raw, article_url=rec_url, logger=logger)
            logger.debug("[%s] idx=%d parsed_events=%d", source, idx, len(evs))

            if not evs:
                noncompliant.append({"idx": idx, "url": rec_url, "reason": "no_blocks"})
                items_zero_events += 1
                logger.warning("[%s] idx=%d parsed zero canonical events", source, idx)
                continue

        items_with_events += 1
        if len(evs) > 1:
            items_multi_events += 1
            logger.info("[%s] idx=%d produced %d events", source, idx, len(evs))
        else:
            logger.debug("[%s] idx=%d produced 1 event", source, idx)

        for ev in evs:
            ev.setdefault("source", source)
            ev.setdefault("tags", _norm_tags(source))
            ev.setdefault("attacks", [])
            all_events.append(ev)

    logger.info(
        "[%s] summary: input_items=%d items_with_events=%d zero_event_items=%d multi_event_items=%d total_events=%d noncompliant=%d",
        source,
        len(results),
        items_with_events,
        items_zero_events,
        items_multi_events,
        len(all_events),
        len(noncompliant),
    )

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
        "items_in": len(results),
        "items_with_events": items_with_events,
        "items_zero_events": items_zero_events,
        "items_multi_events": items_multi_events,
        "noncompliant": len(noncompliant),
        "events_path": str(out_json),
        "input_json": str(artifacts / "json" / f"{source}_filtered_{start}_{end}.json"),
        "prompt_path": str(prompt_path),
    }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — Just Security builder (COPY MODE)")
    ap.add_argument("--source", default="justsecurity", help="Use 'justsecurity' unless overriding")
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