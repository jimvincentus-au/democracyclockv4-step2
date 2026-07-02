#!/usr/bin/env python3
"""
step2_buildscotusopinions_v4.py — official SCOTUS opinions builder for Democracy Clock V4

Purpose:
- Load filtered official SCOTUS slip-opinion entities from artifacts/json
- Convert each official opinion record into one deterministic event
- Write artifacts/eventjson/scotusopinions_events_START_END.json

Design:
- This is not an LLM builder. Official SCOTUS opinions are already structured
  primary-source events. The builder preserves provenance and emits one event per
  official opinion PDF/index item.
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

TZ_DEFAULT = "Australia/Brisbane"
DEFAULT_SOURCE = "scotusopinions"
DEFAULT_CATEGORY = "Courts / Supreme Court / Opinions"

_REVISION_MARKER_RE = re.compile(r"\s+Revisions?\s*:\s*\d{1,2}/\d{1,2}/\d{2,4}\s*$", re.IGNORECASE)


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


def _first_text_value(ent: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        value = ent.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _event_url(ent: Dict[str, Any]) -> str:
    return _first_text_value(ent, ["canonical_url", "url", "pdf_url", "link", "source_url", "permalink"])


def _load_filtered_list(source: str, start: str, end: str, artifacts: Path) -> List[Dict[str, Any]]:
    """Load filtered entities for the requested window.

    Preferred path:
        artifacts/json/{source}_filtered_{start}_{end}.json

    If the exact file is missing or empty, fall back to all available matching
    filtered files, combine records, de-duplicate, and apply the requested date
    window locally.
    """
    json_dir = artifacts / "json"
    exact_path = json_dir / f"{source}_filtered_{start}_{end}.json"

    def _extract_items(path: Path) -> List[Dict[str, Any]]:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            data = payload.get("entities") or payload.get("items") or payload.get("events") or []
            return data if isinstance(data, list) else []
        return payload if isinstance(payload, list) else []

    def _item_date(ent: Dict[str, Any]) -> str:
        for key in ["post_date", "opinion_date", "date", "published", "published_at", "source_date"]:
            iso = _coerce_iso_date(ent.get(key))
            if iso:
                return iso
        return ""

    def _in_window(ent: Dict[str, Any]) -> bool:
        iso = _item_date(ent)
        return bool(iso and start <= iso <= end)

    if exact_path.exists():
        exact_items = _extract_items(exact_path)
        if exact_items:
            return exact_items

    combined: List[Dict[str, Any]] = []
    seen = set()
    for path in sorted(json_dir.glob(f"{source}_filtered_*.json")):
        for ent in _extract_items(path):
            if not isinstance(ent, dict):
                continue
            if not _in_window(ent):
                continue
            key = (
                _event_url(ent),
                _first_text_value(ent, ["case_name", "title", "name"]),
                _item_date(ent),
            )
            if key in seen:
                continue
            seen.add(key)
            combined.append(ent)

    return combined


def _event_date(ent: Dict[str, Any], fallback: str) -> str:
    for key in ["post_date", "opinion_date", "date", "published", "published_at", "source_date"]:
        iso = _coerce_iso_date(ent.get(key))
        if iso:
            return iso
    return fallback[:10]


def _docket_numbers(ent: Dict[str, Any]) -> List[str]:
    raw = ent.get("docket_numbers") or ent.get("dockets") or ent.get("docket") or []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    text = str(raw or "").strip()
    if not text:
        return []
    return [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]


def _clean_title_piece(value: str) -> str:
    return " ".join(str(value or "").replace("\n", " ").split())


def _strip_revision_marker(value: str) -> str:
    """Remove SCOTUS index revision notes accidentally captured in case titles."""
    return _REVISION_MARKER_RE.sub("", str(value or "")).strip()


def _clean_case_name(value: str) -> str:
    return _clean_title_piece(_strip_revision_marker(value))


def _case_name(ent: Dict[str, Any]) -> str:
    raw = _first_text_value(ent, ["case_name", "title", "name", "headline"])
    cleaned = _clean_case_name(raw)
    return cleaned or "Supreme Court opinion"


def _event_title(ent: Dict[str, Any]) -> str:
    case_name = _case_name(ent)
    dockets = _docket_numbers(ent)
    if dockets and not any(d in case_name for d in dockets):
        return f"Supreme Court releases opinion in {case_name} ({', '.join(dockets)})"
    return f"Supreme Court releases opinion in {case_name}"


def _event_summary(ent: Dict[str, Any]) -> str:
    case_name = _case_name(ent)
    dockets = _docket_numbers(ent)
    citation = _first_text_value(ent, ["us_reporter_citation", "citation", "reporter_citation"])
    author = _first_text_value(ent, ["justice_or_author", "author", "justice"])

    pieces = [f"The Supreme Court released an official slip opinion in {case_name.rstrip('.')}."]
    if dockets:
        pieces.append(f"Docket number(s): {', '.join(dockets)}.")
    if citation:
        pieces.append(f"Reporter citation: {citation.rstrip('.')}.")
    if author:
        pieces.append(f"Listed author/justice: {author.rstrip('.')}.")
    return " ".join(pieces)


def _why_relevant(ent: Dict[str, Any]) -> str:
    case_name = _case_name(ent)
    return (
        f"The Supreme Court's official opinion in {case_name} is a primary legal source. "
        "Supreme Court opinions can define binding national law and alter the legal position of "
        "the executive branch, federal agencies, states, litigants, and democratic institutions."
    )


def _tags(ent: Dict[str, Any]) -> List[str]:
    tags = [DEFAULT_SOURCE, "supreme_court", "official_record", "opinion"]
    section = _first_text_value(ent, ["section", "topic", "category"])
    if section:
        tags.append(section.lower().replace(" ", "_"))
    return list(dict.fromkeys(tags))


def _build_event(ent: Dict[str, Any], *, source: str, start: str) -> Dict[str, Any]:
    source_date = _event_date(ent, start)
    url = _event_url(ent)
    title = _event_title(ent)
    case_name = _case_name(ent)

    return {
        "event_id": make_event_id(source, title, url, source_date),
        "source": source,
        "source_date": source_date,
        "post_date": compute_post_date_str(source_date),
        "title": title,
        "url": url,
        "summary": _event_summary(ent),
        "why_relevant": _why_relevant(ent),
        "category": DEFAULT_CATEGORY,
        "sources": [url] if url else [],
        "tags": _tags(ent),
        "attacks": [],
        "case_name": case_name,
        "docket_numbers": _docket_numbers(ent),
        "justice_or_author": _first_text_value(ent, ["justice_or_author", "author", "justice"]),
        "us_reporter_citation": _first_text_value(ent, ["us_reporter_citation", "citation", "reporter_citation"]),
        "pdf_url": _first_text_value(ent, ["pdf_url", "canonical_url", "url"]),
        "discovery_url": _first_text_value(ent, ["discovery_url", "source_url"]),
        "source_record": ent,
    }


def _is_valid_event(ev: Dict[str, Any]) -> bool:
    return bool(ev.get("source_date") and ev.get("title") and ev.get("url"))


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
    """Run deterministic official SCOTUS opinions builder."""
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
    if filtered_path.exists():
        logger.info("Loaded %d official opinion entities for requested window; exact file exists at %s", len(items), filtered_path)
    else:
        logger.info(
            "Loaded %d official opinion entities for requested window via fallback scan; exact file absent at %s",
            len(items),
            filtered_path,
        )

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    if not idxs:
        noncompliant = [{
            "idx": None,
            "url": "",
            "reason": "no_input_entities_for_requested_window",
            "message": f"No {source} filtered entities were available for {start} → {end}.",
        }]
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "source": source,
                    "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
                    "events": [],
                    "noncompliant": noncompliant,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.warning("No input entities for %s %s → %s; wrote diagnostic output to %s", source, start, end, out_json)
        return {"source": source, "events": 0, "path": str(out_json), "noncompliant": 1}

    events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []

    for run_idx, item_idx in enumerate(idxs, 1):
        ent = items[item_idx]
        event = _build_event(ent, source=source, start=start)
        if _is_valid_event(event):
            events.append(event)
            logger.info("[%s] built event %d/%d idx=%d title=%s", source, run_idx, len(idxs), item_idx, event["title"][:120])
        else:
            noncompliant.append({
                "idx": item_idx,
                "url": _event_url(ent),
                "reason": "missing_required_event_fields",
                "source_date": event.get("source_date", ""),
                "title": event.get("title", ""),
            })
            logger.warning("[%s] noncompliant idx=%d reason=missing_required_event_fields", source, item_idx)

    out_payload = {
        "source": source,
        "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
        "events": events,
        "noncompliant": noncompliant,
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, ensure_ascii=False, indent=2)

    logger.info("Wrote %s (events=%d noncompliant=%d)", out_json, len(events), len(noncompliant))
    return {"source": source, "events": len(events), "path": str(out_json), "noncompliant": len(noncompliant)}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — official SCOTUS opinions builder")
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