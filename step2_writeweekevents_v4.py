#!/usr/bin/env python3
# writeweekevents_v4.py — Step-3 writer: merge Step-2 event JSONs → Master Event Log (TXT)

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger, resolve_date_window

# ──────────────────────────────────────────────────────────────────────────────
# Canonical category order (unknowns appended after these)
# ──────────────────────────────────────────────────────────────────────────────
CATEGORY_ORDER = [
    "Executive Actions & Orders",
    "Legislative & Oversight Activity",
    "Judicial Developments",
    "Law Enforcement & Surveillance",
    "Elections & Representation",
    "Civil Society & Protest",
    "Information & Media Control",
    "Economic & Regulatory Power",
    "Appointments & Patronage",
    "Transparency & Records",
    "International Relations",
    "Civil–Military Relations & State Violence",
]

# Render targets
MASTER_TXT_NAME = "master_events_{start}_{end}.txt"
MASTER_IDX_NAME = "master_index_{start}_{end}.json"

HUMANIZED_ATTACKS = {
    # PART I – The People He Harmed
    "children": "Children",
    "women": "Women",
    "minorities": "Minorities",
    "immigrants_refugees": "Immigrants & Refugees",
    "lgbtq": "LGBTQ+ People",
    "workers": "Workers",
    "poor": "The Poor",
    "veterans": "Veterans",
    "disabled": "People with Disabilities",
    "sick_vulnerable": "The Medically Vulnerable",

    # PART II – The Nation He Degraded
    "truth": "Truth & Honesty",
    "science": "Science & Evidence",
    "education": "Education",
    "culture_art": "Culture & Art",
    "public_memory": "Public Memory",
    "faith": "Faith & Religion",
    "decency": "Decency & Ethics",
    "hope": "Hope & Optimism",

    # PART III – The Institutions He Broke
    "presidency": "The Presidency",
    "courts": "The Courts",
    "congress": "Congress",
    "civil_service": "Civil Service",
    "justice_dept": "Justice Department",
    "intelligence": "Intelligence Community",
    "military": "The Military",
    "diplomacy": "Diplomacy",
    "ig_watchdogs": "Inspectors General & Watchdogs",
    "public_service": "Public Service",

    # PART IV – The Truth He Erased
    "press": "Free Press",
    "information": "Information & Transparency",
    "whistleblowers": "Whistleblowers",
    "internet": "Internet & Digital Freedom",
    "knowledge": "Knowledge & Data Integrity",
    "reality": "Reality",

    # PART V – The World He Unmade
    "allies": "Allies & Partnerships",
    "global_democracy": "Global Democracy",
    "trade": "Trade & Commerce",
    "peace": "Peace & Stability",
    "climate_cooperation": "Climate Cooperation",
    "idea_of_america": "The Idea of America",

    # PART VI – The Republic Itself
    "constitution": "The Constitution",
    "separation_of_powers": "Separation of Powers",
    "rule_of_law": "Rule of Law",
    "emoluments": "Emoluments & Self-Enrichment",
    "birthright_citizenship": "Birthright Citizenship",
    "amendment_22": "22nd Amendment (Term Limits)",
    "amendment_25": "25th Amendment (Capacity & Succession)",
    "peaceful_transfer": "Peaceful Transfer of Power",
    "union": "Federal Union",

    # PART VII – The Future We Must Rebuild
    "environment": "Environment",
    "economy": "Economy",
    "public_health": "Public Health",
    "civic_education": "Civic Education",
    "future": "Future Generations",
    "reality_itself": "Reality Itself",
}

def humanize_attacks(handles: list[str]) -> str:
    if not handles:
        return "None"
    readable = [HUMANIZED_ATTACKS.get(h, h.replace("_", " ").title()) for h in handles]
    return ", ".join(readable)

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────

def _iso(d: date) -> str:
    return d.isoformat()

def _compute_window(start_str: str, weeks: Optional[int], end_str: Optional[str]) -> Tuple[str, str]:
    y, m, d = (int(x) for x in start_str.split("-"))
    s = date(y, m, d)
    if weeks is not None:
        e = s + timedelta(days=weeks * 7 - 1)
        return _iso(s), _iso(e)
    y2, m2, d2 = (int(x) for x in (end_str or "").split("-"))
    e = date(y2, m2, d2)
    return _iso(s), _iso(e)

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — write Master Event Log (TXT) from event JSONs")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--weeks", type=int, help="Number of weeks (end = start + 7*weeks - 1)")
    grp.add_argument("--end", help="End date YYYY-MM-DD")
    ap.add_argument("--level", default="INFO", help="Logging level (DEBUG, INFO, ...)")
    ap.add_argument(
        "--artifacts-root",
        default=str(ARTIFACTS_ROOT),
        help="Root folder for artifacts (default from config_v4.py)",
    )
    ap.add_argument("--only", nargs="+", help="Include only these sources (space-separated).")
    ap.add_argument("--skip", nargs="+", help="Exclude these sources (space-separated).")
    ap.add_argument("--preview", type=int, help="Write only the first N events after sorting (for QA).")
    ap.add_argument("--strict", action="store_true", help="Drop events with missing required fields instead of rendering blanks.")
    ap.add_argument("--no-header", action="store_true", help="Omit header block in the TXT.")
    ap.add_argument("--no-footer", action="store_true", help="Omit footer rollups in the TXT.")
    ap.add_argument("--week", type=int, help="Week number where week 1 = 2025-01-20..2025-01-24; others Sat–Fri")
    return ap.parse_args()

def _want_source(source: str, only: Optional[List[str]], skip: Optional[List[str]]) -> bool:
    if only:
        return source in only
    if skip:
        return source not in skip
    return True

def _discover_eventjson_files(root: Path, start: str, end: str) -> List[Path]:
    ej = root / "eventjson"
    if not ej.exists():
        return []
    pattern = re.compile(rf"^(.+)_events_{re.escape(start)}_{re.escape(end)}\.json$")
    files: List[Path] = []
    for p in ej.glob(f"*_*_{start}_{end}.json"):
        if pattern.match(p.name):
            files.append(p)
    return sorted(files)

def _source_from_filename(name: str) -> str:
    # <source>_events_<start>_<end>.json  →  <source>
    m = re.match(r"^(.+)_events_.+\.json$", name)
    return m.group(1) if m else name

def _safe_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    # Accept YYYY-MM-DD or other common strings
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # Try to pick YYYY-MM-DD within the string
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            return None
    return None

def _cat_rank(cat: str) -> Tuple[int, str]:
    if not cat:
        return (len(CATEGORY_ORDER) + 1, "")
    try:
        return (CATEGORY_ORDER.index(cat), cat)
    except ValueError:
        return (len(CATEGORY_ORDER), cat)  # unknowns just before blank

# ──────────────────────────────────────────────────────────────────────────────
# Week mapping (Democracy Clock convention for Trump term)
# Week 1: Mon Jan 20, 2025 – Fri Jan 24, 2025
# Weeks 2+ : Saturday–Friday blocks starting Sat Jan 25, 2025
# ──────────────────────────────────────────────────────────────────────────────
_ANCHOR_W1_START = date(2025, 1, 20)
_ANCHOR_W1_END   = date(2025, 1, 24)
_ANCHOR_W2_START = date(2025, 1, 25)

def dc_week_for(d: date) -> Optional[Tuple[int, date, date]]:
    """Return (week_number, week_start, week_end) for a given date under
    Democracy Clock convention, or None if the date precedes 2025-01-20.
    """
    if d < _ANCHOR_W1_START:
        return None
    if d <= _ANCHOR_W1_END:
        return (1, _ANCHOR_W1_START, _ANCHOR_W1_END)
    # Weeks 2+ are Saturday–Friday blocks from 2025-01-25
    delta_days = (d - _ANCHOR_W2_START).days
    week_offset = delta_days // 7  # 0-based from week 2 start
    week_num = 2 + week_offset
    start = _ANCHOR_W2_START + timedelta(days=7 * week_offset)
    end = start + timedelta(days=6)
    return (week_num, start, end)


# ──────────────────────────────────────────────────────────────────────────────
# Normalization
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EventRow:
    source_key: str
    date_iso: str  # may be ""
    date_obj: Optional[date]  # for sorting
    category: str
    title: str
    source_label: str
    url: str
    summary: str
    why: str
    attacks: list[str]   # ← NEW
    # Provenance:
    origin_file: str
    origin_index: int

def _norm_event(e: Dict[str, any], source_key: str, origin_file: str, idx: int, strict: bool, logger) -> Optional[EventRow]:
    """
    Accepts a single event dict from Step-2 JSON and returns a normalized EventRow or None.
    Required fields for writer: date (can be ""), title, summary, category, why, url (can be "").
    """
    # Flexible key reads (be tolerant)
    date_iso = str(
        e.get("date")
        or e.get("event_date")
        or e.get("source_date")   # ← add this
        or e.get("post_date")     # ← and this (some sources use post/publication dates)
        or ""
    ).strip()
    title = str(e.get("title") or "").strip()
    summary = str(e.get("summary") or "").strip()
    category = str(e.get("category") or "").strip()
    why = str(e.get("why_relevant") or e.get("why") or "").strip()
    url = str(e.get("url") or e.get("canonical_url") or "").strip()
    source_label = str(e.get("publication") or e.get("source") or source_key).strip()

    # attacks is optional, but we always normalize to a list
    raw_attacks = e.get("attacks") or []
    if isinstance(raw_attacks, str):
        attacks = [raw_attacks.strip()] if raw_attacks.strip() else []
    elif isinstance(raw_attacks, list):
        attacks = [str(a).strip() for a in raw_attacks if str(a).strip()]
    else:
        attacks = []

    # Parse date to object
    d_obj = _safe_date(date_iso)
    # date_iso should be kept as original string (possibly ""), but for sorting we use d_obj.
    # If d_obj present, keep canonicalized yyyy-mm-dd for rendering.
    if d_obj:
        date_iso = d_obj.isoformat()

    # Strict mode: must have title, summary, category, why
    if strict and (not title or not summary or not category or not why):
        logger.warning("Strict drop: missing fields at %s[%d] (title=%r, cat=%r)", origin_file, idx, title, category)
        return None

    return EventRow(
        source_key=source_key,
        date_iso=date_iso,
        date_obj=d_obj,
        category=category,
        title=title,
        source_label=source_label,
        url=url,
        summary=summary,
        why=why,
        attacks=attacks,
        origin_file=origin_file,
        origin_index=idx,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Rendering
# ──────────────────────────────────────────────────────────────────────────────

def _render_event_lines(ev: EventRow) -> List[str]:
    header_date = ev.date_iso or ""
    header_title = ev.title or "(untitled)"

    lines: List[str] = []
    lines.append(f"=== {header_date} — {header_title}")
    if ev.url:
        lines.append(ev.url)

    lines.append(f"Summary: {ev.summary}")
    lines.append(f"Source: {ev.source_label}")
    lines.append(f"Category: {ev.category}")
    lines.append(f"Why Relevant: {ev.why}")
    # NEW: always show attacks for this event
    if ev.attacks:
        # Human-readable attacks line for TXT
        lines.append(f"Attacks: {humanize_attacks(ev.attacks)}")
    else:
        lines.append("Attacks: []")
    return lines

def _render_header(start: str, end: str, sources: List[str], total: int) -> str:
    parts = []
    parts.append(f"MASTER EVENT LOG")
    parts.append(f"Window: {start} → {end}")
    parts.append(f"Sources: {', '.join(sources) if sources else '(none)'}")
    parts.append(f"Total events (written): {total}")
    parts.append("")  # blank line after header
    return "\n".join(parts)

def _render_footer(events: List[EventRow]) -> str:
    by_date: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    by_cat: Counter[str] = Counter()

    for ev in events:
        by_date[ev.date_iso or ""] += 1
        by_source[ev.source_key] += 1
        by_cat[ev.category or ""] += 1

    def block(title: str, counts: Counter[str], key_sort=None) -> List[str]:
        lines = [title]
        if not counts:
            lines.append("(none)")
            return lines
        items = list(counts.items())
        if key_sort:
            items.sort(key=key_sort)
        else:
            items.sort(key=lambda kv: kv[0])
        for k, v in items:
            label = k if k else "(no date)"
            lines.append(f"- {label}: {v}")
        return lines

    # Sort dates ascending (empty last)
    def date_key(kv):
        k = kv[0]
        if not k:
            return (date.max, "(no date)")
        try:
            return (datetime.strptime(k, "%Y-%m-%d").date(), k)
        except Exception:
            return (date.max, k)

    # Sort categories by canonical order, then label
    def cat_key(kv):
        k = kv[0]
        return _cat_rank(k)

    parts = []
    parts.append("")
    parts.extend(block("Summary by Date:", by_date, key_sort=date_key))
    parts.append("")
    parts.extend(block("Summary by Source:", by_source))
    parts.append("")
    parts.extend(block("Summary by Category:", by_cat, key_sort=cat_key))
    parts.append("")
    parts.append("[END OF MASTER LOG]")
    return "\n".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    args = _parse_args()
    artifacts = Path(args.artifacts_root)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)
    (artifacts / "events").mkdir(parents=True, exist_ok=True)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)

    logger = setup_logger("dc.writer", args.level)

    try:
        start_d, end_d = resolve_date_window(
            start=args.start,
            end=getattr(args, "end", None),
            weeks=args.weeks,
            week=getattr(args, "week", None),
        )
    except ValueError as ve:
        logger.error("Invalid date window: %s", ve)
        return 1

    start_iso, end_iso = start_d.isoformat(), end_d.isoformat()
    logger.info("Write window %s → %s", start_iso, end_iso)
    logger.info("Artifacts: %s", artifacts)

    # Discover candidate eventjson files and filter by only/skip
    files = _discover_eventjson_files(artifacts, start_iso, end_iso)
    if not files:
        logger.warning("No event JSON files found for window. Expected under %s/eventjson/", artifacts)
    selected_files: List[Tuple[str, Path]] = []  # (source_key, path)
    for p in files:
        src = _source_from_filename(p.name)
        if _want_source(src, args.only, args.skip):
            selected_files.append((src, p))
    logger.info("Selected sources: %s", " ".join([s for s, _ in selected_files]) or "(none)")

    # PASS A: Load + normalize (no writing)
    all_rows: List[EventRow] = []
    for src, p in selected_files:
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            logger.exception("Failed to read JSON: %s", p)
            continue
        events = payload.get("events") or []
        logger.info("Loaded %d events from %s", len(events), p.name)
        for i, e in enumerate(events):
            row = _norm_event(e, src, p.name, i, strict=args.strict, logger=logger)
            if row is not None:
                all_rows.append(row)

    if not all_rows:
        logger.warning("No events to write. Exiting with empty output.")
        # Create empty outputs (TXT + JSON index) for consistency
        out_txt = artifacts / "events" / MASTER_TXT_NAME.format(start=start_iso, end=end_iso)
        out_idx = artifacts / "eventjson" / MASTER_IDX_NAME.format(start=start_iso, end=end_iso)
        out_txt.write_text("", encoding="utf-8")
        out_idx.write_text(json.dumps({"window": {"start": start_iso, "end": end_iso}, "events": []}, indent=2), encoding="utf-8")
        return 0

    # PASS B: Sort
    # Primary: date asc (None → at end)
    # Secondary: category by canonical order
    # Tertiary: source then title (stable readability)
    def sort_key(ev: EventRow):
        dkey = ev.date_obj or date.max
        cat_rank = _cat_rank(ev.category)
        return (dkey, cat_rank[0], ev.source_key.lower(), ev.title.lower())

    all_rows.sort(key=sort_key)

    # Preview
    if args.preview and args.preview > 0:
        logger.info("Preview mode: limiting to first %d events after sort.", args.preview)
        all_rows = all_rows[: args.preview]

    # Render TXT
    out_txt = artifacts / "events" / MASTER_TXT_NAME.format(start=start_iso, end=end_iso)
    lines: List[str] = []
    if not args.no_header:
        lines.append(_render_header(start_iso, end_iso, [s for s, _ in selected_files], total=len(all_rows)))
    for ev in all_rows:
        lines.extend(_render_event_lines(ev))
        lines.append("")  # blank line between events
    if not args.no_footer:
        lines.append(_render_footer(all_rows))

    out_txt.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    logger.info("Wrote Master Event Log TXT: %s (events=%d)", out_txt, len(all_rows))

    # Write index JSON (rollups + provenance)
    out_idx = artifacts / "eventjson" / MASTER_IDX_NAME.format(start=start_iso, end=end_iso)
    idx_payload = {
        "window": {"start": start_iso, "end": end_iso},
        "sources": [s for s, _ in selected_files],
        "counts": {
            "total": len(all_rows),
            "by_date": dict(Counter([ev.date_iso or "" for ev in all_rows])),
            "by_source": dict(Counter([ev.source_key for ev in all_rows])),
            "by_category": dict(Counter([ev.category or "" for ev in all_rows])),
        },
        "events": [
            {
                "source_key": ev.source_key,
                "date": ev.date_iso,
                "category": ev.category,
                "title": ev.title,
                "url": ev.url,
                "summary": ev.summary,
                "why_relevant": ev.why,
                "attacks": ev.attacks,
                "_origin_file": ev.origin_file,
                "_origin_index": ev.origin_index,
            }
            for ev in all_rows
        ],
    }
    out_idx.write_text(json.dumps(idx_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote Master Index JSON: %s", out_idx)

    # Also emit per-week event JSON files for Democracy Clock Step 3 (if window spans multiple weeks)
    # Group normalized rows by DC week number
    week_groups: Dict[int, List[EventRow]] = {}
    week_bounds: Dict[int, Tuple[date, date]] = {}
    for ev in all_rows:
        if not ev.date_obj:
            continue
        wk = dc_week_for(ev.date_obj)
        if not wk:
            continue
        wnum, wstart, wend = wk
        week_groups.setdefault(wnum, []).append(ev)
        week_bounds[wnum] = (wstart, wend)

    if len(week_groups) > 1:
        # Only split when there is more than one week represented
        for wnum in sorted(week_groups.keys()):
            rows = week_groups[wnum]
            wstart, wend = week_bounds[wnum]
            wstart_iso, wend_iso = wstart.isoformat(), wend.isoformat()
            payload_w = {
                "window": {"start": wstart_iso, "end": wend_iso},
                "week_number": wnum,
                "sources": list({r.source_key for r in rows}),
                "counts": {
                    "total": len(rows),
                    "by_date": dict(Counter([r.date_iso or "" for r in rows])),
                    "by_source": dict(Counter([r.source_key for r in rows])),
                    "by_category": dict(Counter([r.category or "" for r in rows])),
                },
                "events": [
                    {
                        "source_key": r.source_key,
                        "date": r.date_iso,
                        "category": r.category,
                        "title": r.title,
                        "url": r.url,
                        "summary": r.summary,
                        "why_relevant": r.why,
                        "attacks": r.attacks,
                        "_origin_file": r.origin_file,
                        "_origin_index": r.origin_index,
                    }
                    for r in rows
                ],
            }
            out_idx_w = artifacts / "eventjson" / f"master_index_week{wnum:02d}_{wstart_iso}_{wend_iso}.json"
            out_idx_w.write_text(json.dumps(payload_w, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Wrote Weekly Index JSON (W%02d): %s (events=%d)", wnum, out_idx_w, len(rows))

    return 0


if __name__ == "__main__":
    sys.exit(main())