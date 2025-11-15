#!/usr/bin/env python3
# traits_v4.py — Build canonical traits_v4.json from a source text file
# Usage:
#   python traits_v4.py --src traits_source.txt --out traits_v4.json

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional

# Canonical categories and order
CATEGORIES = [
    {"name": "Power and Authority", "index": 1},
    {"name": "Institutions and Governance", "index": 2},
    {"name": "Economic Structure", "index": 3},
    {"name": "Civil Rights and Dissent", "index": 4},
    {"name": "Information, Memory, and Manipulation", "index": 5},
]

CATEGORY_ALIASES = {
    # Accept headers like "Category II: Institutions and Governance"
    "category i": "Power and Authority",
    "category 1": "Power and Authority",
    "power and authority": "Power and Authority",
    "category ii": "Institutions and Governance",
    "category 2": "Institutions and Governance",
    "institutions and governance": "Institutions and Governance",
    "category iii": "Economic Structure",
    "category 3": "Economic Structure",
    "economic structure": "Economic Structure",
    "category iv": "Civil Rights and Dissent",
    "category 4": "Civil Rights and Dissent",
    "civil rights and dissent": "Civil Rights and Dissent",
    "category v": "Information, Memory, and Manipulation",
    "category 5": "Information, Memory, and Manipulation",
    "information, memory, and manipulation": "Information, Memory, and Manipulation",
}

def category_index(name: str) -> int:
    for c in CATEGORIES:
        if c["name"] == name:
            return c["index"]
    return 99

def slugify(title: str) -> str:
    s = title.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    # keep it short and stable
    return re.sub(r"_+", "_", s)[:80]

@dataclass
class Example:
    place: str
    era_or_years: str
    note: str

@dataclass
class Trait:
    id: str
    number: int
    name: str
    short_definition: str
    why_it_matters: str
    historical_examples: List[Example]
    category: str
    category_index: int
    scoring: Dict[str, object]
    tags: List[str]
    notes: str

TRAIT_START_RE = re.compile(r"^\s*(\d{1,2})\.\s+(.*\S)\s*$")
CATEGORY_RE = re.compile(r"^\s*Category\s+([IVXLC\d]+)\s*:\s*(.+?)\s*$", re.IGNORECASE)
SECTION_LABEL_RE = re.compile(r"^\s*(Short Definition|Historical Examples|Why It Matters)\s*:\s*(.*)$", re.IGNORECASE)
EXAMPLE_LINE_RE = re.compile(r"^\s*[–-]\s*(.+?)\s*—\s*(.+?)\s*—\s*(.+?)\s*$")  # place — era — note
EXAMPLE_LINE_FALLBACK_RE = re.compile(r"^\s*[–-]\s*(.+?)\s*–\s*(.+)$")  # place – note (will try to split years if possible)

def parse_traits_text(text: str) -> List[Trait]:
    lines = text.splitlines()
    traits: List[Trait] = []

    cur_cat = "Power and Authority"  # default to first until a header appears
    cur_num: Optional[int] = None
    cur_title = ""
    cur_short = ""
    cur_why = ""
    cur_examples: List[Example] = []

    def flush():
        nonlocal cur_num, cur_title, cur_short, cur_why, cur_examples, cur_cat
        if cur_num is None:
            return
        t = Trait(
            id=slugify(cur_title),
            number=cur_num,
            name=cur_title.strip(),
            short_definition=cur_short.strip(),
            why_it_matters=cur_why.strip(),
            historical_examples=cur_examples[:],
            category=cur_cat,
            category_index=category_index(cur_cat),
            scoring={
                "presence_min": 0,
                "presence_max": 12,
                "difficulty_default": 6,
                "clamp_rules": {}
            },
            tags=[],
            notes=""
        )
        traits.append(t)
        # reset
        cur_num = None
        cur_title = ""
        cur_short = ""
        cur_why = ""
        cur_examples = []

    mode = None  # "short", "examples", "why"
    for raw in lines:
        line = raw.rstrip()

        # Category header?
        mcat = CATEGORY_RE.match(line)
        if mcat:
            alias_key = f"category {mcat.group(1).lower()}"
            name_lower = mcat.group(2).strip().lower()
            if alias_key in CATEGORY_ALIASES:
                cur_cat = CATEGORY_ALIASES[alias_key]
            else:
                cur_cat = CATEGORY_ALIASES.get(name_lower, cur_cat)
            continue

        # New trait number/title?
        mstart = TRAIT_START_RE.match(line)
        if mstart:
            flush()
            cur_num = int(mstart.group(1))
            cur_title = mstart.group(2).strip()
            mode = None
            continue

        # Section labels
        msec = SECTION_LABEL_RE.match(line)
        if msec:
            label = msec.group(1).lower()
            rest = msec.group(2).strip()
            if label.startswith("short"):
                mode = "short"
                cur_short = rest
            elif label.startswith("historical"):
                mode = "examples"
                # If rest contains immediate example, try to parse
                if rest:
                    ex = parse_example_line(rest)
                    if ex:
                        cur_examples.append(ex)
            elif label.startswith("why"):
                mode = "why"
                cur_why = rest
            continue

        # Accumulate content
        if mode == "short":
            if line.strip():
                cur_short = (cur_short + " " + line.strip()).strip()
        elif mode == "examples":
            if line.strip().startswith(("–", "-")):
                ex = parse_example_line(line.strip())
                if ex:
                    cur_examples.append(ex)
            elif line.strip():
                # sometimes wrapped example text lines without dash
                # attach to last example note if present
                if cur_examples:
                    cur_examples[-1].note = (cur_examples[-1].note + " " + line.strip()).strip()
        elif mode == "why":
            if line.strip():
                cur_why = (cur_why + " " + line.strip()).strip()

    # final flush
    flush()
    # ensure category indices
    for t in traits:
        t.category_index = category_index(t.category)
    return traits

def parse_example_line(s: str) -> Optional[Example]:
    s = s.lstrip("–- ").strip()
    # Try pattern: place — era — note
    m = re.match(r"^(.+?)\s+—\s+(.+?)\s+—\s+(.+?)$", s)
    if m:
        return Example(place=m.group(1).strip(), era_or_years=m.group(2).strip(), note=m.group(3).strip())

    # Fallback: place – note (no explicit era)
    mf = re.match(r"^(.+?)\s+–\s+(.+)$", s)
    if mf:
        place = mf.group(1).strip()
        rest = mf.group(2).strip()
        my = re.search(r"(\\d{3,4}(?:–\\d{2,4})?)", rest)
        era = my.group(1) if my else ""
        note = rest
        return Example(place=place, era_or_years=era, note=note)

    if s:
        return Example(place="", era_or_years="", note=s)
    return None

def to_json_dict(traits: List[Trait]) -> Dict[str, object]:
    return {
        "version": "4.0",
        "categories": CATEGORIES,
        "traits": [
            {
                "id": t.id,
                "number": t.number,
                "name": t.name,
                "short_definition": t.short_definition,
                "why_it_matters": t.why_it_matters,
                "historical_examples": [asdict(ex) for ex in t.historical_examples],
                "category": t.category,
                "category_index": t.category_index,
                "scoring": t.scoring,
                "tags": t.tags,
                "notes": t.notes,
            }
            for t in traits
        ],
    }

def _read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def _write_json(path: str, payload: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build traits_v4.json from source text")
    ap.add_argument("--src", required=True, help="Path to source traits text (your numbered list)")
    ap.add_argument("--out", required=True, help="Output JSON path (traits_v4.json)")
    return ap.parse_args()

def main() -> int:
    args = _parse_args()
    raw = _read(args.src)
    traits = parse_traits_text(raw)
    if not traits or len(traits) < 10:
        print(f"WARNING: parsed only {len(traits)} traits — check source format.")
    payload = to_json_dict(traits)
    _write_json(args.out, payload)
    print(f"Wrote {len(traits)} traits → {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
