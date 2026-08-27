#!/usr/bin/env python3
"""
Map raw numeric attack handles back to their canonical names. No LLM.

THE DEFECT (measured 2026-08-27, corpus-wide)
    1,743 attack handles across the corpus are bare integers -- "30", "42", "43" --
    rather than handle names. The extractor emitted the POSITION of the handle in
    the canonical list instead of its name. Concentrated in hcr (1,136) and
    guardian (362), but present in 8 sources.

WHY THE MAPPING IS SAFE
    The number is an index into "THE 55 CANONICAL ATTACK HANDLES" in
    step2_prompts_v4.ATTACKS_PREFACE. Verified before writing this:
      • all 1,743 values fall in range 1-55; none are unmappable
      • the numbering is IDENTICAL across all 7 revisions of step2_prompts_v4.py
        in git (2025-11-16 .. 2026-08-27), so no renumbering has ever occurred
    The mapping is therefore deterministic, not a guess.

    Caveat recorded: the oldest revision in git is 2025-11-16 while the corpus
    starts 2025-01-20. Numbering before that commit is unverifiable, but every
    value maps to a handle plausible for its context.

SCOPE
    This fixes ONLY numeric handles. It does not touch the ~3,000 invented
    non-canonical names (transparency, elections, civil_society,
    international_relations ...), which are a vocabulary question for the author,
    not a mechanical error.

Operates on the per-source *_events_*.json files, then requires --rebuild-all.
--apply to write; default is a dry run.
"""
import argparse, collections, json, re, sys
from pathlib import Path

S2 = Path("/Volumes/PRINTIFY24/Democracy Clock Automation/Step 2")
sys.path.insert(0, str(S2))
from step2_prompts_v4 import ATTACKS_PREFACE  # noqa: E402

BY_NUM = {m.group(1): m.group(2) for m in
          re.finditer(r"^\s*(\d+)\.\s+([a-z0-9_]+)\s+—", ATTACKS_PREFACE, re.M)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    assert len(BY_NUM) == 55, f"expected 55 canonical handles, parsed {len(BY_NUM)}"

    files = sorted((S2 / "artifacts" / "eventjson").glob("*_events_*.json"))
    changed_files = 0
    remapped = collections.Counter()
    unmappable = collections.Counter()

    for p in files:
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        evs = d.get("events")
        if not isinstance(evs, list):
            continue
        touched = False
        for e in evs:
            atk = e.get("attacks")
            if not isinstance(atk, list) or not atk:
                continue
            new = []
            for h in atk:
                s = str(h)
                if re.fullmatch(r"\d+", s):
                    if s in BY_NUM:
                        new.append(BY_NUM[s]); remapped[s] += 1; touched = True
                    else:
                        unmappable[s] += 1          # out of range: leave alone
                        new.append(h)
                else:
                    new.append(h)
            # de-dup while preserving order: a remap can collide with an existing name
            if touched:
                seen = set(); dedup = []
                for h in new:
                    if h not in seen:
                        seen.add(h); dedup.append(h)
                e["attacks"] = dedup
        if touched:
            changed_files += 1
            if a.apply:
                p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

    total = sum(remapped.values())
    print(f"files scanned      : {len(files)}")
    print(f"files with numerics: {changed_files}")
    print(f"handles remapped   : {total}")
    print(f"unmappable         : {sum(unmappable.values())} {dict(unmappable) if unmappable else ''}")
    print("\ntop mappings:")
    for n, c in remapped.most_common(12):
        print(f"   {n:>3s} x{c:<5d} -> {BY_NUM[n]}")
    print("\nWRITTEN — now run: python step2_writeweekevents_v4.py --rebuild-all"
          if a.apply else "\nDRY RUN — nothing written. Re-run with --apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
