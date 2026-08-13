#!/usr/bin/env python3
"""act_key_reproducibility_compare_v1.py — measure act_key reproducibility across runs.

Gates the P10 identity decision. The issue-once registry matches events on the
`act_key` fingerprint (`actor|action|date`); if that key does not reproduce when the
SAME week is extracted twice, exact matching fails and the registry mints duplicate
ids. This measures whether it reproduces — BEFORE identity is minted for the whole
corpus (a one-way door).

## How to run (the extraction happens at YOUR terminal; this only compares)
1. Extract ONE week TWICE on IDENTICAL input — same cached articles, no re-fetch,
   nothing changed between runs. The only difference must be LLM stochasticity.
   Save each run's events file to a separate path, e.g.:
       run1/master_index_week34_*.json
       run2/master_index_week34_*.json
   (2–3 runs sharpen the signal; more is better.)
2. Compare:
       python act_key_reproducibility_compare_v1.py run1/*.json run2/*.json
       python act_key_reproducibility_compare_v1.py --out report.json run{1,2,3}/*.json

## What it reports
- EXACT reproducibility — fraction of act_keys present in ALL runs (the number that
  decides whether exact matching is viable).
- FUZZY-recoverable — of the non-exact keys, how many the registry's fuzzy fallback
  (same actor+date, high action-token overlap) would still match.
- DURABLE-anchored — for events carrying a rot-proof key (FR doc #, CourtListener
  docket, Congress bill), the exact-match rate (should be ~100%; if so, anchor the
  fingerprint on these where present).
- COUNT stability and examples of keys that appeared in only one run (would-be dupes).

Reads master_index / events JSON (events under "events" or "categories[].events").
No API, no writes beyond the optional --out report.
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Durable-key fields (as emitted by step2 _derive_durable_keys, tolerant of nesting).
DURABLE_FIELDS = ("federal_register_doc", "courtlistener_docket", "congress_bill",
                  "usc_citation", "cfr_citation")


def _events(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [e for e in obj if isinstance(e, dict)]
    if isinstance(obj, dict):
        if isinstance(obj.get("events"), list):
            return [e for e in obj["events"] if isinstance(e, dict)]
        if isinstance(obj.get("categories"), list):
            return [e for c in obj["categories"] for e in c.get("events", []) if isinstance(e, dict)]
    return []


def _load_run(paths: List[str]) -> List[Dict[str, Any]]:
    evs: List[Dict[str, Any]] = []
    for p in paths:
        for f in glob.glob(p):
            evs += _events(json.loads(Path(f).read_text(encoding="utf-8")))
    return evs


def _act_keys(evs: List[Dict[str, Any]]) -> Set[str]:
    return {k for e in evs if (k := (e.get("act_key") or "").strip())}


def _durable(e: Dict[str, Any]) -> Optional[str]:
    dk = e.get("durable_keys") if isinstance(e.get("durable_keys"), dict) else e
    for f in DURABLE_FIELDS:
        v = dk.get(f) or e.get(f)
        if v:
            return f"{f}:{str(v).upper()}"
    return None


def _parts(k: str) -> Tuple[str, Set[str], str]:
    p = k.split("|")
    return (p[0] if p else "", set(p[1].split()) if len(p) > 1 else set(), p[2] if len(p) > 2 else "")


def _fuzzy_match(k: str, pool: Set[str], thresh: float = 0.6) -> bool:
    a, s, d = _parts(k)
    for k2 in pool:
        a2, s2, d2 = _parts(k2)
        if a == a2 and d == d2 and s and s2:
            j = len(s & s2) / len(s | s2)
            if j >= thresh:
                return True
    return False


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Measure act_key reproducibility across independent extraction runs.")
    ap.add_argument("runs", nargs="+", help="One glob per run, in run order (e.g. run1/*.json run2/*.json). "
                                            "Separate runs by putting each run's files behind a distinct path.")
    ap.add_argument("--fuzzy-threshold", type=float, default=0.6, help="Action-token Jaccard for a fuzzy match.")
    ap.add_argument("--out", help="Optional JSON report path.")
    args = ap.parse_args(argv)

    # Each positional is treated as one run (its glob may expand to many files).
    run_globs = args.runs
    if len(run_globs) < 2:
        print("Need at least two runs to compare. Pass one glob per run.", file=sys.stderr)
        return 2

    runs = [_load_run([g]) for g in run_globs]

    # ── Guard: refuse a byte-identical comparison. If two "runs" are the exact
    # same events, the extraction did not actually re-run (e.g. the write step
    # failed and a stale master index was copied twice) — reporting 100% then
    # would be the "validator that inspects nothing" trap.
    import hashlib
    def _content_hash(evs):
        blob = "\n".join(sorted(json.dumps(e, sort_keys=True, ensure_ascii=False) for e in evs))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()
    hashes = [_content_hash(r) for r in runs]
    if len(set(hashes)) == 1 and runs and runs[0]:
        print("\n*** INVALID TEST ***", file=sys.stderr)
        print("  All runs are BYTE-IDENTICAL — the extraction did not vary between runs.", file=sys.stderr)
        print("  Almost certainly the write step did not run (each run copied the SAME", file=sys.stderr)
        print("  master index). Re-run the extraction with a working write step so the", file=sys.stderr)
        print("  two files genuinely differ, then compare again.", file=sys.stderr)
        print(f"  (shared content sha256: {hashes[0][:16]}…, events={len(runs[0])})", file=sys.stderr)
        return 3

    key_sets = [_act_keys(r) for r in runs]
    counts = [len(r) for r in runs]
    empties = [sum(1 for e in r if not (e.get("act_key") or "").strip()) for r in runs]

    union = set().union(*key_sets)
    inter = set(key_sets[0]).intersection(*key_sets[1:]) if key_sets else set()
    exact_pct = 100.0 * len(inter) / len(union) if union else 0.0

    # Fuzzy recovery: keys not in the intersection, but fuzzy-matchable in every other run.
    non_exact = union - inter
    fuzzy_ok = 0
    for k in non_exact:
        present_or_fuzzy = all((k in ks) or _fuzzy_match(k, ks, args.fuzzy_threshold) for ks in key_sets)
        if present_or_fuzzy:
            fuzzy_ok += 1
    combined_pct = 100.0 * (len(inter) + fuzzy_ok) / len(union) if union else 0.0

    # Durable-anchored: among events carrying a durable key, cross-run exact match on it.
    durable_sets = [{d for e in r if (d := _durable(e))} for r in runs]
    dunion = set().union(*durable_sets) if durable_sets else set()
    dinter = set(durable_sets[0]).intersection(*durable_sets[1:]) if durable_sets else set()
    durable_pct = 100.0 * len(dinter) / len(dunion) if dunion else None

    only_one = [k for k in union if sum(1 for ks in key_sets if k in ks) == 1]

    report = {
        "runs": len(runs),
        "events_per_run": counts,
        "empty_act_key_per_run": empties,
        "distinct_keys_per_run": [len(k) for k in key_sets],
        "union_keys": len(union),
        "exact_reproducible": len(inter),
        "exact_reproducible_pct": round(exact_pct, 1),
        "fuzzy_recoverable_additional": fuzzy_ok,
        "exact_plus_fuzzy_pct": round(combined_pct, 1),
        "durable_anchored_pct": (round(durable_pct, 1) if durable_pct is not None else None),
        "durable_events_union": len(dunion),
        "keys_in_only_one_run": len(only_one),
    }

    print("\n== act_key reproducibility ==")
    print(f"  runs: {len(runs)} | events/run: {counts} | empty act_key/run: {empties}")
    print(f"  EXACT reproducible: {len(inter)}/{len(union)}  ({exact_pct:.1f}%)   <- decides exact-match viability")
    print(f"  + FUZZY recoverable: {fuzzy_ok} more  ->  {combined_pct:.1f}% combined")
    if durable_pct is not None:
        print(f"  DURABLE-anchored exact: {durable_pct:.1f}%  (of {len(dunion)} events with a rot-proof key)")
    else:
        print("  DURABLE-anchored: no durable keys found in inputs (check _derive_durable_keys output is carried)")
    print(f"  keys in only ONE run (would-be duplicate ids): {len(only_one)}")
    for k in only_one[:8]:
        print(f"     only-one: {k[:80]}")

    verdict = ("STRONG — exact matching viable" if exact_pct >= 90 else
               "WEAK exact, but fuzzy/durable may carry it" if combined_pct >= 90 else
               "POOR — fingerprint basis needs strengthening before P10 ratification")
    print(f"  VERDICT: {verdict}")

    if args.out:
        report["verdict"] = verdict
        Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"  report -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
