#!/usr/bin/env python3
"""
step2_v4.py — one-shot runner for Step 1/2/3:
- getweekevents_v4.py
- buildweekevents_v4.py
- writeweekevents_v4.py

It accepts the *same* window args as your other CLIs plus the new week-based ones:
  1) --start YYYY-MM-DD --end YYYY-MM-DD
  2) --start YYYY-MM-DD --weeks N
  3) --week N
  4) --week N --weeks M

Special rule: week 1 is Mon–Fri (2025-01-20 → 2025-01-24).
All other weeks are Sat–Fri (e.g., week 2 = 2025-01-25 → 2025-01-31).

All other options (level, artifacts-root, only, skip, limit) are passed through.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# -----------------------------
# Week window computation
# -----------------------------

W1_START = date(2025, 1, 20)  # Monday
W1_END   = date(2025, 1, 24)  # Friday
W2_START = date(2025, 1, 25)  # Saturday (start of standard Sat→Fri cadence)

SOURCE_ALIASES = {
    "getscotusorders": "scotusorders",
    "buildscotusorders": "scotusorders",
    "scotusorders": "scotusorders",
    "getscotusopinions": "scotusopinions",
    "buildscotusopinions": "scotusopinions",
    "scotusopinions": "scotusopinions",
}

def _iso(d: date) -> str:
    return d.isoformat()

def _compute_window_from_start(start_str: str, weeks: Optional[int], end_str: Optional[str]) -> Tuple[str, str]:
    """Existing behavior: --start with either --weeks or --end."""
    y, m, d = (int(x) for x in start_str.split("-"))
    s = date(y, m, d)
    if weeks is not None:
        e = s + timedelta(days=weeks * 7 - 1)
        return _iso(s), _iso(e)
    if not end_str:
        raise ValueError("When using --start, supply either --weeks or --end.")
    y2, m2, d2 = (int(x) for x in end_str.split("-"))
    e = date(y2, m2, d2)
    if e < s:
        raise ValueError("End date must be >= start date.")
    return _iso(s), _iso(e)

def _compute_window_from_week(week: int, weeks: Optional[int]) -> Tuple[str, str]:
    """New behavior: --week N (and optional --weeks M)."""
    if week < 1:
        raise ValueError("--week must be >= 1")
    if week == 1:
        start = W1_START
        end = W1_END
    else:
        # week 2 starts Saturday 2025-01-25; week k starts W2_START + (k-2)*7 days
        start = W2_START + timedelta(days=(week - 2) * 7)
        end = start + timedelta(days=6)

    if weeks and weeks > 1:
        # Extend M contiguous weeks: inclusive end = start + (7*M - 1)
        end = start + timedelta(days=weeks * 7 - 1)

    return _iso(start), _iso(end)


# -----------------------------
# Source selection normalization
# -----------------------------

def _normalize_source_selection(values: Optional[List[str]]) -> Optional[List[str]]:
    """Normalize top-level --only/--skip source aliases before passing to child CLIs."""
    if not values:
        return values
    normalized: List[str] = []
    for value in values:
        key = SOURCE_ALIASES.get(value, value)
        if key not in normalized:
            normalized.append(key)
    return normalized

# Sources that are harvested/built for downstream ANALYSIS only and must never be
# written into the Step-2 Master Event Log. They remain runnable directly (e.g.
# getweekevents --only scotusblog) for their analysis purpose.
ANALYSIS_ONLY_SOURCES = ["scotusblog"]

def _strip_analysis_only_from_only(values: Optional[List[str]]) -> Optional[List[str]]:
    """Remove analysis-only sources from an --only selection (they are not valid
    Step-2 event sources)."""
    if not values:
        return values
    filtered = [v for v in values if v not in ANALYSIS_ONLY_SOURCES]
    return filtered or None

def _add_analysis_only_to_skip(values: Optional[List[str]]) -> List[str]:
    """H-2 fix (2026-08-09): analysis-only sources default-run into the event log
    because the child get/build/write CLIs include them in their DEFAULT set. The
    orchestrator must therefore ALWAYS skip them (merged with any user --skip),
    regardless of whether the user passed --skip. The previous code instead
    stripped scotusblog OUT of the skip list, so it was never excluded."""
    base = list(values or [])
    for s in ANALYSIS_ONLY_SOURCES:
        if s not in base:
            base.append(s)
    return base


# -----------------------------
# CLI
# -----------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — step2 orchestrator (get→build→write)")

    # Window args (mutually exclusive: either --start ... or --week ...)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--start", help="Start date YYYY-MM-DD")
    g.add_argument("--week", type=int,
                   help="Week index (week 1 = 2025-01-20 → 2025-01-24; others Sat→Fri)")

    ap.add_argument("--weeks", type=int, help="Number of weeks to include")
    ap.add_argument("--end", help="End date YYYY-MM-DD (must be >= start when used)")

    # Common passthrough flags
    ap.add_argument("--level", default="INFO", help="Logging level (DEBUG, INFO, ...)")
    ap.add_argument("--artifacts-root", default="artifacts", help="Artifacts root directory")

    # Passthrough selection flags used by get/build/write
    ap.add_argument("--only", nargs="+", help="Restrict to specific sources/builders (passed to get/build/write); SCOTUS event aliases getscotusorders/getscotusopinions are accepted")
    ap.add_argument("--skip", nargs="+", help="Skip specific sources/builders (passed to get/build/write); SCOTUS event aliases getscotusorders/getscotusopinions are accepted")

    # Optional limits (handy for testing)
    ap.add_argument("--limit", type=int, help="Optional per-source limit (passed where supported)")

    # Path overrides
    ap.add_argument("--get-cmd", default="step2_getweekevents_v4.py")
    ap.add_argument("--build-cmd", default="step2_buildweekevents_v4.py")
    ap.add_argument("--write-cmd", default="step2_writeweekevents_v4.py")

    # LLM provider toggle (backout plan). Convenience wrapper over the
    # DC_LLM_PROVIDER env var that the extractor's call_llm() reads. Setting it
    # here propagates to the get/build/write children (they inherit os.environ).
    # Omit to defer to DC_LLM_PROVIDER, else the extractor default (claude).
    ap.add_argument("--provider", choices=["openai", "claude"],
                    help="LLM backend for extraction (sets DC_LLM_PROVIDER). "
                         "Default: current env, else claude.")

    return ap.parse_args()


# -----------------------------
# Runner
# -----------------------------

@dataclass
class RunResult:
    rc: int
    cmd: List[str]
    log: Optional[str]

def _run_step(cmd: List[str]) -> RunResult:
    # We let each child script manage its own logging and JSON output.
    rc = 0
    try:
        completed = subprocess.run(cmd, check=False)
        rc = completed.returncode
    except Exception:
        rc = 1
    # Best-effort: most of your scripts accept --artifacts-root and write a log path there.
    # We still return the exact invoked command to make debugging trivial.
    return RunResult(rc=rc, cmd=cmd, log=None)

def main() -> int:
    args = _parse_args()

    # Propagate the provider choice to the child steps via the environment.
    # The extractor reads DC_LLM_PROVIDER inside call_llm(); children inherit it.
    if args.provider:
        os.environ["DC_LLM_PROVIDER"] = args.provider

    # Validate window arguments & compute inclusive start/end
    if args.start:
        if args.week is not None:
            print(json.dumps({"error": "Use either --start... or --week..., not both."}))
            return 2
        try:
            start_iso, end_iso = _compute_window_from_start(args.start, args.weeks, args.end)
        except Exception as e:
            print(json.dumps({"error": f"Invalid --start window: {e}"}))
            return 2
    else:
        # week-based path
        if args.week is None:
            print(json.dumps({"error": "Missing --week or --start"}))
            return 2
        try:
            start_iso, end_iso = _compute_window_from_week(args.week, args.weeks)
        except Exception as e:
            print(json.dumps({"error": f"Invalid --week window: {e}"}))
            return 2

    # Build common passthroughs
    common = [
        "--level", args.level,
        "--artifacts-root", str(args.artifacts_root),
    ]

    only_sources = _strip_analysis_only_from_only(_normalize_source_selection(args.only))
    skip_sources = _add_analysis_only_to_skip(_normalize_source_selection(args.skip))

    if args.only and only_sources is None:
        print(json.dumps({
            "error": "No Step 2 event sources selected after removing analysis-only sources.",
            "analysis_only_sources": ANALYSIS_ONLY_SOURCES,
        }))
        return 2

    # getweekevents passthroughs
    # Remove non-event/pipeline-only selections before passing to get_cmd
    get_cmd = [sys.executable, args.get_cmd, "--start", start_iso, "--end", end_iso, *common]
    get_only = [x for x in only_sources or [] if x != "tracker"]
    if get_only:
        get_cmd += ["--only", *get_only]
    if skip_sources:
        get_cmd += ["--skip", *skip_sources]

    # buildweekevents passthroughs
    # Remove non-event/pipeline-only selections before passing to build_cmd
    build_cmd = [sys.executable, args.build_cmd, "--start", start_iso, "--end", end_iso, *common]
    build_only = [x for x in only_sources or [] if x != "tracker"]
    if build_only:
        build_cmd += ["--only", *build_only]
    if skip_sources:
        build_cmd += ["--skip", *skip_sources]
    if args.limit is not None:
        # Fix (2026-08-09): buildweekevents_v4 accepts --limit-per-source, not
        # --limit. Passing --limit made the build stage crash with an argparse
        # "unrecognized arguments" error, so --limit was unusable end-to-end.
        build_cmd += ["--limit-per-source", str(args.limit)]

    # writeweekevents passthroughs
    write_cmd = [sys.executable, args.write_cmd, "--start", start_iso, "--end", end_iso, *common]
    write_only = [x for x in only_sources or [] if x != "tracker"]
    if write_only:
        write_cmd += ["--only", *write_only]
    if skip_sources:
        write_cmd += ["--skip", *skip_sources]

    # Run steps in order.
    #
    # H-1 fix (2026-08-09): previously build ran only if get returned rc==0, and
    # getweekevents returns rc=1 if ANY single harvester fails. With ~17 live
    # scrapers, one site 500/timeout/layout-change made get fail, which skipped
    # build AND write entirely — silently dropping the whole week's data.
    #
    # Build and write are now BEST-EFFORT: they run on whatever harvested
    # successfully. A failed harvester just leaves no filtered file for its
    # source, so its builder is a no-op. Per-stage rc and a `partial` flag are
    # reported so an incomplete harvest is visible, not blocking.
    r_get = _run_step(get_cmd)
    r_build = _run_step(build_cmd)

    # Run the writer if the build produced/updated outputs, or if any per-source
    # eventjson already exists for this window (so a partial build still merges
    # what succeeded). This still avoids overwriting a good master with an empty
    # one when there is genuinely nothing to write.
    should_write = False
    if r_build.rc == 0:
        should_write = True
    else:
        try:
            ej_dir = Path(args.artifacts_root) / "eventjson"
            pattern = f"*__events_{start_iso}_{end_iso}.json"
            # Some builders emit names like '<source>_events_<start>_<end>.json'
            # Allow both single underscore and double underscore separators.
            # (master_index_* files do not contain 'events_' and are not matched.)
            matches = list(ej_dir.glob(f"*events_{start_iso}_{end_iso}.json"))
            matches += list(ej_dir.glob(pattern))
            should_write = any(p.is_file() and p.stat().st_size > 0 for p in matches)
        except Exception:
            should_write = False

    r_write = _run_step(write_cmd) if should_write else RunResult(rc=99, cmd=write_cmd, log=None)

    all_ok = (r_get.rc == 0 and r_build.rc == 0 and r_write.rc == 0)
    # A "partial" run is one where some stage reported failure but the writer
    # still produced/updated a master log from whatever succeeded.
    partial = (not all_ok) and (r_write.rc == 0)

    summary: Dict[str, object] = {
        "window": {"start": start_iso, "end": end_iso},
        "artifacts_root": str(args.artifacts_root),
        "steps": {
            "get": {"rc": r_get.rc, "cmd": r_get.cmd},
            "build": {"rc": r_build.rc, "cmd": r_build.cmd},
            "write": {"rc": r_write.rc, "cmd": r_write.cmd},
        },
        "ok": all_ok,
        "partial": partial,
    }

    print(json.dumps(summary, indent=2))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())