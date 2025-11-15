#!/usr/bin/env python3
# buildweekevents_v4.py
from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger, resolve_date_window

# -------------------------------------------------------------------
# Builder registry: key -> (module_name, function_name)
# Add new builders here as you create them.
# -------------------------------------------------------------------
BUILDER_SPECS: Dict[str, Tuple[str, str]] = {
    # Substack family (all use the common Substack builder)
    "meidas":   ("step2_buildsubstack_v4", "run_builder"),
    "hcr":      ("step2_buildhcr_v4", "run_builder"),
    "zeteo":    ("step2_buildsubstack_v4", "run_builder"),
    "popinfo":  ("step2_buildsubstack_v4", "run_builder"),
    "50501":    ("step2_build50501_v4", "run_builder"),
    "noah":     ("step2_buildnoah_v4", "run_builder"),
    "outloud":  ("step2_buildoutloud_v4", "run_builder"),
    "orders":   ("step2_buildballotpedia_orders_v4", "run_builder"),
    "shadow":   ("step2_buildballotpedia_shadow_v4", "run_builder"),
    "congress": ("step2_buildcongress_v4", "run_builder"),
    "federalregister": ("step2_buildfederalregister_v4", "run_builder"),
    "guardian": ("step2_buildguardian_v4", "run_builder"),
    "econ":     ("step2_buildecon_v4", "run_builder"),
    "democracydocket": ("builddemocracydocket_v4", "run_builder"),
    # Non-Substack examples (uncomment when implemented)
    "justsecurity": ("build_justsecurity_events_v4", "run_builder"),
}

DEFAULT_SOURCES = list(BUILDER_SPECS.keys())


# -------------------------------------------------------------------
# CLI parsing & window calculation (must mirror getweekevents_v4)
# -------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — weekly LLM event builder")

    # match harvester: --start is OPTIONAL because you can drive by --week
    ap.add_argument("--start", required=False, help="Start date YYYY-MM-DD")

    grp = ap.add_mutually_exclusive_group(required=False)
    grp.add_argument("--weeks", type=int, help="Number of weeks from the start/window anchor")
    grp.add_argument("--end", help="End date YYYY-MM-DD")

    ap.add_argument("--level", default="INFO", help="Logging level (DEBUG, INFO, ...)")
    ap.add_argument(
        "--artifacts-root",
        default=str(ARTIFACTS_ROOT),
        help="Root folder for artifacts (default from config_v4.py)",
    )

    # Selection controls (match getweekevents_v4 semantics)
    ap.add_argument(
        "--only",
        nargs="+",
        help="Build only these sources (space-separated). Known: " + ", ".join(DEFAULT_SOURCES),
    )
    ap.add_argument(
        "--skip",
        nargs="+",
        help="Skip these sources (space-separated). Known: " + ", ".join(DEFAULT_SOURCES),
    )

    # Builder-wide tuning knobs
    ap.add_argument("--limit-per-source", type=int, help="Process only the first N items per source")
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="Resume mode: keep existing outputs per source",
    )

    # the important bit: same week semantics as getweekevents_v4
    ap.add_argument(
        "--week",
        type=int,
        help="Week number where week 1 = 2025-01-20..2025-01-24; week 2 = 2025-01-25..2025-01-31; later weeks Sat–Fri",
    )

    return ap.parse_args()


def _resolve_selection(only: List[str] | None, skip: List[str] | None, logger) -> List[str]:
    available = list(DEFAULT_SOURCES)

    if only:
        selected = [k for k in only if k in BUILDER_SPECS]
        unknown = [k for k in only if k not in BUILDER_SPECS]
        if unknown:
            logger.warning("Ignoring unknown --only keys: %s", ", ".join(unknown))
    else:
        selected = available

    if skip:
        skipped_known = [k for k in skip if k in BUILDER_SPECS]
        unknown = [k for k in skip if k not in BUILDER_SPECS]
        if unknown:
            logger.warning("Ignoring unknown --skip keys: %s", ", ".join(unknown))
        selected = [k for k in selected if k not in skipped_known]

    return selected


def _load_builder(key: str, logger):
    """
    Lazy-load the builder module only when it's selected.
    Returns run_builder callable or None if missing.
    """
    mod_name, fn_name = BUILDER_SPECS[key]
    try:
        mod = importlib.import_module(mod_name)
    except Exception as e:
        logger.error("Import error for %s: %s", mod_name, e)
        return None

    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        logger.error("Builder '%s' does not expose %s()", key, fn_name)
        return None
    return fn


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main() -> int:
    args = _parse_args()

    artifacts = Path(args.artifacts_root)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    logger = setup_logger("dc.builder.orchestrator", args.level)
    logger.info("Artifacts: %s", artifacts)

    # EXACTLY the same date resolver the harvester uses
    if not (args.start or args.week):
        logger.error("Missing window: provide (--start with --end|--weeks) or (--week [--weeks]).")
        return 1

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
    logger.info("Build window %s → %s", start_iso, end_iso)

    selected = _resolve_selection(args.only, args.skip, logger)
    logger.info("Selected sources: %s", " ".join(selected) if selected else "(none)")

    results: List[Dict] = []
    ok, failed = [], []

    for key in selected:
        run_fn = _load_builder(key, logger)
        if not run_fn:
            failed.append(key)
            continue

        log_file = artifacts / "log" / f"{key}_build_{start_iso}_{end_iso}.log"
        logger.info("→ Building '%s' (log: %s)", key, log_file)

        try:
            meta = run_fn(
                source=key,
                start=start_iso,
                end=end_iso,
                artifacts_root=str(artifacts),
                level=args.level,
                log_path=str(log_file),
                limit=args.limit_per_source,
                ids=None,          # Orchestrator doesn’t pick per-item IDs; use builder CLI for that.
                skip_existing=args.skip_existing,
            )
            results.append({"key": key, "ok": True, "meta": meta})
            ok.append(key)
            logger.info(
                "← Done '%s' | events=%s | out=%s",
                key,
                meta.get("count"),
                meta.get("events_path"),
            )
        except Exception as e:
            results.append({"key": key, "ok": False, "error": str(e)})
            failed.append(key)
            logger.exception("Builder '%s' failed", key)

    summary = {
        "start": start_iso,
        "end": end_iso,
        "artifacts_root": str(artifacts),
        "ran": selected,
        "ok": ok,
        "failed": failed,
        "details": results,
    }

    print(json.dumps(summary, indent=2))
    logger.info("Summary: %s", json.dumps(summary, separators=(",", ": ")))

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())