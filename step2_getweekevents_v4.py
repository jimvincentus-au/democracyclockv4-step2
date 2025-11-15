#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import setup_logger, resolve_date_window

# -------------------------------------------------------------------
# Harvester registry: key -> (module_name, function_name)
# Add new harvesters here as you build them.
# -------------------------------------------------------------------
HARVESTER_SPECS: Dict[str, Tuple[str, str]] = {
    "orders": ("step2_getballotpedia_order_v4", "run_harvester"),
    "shadow": ("step2_getballotpedia_shadow_v4", "run_harvester"),
    "congress": ("step2_getcongress_v4", "run_harvester"),
#    "democracydocket": ("step2_getdemocracydocket_v4", "run_harvester"),
    "federalregister": ("step2_getfederalregister_v4", "run_harvester"),
    "meidas": ("step2_getmeidas_v4", "run_harvester"),
    "hcr": ("step2_gethcr_v4", "run_harvester"),
    "zeteo": ("step2_getzeteo_v4", "run_harvester"),
    "popinfo": ("step2_getpopinfo_v4", "run_harvester"),
    "justsecurity": ("step2_getjustsecurity_v4", "run_harvester"),
    "guardian": ("step2_getguardian_v4", "run_harvester"),
#    "econ": ("step2_getecon_v4", "run_harvester"),
    "50501": ("step2_get50501_v4", "run_harvester"),
    "outloud": ("step2_getoutloud_v4", "run_harvester"),
    "noah": ("step2_getnoah_v4", "run_harvester"),
}

DEFAULT_HARVESTERS = list(HARVESTER_SPECS.keys())

# -------------------------------
# Cleanup helpers
# -------------------------------
_CLEAN_KEYS = {"logs", "json", "eventjson", "eventtxt", "all"}

def _normalize_clean_list(arg_list: Optional[List[str]], flags: Dict[str, bool]) -> List[str]:
    """
    Merge --clean (comma or space separated) with legacy boolean flags.
    Return a de-duplicated list among: logs, json, eventjson, eventtxt
    """
    wanted: List[str] = []
    # from --clean
    if arg_list:
        for raw in arg_list:
            if not raw:
                continue
            for part in str(raw).split(","):
                k = part.strip().lower()
                if k:
                    wanted.append(k)
    # from individual boolean flags
    for k, on in flags.items():
        if on:
            wanted.append(k)
    # normalize
    norm: List[str] = []
    for k in wanted:
        if k == "all":
            return ["logs", "json", "eventjson", "eventtxt"]
        if k in _CLEAN_KEYS and k not in ("all",) and k not in norm:
            norm.append(k)
    return norm

def _do_cleanup(artifacts: Path, targets: List[str], logger) -> Dict[str, int]:
    """
    Delete files under selected artifact folders.
      logs:      artifacts/log/* (all files)
      json:      artifacts/json/*.json
      eventjson: artifacts/eventjson/*.json
      eventtxt:  artifacts/events/**/*.txt
    Returns {key: deleted_count}
    """
    counts: Dict[str, int] = {}

    def _rm_globs(base: Path, patterns: List[str]) -> int:
        n = 0
        for pat in patterns:
            for p in base.glob(pat):
                try:
                    if p.is_file():
                        p.unlink()
                        n += 1
                except Exception as e:
                    logger.debug("Failed to remove %s: %s", p, e)
        return n

    if "logs" in targets:
        d = artifacts / "log"
        counts["logs"] = _rm_globs(d, ["*"])
        logger.info("Cleaned logs: %s files", counts["logs"])
    if "json" in targets:
        d = artifacts / "json"
        counts["json"] = _rm_globs(d, ["*.json"])
        logger.info("Cleaned json: %s files", counts["json"])
    if "eventjson" in targets:
        d = artifacts / "eventjson"
        counts["eventjson"] = _rm_globs(d, ["*.json"])
        logger.info("Cleaned eventjson: %s files", counts["eventjson"])
    if "eventtxt" in targets:
        d = artifacts / "events"
        counts["eventtxt"] = _rm_globs(d, ["**/*.txt", "*.txt"])
        logger.info("Cleaned eventtxt: %s files", counts["eventtxt"])

    return counts


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — weekly harvester runner")
    ap.add_argument("--start", required=False, help="Start date YYYY-MM-DD")
    grp = ap.add_mutually_exclusive_group(required=False)
    grp.add_argument("--weeks", type=int, help="Number of weeks (end = start + 7*weeks - 1)")
    grp.add_argument("--end", help="End date YYYY-MM-DD")
    ap.add_argument("--level", default="INFO", help="Logging level (DEBUG, INFO, ...)")
    # Cleanup controls
    ap.add_argument("--clean-logs", action="store_true", dest="clean_logs", help="(legacy) Clean artifacts/log/*")
    ap.add_argument("--clean-json", action="store_true", dest="clean_json", help="(legacy) Clean artifacts/json/*.json")
    ap.add_argument("--clean-event-json", action="store_true", dest="clean_event_json",
                    help="(legacy) Clean artifacts/eventjson/*.json")
    ap.add_argument("--clean-event-txt", action="store_true", dest="clean_event_txt",
                    help="(legacy) Clean artifacts/events/**/*.txt")
    ap.add_argument("--clean-all", action="store_true", help="(legacy) Clean logs, json, eventjson, eventtxt")
    ap.add_argument("--week", type=int, help="Week number where week 1 = 2025-01-20..2025-01-24; others Sat–Fri")

    ap.add_argument(
        "--artifacts-root",
        default=str(ARTIFACTS_ROOT),
        help="Root folder for artifacts (default from config_v4.py)",
    )
    # Selection controls
    ap.add_argument(
        "--only",
        nargs="+",
        help="Run only these harvesters (space-separated). Known: " + ", ".join(DEFAULT_HARVESTERS),
    )
    ap.add_argument(
        "--skip",
        nargs="+",
        help="Skip these harvesters (space-separated). Known: " + ", ".join(DEFAULT_HARVESTERS),

    )
    return ap.parse_args()


def _resolve_selection(only: List[str] | None, skip: List[str] | None, logger) -> List[str]:
    available = list(DEFAULT_HARVESTERS)

    if only:
        # Keep order as provided by user, but ensure they exist
        selected = [k for k in only if k in HARVESTER_SPECS]
        unknown = [k for k in only if k not in HARVESTER_SPECS]
        if unknown:
            logger.warning("Ignoring unknown --only keys: %s", ", ".join(unknown))
    else:
        selected = available

    if skip:
        skipped_known = [k for k in skip if k in HARVESTER_SPECS]
        unknown = [k for k in skip if k not in HARVESTER_SPECS]
        if unknown:
            logger.warning("Ignoring unknown --skip keys: %s", ", ".join(unknown))
        selected = [k for k in selected if k not in skipped_known]

    return selected


def _load_harvester(key: str, logger):
    """
    Lazy-load the harvester module only when it's selected.
    Returns run_harvester callable or None if missing.
    """
    mod_name, fn_name = HARVESTER_SPECS[key]
    try:
        mod = importlib.import_module(mod_name)
    except Exception as e:
        logger.error("Import error for %s: %s", mod_name, e)
        return None

    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        logger.error("Harvester '%s' does not expose %s()", key, fn_name)
        return None
    return fn

def _is_clean_requested(args) -> bool:
    return any([
        getattr(args, "clean_logs", False),
        getattr(args, "clean_json", False),
        getattr(args, "clean_event_json", False),
        getattr(args, "clean_event_txt", False),
        getattr(args, "clean_all", False),
    ])

def main() -> int:
    args = _parse_args()
    level_norm = (args.level or "INFO").upper()

    # Prepare logging and artifacts
    artifacts = Path(args.artifacts_root)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    logger = setup_logger("dc.orchestrator", level_norm)

    logger.info("Artifacts: %s", artifacts)

    # Optional cleanup pass (can be run without a date window)
    clean_flags = {
        "logs": args.clean_logs,
        "json": args.clean_json,
        "eventjson": getattr(args, "clean_event_json", False),
        "eventtxt": getattr(args, "clean_event_txt", False),
    }
    # Accept legacy flags only (as defined in this file)
    clean_list = _normalize_clean_list(
        arg_list=None,  # no aggregated --clean in this script
        flags=clean_flags
    )
    if args.clean_all and "all" not in clean_list:
        clean_list = ["logs", "json", "eventjson", "eventtxt"]

    if clean_list:
        (artifacts / "log").mkdir(parents=True, exist_ok=True)
        (artifacts / "json").mkdir(parents=True, exist_ok=True)
        (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
        (artifacts / "events").mkdir(parents=True, exist_ok=True)
        counts = _do_cleanup(artifacts, clean_list, logger)
        logger.info("Cleanup summary: %s", counts)
        # If user only asked to clean, and provided no window args, exit now.
        if _is_clean_requested(args) and not args.start and not args.week and not args.weeks and not getattr(args, "end", None):
            summary = {
                "start": None,
                "end": None,
                "artifacts_root": str(artifacts),
                "ran": [],
                "ok": [],
                "failed": [],
                "details": [{"cleaned": clean_list, "counts": counts}],
            }
            print(json.dumps(summary, indent=2))
            logger.info("Summary: %s", json.dumps(summary, separators=(",", ": ")))
            return 0

    # If not clean-only, we need a window via either --start (with --end|--weeks) or --week (optionally with --weeks)
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
    logger.info("Harvest window %s → %s", start_iso, end_iso)

    selected = _resolve_selection(args.only, args.skip, logger)
    logger.info("Selected harvesters: %s", " ".join(selected) if selected else "(none)")

    results: List[Dict] = []
    ok, failed = [], []

    for key in selected:
        run_fn = _load_harvester(key, logger)
        if not run_fn:
            failed.append(key)
            continue

        log_file = artifacts / "log" / f"{key}_{start_iso}_{end_iso}.log"
        logger.info("→ Running '%s' (log: %s)", key, log_file)

        try:
            meta = run_fn(
                start=start_iso,
                end=end_iso,
                artifacts_root=str(artifacts),
                level=level_norm,
                log_path=str(log_file),
            )
            results.append({"key": key, "ok": True, "meta": meta})
            ok.append(key)
            logger.info(
                "← Done '%s' | entities=%s | out=%s",
                key,
                meta.get("entity_count"),
                meta.get("entities_path"),
            )
        except Exception as e:
            results.append({"key": key, "ok": False, "error": str(e)})
            failed.append(key)
            logger.exception("Harvester '%s' failed", key)

    summary = {
        "start": start_iso,
        "end": end_iso,
        "artifacts_root": str(artifacts),
        "ran": selected,
        "ok": ok,
        "failed": failed,
        "details": results,
    }

    # Print machine-readable summary to stdout
    print(json.dumps(summary, indent=2))
    logger.info("Summary: %s", json.dumps(summary, separators=(",", ": ")))

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())