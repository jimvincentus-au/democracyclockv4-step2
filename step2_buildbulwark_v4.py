#!/usr/bin/env python3
# buildbulwark_v4.py — The Bulwark builder
#
# Thin wrapper: The Bulwark is on the Substack-API family, so the build pattern
# is identical to meidas/zeteo/popinfo. This wrapper exists so the source has
# its own standalone-executable script (matching get/build pair convention) and
# so future Bulwark-specific divergence has a clean home.
#
# Delegates to step2_buildsubstack_v4.run_builder with source="bulwark".
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_buildsubstack_v4 import run_builder as _substack_run_builder

SOURCE_DEFAULT = "bulwark"


def run_builder(
    *,
    source: str = SOURCE_DEFAULT,
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    limit: Optional[int] = None,
    ids: Optional[List[int]] = None,
    skip_existing: bool = False,
):
    """Delegate to the shared Substack builder with Bulwark's source key."""
    return _substack_run_builder(
        source=source,
        start=start,
        end=end,
        artifacts_root=artifacts_root,
        level=level,
        log_path=log_path,
        limit=limit,
        ids=ids,
        skip_existing=skip_existing,
    )


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — The Bulwark builder (delegates to buildsubstack)")
    ap.add_argument("--source", default=SOURCE_DEFAULT, help="Source key (default: bulwark)")
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
