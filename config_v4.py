#!/usr/bin/env python3
# config_v4.py — shared configuration for Democracy Clock V4

from __future__ import annotations

from pathlib import Path

# ── Project structure ────────────────────────────────────────────────────────────
# Base project folder (Step 2 project root; ALL artifacts go under /artifacts)
BASE_DIR: Path = Path(__file__).resolve().parent

# Root folder for generated files (JSON, event logs, etc.)
ARTIFACTS_ROOT: Path = BASE_DIR / "artifacts"          # helper_v4.create_artifact_paths() writes under this
# (Do not create directories here; helpers will create as needed.)

# ── HTTP client settings ────────────────────────────────────────────────────────
# Conservative, browser-like UA; adjust if a site begins blocking.
USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT: int = 40   # seconds
RETRY_TOTAL: int = 3        # total retry attempts for 429/5xx
RETRY_BACKOFF: float = 0.5  # seconds backoff factor (exponential)

# ── Timezone used in event metadata (IANA name) ────────────────────────────────
# This is written into builder outputs under window.tz.
# Pick the zone you want your logs to reflect.
TZ_DEFAULT: str = "America/New_York"

# ── Congress API (optional, used by getcongress_v4) ─────────────────────────────
CONGRESS_BASE: str = "https://api.congress.gov/v3"
CONGRESS_API_KEY: str | None = None   # or os.getenv("CONGRESS_API_KEY")
CONGRESS_NUMBER: int = 119            # update as needed

# ── Ballotpedia (2025) ──────────────────────────────────────────────────────────
BP_BASE: str = "https://ballotpedia.org"
# Canonical 2025 index page we scrape for Orders/Procs/Memos
BP_URL_2025: str = (
    "https://ballotpedia.org/Donald_Trump%27s_executive_orders_and_actions,_2025"
)

# Section <span id="…"> anchors on the Ballotpedia page → internal doc types
BP_SECTION_IDS: dict[str, str] = {
    "Executive_orders_issued_by_Trump": "executive_order",
    "Memoranda_issued_by_Trump": "memorandum",
    "Proclamations_issued_by_Trump": "proclamation",
}

# Some harvesters still use an allow-list when parsing anchors; safe to keep.
BP_URL_PREFIX_ALLOW: tuple[str, ...] = (
    "/Executive_Order:",
    "/Proclamation:",
    "/Presidential_Memorandum:",
)

# ── Optional logging locations (used only if a caller passes logfile paths) ─────
# Example usage:
#   log_path = (ARTIFACTS_ROOT / "logs" / f"{harvester_id}_{start}_{end}.log")
# Callers should create the parent directory if they choose to log to file.