#!/usr/bin/env python3
# helper_v4.py — shared helpers for Democracy Clock V4

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter, Retry

# helper_v4.py — replace the existing extract_iso_from_text with this version
import re
from datetime import date

# Matches Month D, YYYY with optional ASCII/Unicode parens and stray punctuation.
# We prefer the LAST parenthetical Month D, YYYY on the line (how Ballotpedia formats it).
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], start=1)
}

# parenthetical form: ... (January 20, 2025) [with possible NBSPs and unicode brackets]
_DATE_PARENS_RE = re.compile(
    r"[\(\[（【]\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*,\s*(20\d{2})\s*[\)\]】）]",
    re.I,
)

# anywhere-on-line fallback (no required parens)
_DATE_ANYWHERE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)[\s\u00A0]+(\d{1,2})\s*,\s*(20\d{2})",
    re.I,
)

import re
import unicodedata
from datetime import datetime

def extract_iso_from_text(text: str) -> str:
    """
    Extract ISO date (YYYY-MM-DD) from strings like:
      'Executive Order … (September 30, 2025)'
    Robust to NBSPs, smart quotes/parentheses, and spacing variants.
    """
    if not text:
        return ""

    # Normalize Unicode punctuation & spaces
    t = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()

    # Strip wrapping quotes around the trailing parenthetical if present
    # e.g. … ' "(January 20, 2025)" ' → … (January 20, 2025)
    t = re.sub(r'["“”]\s*\(([^)]+)\)\s*["“”]\s*$', r'(\1)', t)

    # Look for a month-name date inside parentheses:
    # allows optional comma after the day, mixed spacing, etc.
    m = re.search(r"\(([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})\)", t)
    if not m:
        return ""

    month_name, day, year = m.groups()
    for fmt in ("%B %d %Y", "%b %d %Y"):  # September vs Sept
        try:
            dt = datetime.strptime(f"{month_name} {day} {year}", fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""

# ── Config (hard fail if these aren’t present in config_v4) ─────────────────────
from config_v4 import (
    USER_AGENT,
    REQUEST_TIMEOUT,   # seconds (int)
    RETRY_TOTAL,       # int
    RETRY_BACKOFF,     # float
)

# ── Logging ─────────────────────────────────────────────────────────────────────
def setup_logger(name: str, level: str = "INFO", logfile: Optional[Path] = None) -> logging.Logger:
    """
    Create/reuse a logger. Honors:
      - DC_LOG_POLICY=never  → never create a file handler even if `logfile` is provided
      - DC_LOG_LEVEL=<LEVEL> → overrides the `level` argument (e.g., DEBUG, INFO)
    Idempotent: won't add duplicate handlers on repeated calls.
    Accepts `logfile` as str or Path.
    """
    # Environment overrides
    env_level = (os.getenv("DC_LOG_LEVEL") or "").strip()
    if env_level:
        level = env_level
    log_policy = (os.getenv("DC_LOG_POLICY") or "").strip().lower()

    logger = logging.getLogger(name)

    # Always (re)set level in case an existing logger is reused
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Ensure a single console (stream) handler is present
    has_stream = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in logger.handlers
    )
    if not has_stream:
        fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    # Coerce logfile to a Path if provided (supports str or Path)
    logfile_path: Optional[Path] = None
    if logfile is not None:
        try:
            from pathlib import Path as _Path
            logfile_path = logfile if isinstance(logfile, _Path) else _Path(str(logfile))
        except Exception:
            logfile_path = None

    # Optionally add a file handler unless policy forbids it
    if logfile_path and log_policy != "never":
        has_file = any(
            isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(logfile_path)
            for h in logger.handlers
        )
        if not has_file:
            logfile_path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(str(logfile_path), encoding="utf-8")
            fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
            fh.setFormatter(fmt)
            logger.addHandler(fh)

    return logger

# ── Text & date utils ───────────────────────────────────────────────────────────
_WS_RE = re.compile(r"\s+")
def normalize_ws(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").replace("\xa0", " ").strip())

_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], start=1)
}

# Accept Month D, YYYY optionally wrapped in ASCII/Unicode parens/brackets
DATE_LONG_RE = re.compile(
    r"[([{（]?\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(20\d{2})\s*[\])】）]?",
    re.I,
)

from datetime import date, timedelta

INAUGURATION_DAY = date(2025, 1, 20)   # Monday
FIRST_FRIDAY     = date(2025, 1, 24)   # Friday of Week 1
FIRST_SATURDAY   = date(2025, 1, 25)   # Start of Week 2

def resolve_date_window(*, start=None, end=None, weeks=None, week=None) -> tuple[date, date]:
    """
    Resolve any of the allowed CLI date specifications into (start_date, end_date).

    Modes:
      1. --start yyyy-mm-dd --end yyyy-mm-dd
      2. --start yyyy-mm-dd --weeks n
      3. --week n                     (week 1 = Jan20–Jan24, others Sat–Fri)
      4. --week n --weeks m           (m full Saturday–Friday weeks)

    Returns:
        (start_date, end_date)
    Raises:
        ValueError on invalid combinations.
    """

    # ---- Group A: explicit start ----
    if start:
        start_d = date.fromisoformat(start)
        if end:
            end_d = date.fromisoformat(end)
        elif weeks:
            if weeks < 1:
                raise ValueError("--weeks must be ≥ 1")
            end_d = start_d + timedelta(days=7 * weeks - 1)
        else:
            raise ValueError("Must specify either --end or --weeks with --start")
        return start_d, end_d

    # ---- Group B: week numbering ----
    if week:
        if week < 1:
            raise ValueError("--week must be ≥ 1")

        if week == 1:
            start_d = INAUGURATION_DAY
            end_d   = FIRST_FRIDAY
        else:
            start_d = FIRST_SATURDAY + timedelta(days=(week - 2) * 7)
            end_d   = start_d + timedelta(days=6)

        if weeks and weeks > 1:
            # Extend beyond the initial week
            end_d = end_d + timedelta(days=7 * (weeks - 1))
        return start_d, end_d

    raise ValueError("Must specify either (--start and --end|--weeks) or (--week [--weeks])")

def _to_date(iso: str) -> date:
    return datetime.strptime(iso, "%Y-%m-%d").date()

def within_window(iso: str, start_iso: str, end_iso: str) -> bool:
    if not iso:
        return False
    try:
        d = _to_date(iso)
        return _to_date(start_iso) <= d <= _to_date(end_iso)
    except Exception:
        return False

def now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

# ── URL helpers ─────────────────────────────────────────────────────────────────
def canonicalize_url(href: str, base: Optional[str] = None) -> str:
    """
    Join against base (if provided), strip fragments, and normalize scheme/host.
    """
    url = urljoin(base or "", href or "")
    parts = urlsplit(url)
    # Drop fragment, keep query
    clean = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ""))
    return clean

# ── HTTP session & GET with retries ─────────────────────────────────────────────
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def http_get(session: requests.Session, url: str, logger: Optional[logging.Logger] = None, timeout: Optional[int] = None) -> Tuple[int, Optional[str]]:
    try:
        resp = session.get(url, timeout=timeout or REQUEST_TIMEOUT)
        if logger:
            logger.debug("%s %s %s", url, resp.status_code, len(resp.content or b""))
        if resp.status_code == 200:
            resp.encoding = resp.encoding or "utf-8"
            return resp.status_code, resp.text
        return resp.status_code, None
    except requests.RequestException as e:
        if logger:
            logger.debug("GET failed %s: %s", url, e)
        return 0, None

# ── JSON I/O ────────────────────────────────────────────────────────────────────
def write_json(path: Path | str, obj: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ── Artifact paths ──────────────────────────────────────────────────────────────
def create_artifact_paths(artifacts_root: Path | str, harvester_id: str, start_iso: str, end_iso: str) -> Tuple[Path, Path]:
    """
    Returns (raw_path, filtered_path) under {artifacts_root}/json/
      raw:      {harvester}_raw_{start}_{end}.json
      filtered: {harvester}_filtered_{start}_{end}.json
    """
    root = Path(artifacts_root)
    json_dir = root / "json"
    json_dir.mkdir(parents=True, exist_ok=True)
    raw_path = json_dir / f"{harvester_id}_raw_{start_iso}_{end_iso}.json"
    filtered_path = json_dir / f"{harvester_id}_filtered_{start_iso}_{end_iso}.json"
    return raw_path, filtered_path

# ── Misc helpers ────────────────────────────────────────────────────────────────
def polite_sleep(seconds: Optional[float] = None) -> None:
    """
    Sleep a small, human-ish interval to be polite to hosts.
    """
    delay = seconds if seconds is not None else (0.40 + random.random() * 0.55)
    time.sleep(delay)

def stable_dedupe(seq: Iterable[Any]) -> List[Any]:
    seen: set = set()
    out: List[Any] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out