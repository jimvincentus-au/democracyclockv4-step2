# getjustsecurity_v5.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime, date, timedelta, UTC
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse as _urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- V4 infra (consistent with your other harvesters) ----
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,   # returns (raw_path, filtered_path)
    write_json,              # write_json(path, obj)
)

HARVESTER_ID = "justsecurity"

TRACKER_URL = "https://www.justsecurity.org/107087/tracker-litigation-legal-challenges-trump-administration/"
TABLE_WRAPPER_ID = "tablepress-42_wrapper"   # as used today; we also try fallbacks

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# ---------------------------------------------------------------------------
# Raw JSON write policy
#   Controlled by env var DC_WRITE_RAW = {always|never|auto}
#   - always: always write the RAW snapshot
#   - never : never write RAW
#   - auto  : write RAW unless it's very large (>1000 rows) and log level is not DEBUG/TRACE
# ---------------------------------------------------------------------------

def _raw_policy() -> str:
    v = os.getenv("DC_WRITE_RAW", "").strip().lower()
    if v in {"always", "never", "auto"}:
        return v
    return "auto"


def _should_write_raw(start: str, end: str, parsed_total: int) -> bool:
    policy = _raw_policy()
    if policy == "always":
        return True
    if policy == "never":
        return False
    # auto: prefer to keep things tidy
    lvl = (os.getenv("DC_LOG_LEVEL", "") or "").strip().upper()
    if lvl in {"DEBUG", "TRACE"}:
        return True
    return parsed_total <= 1000


DATE_COL_CANDIDATES = [
    "Last Case Update",
    "Last Updated",
    "Updated",
    "Date",
    "Last update",
    "Last Update",
    "Date Case Filed",
]

# --- Helper to normalize titles and strip bracketed flags like [NEW], [UPDATED], etc.
_BRACKET_FLAG_RE = re.compile(r"\s*\[(?:NEW|UPDATED|UPDATE|AMENDED|CORRECTED)[^\]]*\]\s*", re.I)


def _normalize_title(raw: str) -> tuple[str, str]:
    """
    Return (normalized_title, raw_title). Strips bracketed markers like [NEW], [UPDATED].
    """
    raw_title = (raw or "").strip()
    if not raw_title:
        return "", ""
    norm = _BRACKET_FLAG_RE.sub(" ", raw_title)
    norm = " ".join(norm.split())
    return norm, raw_title


# --- Case-update parsing helpers ---------------------------------------------

_UPDATE_LINE_RE = re.compile(
    r"""
    (?P<month>Jan\.?|Feb\.?|Mar\.?|Apr\.?|May|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?
      |January|February|March|April|May|June|July|August|September|October|November|December)
    \s+
    (?P<day>\d{1,2}),
    \s+
    (?P<year>\d{4})
    \s*:
    \s*
    (?P<text>.*?)
    (?=(?:\s+(?:Jan\.?|Feb\.?|Mar\.?|Apr\.?|May|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?|January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\s*:)|\Z)
    """,
    re.I | re.S | re.X,
)

_DOCKET_IN_CASE_NAME_RE = re.compile(r"\b\d{1,2}:\d{2}-[A-Za-z]{2,3}-\d{3,6}\b", re.I)

_STRICT_DOCKET_LABEL_RE = re.compile(
    r"\b(?:No\.?|Case No\.?|Docket No\.?|Case\s*#)\s*[:#]?\s*([A-Za-z0-9_.:\-]{3,})",
    re.I,
)

_STRICT_DOCKET_PATTERNS = [
    re.compile(r"\b\d{1,2}:\d{2}-[A-Za-z]{2,3}-\d{3,6}\b", re.I),
    re.compile(r"\b\d{2}-\d{3,6}\b"),
    re.compile(r"\b[A-Z]{1,4}-\d{2}-\d{3,6}\b", re.I),
]


def _parse_case_updates(case_updates: str) -> List[Dict[str, str]]:
    """
    Explode the free-text 'Case Updates' field into dated update records.
    Each record has: event_date, event_date_raw, update_text.
    """
    text = (case_updates or "").strip()
    if not text:
        return []

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u2019", "'")
    text = text.replace("\uff08", "(").replace("\uff09", ")")
    text = text.replace("\u200b", "")
    items: List[Dict[str, str]] = []
    for m in _UPDATE_LINE_RE.finditer(text):
        raw_date = f"{m.group('month')} {m.group('day')}, {m.group('year')}"
        dd = _parse_date(raw_date)
        body = " ".join((m.group("text") or "").split())
        if not dd or not body:
            continue
        items.append({
            "event_date": dd.isoformat(),
            "event_date_raw": raw_date,
            "update_text": body,
        })
    return items


# --- Batch-2 helpers: action summary + event date hint -----------------------

# Light sentence/clause splitter (first clause/sentence is usually the action)
# Keep this regex simple and fixed-width-safe; abbreviation protection happens in
# `_extract_action_summary` instead of in a variable-width lookbehind.
_SENT_SPLIT_RE = re.compile(r"(?:[?!]\s+|;\s+|:\s+|\.(?=\s+[A-Z]))")


def _protect_sentence_abbrevs(text: str) -> str:
    """
    Protect common legal/court abbreviations so sentence splitting does not break on
    internal periods. This is LOCAL to action-summary extraction only; it does not
    alter stored source text.
    """
    s = text

    # Literal replacements are more reliable here than regex word-boundary patterns,
    # because abbreviations like `v.` and `D.C.` often sit next to spaces, commas,
    # or parentheses in ways that make `\b` matching brittle.
    literal_replacements = [
        ("S.D.N.Y.", "SDNY§"),
        ("E.D.N.Y.", "EDNY§"),
        ("N.D.Cal.", "NDCal§"),
        ("C.D.Cal.", "CDCal§"),
        ("S.D.Cal.", "SDCal§"),
        ("D.D.C.", "DDC§"),
        ("D.Mass.", "DMass§"),
        ("D.Md.", "DMd§"),
        ("D.Minn.", "DMinn§"),
        ("D.Or.", "DOr§"),
        ("W.D.Wash.", "WDWash§"),
        ("W.D.N.C.", "WDNC§"),
        ("U.S.", "US§"),
        ("D.C.", "DC§"),
        ("S.D.", "SD§"),
        ("N.D.", "ND§"),
        ("E.D.", "ED§"),
        ("W.D.", "WD§"),
        (" v. ", " v§ "),
        (" No. ", " No§ "),
        (" Nos. ", " Nos§ "),
        (" Inc. ", " Inc§ "),
        (" Co. ", " Co§ "),
        (" Corp. ", " Corp§ "),
        (" Dept. ", " Dept§ "),
        (" Dep't. ", " Dep't§ "),
    ]
    for old, new in literal_replacements:
        s = s.replace(old, new)

    # Handle abbreviation-at-end cases that do not have trailing spaces.
    end_replacements = [
        (r"\bv\.$", "v§"),
        (r"\bNo\.$", "No§"),
        (r"\bNos\.$", "Nos§"),
        (r"\bInc\.$", "Inc§"),
        (r"\bCo\.$", "Co§"),
        (r"\bCorp\.$", "Corp§"),
        (r"\bDept\.$", "Dept§"),
        (r"\bDep't\.$", "Dep't§"),
    ]
    for pat, repl in end_replacements:
        s = re.sub(pat, repl, s)

    return s


def _restore_sentence_abbrevs(text: str) -> str:
    s = text
    replacements = [
        ("SDNY§", "S.D.N.Y."),
        ("EDNY§", "E.D.N.Y."),
        ("NDCal§", "N.D.Cal."),
        ("CDCal§", "C.D.Cal."),
        ("SDCal§", "S.D.Cal."),
        ("DDC§", "D.D.C."),
        ("DMass§", "D.Mass."),
        ("DMd§", "D.Md."),
        ("DMinn§", "D.Minn."),
        ("DOr§", "D.Or."),
        ("WDWash§", "W.D.Wash."),
        ("WDNC§", "W.D.N.C."),
        ("US§", "U.S."),
        ("DC§", "D.C."),
        ("SD§", "S.D."),
        ("ND§", "N.D."),
        ("ED§", "E.D."),
        ("WD§", "W.D."),
        (" v§ ", " v. "),
        (" No§ ", " No. "),
        (" Nos§ ", " Nos. "),
        (" Inc§ ", " Inc. "),
        (" Co§ ", " Co. "),
        (" Corp§ ", " Corp. "),
        (" Dept§ ", " Dept. "),
        (" Dep't§ ", " Dep't. "),
        ("v§", "v."),
        ("No§", "No."),
        ("Nos§", "Nos."),
        ("Inc§", "Inc."),
        ("Co§", "Co."),
        ("Corp§", "Corp."),
        ("Dept§", "Dept."),
        ("Dep't§", "Dep't."),
    ]
    for old, new in replacements:
        s = s.replace(old, new)
    return s

# Date token regexes to *find* a date mention inside free text.
# We will pass matches to _parse_date() for normalization.
_DATE_TOKEN_RES = [
    re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.? \d{1,2}, \d{4}\b", re.I),
    re.compile(r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b", re.I),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b"),
]

def _extract_action_summary(*texts: str, max_len: int = 160) -> str:
    """
    Pick the first non-empty text among inputs and return its first clause/sentence,
    trimmed to ~max_len. Protect common legal/court abbreviations only within this
    local summary extraction step so the underlying harvested text remains unchanged.
    """
    base = ""
    for t in texts:
        t = (t or "").strip()
        if t:
            base = " ".join(t.split())
            break
    if not base:
        return ""

    low = base.lower()
    if low in {"awaiting court ruling", "pending", "active", "closed"}:
        return base

    protected = _protect_sentence_abbrevs(base)
    parts = _SENT_SPLIT_RE.split(protected, maxsplit=1)
    summary = parts[0].strip() if parts else protected.strip()
    summary = _restore_sentence_abbrevs(summary)

    if len(summary) > max_len:
        summary = summary[:max_len].rstrip(" ,;:-") + "…"
    return summary

def _find_event_date_hint_in_text(*texts: str) -> Tuple[Optional[str], str]:
    """
    Scan input texts for a concrete date token (e.g., 'Oct. 22, 2025', '2025-10-22').
    Returns (iso_date_or_None, raw_match_or_"").
    """
    blob = "  ".join([(t or "") for t in texts if t]).strip()
    if not blob:
        return None, ""
    for rx in _DATE_TOKEN_RES:
        m = rx.search(blob)
        if m:
            raw = m.group(0)
            d = _parse_date(raw)
            if d:
                return d.isoformat(), raw
    return None, ""

# Normalize common punctuation/whitespace quirks so our regexes are robust
def _normalize_punct(s: str) -> str:
    if not s:
        return ""
    # unify fancy dashes/quotes/parens and strip zero-widths
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    s = s.replace("\u2019", "'")
    s = s.replace("\uff08", "(").replace("\uff09", ")")
    s = s.replace("\u200b", "")
    # collapse whitespace
    s = " ".join(s.split())
    return s

# --- Batch-3 helpers: heuristic court + docket extraction --------------------

# Minimal but high-yield detectors for courts and dockets.
_COURT_PATTERNS = [
    # Supreme Court (federal)
    (re.compile(r"\b(Supreme Court|SCOTUS)\b", re.I), ("U.S. Supreme Court", "federal")),
    (re.compile(r"(^|//|\.)supremecourt\.gov", re.I), ("U.S. Supreme Court", "federal")),

    # Courts of Appeals (federal)
    (re.compile(r"\b(\d{1,2})(st|nd|rd|th)\s+Cir\b", re.I), ("{ord} Cir.", "federal")),
    (re.compile(r"\bD\.C\.\s*Cir\b", re.I), ("D.C. Cir.", "federal")),

    # District Courts (federal)
    (re.compile(r"\bD\.D\.C\.\b", re.I), ("D.D.C.", "federal")),
    (re.compile(r"\bS\.D\.N\.Y\.\b", re.I), ("S.D.N.Y.", "federal")),
    (re.compile(r"\bS\.D\.N\.Y\b", re.I), ("S.D.N.Y.", "federal")),
    (re.compile(r"\bE\.D\.N\.Y\.\b", re.I), ("E.D.N.Y.", "federal")),
    (re.compile(r"\bN\.D\.Tex\.\b", re.I), ("N.D.Tex.", "federal")),
    (re.compile(r"\bS\.D\.Tex\.\b", re.I), ("S.D.Tex.", "federal")),
    (re.compile(r"\bN\.D\.Cal\.\b", re.I), ("N.D.Cal.", "federal")),
    (re.compile(r"\bN\.D\.Cal\b", re.I), ("N.D.Cal.", "federal")),
    (re.compile(r"\bC\.D\.Cal\.\b", re.I), ("C.D.Cal.", "federal")),
    (re.compile(r"\bS\.D\.Cal\.\b", re.I), ("S.D.Cal.", "federal")),
    (re.compile(r"\bE\.D\.Va\.\b", re.I), ("E.D.Va.", "federal")),
    (re.compile(r"\bD\.Mass\.\b", re.I), ("D.Mass.", "federal")),
    (re.compile(r"\bD\.Ariz\.\b", re.I), ("D.Ariz.", "federal")),
    (re.compile(r"\bD\.Colo\.\b", re.I), ("D.Colo.", "federal")),
    (re.compile(r"\bM\.D\.Fla\.\b", re.I), ("M.D.Fla.", "federal")),
    (re.compile(r"\bS\.D\.Fla\.\b", re.I), ("S.D.Fla.", "federal")),
    (re.compile(r"\bN\.D\.Ill\.\b", re.I), ("N.D.Ill.", "federal")),
    (re.compile(r"\bD\.Minn\.\b", re.I), ("D.Minn.", "federal")),
    (re.compile(r"\bD\.Or\.\b", re.I), ("D.Or.", "federal")),
    (re.compile(r"\bW\.D\.Wash\.\b", re.I), ("W.D.Wash.", "federal")),
    (re.compile(r"\bW\.D\.N\.C\.\b", re.I), ("W.D.N.C.", "federal")),
    (re.compile(r"\bD\.Md\.\b", re.I), ("D.Md.", "federal")),
    (re.compile(r"\bD\.N\.H\.\b", re.I), ("D.N.H.", "federal")),
    (re.compile(r"\bD\.R\.I\.\b", re.I), ("D.R.I.", "federal")),
    (re.compile(r"\bD\.Me\.\b", re.I), ("D.Me.", "federal")),
    (re.compile(r"\bD\.Vt\.\b", re.I), ("D.Vt.", "federal")),
    (re.compile(r"\bD\.Conn\.\b", re.I), ("D.Conn.", "federal")),
    (re.compile(r"\bE\.D\.Pa\.\b", re.I), ("E.D.Pa.", "federal")),
    (re.compile(r"\bM\.D\.Pa\.\b", re.I), ("M.D.Pa.", "federal")),
    (re.compile(r"\bW\.D\.Pa\.\b", re.I), ("W.D.Pa.", "federal")),
    (re.compile(r"\bE\.D\.Mich\.\b", re.I), ("E.D.Mich.", "federal")),
    (re.compile(r"\bW\.D\.Mich\.\b", re.I), ("W.D.Mich.", "federal")),
    (re.compile(r"\bN\.D\.Ohio\.\b", re.I), ("N.D.Ohio.", "federal")),
    (re.compile(r"\bS\.D\.Ohio\.\b", re.I), ("S.D.Ohio.", "federal")),
    (re.compile(r"\bE\.D\.Wis\.\b", re.I), ("E.D.Wis.", "federal")),
    (re.compile(r"\bW\.D\.Wis\.\b", re.I), ("W.D.Wis.", "federal")),

    # State supreme/appellate
    (re.compile(r"\bSupreme Court of [A-Z][a-z]+", re.I), ("{match}", "state")),
    (re.compile(r"\bCourt of Appeals of [A-Z][a-z]+", re.I), ("{match}", "state")),
]

# Docket patterns
_DOCKET_RES = [
    re.compile(r"\b(?:No\.?|Case No\.?|Docket No\.?)\s*[:#]?\s*([A-Za-z0-9\-\.:]{3,})", re.I),
    re.compile(r"\b\d{1,2}:\d{2}[-–][A-Za-z]{2}[-–]\d{3,5}\b", re.I),
    re.compile(r"\b\d{2}-\d{3,6}\b"),
]

def _to_ordinal(n_str: str) -> str:
    try:
        n = int(n_str)
    except Exception:
        return n_str
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1:"st",2:"nd",3:"rd"}.get(n % 10, "th")
    return f"{n}{suf}"

def _detect_court(blob: str, url: str = "") -> Tuple[str, str]:
    text = _normalize_punct(((blob or "") + " " + (url or "")).strip())

    # Parenthetical court abbreviations like (D.D.C.), (S.D.N.Y.), (N.D.Cal.), etc.
    m_abbr = re.search(r"\(((?:[A-Z][a-z]{0,4}\.|[A-Z]\.)(?:\s*(?:[A-Z][a-z]{0,4}\.|[A-Z]\.)){0,5})\)", text)
    if m_abbr:
        abbr = m_abbr.group(1)
        return (abbr, "federal")

    m_abbr_loose = re.search(r"\(((?:[A-Z][a-z]{0,4}\.?|[A-Z]\.)(?:\s*(?:[A-Z][a-z]{0,4}\.?|[A-Z]\.)){0,5})\)", text)
    if m_abbr_loose:
        abbr = m_abbr_loose.group(1).strip()
        if abbr and not abbr.endswith("."):
            abbr = abbr + "."
        return (abbr, "federal")

    # Explicit circuit in text, e.g., "5th Cir" or "5th Cir."
    m_cir = re.search(r"\b(\d{1,2})(st|nd|rd|th)\s+Cir\.?\b", text, re.I)
    if m_cir:
        ord_txt = _to_ordinal(m_cir.group(1))
        return (f"{ord_txt} Cir.", "federal")

    # D.C. Circuit spelled out
    if re.search(r"\bD\.C\.\s*Cir\.?\b", text, re.I):
        return ("D.C. Cir.", "federal")

    # Known textual patterns from _COURT_PATTERNS (fallback sweep)
    for rx, (label, juris) in _COURT_PATTERNS:
        m = rx.search(text)
        if not m:
            continue
        if "{ord}" in label and m.lastindex and m.group(1):
            ord_txt = _to_ordinal(m.group(1))
            return (label.format(ord=ord_txt), juris)
        if "{match}" in label:
            return (label.format(match=m.group(0)), juris)
        return (label, juris)

    # CourtListener hints in URL
    if "courtlistener.com" in text:
        m = re.search(r"/docket/[^/]*\bca(\d{1,2})\b", text, re.I)
        if m:
            return (f"{_to_ordinal(m.group(1))} Cir.", "federal")
        if re.search(r"/docket/[^/]*\bscotus\b", text, re.I):
            return ("U.S. Supreme Court", "federal")

    return ("", "")

def _detect_docket(*texts: str) -> str:
    blob = _normalize_punct("  ".join([t for t in texts if t]))
    if not blob:
        return ""

    m = _STRICT_DOCKET_LABEL_RE.search(blob)
    if m:
        cand = m.group(1).rstrip(").,;: ")
        if any(rx.fullmatch(cand) for rx in _STRICT_DOCKET_PATTERNS):
            return cand

    for rx in _STRICT_DOCKET_PATTERNS:
        m2 = rx.search(blob)
        if m2:
            return m2.group(0)

    if "courtlistener.com" in blob:
        m3 = re.search(r"/docket/\d+/([^/]+)/?$", blob, re.I)
        if m3:
            slug = m3.group(1)
            if any(ch.isdigit() for ch in slug):
                return slug

    return ""

def _extract_court_and_docket(title: str, url: str, *extra_cells: str) -> Tuple[str, str, str]:
    """
    Heuristically infer court_name, jurisdiction, docket from title/url/adjacent cells.
    Docket extraction is intentionally conservative to avoid junk strings.
    """
    norm_title = _normalize_punct(title)
    norm_url = _normalize_punct(url)
    norm_extras = tuple(_normalize_punct(x or "") for x in extra_cells)
    court_name, jurisdiction = _detect_court(" ".join((norm_title,) + norm_extras), norm_url)
    docket = _detect_docket(norm_title, norm_url, *norm_extras)

    if not docket:
        m = _DOCKET_IN_CASE_NAME_RE.search(norm_title)
        if m:
            docket = m.group(0)

    return court_name, jurisdiction, docket


# ---------------------------------------------------------------------------
# Networking / session
# ---------------------------------------------------------------------------

def _make_retry_session(timeout: int) -> requests.Session:
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    retry = Retry(
        total=6,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    orig = s.request
    def _with_timeout(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return orig(method, url, **kw)
    s.request = _with_timeout  # type: ignore[assignment]
    return s


# ---------------------------------------------------------------------------
# V3-equivalent helpers (parsing, dating, filtering)
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    s = re.sub(r"^[A-Za-z ]*:\s*", "", s)
    s = re.sub(r"(\b[A-Za-z]{3,9})\.", r"\1", s)
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            return None
    return None

def _end_date_from_weeks(start_d: date, weeks: int) -> date:
    delta_to_fri = (4 - start_d.weekday()) % 7
    first_week_end = start_d + timedelta(days=delta_to_fri)
    return first_week_end + timedelta(days=7 * (weeks - 1)) if weeks > 1 else first_week_end

def _find_date_col(cols: List[str]) -> str:
    preferred = [
        "Last Case Update",
        "Last Updated",
        "Updated",
        "Last update",
        "Last Update",
        "Date Case Filed",
        "Date",
    ]
    cols_stripped = {c.strip(): c for c in cols}

    for want in preferred:
        if want in cols_stripped:
            return cols_stripped[want]

    lc = {c.lower(): c for c in cols}
    for needle in ("last case update", "last updated", "updated", "last update", "date case filed", "date"):
        for k, orig in lc.items():
            if needle in k:
                return orig
    return "Last Case Update"

def _extract_table_html_and_links(html: str, logger) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parse the current Just Security TablePress tracker table directly from DOM.
    Expected current layout:
      1 Case Name
      2 Filings
      3 Date Case Filed
      4 State A.G.'s
      5 Case Status
      6 Issue
      7 Executive Action
      8 Last Case Update
      9 Case Summary
      10 Case Updates
    Returns (DataFrame, urls[]) aligned by row.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table#tablepress-42")
    if not table:
        logger.warning("table#tablepress-42 NOT found.")
        return pd.DataFrame(columns=[
            "Case Name",
            "Filings",
            "Date Case Filed",
            "State A.G.'s",
            "Case Status",
            "Issue",
            "Executive Action",
            "Last Case Update",
            "Case Summary",
            "Case Updates",
        ]), []

    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")
    logger.debug("DOM Strategy: found %d <tr> under table#tablepress-42.", len(trs))

    records: List[Dict[str, Any]] = []
    urls: List[str] = []

    def txt_from_tr(tr, col_num: int) -> str:
        node = tr.select_one(f"td.column-{col_num}")
        return node.get_text(" ", strip=True) if node else ""

    for i, tr in enumerate(trs, 1):
        td1 = tr.find("td", class_=re.compile(r"\bcolumn-1\b"))
        td8 = tr.find("td", class_=re.compile(r"\bcolumn-8\b"))
        if not td1 or not td8:
            logger.debug("DOM skip row %d: missing column-1 or column-8 (classes=%s)", i, tr.get("class"))
            continue

        a = td1.find("a", href=True)
        if not a:
            logger.debug("DOM skip row %d: column-1 has no <a href>", i)
            continue

        rec = {
            "Case Name": txt_from_tr(tr, 1),
            "Filings": txt_from_tr(tr, 2),
            "Date Case Filed": txt_from_tr(tr, 3),
            "State A.G.'s": txt_from_tr(tr, 4),
            "Case Status": txt_from_tr(tr, 5),
            "Issue": txt_from_tr(tr, 6),
            "Executive Action": txt_from_tr(tr, 7),
            "Last Case Update": txt_from_tr(tr, 8),
            "Case Summary": txt_from_tr(tr, 9),
            "Case Updates": txt_from_tr(tr, 10),
        }
        records.append(rec)
        url = (a["href"] or "").strip()
        urls.append(url)

        if i <= 5 or i % 50 == 0:
            logger.debug(
                "DOM keep row %d: name=%r | url=%s | last_case_update=%s",
                i,
                (rec["Case Name"] or "")[:80],
                url,
                rec["Last Case Update"],
            )

    df = pd.DataFrame.from_records(
        records,
        columns=[
            "Case Name",
            "Filings",
            "Date Case Filed",
            "State A.G.'s",
            "Case Status",
            "Issue",
            "Executive Action",
            "Last Case Update",
            "Case Summary",
            "Case Updates",
        ],
    )

    logger.info("DOM Strategy parsed %d kept rows with case link and last-case-update.", len(df))
    if len(df) < 350:
        logger.warning("Parsed fewer than expected rows (kept=%d; expected ≈≥ 400).", len(df))
    return df, urls

def _parse_tracker_rows(html: str, logger) -> list[dict]:
    """
    Parse the Just Security tracker table (#tablepress-42) directly.
    Returns a list of dicts: {row, title, url, raw_date}.
    Uses Last Case Update as the row-level date field.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table#tablepress-42")
    if not table:
        ids = [t.get("id", "") for t in soup.find_all("table")]
        logger.warning("table#tablepress-42 NOT found. First few table ids: %s", ", ".join(ids[:8]))
        return []

    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")
    logger.debug("Found %d <tr> under tbody for #tablepress-42.", len(trs))

    rows: list[dict] = []
    kept = 0
    for i, tr in enumerate(trs, 1):
        td1 = tr.find("td", class_=re.compile(r"\bcolumn-1\b"))
        td8 = tr.find("td", class_=re.compile(r"\bcolumn-8\b"))
        if not td1 or not td8:
            logger.debug("skip row %d: missing column-1 or column-8 (classes=%s)", i, tr.get("class"))
            continue

        a = td1.find("a", href=True)
        if not a:
            logger.debug("skip row %d: column-1 has no <a href>", i)
            continue

        url = (a["href"] or "").strip()
        title_text = " ".join(a.get_text(" ", strip=True).split())
        raw_date = " ".join(td8.get_text(" ", strip=True).split())

        rows.append({
            "row": i,
            "title": title_text,
            "url": url,
            "raw_date": raw_date,
        })
        kept += 1
        if i <= 8 or i % 50 == 0:
            logger.debug("row %d KEEP: title=%r | url=%s | raw_date=%s", i, title_text[:100], url, raw_date)

    logger.info("Parsed tracker rows under #tablepress-42: total_tr=%d, kept_with_link_and_last_case_update=%d", len(trs), kept)
    if kept < 350:
        logger.warning("Parsed fewer than expected rows (kept=%d; expected ≈≥ 400).", kept)
    return rows

def _df_to_records(df: pd.DataFrame, urls: List[str]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for i, row in df.iterrows():
        d = {str(k): ("" if pd.isna(v) else str(v)) for k, v in row.to_dict().items()}
        if i < len(urls):
            d.setdefault("URL", urls[i])
        recs.append(d)
    return recs

def _filter_window(records: List[Dict[str, Any]], start_d: date, end_d: date, logger) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (matches_in_window_with__date, audit_rows)

    For Just Security, the raw row is a case record, but the actual newsworthy unit
    is each dated item inside 'Case Updates'. We therefore explode each case row into
    zero or more update-events, and only then apply the date window.
    """
    if not records:
        return [], []

    cols = list(records[0].keys())
    date_col = _find_date_col(cols)
    logger.info("Chosen row-level date column: %r", date_col)

    matches: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []

    skipped_no_url = 0
    skipped_no_updates = 0
    newest: Optional[date] = None
    total_update_events = 0
    no_update_examples: List[Tuple[str, str]] = []

    for idx, r in enumerate(records, 1):
        case_name = r.get("Case Name") or r.get("Title") or r.get("Case") or ""
        url_val = (r.get("URL", "") or "").strip()
        row_raw_date = (r.get(date_col, "") or "").strip()
        row_dd = _parse_date(row_raw_date) if row_raw_date else None

        audit.append({
            "row": idx,
            "title": case_name,
            "url": url_val,
            "raw_date": row_raw_date,
            "parsed_date": row_dd.isoformat() if row_dd else "",
        })

        if not url_val:
            skipped_no_url += 1
            continue

        updates = _parse_case_updates(r.get("Case Updates", ""))
        if updates:
            for j, upd in enumerate(updates, 1):
                dd = _parse_date(upd["event_date_raw"])
                if not dd:
                    continue
                total_update_events += 1
                if newest is None or dd > newest:
                    newest = dd
                if start_d <= dd <= end_d:
                    r2 = dict(r)
                    r2["_date"] = dd.isoformat()
                    r2["_raw_date"] = upd["event_date_raw"]
                    r2["_event_kind"] = "case_update"
                    r2["_update_index"] = j
                    r2["_update_text"] = upd["update_text"]
                    matches.append(r2)
            continue

        skipped_no_updates += 1
        if len(no_update_examples) < 5:
            no_update_examples.append((case_name, (r.get("Case Updates", "") or "")[:300]))

        # Fallback: if there are no parseable update entries, allow the filing date
        # itself to act as the raw event so newly filed cases are not lost.
        filed_raw = (r.get("Date Case Filed", "") or "").strip()
        filed_dd = _parse_date(filed_raw) if filed_raw else None
        if filed_dd:
            if newest is None or filed_dd > newest:
                newest = filed_dd
            if start_d <= filed_dd <= end_d:
                r2 = dict(r)
                r2["_date"] = filed_dd.isoformat()
                r2["_raw_date"] = filed_raw
                r2["_event_kind"] = "case_filed"
                r2["_update_index"] = 0
                r2["_update_text"] = ""
                matches.append(r2)

    logger.debug(
        "Skip counts: no_url=%d no_parseable_updates=%d total_update_events=%d",
        skipped_no_url,
        skipped_no_updates,
        total_update_events,
    )
    if no_update_examples:
        for idx, (nm, sample) in enumerate(no_update_examples, 1):
            logger.debug("No-parse example %d: %s | %r", idx, nm[:120], sample)

    if newest and newest < start_d:
        logger.warning("Data may be stale vs your window: newest %s < start %s", newest.isoformat(), start_d.isoformat())

    logger.info("Filter kept %d event-level records from %d case rows in window.", len(matches), len(records))
    return matches, audit


# ---------------------------------------------------------------------------
# Transform to V4 entity schema
# ---------------------------------------------------------------------------

def _to_entity_v4(rec: Dict[str, Any]) -> Dict[str, Any]:
    raw_title = (
        rec.get("Lawsuit")
        or rec.get("Case Name")
        or rec.get("Title")
        or rec.get("Case")
        or ""
    ).strip()
    title_norm, title_raw = _normalize_title(raw_title)

    url = (rec.get("URL") or rec.get("Link") or rec.get("Case URL") or "").strip()
    post_date = (rec.get("_date") or "").strip()
    tracker_last_update_raw = (rec.get("_raw_date") or rec.get("Last Case Update") or rec.get("Last Update") or "").strip()
    event_kind = (rec.get("_event_kind") or "").strip()
    update_index = rec.get("_update_index") or 0
    update_text = (rec.get("_update_text") or "").strip()
    case_summary = (rec.get("Case Summary") or "").strip()
    case_status = (rec.get("Case Status") or "").strip()

    if event_kind == "case_filed":
        action_summary = "Case filed"
        event_date_iso = post_date or ""
        event_date_raw = tracker_last_update_raw or ""
    else:
        action_summary = _extract_action_summary(
            update_text,
            rec.get("Case Updates", ""),
            case_status,
            case_summary,
        )
        event_date_iso = post_date or ""
        event_date_raw = tracker_last_update_raw or ""
        if not event_date_iso:
            event_date_iso, event_date_raw = _find_event_date_hint_in_text(
                update_text,
                rec.get("Case Updates", ""),
                case_status,
                case_summary,
                title_norm or title_raw,
            )

    court_name, jurisdiction, docket = _extract_court_and_docket(
        title_norm or title_raw,
        url,
        rec.get("Filings", ""),
        case_status,
        case_summary,
    )

    return {
        "source": "Just Security",
        "source_label": "Just Security — Litigation Tracker",
        "doc_type": "litigation",
        "topic_hint": "Judicial Developments",
        "title": title_norm or title_raw,
        "raw_title": title_raw,
        "url": url,
        "canonical_url": url,
        "summary_url": url,
        "summary": case_summary,
        "summary_origin": "Case Summary",
        "summary_timestamp": "",
        "post_date": post_date,
        "tracker_last_update_raw": tracker_last_update_raw,
        "event_kind": event_kind,
        "update_index": update_index,
        "update_text": update_text,
        "action_summary": action_summary,
        "event_date_hint": (event_date_iso or ""),
        "event_date_hint_raw": (event_date_raw or ""),
        "court_name": court_name,
        "jurisdiction": jurisdiction,
        "docket": docket,
        "raw_line": f"[justsecurity] {title_norm or title_raw} ({post_date})",
    }


# ---------------------------------------------------------------------------
# Discovery (fetch + parse + filter) and public entry
# ---------------------------------------------------------------------------

def _discover_copy_mode(
    start_iso: str,
    end_iso: str,
    *,
    timeout: int,
    logger
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    COPY-mode:
      - Fetch tracker page once
      - Parse main table → records
      - Filter rows to date window by exploding case updates into event-level records
    Returns (matches_in_window, all_records_snapshot, audit_rows)
    """
    try:
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    s = _make_retry_session(timeout)
    logger.info("Fetching tracker: %s", TRACKER_URL)
    r = s.get(TRACKER_URL)
    logger.debug("HTTP status=%s bytes=%s", r.status_code, len(r.content) if r.content is not None else 0)
    if r.status_code != 200 or not r.text:
        logger.error("Failed to fetch tracker page: HTTP %s", getattr(r, "status_code", "?"))
        return [], [], []

    try:
        df, urls = _extract_table_html_and_links(r.text, logger)
    except Exception as e:
        logger.error("Failed to parse litigation table: %s", e)
        return [], [], []

    records = _df_to_records(df, urls)
    logger.info("Parsed %d rows from tracker table.", len(records))

    matches, audit = _filter_window(records, start_d, end_d, logger)
    return matches, records, audit

def _normalize_url(u: str) -> str:
    """Return a clean, canonical URL (unwrap Proofpoint + strip utm_*)."""
    if not u:
        return ""

    u = u.strip()

    if "urldefense.proofpoint.com" in u:
        parsed = _urlparse.urlparse(u)
        q = _urlparse.parse_qs(parsed.query)
        if "u" in q and q["u"]:
            candidate = q["u"][0]
            if candidate.startswith("http"):
                u = _urlparse.unquote(candidate)

    parsed = _urlparse.urlparse(u)
    q = _urlparse.parse_qsl(parsed.query)
    q_clean = [(k, v) for (k, v) in q if not k.lower().startswith("utm_")]
    new_query = _urlparse.urlencode(q_clean, doseq=True)
    u = _urlparse.urlunparse(parsed._replace(query=new_query))

    u = u.rstrip("?/&#")

    return u

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Just Security (Litigation Tracker) — V5 event-level harvester.
      RAW:      snapshot of ALL parsed case rows + audit
      FILTERED: event-level records in date window normalized to V4 entity schema
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)
    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering Just Security (COPY mode): tracker=%s", TRACKER_URL)

    matches, all_rows, audit_rows = _discover_copy_mode(
        start_iso=start, end_iso=end, timeout=TIMEOUT_S, logger=logger
    )

    entities = [_to_entity_v4(r) for r in matches]

    logger.info("DC_WRITE_RAW policy resolved to: %s", _raw_policy())
    if _should_write_raw(start, end, len(all_rows)):
        raw_payload = {
            "source": HARVESTER_ID,
            "entity_type": "litigation_case_rows",
            "window": {"start": start, "end": end},
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "tracker_url": TRACKER_URL,
            "parsed_total": len(all_rows),
            "audit": audit_rows,
            "items": all_rows,
        }
        write_json(raw_path, raw_payload)
        logger.info("Wrote raw JSON: %s", raw_path)
    else:
        logger.info(
            "Skipped RAW JSON by policy DC_WRITE_RAW=%s (parsed_total=%d)",
            _raw_policy(), len(all_rows)
        )

    # FILTERED entities — preserve distinct dated updates within the same case.
    # Primary dedupe key: (court_name, docket, post_date, event_kind, update_index).
    # Fallback: (canonical_url, post_date, event_kind, update_index).
    seen_pair = set()
    seen_url = set()
    deduped: List[Dict[str, Any]] = []
    dupes_pair = 0
    dupes_url = 0

    for e in entities:
        raw_url = e.get("canonical_url") or e.get("url") or ""
        norm_url = _normalize_url(raw_url)
        e["canonical_url"] = norm_url

        c = (e.get("court_name") or "").strip().lower()
        d = (e.get("docket") or "").strip().lower()
        post_date = (e.get("post_date") or "").strip().lower()
        event_kind = (e.get("event_kind") or "").strip().lower()
        update_index = str(e.get("update_index") or 0)

        if c and d and post_date:
            key = (c, d, post_date, event_kind, update_index)
            if key in seen_pair:
                dupes_pair += 1
                logger.debug("Dedupe(pair): SKIP %s", key)
                continue
            seen_pair.add(key)
        else:
            url_key = (norm_url, post_date, event_kind, update_index)
            if not norm_url or url_key in seen_url:
                dupes_url += 1
                logger.debug("Dedupe(url): SKIP %r", url_key)
                continue
            seen_url.add(url_key)

        deduped.append(e)

    dupes_total = dupes_pair + dupes_url
    logger.info(
        "Window %s → %s | total_case_rows=%d kept_after_filter=%d kept_after_dedup=%d | dupes_pair=%d dupes_url=%d dupes_total=%d",
        start, end, len(all_rows), len(entities), len(deduped), dupes_pair, dupes_url, dupes_total
    )

    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "litigation",
        "window": {"start": start, "end": end},
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "count": len(deduped),
        "entities": deduped,
        "window_stats": {
            "inside": len(entities),
            "outside": max(0, len(all_rows) - len(entities)),
            "nodate": sum(1 for a in audit_rows if not a.get("parsed_date")),
            "dupes_pair": dupes_pair,
            "dupes_url": dupes_url,
            "dupes_total": dupes_total,
        },
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(deduped))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(deduped),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V5 — Just Security harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    log = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        log_path=None,
        session=None,
    )
    log.info("Summary: %s", meta)