# getjustsecurity_v4.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime, date, timedelta
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


DATE_COL_CANDIDATES = ["Last Updated", "Updated", "Date", "Last update", "Last Update"]

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

# --- Batch-2 helpers: action summary + event date hint -----------------------

# Light sentence/clause splitter (first clause/sentence is usually the action)
_SENT_SPLIT_RE = re.compile(r"(?:[.?!]\s+|—\s+|;\s+|:\s+)")

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
    trimmed to ~max_len. Used to help Step-2 produce crisper event summaries.
    """
    base = ""
    for t in texts:
        t = (t or "").strip()
        if t:
            base = t
            break
    if not base:
        return ""
    # first sentence/clause
    parts = _SENT_SPLIT_RE.split(base, maxsplit=1)
    summary = parts[0].strip() if parts else base.strip()
    if len(summary) > max_len:
        summary = summary[:max_len].rstrip(" ,;:—") + "…"
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
    (re.compile(r"\b(\d{1,2})(st|nd|rd|th)\s+Cir\b", re.I), ("{ord} Cir.", "federal")),   # e.g., "5th Cir"
    (re.compile(r"\bD\.C\.\s*Cir\b", re.I), ("D.C. Cir.", "federal")),

    # District Courts (federal) — common forms like D.D.C., S.D.N.Y., N.D.Tex.
    (re.compile(r"\bD\.D\.C\.\b", re.I), ("D.D.C.", "federal")),
    (re.compile(r"\bS\.D\.N\.Y\.\b", re.I), ("S.D.N.Y.", "federal")),
    (re.compile(r"\bE\.D\.N\.Y\.\b", re.I), ("E.D.N.Y.", "federal")),
    (re.compile(r"\bN\.D\.Tex\.\b", re.I), ("N.D.Tex.", "federal")),
    (re.compile(r"\bS\.D\.Tex\.\b", re.I), ("S.D.Tex.", "federal")),
    (re.compile(r"\bN\.D\.Cal\.\b", re.I), ("N.D.Cal.", "federal")),
    (re.compile(r"\bC\.D\.Cal\.\b", re.I), ("C.D.Cal.", "federal")),
    (re.compile(r"\bS\.D\.Cal\.\b", re.I), ("S.D.Cal.", "federal")),
    (re.compile(r"\bE\.D\.Va\.\b", re.I), ("E.D.Va.", "federal")),
    (re.compile(r"\bD\.Mass\.\b", re.I), ("D.Mass.", "federal")),
    (re.compile(r"\bD\.Ariz\.\b", re.I), ("D.Ariz.", "federal")),
    (re.compile(r"\bD\.Colo\.\b", re.I), ("D.Colo.", "federal")),
    (re.compile(r"\bM\.D\.Fla\.\b", re.I), ("M.D.Fla.", "federal")),
    (re.compile(r"\bS\.D\.Fla\.\b", re.I), ("S.D.Fla.", "federal")),

    # State supreme/appellate (keep generic; we only need jurisdiction=state)
    (re.compile(r"\bSupreme Court of [A-Z][a-z]+", re.I), ("{match}", "state")),
    (re.compile(r"\bCourt of Appeals of [A-Z][a-z]+", re.I), ("{match}", "state")),
]

# Docket patterns — common "No." / "Case No." notations and typical number forms.
_DOCKET_RES = [
    re.compile(r"\b(?:No\.?|Case No\.?|Docket No\.?)\s*[:#]?\s*([A-Za-z0-9\-\.:]{3,})", re.I),
    # Plain docket-like tokens (e.g., 25-1234, 2:25-cv-01234, 1:25-cr-0001, 23-12345)
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

    # Parenthetical court abbreviations like (D.D.C.), (S.D.N.Y.), (N.D.Cal.)
    m_abbr = re.search(r"\(([A-Za-z](?:\.[A-Za-z]){1,4}\.)\)", text)
    if m_abbr:
        abbr = m_abbr.group(1)
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
    # Combine inputs and normalize punctuation (en-dash/em-dash → '-')
    blob = _normalize_punct("  ".join([t for t in texts if t]))
    if not blob:
        return ""

    # Try explicit labels first
    m = re.search(r"\b(?:No\.?|Case No\.?|Docket No\.?|Case\s*#)\s*[:#]?\s*([A-Za-z0-9_.:\-]{3,})", blob, re.I)
    if m:
        return m.group(1).rstrip(").,;: ")

    # Common federal formats: 1:25-cv-01161, 2:25-cr-00123, 23-12345, 25-1234
    for rx in (
        re.compile(r"\b\d{1,2}:\d{2}-[A-Za-z]{2,3}-\d{3,6}\b", re.I),
        re.compile(r"\b\d{2}-\d{3,6}\b"),
        re.compile(r"\b\d{1,2}:\d{2}-[A-Za-z]{2}-\d{3,6}\b", re.I),
    ):
        m2 = rx.search(blob)
        if m2:
            return m2.group(0)

    # CourtListener often includes a short docket token in the path
    if "courtlistener.com" in blob:
        m3 = re.search(r"/docket/[^/]+/([A-Za-z0-9_.:\-]+)/", blob)
        if m3:
            return m3.group(1)

    return ""

def _extract_court_and_docket(title: str, url: str, *extra_cells: str) -> Tuple[str, str, str]:
    """
    Heuristically infer court_name, jurisdiction, docket from title/url/adjacent cells.
    """
    norm_title = _normalize_punct(title)
    norm_url = _normalize_punct(url)
    norm_extras = tuple(_normalize_punct(x or "") for x in extra_cells)
    court_name, jurisdiction = _detect_court(" ".join((norm_title,) + norm_extras), norm_url)
    docket = _detect_docket(norm_title, norm_url, *norm_extras)
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
    # strip "Updated:" etc.
    s = re.sub(r"^[A-Za-z ]*:\s*", "", s)
    # "Oct." -> "Oct"
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
    # Democracy Clock: week ends Friday
    delta_to_fri = (4 - start_d.weekday()) % 7
    first_week_end = start_d + timedelta(days=delta_to_fri)
    return first_week_end + timedelta(days=7 * (weeks - 1)) if weeks > 1 else first_week_end

def _find_date_col(cols: List[str]) -> str:
    # Exact first
    for c in cols:
        if c.strip() in DATE_COL_CANDIDATES:
            return c
    # Case-insensitive contains
    lc = {c.lower(): c for c in cols}
    for needle in ("last updated", "updated", "date", "last update"):
        for k, orig in lc.items():
            if needle in k:
                return orig
    # Fallback
    return "Last Updated"

def _extract_table_html_and_links(html: str, logger) -> Tuple[pd.DataFrame, List[str]]:
    """
    Return (DataFrame, urls[]) for the Just Security tracker table.
    Strategy A: Parse table#tablepress-42 directly from DOM (preferred).
    Strategy B: If the DOM only holds a paginated slice (<200 rows), parse the
                largest matching table via pandas.read_html and align links
                mined from the same table node with BeautifulSoup.

    Very verbose logging at DEBUG to aid troubleshooting.
    """
    from bs4 import BeautifulSoup
    import pandas as _pd
    import re

    soup = BeautifulSoup(html, "html.parser")

    # -------------------------
    # Strategy A: direct DOM read
    # -------------------------
    table = soup.select_one("table#tablepress-42")
    if not table:
        logger.warning("table#tablepress-42 NOT found; falling back to pandas strategy.")
    else:
        tbody = table.find("tbody") or table
        # Collect all TRs that look like real data rows
        trs = tbody.find_all("tr")
        logger.debug("DOM Strategy: found %d <tr> under table#tablepress-42 (tbody=%s).",
                     len(trs), "yes" if table.find("tbody") else "no")

        records: List[Dict[str, Any]] = []
        urls: List[str] = []
        kept = 0

        for i, tr in enumerate(trs, 1):
            td1 = tr.find("td", class_=re.compile(r"\bcolumn-1\b"))
            td7 = tr.find("td", class_=re.compile(r"\bcolumn-7\b"))
            if not td1 or not td7:
                logger.debug("DOM skip row %d: missing column-1 or column-7 (classes=%s)", i, tr.get("class"))
                continue
            a = td1.find("a", href=True)
            if not a:
                logger.debug("DOM skip row %d: column-1 has no <a href>", i)
                continue

            def txt(sel: str) -> str:
                node = tr.select_one(sel)
                return node.get_text(" ", strip=True) if node else ""

            rec = {
                "Case Name"      : txt("td.column-1"),
                "Filings"        : txt("td.column-2"),
                "Date Case Filed": txt("td.column-3"),
                "Case Status"    : txt("td.column-4"),
                "Last Update"    : txt("td.column-7"),
                "Case Summary"   : txt("td.column-8"),
                "Case Updates"   : txt("td.column-9"),
            }
            records.append(rec)
            url = (a["href"] or "").strip()
            urls.append(url)
            kept += 1
            if i <= 5 or i % 50 == 0:
                logger.debug("DOM keep row %d: name=%r | url=%s | last=%s",
                             i, (rec['Case Name'] or "")[:80], url, rec["Last Update"])

        if kept:
            logger.info("DOM Strategy parsed %d kept rows with link & last-update (total tr seen=%d).",
                        kept, len(trs))
        if kept >= 200:
            # Good enough; return now.
            df = _pd.DataFrame.from_records(records, columns=[
                "Case Name", "Filings", "Date Case Filed", "Case Status",
                "Last Update", "Case Summary", "Case Updates"
            ])
            if kept < 350:
                logger.warning("Parsed fewer than expected rows (kept=%d; expected ≈≥ 400).", kept)
            return df, urls
        else:
            logger.warning("DOM Strategy kept only %d rows (<200) — likely server-side pagination. "
                           "Falling back to pandas strategy.", kept)

    # -------------------------
    # Strategy B: pandas.read_html + link alignment
    # -------------------------
    logger.info("Pandas Strategy: scanning all tables in page HTML.")
    try:
        tables = _pd.read_html(StringIO(html))
    except Exception as e:
        logger.error("pandas.read_html failed: %s", e)
        return pd.DataFrame(columns=[
            "Case Name", "Filings", "Date Case Filed", "Case Status",
            "Last Update", "Case Summary", "Case Updates"
        ]), []

    if not tables:
        logger.error("pandas.read_html found no tables in page.")
        return pd.DataFrame(columns=[
            "Case Name", "Filings", "Date Case Filed", "Case Status",
            "Last Update", "Case Summary", "Case Updates"
        ]), []

    # Pick the best candidate: must contain 'Case Name' & 'Last Update', then prefer largest row count.
    def _score(df: _pd.DataFrame) -> tuple[int, int]:
        cols = [str(c).strip() for c in df.columns]
        has_case = any("case name" == c.lower() for c in cols)
        has_last = any("last update" == c.lower() for c in cols)
        return (1 if (has_case and has_last) else 0, len(df))

    scored = sorted(((i, t, _score(t)) for i, t in enumerate(tables)), key=lambda x: x[2], reverse=True)
    best_idx, best_df, (ok_cols, rows_ct) = scored[0]
    logger.info("pandas: selected table %d with %d rows (has required cols=%s).",
                best_idx, rows_ct, "yes" if ok_cols else "no")

    # Normalize columns to our canonical names, when possible
    def _canon(col: str) -> str:
        c = col.strip()
        m = {
            "Case Name": "Case Name",
            "Filings": "Filings",
            "Date Case Filed": "Date Case Filed",
            "Case Status": "Case Status",
            "Last Update": "Last Update",
            "Case Summary": "Case Summary",
            "Case Updates": "Case Updates",
        }
        # Basic loose matching
        lc = c.lower()
        if "case name" in lc: return "Case Name"
        if lc.startswith("filings"): return "Filings"
        if "date case filed" in lc or "date filed" in lc: return "Date Case Filed"
        if "case status" in lc or "status" == lc: return "Case Status"
        if "last update" in lc or "last updated" in lc or lc == "updated": return "Last Update"
        if "case summary" in lc: return "Case Summary"
        if "case updates" in lc: return "Case Updates"
        return c

    best_df.columns = [_canon(str(c)) for c in best_df.columns]
    # Ensure every expected column is present
    for need in ["Case Name","Filings","Date Case Filed","Case Status","Last Update","Case Summary","Case Updates"]:
        if need not in best_df.columns:
            best_df[need] = ""

    # Now mine links from the corresponding table node in the HTML.
    # Prefer the concrete tracker table, else fallback to first table whose header set matches.
    link_scope = soup.select_one("table#tablepress-42") or soup.find("table")
    if not link_scope:
        logger.warning("pandas: could not find any <table> node to mine links from; URLs may be empty.")
        urls: List[str] = []
    else:
        # Collect anchor hrefs in FIRST column by row order. We allow either explicit column-1 TD
        # or simply take the first <td> in each <tr>.
        urls = []
        trs = link_scope.find("tbody").find_all("tr") if link_scope.find("tbody") else link_scope.find_all("tr")
        logger.debug("pandas: link-mining scope has %d <tr>.", len(trs))
        for i, tr in enumerate(trs, 1):
            # First td present?
            first_td = tr.find("td") or tr.find("th")
            href = ""
            if first_td:
                a = first_td.find("a", href=True)
                if a and a.get("href"):
                    href = a["href"].strip()
            urls.append(href)

        # Align row counts (pad/truncate) to DataFrame rows
        if len(urls) != len(best_df):
            logger.debug("pandas: URL count %d != df rows %d; realigning.", len(urls), len(best_df))
            if len(urls) < len(best_df):
                urls.extend([""] * (len(best_df) - len(urls)))
            else:
                urls = urls[:len(best_df)]

        for j in range(min(5, len(best_df))):
            logger.debug("pandas keep row %d: name=%r | url=%s | last=%s",
                         j+1,
                         str(best_df.iloc[j].get("Case Name",""))[:80],
                         urls[j],
                         best_df.iloc[j].get("Last Update",""))

    # Final sanity logs
    if len(best_df) < 350:
        logger.warning("pandas: parsed fewer than expected rows (got=%d; expected ≈≥ 400).", len(best_df))

    # Keep just the canonical ordered columns
    best_df = best_df[["Case Name","Filings","Date Case Filed","Case Status","Last Update","Case Summary","Case Updates"]]
    return best_df, urls

def _parse_tracker_rows(html: str, logger) -> list[dict]:
    """
    Parse the Just Security tracker table (#tablepress-42) directly.
    Returns a list of dicts: {row, title, url, raw_date}.
    Extremely chatty logging for diagnostics.
    """
    from bs4 import BeautifulSoup
    import re

    soup = BeautifulSoup(html, "html.parser")

    # Find the exact table by ID
    table = soup.select_one("table#tablepress-42")
    if not table:
        ids = [t.get("id", "") for t in soup.find_all("table")]
        logger.warning("table#tablepress-42 NOT found. First few table ids: %s", ", ".join(ids[:8]))
        return []

    tbody = table.find("tbody")
    if not tbody:
        logger.warning("table#tablepress-42 has no <tbody>; falling back to table node.")
        tbody = table

    # Prefer explicit row-N classes; fall back to all rows
    trs = tbody.find_all("tr", class_=re.compile(r"\brow-\d+\b"))
    if not trs:
        trs = tbody.find_all("tr")
        logger.debug("No 'row-N' classes found; using all %d <tr> under tbody.", len(trs))
    else:
        logger.debug("Found %d <tr class='row-N'> under tbody.", len(trs))

    rows: list[dict] = []
    kept = 0
    for i, tr in enumerate(trs, 1):
        td1 = tr.find("td", class_=re.compile(r"\bcolumn-1\b"))
        td7 = tr.find("td", class_=re.compile(r"\bcolumn-7\b"))

        if not td1 or not td7:
            logger.debug("skip row %d: missing column-1 or column-7 (classes=%s)", i, tr.get("class"))
            continue

        a = td1.find("a", href=True)
        if not a:
            logger.debug("skip row %d: column-1 has no <a href>", i)
            continue

        url = (a["href"] or "").strip()
        title_text = " ".join(a.get_text(" ", strip=True).split())
        raw_date = " ".join(td7.get_text(" ", strip=True).split())

        rows.append({
            "row": i,
            "title": title_text,
            "url": url,
            "raw_date": raw_date,
        })
        kept += 1
        if i <= 8 or i % 50 == 0:
            logger.debug("row %d KEEP: title=%r | url=%s | raw_date=%s",
                         i, title_text[:100], url, raw_date)

    logger.info("Parsed tracker rows under #tablepress-42: total_tr=%d, kept_with_link_and_last_update=%d",
                len(trs), kept)
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
    """
    if not records:
        return [], []

    cols = list(records[0].keys())
    date_col = _find_date_col(cols)
    logger.info("Chosen date column: %r", date_col)

    matches: List[Dict[str, Any]] = []
    audit:   List[Dict[str, Any]] = []

    # Initialize skip counters
    skipped_meta = 0
    skipped_no_url = 0

    newest: Optional[date] = None
    for idx, r in enumerate(records, 1):
        dd = _parse_date(r.get(date_col, ""))
        audit.append({
            "row": idx,
            "title": r.get("Lawsuit") or r.get("Case Name") or r.get("Title") or r.get("Case") or "",
            "url": r.get("URL", ""),
            "raw_date": r.get(date_col, ""),
            "parsed_date": dd.isoformat() if dd else "",
        })

        url_val = (r.get("URL", "") or "").strip()
        raw_date_val = (r.get(date_col, "") or "").strip()

        # Require a URL and a non-empty 'Last Update' cell
        if not url_val:
            skipped_no_url += 1
            continue
        if not raw_date_val:
            skipped_meta += 1
            continue

        # Require at least one alpha in a descriptive cell to avoid header/meta rows
        desc_blob = " ".join([
            str(r.get("Case Updates", "")),
            str(r.get("Case Summary", "")),
            str(r.get("Case Status", "")),
            str(r.get("Filings", "")),
        ])
        if not re.search(r"[A-Za-z]", desc_blob):
            skipped_meta += 1
            continue

        if not dd:
            continue
        if newest is None or dd > newest:
            newest = dd
        if start_d <= dd <= end_d:
            r2 = dict(r)
            r2["_date"] = dd.isoformat()
            r2["_raw_date"] = raw_date_val
            matches.append(r2)

    logger.debug("Skip counts: no_url=%d meta_or_empty=%d", skipped_no_url, skipped_meta)

    if newest and newest < start_d:
        logger.warning("Data may be stale vs your window: newest %s < start %s",
                       newest.isoformat(), start_d.isoformat())

    logger.info("Filter kept %d of %d rows in window.", len(matches), len(records))
    return matches, audit


# ---------------------------------------------------------------------------
# Transform to V4 entity schema
# ---------------------------------------------------------------------------


def _to_entity_v4(rec: Dict[str, Any]) -> Dict[str, Any]:
    # Title preference order (mirrors V3)
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
    tracker_last_update_raw = (rec.get("_raw_date") or rec.get("Last Update") or "").strip()

    # Batch-2 enrichments
    action_summary = _extract_action_summary(
        rec.get("Case Updates", ""),
        rec.get("Case Status", ""),
        rec.get("Case Summary", ""),
    )
    event_date_iso, event_date_raw = _find_event_date_hint_in_text(
        rec.get("Case Updates", ""),
        rec.get("Case Status", ""),
        rec.get("Case Summary", ""),
        title_norm or title_raw,
    )

    # Batch-3: court + docket extraction
    court_name, jurisdiction, docket = _extract_court_and_docket(
        title_norm or title_raw,
        url,
        rec.get("Filings", ""),
        rec.get("Case Status", ""),
        rec.get("Case Summary", ""),
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
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date,  # tracker last-update (ISO)
        "tracker_last_update_raw": tracker_last_update_raw,
        # Batch-2 fields:
        "action_summary": action_summary,
        "event_date_hint": (event_date_iso or ""),
        "event_date_hint_raw": (event_date_raw or ""),
        # Batch-3 fields:
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
    COPY-mode from V3:
      - Fetch tracker page once
      - Parse main table → records (with best-effort link alignment)
      - Filter rows to date window using the chosen date column
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

    # --- unwrap Proofpoint (urldefense.proofpoint.com/v2/url?u=...) ---
    if "urldefense.proofpoint.com" in u:
        parsed = _urlparse.urlparse(u)
        q = _urlparse.parse_qs(parsed.query)
        if "u" in q and q["u"]:
            candidate = q["u"][0]
            if candidate.startswith("http"):
                u = _urlparse.unquote(candidate)

    # --- strip UTM & tracking params ---
    parsed = _urlparse.urlparse(u)
    q = _urlparse.parse_qsl(parsed.query)
    q_clean = [(k, v) for (k, v) in q if not k.lower().startswith("utm_")]
    new_query = _urlparse.urlencode(q_clean, doseq=True)
    u = _urlparse.urlunparse(parsed._replace(query=new_query))

    # --- drop trailing punctuation or slashes ---
    u = u.rstrip("?/&#")

    return u

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,  # unused; kept for parity with other V4 harvesters
) -> Dict[str, Any]:
    """
    Just Security (Litigation Tracker) — V4 copy-mode harvester.
      RAW:      snapshot of ALL parsed rows + audit
      FILTERED: rows in date window normalized to V4 entity schema
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

    # Transform matches -> V4 entities
    entities = [_to_entity_v4(r) for r in matches]

    # RAW snapshot (all rows) + audit — policy-controlled
    logger.info("DC_WRITE_RAW policy resolved to: %s", _raw_policy())
    if _should_write_raw(start, end, len(all_rows)):
        raw_payload = {
            "source": HARVESTER_ID,
            "entity_type": "litigation",
            "window": {"start": start, "end": end},
            "generated_at": datetime.utcnow().isoformat() + "Z",
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

    # FILTERED entities — primary dedupe by (court_name, docket), fallback to canonical_url
    seen_pair = set()
    seen_url = set()
    deduped: List[Dict[str, Any]] = []
    dupes_pair = 0
    dupes_url = 0

    for e in entities:
        # normalize URL
        raw_url = e.get("canonical_url") or e.get("url") or ""
        norm_url = _normalize_url(raw_url)
        e["canonical_url"] = norm_url

        # build pair key if possible
        c = (e.get("court_name") or "").strip().lower()
        d = (e.get("docket") or "").strip().lower()
        if c and d:
            key = (c, d)
            if key in seen_pair:
                dupes_pair += 1
                logger.debug("Dedupe(pair): SKIP %s %s | url=%s", c, d, norm_url)
                continue
            seen_pair.add(key)
        else:
            # fallback to URL-based
            if not norm_url or norm_url in seen_url:
                dupes_url += 1
                logger.debug("Dedupe(url): SKIP canonical=%r", norm_url)
                continue
            seen_url.add(norm_url)

        deduped.append(e)

    dupes_total = dupes_pair + dupes_url
    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | dupes_pair=%d dupes_url=%d dupes_total=%d",
        start, end, len(all_rows), len(entities), len(deduped), dupes_pair, dupes_url, dupes_total
    )

    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "litigation",
        "window": {"start": start, "end": end},
        "generated_at": datetime.utcnow().isoformat() + "Z",
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


# ---------------------------
# Optional direct CLI (parity with others)
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — Just Security harvester")
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