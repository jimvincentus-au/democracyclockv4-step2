# getecon_v4.py

# -*- coding: utf-8 -*-
# getecon_v4.py
from __future__ import annotations

import json
import os
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import email.utils as eut
import xml.etree.ElementTree as ET

import logging
from pathlib import Path
import requests

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

# Lightweight logger/session helpers to mirror other harvesters

def _setup_logger(name: str, level: str, log_path: Optional[str]):
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Avoid duplicate handlers if re-run in same process
    if not logger.handlers:
        fmt = logging.Formatter('%(asctime)s %(levelname)-7s %(message)s')
        if log_path:
            fh = logging.FileHandler(log_path, encoding='utf-8')
            fh.setFormatter(fmt)
            fh.setLevel(getattr(logging, level.upper(), logging.INFO))
            logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        sh.setLevel(getattr(logging, level.upper(), logging.INFO))
        logger.addHandler(sh)
        logger.propagate = False
    return logger


def _build_session() -> requests.Session:
    s = requests.Session()
    # Default headers are set per-request in _fetch_rss, but keep UA here as well
    s.headers.update({
        'User-Agent': UA,
    })
    return s

ISO_FMT = "%Y-%m-%d"

def _to_dt(s: str) -> Optional[datetime]:
    """Best-effort RSS datetime parsing (RFC 2822 pubDate or ISO 8601)."""
    if not s:
        return None
    # Try RFC 2822
    try:
        dt = eut.parsedate_to_datetime(s)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # Try ISO-8601
    try:
        # Normalize trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _iso_date(dt: datetime) -> str:
    return dt.date().isoformat()

def _within_window(dt: Optional[datetime], start_dt: datetime, end_dt: datetime) -> bool:
    if dt is None:
        return False
    return start_dt <= dt <= end_dt

def _ensure_dir(p: str) -> None:
    os.makedirs(os.path.dirname(p), exist_ok=True)

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

# --------------------------------------------------------------------------------------
# Core harvester
# --------------------------------------------------------------------------------------

DEFAULT_FEEDS: List[Tuple[str, str]] = [
    # (name, url)
    ("Apricitas Economics", "https://apricitas.substack.com/feed"),
    ("The Overshoot", "https://theovershoot.co/feed"),
    ("Noahpinion", "https://www.noahpinion.blog/feed"),
    ("Calculated Risk", "https://www.calculatedriskblog.com/feeds/posts/default"),
    ("FRED Blog", "https://fredblog.stlouisfed.org/feed/"),
]

# Economic keywords to keep signal high
ECON_KEYWORDS = re.compile(r"""
\# Patterns for low-signal/meta posts to skip unless they match econ keywords
LOW_SIGNAL_PATTERNS = (
    "schedule for week of",
    "calendar for week of",
    "weekly calendar",
    "newsletter",
    "links for",
    "what to expect this week",
    "preview:",
    "week ahead",
    "open thread",
)
\b(
# Core indicators
cpi|pce|inflation|deflation|disinflation|
jobs?|employment|unemployment|payrolls?|jolts|wages?|earnings|
gdp|gdi|gni|pmi|ism|ppi|core|headline|
retail(?:\s+sales)?|housing|starts|permits|industrial\s+production|capacity|
fed|federal\s+reserve|rates?|interest\s+rates?|
export[s]?|import[s]?|trade|trade\s+war|tariff[s]?|dut(?:y|ies)|customs\s+duties?|current\s+account|
labor|labour|u[- ]?6|participation|productivity|claims|nfib|beige\s+book|
# Layer 1: pocketbook
salaries|cost\s+of\s+living|energy|fuel|gas|oil|rent|rents|mortgages?|mortgage\s+rates?|
affordability|grocer(?:y|ies)|food|child\s*care|tuition|health\s*care|healthcare|insurance|bills|savings|pensions?|retirement|
# Layer 2: policy/markets
deficit|debt|budget|spending|austerity|stimulus|subsid(?:y|ies)|tax(?:es|ation)?|tax\s+cuts?|
supply\s*chains?|reshoring|deregulation|privatization|monopol(?:y|ies)|corporations?|profits?|stock\s+buybacks?|
markets?|bonds?|yields?|treasury|central\s+bank|credit|lending|bank(?:s|ing)|
# Layer 3: social/labor
inequal(?:ity|ities)?|wealth\s+gap|poverty|middle\s+class|working\s+class|
collective\s+bargaining|unions?|strikes?|cost\s+burden|social\s+safety\s+net|
unemployment\s+benefits|jobless\s+benefits|welfare|minimum\s+wage|affordable\s+housing
)\b
""", re.IGNORECASE | re.VERBOSE)

# Patterns for low-signal/meta posts to skip unless they also match ECON_KEYWORDS
LOW_SIGNAL_PATTERNS = (
    "schedule for week of",
    "calendar for week of",
    "weekly calendar",
    "newsletter",
    "links for",
    "what to expect this week",
    "preview:",
    "week ahead",
    "open thread",
)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

def _fetch_rss(session, url: str, logger) -> Tuple[int, str, Dict[str, str]]:
    """Fetch an RSS/Atom feed. Return (status, text, headers)."""
    headers = {
        "User-Agent": UA,
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    try:
        logger.debug(f"GET URL: {url}")
        resp = session.get(url, headers=headers, timeout=30)
        status = getattr(resp, "status_code", None) or getattr(resp, "status", 0)
        text = resp.text if hasattr(resp, "text") else resp.content.decode("utf-8", "replace")
        hdrs = dict(getattr(resp, "headers", {}))
        if status != 200:
            logger.debug(
                "Response ERROR: status=%s reason=%s len=%s\n=== RESPONSE HEADERS ===\n%s\n=== BEGIN BODY ===\n%s\n=== END BODY ===",
                status,
                getattr(resp, "reason", ""),
                len(text),
                "\n".join(f"{k}: {v}" for k, v in hdrs.items()),
                text,
            )
        else:
            logger.debug("Response: status=%s len=%s head=%r", status, len(text), text[:200])
        return status, text, hdrs
    except Exception as e:
        logger.debug("Exception fetching %s: %s\n%s", url, e, traceback.format_exc())
        return 0, "", {}

def _parse_rss(text: str) -> List[Dict]:
    """Parse RSS/Atom using ElementTree to avoid extra dependencies."""
    items: List[Dict] = []
    if not text:
        return items
    try:
        root = ET.fromstring(text)
    except Exception:
        return items

    # Handle RSS 2.0
    channel = root.find("channel")
    if channel is not None:
        for it in channel.findall("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub = it.findtext("pubDate") or it.findtext("{http://purl.org/dc/elements/1.1/}date") or ""
            items.append({"title": title, "link": link, "published_raw": pub})
        return items

    # Handle Atom
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    for it in root.findall("atom:entry", ns):
        title = (it.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = it.find("atom:link", ns)
        link = link_el.get("href").strip() if link_el is not None and link_el.get("href") else ""
        pub = it.findtext("atom:updated", default="", namespaces=ns) or it.findtext("atom:published", default="", namespaces=ns)
        items.append({"title": title, "link": link, "published_raw": pub})
    return items

def _tag_for_source(name: str) -> List[str]:
    host = urlparse(name).netloc or ""
    return [name, host] if host else [name]

def _as_entity(source_name: str, item: Dict) -> Dict:
    title = item.get("title", "").strip()
    url = item.get("link", "").strip()
    published_dt = _to_dt(item.get("published_raw", ""))
    return {
        "source": "econ",
        "origin": source_name,
        "title": title,
        "url": url,
        "published_at": published_dt.isoformat() if published_dt else None,
        "tags": [],
        "raw": item,
    }

def _filter_and_tag(entities: List[Dict], start_dt: datetime, end_dt: datetime) -> List[Dict]:
    kept: List[Dict] = []
    for e in entities:
        dt = _to_dt(e.get("raw", {}).get("published_raw", "")) or (
            datetime.fromisoformat(e.get("published_at")) if e.get("published_at") else None
        )
        # 1) keep anything in window
        if not _within_window(dt, start_dt, end_dt):
            continue

        title = e.get("title", "")
        url = e.get("url", "")
        lower = (title + " " + url).lower()

        # Drop low-signal or meta posts unless they also match key econ keywords
        title_lower = (title or "").lower()
        if any(p in title_lower for p in LOW_SIGNAL_PATTERNS):
            if not ECON_KEYWORDS.search(title_lower):
                logger = logging.getLogger("econ.filter")
                logger.debug("Dropped low-signal post: %s", title)
                continue

        # 2) start with empty tags, then add based on content
        tags: List[str] = []
        for kw, tag in [
            ("inflation", "inflation"),
            ("cpi", "inflation"),
            ("pce", "inflation"),
            ("deflation", "inflation"),
            ("disinflation", "inflation"),
            ("gdp", "gdp"),
            ("gdi", "gdp"),
            ("gni", "gdp"),
            ("jobs", "labor"),
            ("employment", "labor"),
            ("unemployment", "labor"),
            ("payroll", "labor"),
            ("jolts", "labor"),
            ("union", "labor"),
            ("strike", "labor"),
            ("housing", "housing"),
            ("mortgage", "housing"),
            ("rent", "housing"),
            ("starts", "construction"),
            ("permits", "construction"),
            ("industrial production", "industry"),
            ("trade war", "trade"),
            ("tariff", "trade"),
            ("trade", "trade"),
            ("imports", "trade"),
            ("exports", "trade"),
            ("fed", "fed"),
            ("federal reserve", "fed"),
            ("rates", "rates"),
            ("interest rate", "rates"),
            ("treasury", "rates"),
            ("bond", "rates"),
            ("yield", "rates"),
            ("budget", "fiscal"),
            ("deficit", "fiscal"),
            ("debt", "fiscal"),
            ("stimulus", "fiscal"),
            ("tax", "tax"),
            ("inequal", "inequality"),
            ("poverty", "inequality"),
            ("minimum wage", "inequality"),
        ]:
            if kw in lower and tag not in tags:
                tags.append(tag)

        # 3) optional priority: highlight core sources (CR, FRED)
        origin = (e.get("origin") or "").lower()
        priority = 1 if ("calculated risk" in origin or "fred" in origin) else 0

        e["tags"] = tags
        e["priority"] = priority
        kept.append(e)
    return kept

# --------------------------------------------------------------------------------------
# Public entry point expected by the orchestrator
# --------------------------------------------------------------------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session: Optional[requests.Session] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict:
    """
    Harvest economic items from stable, non-blocked feeds within [start, end].
    Returns meta compatible with getweekevents_v4 orchestrator.
    """
    # Use provided logger if available; otherwise create a local one
    logger = logger or _setup_logger('econ', level, log_path)
    if session is None:
        session = _build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    # end as end-of-day inclusive
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)

    logger.info("Discovering Economic items (RSS/Atom)")
    logger.debug("Econ window: start=%s end=%s", start, end)

    # Allow runtime override of feeds via ENV (comma separated URL list)
    extra = os.environ.get("DC_ECON_EXTRA_FEEDS", "").strip()
    feeds: List[Tuple[str, str]] = list(DEFAULT_FEEDS)
    if extra:
        for u in extra.split(","):
            u = u.strip()
            if not u:
                continue
            feeds.append((u, u))

    all_items: List[Dict] = []
    for name, url in feeds:
        status, text, _hdrs = _fetch_rss(session, url, logger)
        if status != 200:
            logger.debug("Non-200 from feed '%s' (%s)", name, status)
            continue
        parsed = _parse_rss(text)
        logger.debug("Parsed %s items from '%s'", len(parsed), name)
        for it in parsed:
            ent = _as_entity(name, it)
            all_items.append(ent)

    logger.debug("Total items from feeds: %s", len(all_items))
    kept = _filter_and_tag(all_items, start_dt, end_dt)
    logger.debug("Filtered items within window & keywords: %s", len(kept))

    # Write artifacts
    raw_out = os.path.join(artifacts_root, "json", f"econ_raw_{start}_{end}.json")
    filtered_out = os.path.join(artifacts_root, "json", f"econ_filtered_{start}_{end}.json")
    _ensure_dir(raw_out)
    _ensure_dir(filtered_out)

    now_iso = _now_utc().isoformat()

    # raw dump (keep as-is shape for debugging)
    raw_payload = {
        "generated_at": now_iso,
        "window": {"start": start, "end": end},
        "feeds": [dict(name=n, url=u) for n, u in feeds],
        "items": all_items,
    }
    with open(raw_out, "w", encoding="utf-8") as f:
        json.dump(raw_payload, f, ensure_ascii=False, indent=2)

    # filtered dump in canonical filtered schema
    filtered_payload = {
        "generated_at": now_iso,
        "window": {"start": start, "end": end},
        "source": "econ",
        "entity_type": "news_article",
        "count": len(kept),
        "entities": kept,
        "meta": {
            "feeds": [dict(name=n, url=u) for n, u in feeds]
        },
    }
    with open(filtered_out, "w", encoding="utf-8") as f:
        json.dump(filtered_payload, f, ensure_ascii=False, indent=2)

    logger.info("Wrote raw JSON: %s", raw_out)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_out, len(filtered_payload.get("entities", [])))

    meta = {
        "source": "econ",
        "entity_count": len(kept),
        "entities_path": filtered_out,
        "raw_path": raw_out,
        "log_path": log_path,
    }
    return meta