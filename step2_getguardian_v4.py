# getguardian_v4.py
from __future__ import annotations

import os
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import re


import requests

# Canonical V4 schema helpers
try:
    from step2_schema_v4 import new_filtered_pack, validate_schema
except Exception as e:
    raise RuntimeError("schema_v4.py with canonical helpers is required") from e

# V4 helpers
try:
    from config_v4 import ARTIFACTS_ROOT  # artifacts root directory (Path or str)
except Exception:
    ARTIFACTS_ROOT = Path("./artifacts")

try:
    from step2_helper_v4 import (
        setup_logger,
        build_session,
        create_artifact_paths,
        write_json,
        normalize_ws,
    )
except Exception as e:
    raise RuntimeError("helper_v4.py is required in V4") from e


HARVESTER_ID = "guardian"

# ---------------------------
# Raw/Log policy (env-driven)
# ---------------------------

def _raw_policy() -> str:
    """
    Returns one of: 'always', 'auto', 'never' (default 'auto')
    Controlled by env var DC_WRITE_RAW.
    """
    val = (os.getenv("DC_WRITE_RAW") or "auto").strip().lower()
    if val not in {"always", "auto", "never"}:
        return "auto"
    return val

def _should_write_raw(level: str) -> bool:
    pol = _raw_policy()
    if pol == "always":
        return True
    if pol == "never":
        return False
    # auto: write for DEBUG level, else skip
    return (level or "").upper() == "DEBUG"


# ---------------------------
# Guardian API plumbing
# ---------------------------

GUARDIAN_BASE = "https://content.guardianapis.com/search"

def _guardian_api_key() -> str:
    """
    The Guardian Content API key.
    Prefer environment: GUARDIAN_API_KEY
    Optionally allow config_v4.GUARDIAN_API_KEY if present.
    """
    k = (os.getenv("GUARDIAN_API_KEY") or "").strip()
    if k:
        return k
    try:
        from config_v4 import GUARDIAN_API_KEY as CFG_KEY  # type: ignore
        return (CFG_KEY or "").strip()
    except Exception:
        return ""

_EXCLUDE_SECTION_IDS = {
    # Opinion/columns/letters sections that we do not want by default
    "commentisfree",  # Guardian opinion hub
    "opinion",
    "letters",
}

def _params(page: int, page_size: int, start: str, end: str, api_key: str) -> Dict[str, Any]:
    """
    Build Content API params for a windowed search.
    We fetch broad news and filter out opinion client-side.
    """
    p: Dict[str, Any] = {
        "from-date": start,
        "to-date": end,
        "page": str(page),
        "page-size": str(page_size),
        "order-by": "newest",
        "section": "us-news",
        "type": "article",
        "api-key": api_key,
        # Bring useful fields for writer/Step-2
        "show-fields": "byline,trailText,bodyText",
    }
    return p


def _walk_results(
    session: requests.Session,
    start: str,
    end: str,
    logger,
    page_size: int = 200,
    max_pages: int = 50,
    api_key: str = "",
) -> Iterable[Dict[str, Any]]:
    """
    Iterate Guardian search results for the date window.
    """
    if not api_key:
        logger.warning("No GUARDIAN_API_KEY set; the API may reject requests.")
    page = 1
    while page <= max_pages:
        params = _params(page, page_size, start, end, api_key)
        resp = session.get(GUARDIAN_BASE, params=params, timeout=30)
        status = resp.status_code
        try:
            data = resp.json()
        except Exception:
            data = {}

        resp_obj = (data.get("response") or {})
        results = resp_obj.get("results") or []
        total = int(resp_obj.get("total", 0) or 0)
        current_page = int(resp_obj.get("currentPage", page) or page)
        pages = int(resp_obj.get("pages", 0) or 0)

        logger.debug(
            "GET %s status=%s page=%s/%s page_items=%s total=%s",
            resp.url, status, current_page, pages or "?", len(results), total
        )

        for r in results:
            yield r

        if pages and page >= pages:
            break
        page += 1


# ---------------------------
# Mapping & filtering
# ---------------------------

def _iso_date(s: str) -> str:
    """
    Normalize Guardian's webPublicationDate (ISO-like) to YYYY-MM-DD.
    """
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
        except Exception:
            return s[:10]

def _looks_governance_title(item: Dict[str, Any]) -> bool:
    title = (item.get("webTitle") or "").lower()
    keywords = [
        "executive order","order","proclamation","memorandum","policy","policies","rule","rules","regulation","regulations",
        "bill","bills","law","laws","congress","senate","house","committee","subpoena","oversight","hearing",
        "court","judge","ruling","appeals","supreme court","scotus","injunction","block","stay",
        "doj","department of justice","fbi","homeland security","dhs","ice","usda","epa","pentagon","dod","white house",
        "arrest","detain","indict","charge","plea","convict","pardon","commute","commutation","sue","lawsuit","settle","settlement",
        "governor","legislature","attorney general","ag","secretary","agency","agencies","order to","signs","sign","veto","appoint","nominate"
    ]
    # quick pass
    if any(k in title for k in keywords):
        return True
    # trump proximity rule
    tokens = re.findall(r"[a-z0-9']+", title)
    for i, tok in enumerate(tokens):
        if tok == "trump":
            start = max(0, i - 4)
            end = min(len(tokens), i + 5)
            window = " ".join(tokens[start:end])
            if any(k in window for k in keywords):
                return True
    return False

# MINIMAL CHANGE BELOW: only expand the existing opinion check

def _looks_opinion(item: Dict[str, Any]) -> bool:
    sec_id = (item.get("sectionId") or "").strip().lower()
    if sec_id in _EXCLUDE_SECTION_IDS:
        return True
    # NEW (minimal): also treat explicit Opinion/Letters sectionName as opinion
    sec_name = (item.get("sectionName") or "").strip().lower()
    if sec_name in {"opinion", "comment is free", "letters"}:
        return True
    # NEW (minimal): treat URL path signals as opinion
    url = (item.get("webUrl") or "").strip().lower()
    if any(seg in url for seg in ("/commentisfree/", "/opinion/", "/letters/")):
        return True
    # Existing fallback on title marker
    title = (item.get("webTitle") or "").lower()
    if title.startswith("opinion:") or "[opinion]" in title:
        return True
    return False

def _is_us_news(item: Dict[str, Any]) -> bool:
    """
    Return True iff the Guardian item is clearly U.S.-focused news.
    We prefer the official sectionId 'us-news' or sectionName 'US news'.
    Fall back to a tolerant check on the sectionName.
    """
    sec_id = (item.get("sectionId") or "").strip().lower()
    sec_name = (item.get("sectionName") or "").strip().lower()
    if sec_id == "us-news":
        return True
    if sec_name == "us news":
        return True
    # Strict only; do not allow substring fallbacks (avoids "australia news", etc.)
    return False

def _is_live(item: Dict[str, Any]) -> bool:
    """
    Drop live blogs/rolling coverage.
    Criteria:
      - id or webUrl contains '/live/'
      - title contains 'as it happened' or 'live updates'
    """
    ident = (item.get("id") or "").lower()
    url = (item.get("webUrl") or "").lower()
    title = (item.get("webTitle") or "").lower()
    if "/live/" in ident or "/live/" in url:
        return True
    if "as it happened" in title or "live updates" in title:
        return True
    if "politics live" in title:
        return True
    return False

_DIGEST_TITLE_NEEDLES = [
    "first thing",
    "afternoon update",
    "five things",
    "at a glance",
    "morning mail",
]

def _is_digest(item: Dict[str, Any]) -> bool:
    """
    Drop digest/roundup formats that aren't discrete events.
    """
    title = (item.get("webTitle") or "").strip().lower()
    return any(needle in title for needle in _DIGEST_TITLE_NEEDLES)

def _entity_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a Guardian item to our V4 entity shape.
    """
    title = normalize_ws(item.get("webTitle") or "")
    url = (item.get("webUrl") or "").strip()
    pub_iso = _iso_date(item.get("webPublicationDate") or "")
    sec_name = (item.get("sectionName") or "").strip()
    byline = normalize_ws(((item.get("fields") or {}).get("byline") or ""))
    trail = normalize_ws(((item.get("fields") or {}).get("trailText") or ""))

    raw_line = normalize_ws(f"{sec_name} — {byline}".strip(" —"))

    return {
        "source": "The Guardian",
        "doc_type": "news",
        "source_label": "The Guardian",
        "title": title,
        "url": url,
        "canonical_url": url,
        # Optional helpers for Step-2
        "section": sec_name,
        "byline": byline,
        "summary_url": "",
        "summary": trail,  # short standfirst if present; safe to keep
        "summary_origin": "guardian:trailText" if trail else "",
        "summary_timestamp": "",
        "post_date": pub_iso,
        "raw_line": raw_line,
    }


def _filter_window_keep(item: Dict[str, Any], start: str, end: str) -> bool:
    """
    Even though the API is windowed, keep a defensive check on date bounds,
    exclude opinion, and keep only U.S.-focused news items.
    """
    # Type gate: keep only canonical articles
    if (item.get("type") or "").strip().lower() != "article":
        return False
    # Live coverage gate
    if _is_live(item):
        return False
    # Digest/roundup gate
    if _is_digest(item):
        return False
    if _looks_opinion(item):
        return False
    if not _is_us_news(item):
        return False
    if "/us-news/" not in ((item.get("webUrl") or "").lower()):
        return False
    d = _iso_date(item.get("webPublicationDate") or "")
    if (d < start) or (d > end):
        return False
    if not _looks_governance_title(item):
        return False
    return True


# ---------------------------
# Public entry point
# ---------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Step-1 harvester for The Guardian (Content API).
    Writes RAW (policy-dependent) and FILTERED entities.
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)
    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    api_key = _guardian_api_key()
    if not api_key:
        logger.warning("GUARDIAN_API_KEY is not set; requests will likely fail.")

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering The Guardian (COPY mode): %s", GUARDIAN_BASE)

    # Fetch
    snapshot: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for item in _walk_results(sess, start, end, logger, api_key=api_key):
        snapshot.append({
            "id": item.get("id"),
            "type": item.get("type"),
            "sectionId": item.get("sectionId"),
            "sectionName": item.get("sectionName"),
            "webTitle": item.get("webTitle"),
            "webUrl": item.get("webUrl"),
            "webPublicationDate": item.get("webPublicationDate"),
            "fields": item.get("fields"),
        })

        if not _filter_window_keep(item, start, end):
            continue

        entity = _entity_from_item(item)
        url_key = entity.get("canonical_url") or entity.get("url")
        if not url_key:
            continue
        if url_key in seen_urls:
            continue
        seen_urls.add(url_key)
        kept.append(entity)

    # ---- RAW write (always write) ----
    raw_payload = {
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "api_scope": {
            "base": GUARDIAN_BASE,
            "page_size": 200,
        },
        "parsed_total": len(snapshot),
        "items_snapshot": snapshot,
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # ---- FILTERED write (canonical V4 pack) ----
    generated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    filtered_pack = new_filtered_pack(
        source=HARVESTER_ID,
        window={"start": start, "end": end},
        entities=kept,
        meta={
            "entity_type": "news_article",
            "window_stats": {
                "total_seen": len(snapshot),
                "kept_after_filter": len(kept),
                "deduped_by_url": len(kept),
            },
            "api_scope": {
                "base": GUARDIAN_BASE,
                "page_size": 200,
            },
        },
        generated_at=generated_at,
    )
    # Validate before writing to catch shape regressions early
    try:
        validate_schema("filtered", filtered_pack)
    except Exception as ve:
        logger.error("Filtered pack failed schema validation: %s", ve)
        # Proceed to write anyway for forensics
    write_json(filtered_path, filtered_pack)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(kept))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(kept),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


# ---------------------------
# Direct CLI (optional)
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — The Guardian harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO", help="log level")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="artifacts root")
    args = p.parse_args()

    logger = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        log_path=None,
        session=None,
    )
    logger.info("Summary: %s", meta)