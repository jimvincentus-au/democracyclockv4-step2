#!/usr/bin/env python3
# get50501_v4.py
"""
Democracy Clock V4 — 50501 Substack harvester

COPY MODE from getmeidas_v4.py:
- same logger pattern
- same artifact pattern
- same return meta
- only the fetch logic + source id differ
"""

from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

HARVESTER_ID = "50501"
API_URL = "https://50501.substack.com/api/v1/archive"
PAGE_SIZE = 25  # safe default for substack archive


# ---------------------------------------------------------------------------
# date helpers (same style as meidas, but simpler)
# ---------------------------------------------------------------------------

def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_substack_date(s: str) -> Optional[date]:
    """
    Substack emits e.g. "2025-10-29T12:33:01.123Z" → we only care about the date part.
    """
    if not s:
        return None
    try:
        dpart = s.split("T", 1)[0]
        return datetime.strptime(dpart, "%Y-%m-%d").date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# fetch one page from 50501
# ---------------------------------------------------------------------------

def _fetch_archive_page(offset: int, logger) -> List[Dict[str, Any]]:
    params = {
        "sort": "new",
        "offset": offset,
        "limit": PAGE_SIZE,
    }
    try:
        r = requests.get(API_URL, params=params, timeout=10)
    except requests.RequestException as e:
        logger.warning("50501: request failed for offset=%d: %s", offset, e)
        return []

    if r.status_code != 200:
        logger.warning("50501: non-200 (%s) for offset=%d", r.status_code, offset)
        return []

    try:
        data = r.json()
    except Exception:
        logger.warning("50501: JSON decode failed for offset=%d", offset)
        return []

    # substack archive returns a plain list
    if isinstance(data, list):
        logger.debug("50501: fetched %d items at offset=%d", len(data), offset)
        return data

    logger.debug("50501: unexpected shape at offset=%d: %s", offset, type(data).__name__)
    return []


# ---------------------------------------------------------------------------
# discovery (COPY mode) — newest → older, stop when below window
# ---------------------------------------------------------------------------

def _discover_50501(start_iso: str, end_iso: str, logger) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    start_d = _to_date(start_iso)
    end_d = _to_date(end_iso)

    all_seen: List[Dict[str, Any]] = []
    in_window: List[Dict[str, Any]] = []

    offset = 0
    keep_going = True

    logger.info("50501: starting archive fetch %s → %s", start_d, end_d)

    while keep_going:
        page = _fetch_archive_page(offset, logger)
        if not page:
            break

        for raw_item in page:
            post_date_raw = raw_item.get("post_date") or raw_item.get("publication_date") or ""
            post_d = _parse_substack_date(post_date_raw)

            # normalize to entity-like shape here; we’ll re-wrap later
            title = (raw_item.get("title") or raw_item.get("subject") or "").strip()
            url = (raw_item.get("canonical_url") or raw_item.get("url") or "").strip()

            norm = {
                "title": title,
                "url": url,
                "post_date_raw": post_date_raw,
                "post_date": post_d.isoformat() if post_d else "",
                "raw": raw_item,
            }
            all_seen.append(norm)

            if post_d is None:
                # no date → keep in raw only
                continue

            if start_d <= post_d <= end_d:
                in_window.append(norm)
            elif post_d < start_d:
                # we’ve fallen out of window; we can stop entire loop
                keep_going = False

        offset += PAGE_SIZE

    logger.info(
        "50501: archive complete. total_seen=%d in_window=%d",
        len(all_seen), len(in_window)
    )
    return all_seen, in_window


# ---------------------------------------------------------------------------
# transform to v4 entity schema (same as meidas, minimal fields)
# ---------------------------------------------------------------------------

def _to_entity_v4(item: Dict[str, Any]) -> Dict[str, Any]:
    title = item.get("title") or ""
    url = item.get("url") or ""
    post_date = item.get("post_date") or ""

    return {
        "source": "50501",
        "doc_type": "news_article",
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date,
        "raw_line": f"[50501] {title} ({post_date})",
    }


# ---------------------------------------------------------------------------
# public entry (V4 standard) — EXACTLY like getmeidas_v4.run_harvester
# ---------------------------------------------------------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,  # kept for parity with meidas
) -> Dict[str, Any]:
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering 50501 (COPY mode via API): %s", API_URL)

    all_seen, windowed = _discover_50501(start, end, logger)

    entities = [_to_entity_v4(it) for it in windowed]

    # RAW payload — same structure as meidas
    raw_payload = {
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "archive_url": API_URL,
        "parsed_total": len(all_seen),
        "items_snapshot": [
            {
                "url": it.get("url"),
                "title": it.get("title"),
                "post_date": it.get("post_date"),
                "doc_type": "news_article",
                "raw_line": it.get("raw_line") or f"[50501_raw] {it.get('title')}",
            }
            for it in all_seen
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED payload — same keys as meidas
    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "news_article",
        "window": {"start": start, "end": end},
        "count": len(entities),
        "entities": entities,
        "window_stats": {
            "inside": len(entities),
            "outside": 0,
            "nodate": 0,
            "no_title": 0,
            "no_url": 0,
            "dupes": 0,
        },
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(entities))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(entities),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


# ---------------------------------------------------------------------------
# CLI (same style)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock V4 — 50501 harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--level", default="INFO")
    p.add_argument("--artifacts-root", default=str(ARTIFACTS_ROOT))
    args = p.parse_args()

    log = setup_logger(f"dc.{HARVESTER_ID}", args.level)
    meta = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts_root,
        level=args.level,
        log_path=None,
        session=None,
    )
    log.info("Summary: %s", meta)