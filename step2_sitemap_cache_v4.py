"""
step2_sitemap_cache_v4.py — persistent cache of sitemap chunk metadata

Lets sitemap-based harvesters skip chunks they've already scanned that
either haven't changed since last fetch OR whose known date range doesn't
intersect the requested window.

Assumption: sitemap chunks are append-only — once a chunk is "full" (e.g.,
1000 URLs in WordPress conventions), its date range does not change.
Only the currently-being-filled chunk grows. This makes the date-range
cache safe to rely on for skip decisions.

Cache file: {artifacts_root}/cache/{source}_sitemap_cache.json

Cache schema (v1):

  {
    "schema": "sitemap_cache.v1",
    "source": "freebeacon",
    "first_fetched_at": "2026-05-12T17:55:00+00:00",
    "last_updated_at": "2026-05-12T18:42:00+00:00",
    "chunks": {
      "https://freebeacon.com/post-sitemap.xml": {
        "index_lastmod": "2026-05-12T13:42:00Z",
        "url_count": 1001,
        "date_min": "2026-05-09",
        "date_max": "2026-05-12",
        "last_fetched_at": "2026-05-12T17:55:00+00:00"
      },
      ...
    }
  }

Usage from a harvester:

  from step2_sitemap_cache_v4 import (
      load_cache, save_cache, should_skip_chunk, update_chunk,
  )

  cache = load_cache(artifacts_root, source="freebeacon")
  for chunk_url, chunk_lastmod in selected_chunks_with_lastmod:
      skip, reason = should_skip_chunk(chunk_url, chunk_lastmod, cache, window=(start, end))
      if skip:
          logger.info("Cache skip %s (%s)", chunk_url, reason)
          continue
      # fetch chunk, extract URLs and url_dates
      ...
      update_chunk(cache, chunk_url, chunk_lastmod, url_dates, url_count=len(urls))
  save_cache(artifacts_root, "freebeacon", cache)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


SCHEMA_VERSION = "sitemap_cache.v1"


def cache_path(artifacts_root: Path | str, source: str) -> Path:
    """Return the canonical cache file path for a given source."""
    return Path(artifacts_root) / "cache" / f"{source}_sitemap_cache.json"


def _empty_cache(source: str) -> Dict[str, Any]:
    return {
        "schema": SCHEMA_VERSION,
        "source": source,
        "first_fetched_at": None,
        "last_updated_at": None,
        "chunks": {},
    }


def load_cache(artifacts_root: Path | str, source: str) -> Dict[str, Any]:
    """Load the cache file for this source. Returns an empty cache if not
    present or corrupt."""
    p = cache_path(artifacts_root, source)
    if not p.exists():
        return _empty_cache(source)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("schema") != SCHEMA_VERSION:
            return _empty_cache(source)
        # Defensive: ensure expected keys exist
        data.setdefault("source", source)
        data.setdefault("chunks", {})
        return data
    except Exception:
        # Corrupt cache — fail closed (start fresh)
        return _empty_cache(source)


def save_cache(artifacts_root: Path | str, source: str, cache: Dict[str, Any]) -> Path:
    """Persist the cache to disk. Updates `last_updated_at` and sets
    `first_fetched_at` on first save. Returns the path written."""
    p = cache_path(artifacts_root, source)
    p.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if not cache.get("first_fetched_at"):
        cache["first_fetched_at"] = now
    cache["last_updated_at"] = now
    cache.setdefault("schema", SCHEMA_VERSION)
    cache.setdefault("source", source)
    p.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def should_skip_chunk(
    chunk_url: str,
    current_index_lastmod: Optional[str],
    cache: Dict[str, Any],
    window: Optional[Tuple[str, str]] = None,
) -> Tuple[bool, str]:
    """Decide whether to skip fetching this sitemap chunk.

    Returns (skip, reason). Reason is a short tag suitable for logging.

    Skip rules (in order):
      1. Never cached -> fetch (skip=False, reason="uncached")
      2. Index lastmod changed since cache -> fetch (skip=False, reason="lastmod_changed")
      3. Window provided and cached date range is entirely before window_start
         -> skip (chunk is older than window)
      4. Window provided and cached date range is entirely after window_end
         -> skip (chunk is newer than window; unusual for append-only sitemaps)
      5. Cached and unchanged but no window provided -> fetch (skip=False)
      6. Cached, unchanged, and window overlaps cached range -> fetch (skip=False)
    """
    entry = cache.get("chunks", {}).get(chunk_url)
    if entry is None:
        return False, "uncached"

    cached_lastmod = entry.get("index_lastmod")
    if current_index_lastmod and cached_lastmod and current_index_lastmod != cached_lastmod:
        return False, "lastmod_changed"

    if window:
        ws, we = window
        dmin = entry.get("date_min")
        dmax = entry.get("date_max")
        if dmin and dmax:
            if dmax < ws:
                return True, f"chunk_dates_end_{dmax}_before_window_start_{ws}"
            if dmin > we:
                return True, f"chunk_dates_start_{dmin}_after_window_end_{we}"
            # else: overlaps -> must fetch
            return False, f"cached_range_{dmin}..{dmax}_overlaps_window"

    return False, "cached_no_window_info"


def update_chunk(
    cache: Dict[str, Any],
    chunk_url: str,
    index_lastmod: Optional[str],
    url_dates: List[str],
    url_count: int,
) -> None:
    """Update the cache entry for a chunk after fetching it.

    url_dates: list of YYYY-MM-DD strings extracted from the chunk's URLs
               (some may be empty strings; filtered here).
    url_count: total URL entries in the chunk (whether dated or not).
    """
    chunks = cache.setdefault("chunks", {})
    valid_dates = [d for d in url_dates if d]
    entry = {
        "index_lastmod": index_lastmod,
        "url_count": int(url_count),
        "date_min": min(valid_dates) if valid_dates else None,
        "date_max": max(valid_dates) if valid_dates else None,
        "last_fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    chunks[chunk_url] = entry


def cache_stats(cache: Dict[str, Any]) -> Dict[str, Any]:
    """Quick summary of cache state — for log messages."""
    chunks = cache.get("chunks", {})
    dated = [
        (c.get("date_min"), c.get("date_max"))
        for c in chunks.values()
        if c.get("date_min") and c.get("date_max")
    ]
    overall_min = min((d[0] for d in dated), default=None)
    overall_max = max((d[1] for d in dated), default=None)
    return {
        "chunk_count": len(chunks),
        "dated_chunk_count": len(dated),
        "overall_date_min": overall_min,
        "overall_date_max": overall_max,
        "first_fetched_at": cache.get("first_fetched_at"),
        "last_updated_at": cache.get("last_updated_at"),
    }
