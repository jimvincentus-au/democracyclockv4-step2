#!/usr/bin/env python3
from __future__ import annotations

import re
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- V4 infra (consistent with your other harvesters) ----
from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    create_artifact_paths,
    write_json,
)

def _dump_archive_payload(payload: Any, artifacts_root: Path, start_iso: str, end_iso: str, page_number: int, logger) -> None:
    # Archive page dumps are no longer needed; keeping signature for compatibility.
    logger.debug(
        "HCR archive page %d fetched for window %s→%s (dump disabled).",
        page_number,
        start_iso,
        end_iso,
    )
    return

HARVESTER_ID = "hcr"

# IMPORTANT: HCR is Substack-hosted (no "www")
API_URL = "https://heathercoxrichardson.substack.com/api/v1/archive"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# Helpers (shared pattern with Meidas V4; tuned for HCR)
# ---------------------------------------------------------------------------

def _is_podcast_post(p: Dict[str, Any]) -> bool:
    """True if this archive item is a podcast."""
    t = (p.get("type") or "").lower()
    return ("podcast" in t) or bool(p.get("audio") or p.get("podcastUpload"))

def _iso_date_from_any(obj: Dict[str, Any]) -> Optional[date]:
    """Prefer post_date (Substack-style ISO), fallback to other common keys."""
    candidates = [obj.get("post_date"), obj.get("published_at"), obj.get("created_at"), obj.get("date")]
    for s in candidates:
        if not s:
            continue
        try:
            return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
        except Exception:
            continue
    return None

def _title_of(p: Dict[str, Any]) -> str:
    return (p.get("title") or p.get("social_title") or "").strip()

def _url_of(p: Dict[str, Any]) -> str:
    return (p.get("canonical_url") or p.get("url") or "").strip()

def _posts_from_json(payload: Any) -> List[Dict[str, Any]]:
    # Substack archive often returns a raw list; some stacks wrap under items[]
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return payload["items"]  # type: ignore[return-value]
    return []

def _make_retry_session(timeout: int) -> requests.Session:
    s = requests.Session()
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
    s.headers.update(BROWSER_HEADERS)

    orig_request = s.request
    def _with_timeout(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return orig_request(method, url, **kwargs)
    s.request = _with_timeout  # type: ignore[assignment]
    return s

def _print_titles_and_dates(posts: Iterable[Dict[str, Any]], logger) -> None:
    for p in posts or []:
        title = _title_of(p)
        dstr = (p.get("post_date") or p.get("published_at") or p.get("created_at") or p.get("date") or "").strip()
        dshow = dstr[:19] if dstr else ""
        logger.debug("%s | %s", dshow, title)

# ---------------------------------------------------------------------------
# HCR-specific: choose LLM-target content URL
# ---------------------------------------------------------------------------

def _hcr_transcript_from(p: Dict[str, Any]) -> tuple[str, str]:
    """
    For podcasts, prefer signed HTTPS transcript JSON (aligned), then unaligned JSON, then captions .vtt.
    Returns (content_url, content_kind).
    """
    up = (p.get("podcastUpload") or {})  # dict or {}
    trans = up.get("transcription") or {}

    cdn = trans.get("cdn_url") or ""
    if isinstance(cdn, str) and cdn.startswith("https://"):
        return cdn, "transcript_json"

    cdn_un = trans.get("cdn_unaligned_url") or ""
    if isinstance(cdn_un, str) and cdn_un.startswith("https://"):
        return cdn_un, "transcript_json_unaligned"

    signed_caps = trans.get("signed_captions") or []
    if isinstance(signed_caps, list):
        for cap in signed_caps:
            url = (cap or {}).get("url") or ""
            if isinstance(url, str) and url.startswith("https://"):
                return url, "captions_vtt"

    return "", ""  # transcript not found in this archive item

def _hcr_content_url_for(p: Dict[str, Any]) -> tuple[str, str]:
    """
    Decide which URL the LLM should fetch:
      - podcast → transcript (preferred) / captions / fallback to the post page
      - text    → post page (HTML)
    Returns (content_url, content_kind).
    """
    typ = (p.get("type") or "").lower().strip()
    page = _url_of(p)

    if typ == "podcast":
        t_url, kind = _hcr_transcript_from(p)
        if t_url:
            return t_url, (kind or "transcript_json")
        return page, "post_html"

    return page, "post_html"

# ---------------------------------------------------------------------------
# COPY-mode discovery (newest→older), window gating by post_date only
# ---------------------------------------------------------------------------

def _discover_copy_mode(
    start_iso: str,
    end_iso: str,
    *,
    pages: int,          # max pages safety ceiling
    per: int,
    timeout: int,
    artifacts_root: Path,   # <-- NEW
    logger
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    try:
        start_d = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    except ValueError:
        logger.error("Bad date range: %s → %s", start_iso, end_iso)
        return [], [], []

    s = _make_retry_session(timeout)

    all_seen:  List[Dict[str, Any]] = []
    matches:   List[Dict[str, Any]] = []
    audit:     List[Dict[str, Any]] = []

    seen_ids:  set[str] = set()   # de-dup for snapshot
    seen_keys: set[str] = set()   # de-dup for matches

    max_pages = max(1, int(pages))
    per = min(50, max(1, int(per)))  # Substack cap ~50

    logger.info(
        "Starting HCR archive fetch: window %s → %s (max_pages=%d, per_page=%d)",
        start_d, end_d, max_pages, per
    )

    page_idx = 0
    while page_idx < max_pages:
        offset = page_idx * per
        params = {"sort": "new", "offset": offset, "limit": per}
        logger.info("REQUEST page=%d: GET %s params=%s", page_idx + 1, API_URL, params)

        try:
            r = s.get(API_URL, params=params)
            logger.info("FETCHED page=%d → %s", page_idx + 1, r.url)
            logger.debug("HTTP status=%s bytes=%s", r.status_code, len(r.content))
        except requests.RequestException as e:
            logger.warning("Request error on page %d: %s", page_idx + 1, e)
            break

        if r.status_code != 200:
            logger.warning("Non-200 from archive on page %d (status=%s). Stopping.", page_idx + 1, r.status_code)
            break

        try:
            payload = r.json()
        except Exception as e:
            logger.warning("JSON parse error on page %d: %s", page_idx + 1, e)
            break

        # Pretty-print the exact JSON returned by the archive endpoint for this page
        _dump_archive_payload(payload, Path(artifacts_root), start_iso, end_iso, page_idx + 1, logger)

        posts = _posts_from_json(payload)
        logger.info("Page %d returned %d posts", page_idx + 1, (len(posts) if isinstance(posts, list) else 0))

        if not isinstance(posts, list):
            logger.warning("Unexpected JSON shape on page %d; stopping.", page_idx + 1)
            break

        if not posts:
            logger.info("Empty page at %d; stopping.", page_idx + 1)
            break

        # Normalize + de-dup within this page
        page_posts: List[Dict[str, Any]] = []
        for idx, p in enumerate(posts):
            pid = (
                str(p.get("canonical_url") or p.get("url") or "").strip()
                or f"{_iso_date_from_any(p) or ''}|{_title_of(p)}"
                or f"{page_idx}:{idx}"
            )
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            page_posts.append(p)

        # Page-level window stats & early-stop detection
        in_range_found = False
        older_seen = False
        kept_on_page = 0

        # First pass: gather dates for page summary
        dates_on_page: List[date] = []
        for p in page_posts:
            d_post  = _iso_date_from_any(p)
            if d_post:
                dates_on_page.append(d_post)
        earliest = min(dates_on_page).isoformat() if dates_on_page else ""
        latest   = max(dates_on_page).isoformat() if dates_on_page else ""

        # Add to global snapshot
        all_seen.extend(page_posts)

        logger.info(
            "Page %d: total=%d new_unique=%d date_range=[%s .. %s]",
            page_idx + 1, len(posts), len(page_posts), earliest or "?", latest or "?"
        )

        # Per-item decisions (HCR filter: keep standard “Letters” + podcasts; use post_date only)
        for p in page_posts:
            title = _title_of(p)
            url   = _url_of(p)
            d_post = _iso_date_from_any(p)
            typ = (p.get("type") or "").lower().strip()

            audit_row: Dict[str, Any] = {
                "page": page_idx + 1,
                "title": title,
                "url": url,
                "date_post": d_post.isoformat() if d_post else "",
                "type": typ,
            }

            if not d_post:
                audit_row["decision"] = "skip:no_date"
                audit.append(audit_row)
                continue

            if d_post < start_d:
                older_seen = True

            if start_d <= d_post <= end_d:
                in_range_found = True

                # KEEP ONLY non-podcast items
                if _is_podcast_post(p):
                    audit_row["decision"] = "skip:podcast"
                    audit.append(audit_row)
                    continue

                # HCR rule (text posts): keep daily letters / newsletter / threads / articles
                if typ in ("post", "newsletter", "thread", "article"):
                    key = str(p.get("id") or url or title)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        q = dict(p)
                        q["_matched_date"] = d_post.isoformat()

                        # Decide content target (will be post_html for non-podcast)
                        c_url, c_kind = _hcr_content_url_for(p)
                        q["_content_url"] = c_url
                        q["_content_kind"] = c_kind

                        matches.append(q)
                        kept_on_page += 1
                        audit_row["decision"] = f"keep:{typ or 'post'}"
                        audit_row["content_url"] = c_url
                        audit_row["content_kind"] = c_kind
                        audit.append(audit_row)
                    else:
                        audit_row["decision"] = "skip:dup_key"
                        audit.append(audit_row)
                else:
                    audit_row["decision"] = f"skip:type:{typ}"
                    audit.append(audit_row)

        logger.info(
            "Page %d decisions: kept=%d, in_window=%s, saw_older=%s",
            page_idx + 1, kept_on_page, in_range_found, older_seen
        )

        # Early stop: once we've dropped below start and found zero in-window items on this page
        if older_seen and not in_range_found:
            logger.info("Early stop at page %d (older-than-start and no in-window hits).", page_idx + 1)
            break

        page_idx += 1  # next page

    logger.info(
        "Archive fetch complete. total_unique_seen=%d in_window_kept=%d",
        len(all_seen), len(matches)
    )

    _print_titles_and_dates(all_seen, logger)
    return matches, all_seen, audit

# ---------------------------------------------------------------------------
# Transform to V4 entity schema (now choosing transcript/article URL)
# ---------------------------------------------------------------------------

def _to_entity_v4(p: Dict[str, Any]) -> Dict[str, Any]:
    title = _title_of(p)
    page_url = _url_of(p)
    post_date = p.get("_matched_date") or ( _iso_date_from_any(p).isoformat() if _iso_date_from_any(p) else "" )

    # Decide LLM-target content URL & kind (if not already carried from discovery)
    content_url = (p.get("_content_url") or "").strip()
    content_kind = (p.get("_content_kind") or "").strip()
    if not content_url:
        content_url, content_kind = _hcr_content_url_for(p)

    # For humans, keep the post page as url/canonical_url; put LLM-target in summary_url
    return {
        "source": "Letters from an American",
        "doc_type": "news_article",
        "title": title,
        "url": page_url,
        "canonical_url": page_url,
        "summary_url": content_url,     # Transcript (podcast) or page HTML (text)
        "summary": "",
        "summary_origin": content_kind, # transcript_json / transcript_json_unaligned / captions_vtt / post_html
        "summary_timestamp": "",
        "post_date": post_date or "",
        "raw_line": f"[hcr] {title} ({post_date or ''})",
    }

# ---------------------------------------------------------------------------
# Public entry (V4 standard)
# ---------------------------------------------------------------------------

def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    COPY-mode harvester for HCR (Substack):
      RAW:      snapshot of ALL parsed archive items (pre-window) + audit + chosen content URL
      FILTERED: windowed list of posts with LLM-ready content URLs
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    # Same conservative defaults as Meidas V4
    PAGES_CAP = 2000
    PER_PAGE  = 50
    TIMEOUT_S = 25

    logger.info("Session ready. Harvesting %s → %s", start, end)
    logger.info("Discovering HCR (COPY mode): archive=%s", API_URL)

    matches, all_seen, audit_rows = _discover_copy_mode(
        start_iso=start, 
        end_iso=end, 
        pages=PAGES_CAP, 
        per=PER_PAGE, 
        timeout=TIMEOUT_S, 
        artifacts_root=artifacts,            # <-- NEW
        logger=logger
    )

    # Transform matches → V4 entities
    entities = [_to_entity_v4(p) for p in matches]

    # RAW write — canonical schema: source, entity_type, window, count, entities, window_stats
    raw_entities = []
    for it in all_seen:
        ent = {
            "source": "Letters from an American",
            "doc_type": "news_article",
            "title": _title_of(it),
            "url": _url_of(it),
            "canonical_url": _url_of(it),
            "summary_url": it.get("_content_url", ""),
            "summary": "",
            "summary_origin": it.get("_content_kind", ""),
            "summary_timestamp": "",
            "post_date": (it.get("_matched_date") or it.get("post_date") or it.get("published_at") or "")[:10],
            "raw_line": f"[hcr_raw] {_title_of(it)}",
        }
        raw_entities.append(ent)
    raw_win_stats = {
        "parsed_total": len(all_seen),
        "audit_count": len(audit_rows),
    }
    raw_payload = {
        "source": HARVESTER_ID,
        "entity_type": "news_article",
        "window": {"start": start, "end": end},
        "count": len(raw_entities),
        "entities": raw_entities,
        "window_stats": raw_win_stats,
        # Optionally, for debugging, include audit and archive_url as non-canonical extras
        "audit": audit_rows,
        "archive_url": API_URL,
    }
    logger.debug(f"Writing canonical schema with {len(raw_entities)} entities to RAW output")
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    # FILTERED write — de-dup by canonical_url
    seen = set()
    deduped: List[Dict[str, Any]] = []
    dupes = 0
    for e in entities:
        k = e.get("canonical_url") or e.get("url") or ""
        if not k or k in seen:
            dupes += 1
            continue
        seen.add(k)
        deduped.append(e)

    win_stats = {
        "inside": len(entities),
        "outside": 0,
        "nodate": 0,
        "no_title": 0,
        "no_url": 0,
        "dupes": dupes,
    }

    logger.info(
        "Window %s → %s | total=%d kept_after_filter=%d kept_after_dedup=%d | dupes=%d",
        start, end, len(all_seen), len(entities), len(deduped), dupes
    )

    filtered_payload = {
        "source": HARVESTER_ID,
        "entity_type": "news_article",
        "window": {"start": start, "end": end},
        "count": len(deduped),
        "entities": deduped,
        "window_stats": win_stats,
    }
    logger.debug(f"Writing canonical schema with {len(deduped)} entities to FILTERED output")
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
# Optional direct CLI (matches other V4 modules)
# ---------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Democracy Clock V4 — HCR (Letters from an American) harvester")
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