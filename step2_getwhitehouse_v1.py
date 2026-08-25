# step2_getwhitehouse_v1.py — White House statement-act harvester (Democracy Clock V4 contract)
"""
White House official-channel harvester — STATEMENT ACTS.

WHAT THIS IS FOR
    Under the author ruling of 2026-08-25 (which superseded and withdrew the
    2026-08-13 "TOXIC" ruling), whitehouse.gov is an OFFICIAL ACTOR RECORD. It
    directly documents the White House's OWN acts and statements — that a thing
    was said, issued, proclaimed or announced, on that date, in those words.

    It does NOT establish the truth of any claim, allegation, characterization or
    factual predicate contained in that material.

    Absolute constraints carried forward from the withdrawn ruling:
      1. NEVER corroboration — contributes ZERO to any desk count and can never
         help an event reach `multi_desk`.
      2. NEVER validates an underlying claim.
      3. An executive order, proclamation or presidential memorandum is cited
         from the FEDERAL REGISTER, not from here.

    Consequently the PRIMARY channel here is /briefings-statements/, which has no
    Federal Register equivalent and is exactly the gap: 16% of archive events are
    speech acts, currently attested secondhand by advocacy newsletters rather than
    by the speaker's own channel.

    /presidential-actions/ is SECONDARY and off by default. It is useful only for
    the ANNOUNCEMENT stage (SED-045 — signing precedes FR publication); the
    citation of record remains the Federal Register.

DOWNSTREAM DISCIPLINE (enforced by the builder and the trust grader, not here)
    The event recorded must be the SPEECH ACT — actor = the speaker, action = the
    saying. Never the content of the statement as a fact about the world.

WHY SCRAPING
    whitehouse.gov exposes no usable machine surface: wp-json REST returns 403 and
    /feed/ returns 404. The listing pages are server-rendered WordPress and carry
    title, URL, category and an ISO datetime — enough for a DETERMINISTIC builder
    with zero LLM calls (the statement IS the event). robots.txt is
    "User-agent: * / Disallow:" — i.e. no restriction — checked 2026-08-25.

OUTPUT (standard V4 contract)
    {artifacts}/json/whitehouse_raw_{start}_{end}.json
    {artifacts}/json/whitehouse_filtered_{start}_{end}.json
"""
from __future__ import annotations

import html as _html
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config_v4 import ARTIFACTS_ROOT
from step2_helper_v4 import (
    setup_logger,
    build_session,
    create_artifact_paths,
    http_get,
    write_json,
    normalize_ws,
    canonicalize_url,
    within_window,
    polite_sleep,
)

HARVESTER_ID = "whitehouse"

__all__ = ["run_harvester"]

BASE = "https://www.whitehouse.gov"

# Channel -> listing path, doc_type, and the SPEECH-ACT VERB the builder must use.
#
# The verb is the whole safety mechanism. White House release and fact-sheet titles
# are written as accomplished facts ("President Trump Delivers Largest Drop in Violent
# Crime in American History"), not as acts. Copying such a title into an event `action`
# would assert the administration's claim as archive fact. The builder therefore never
# emits a bare title: it emits <actor> <verb> "<title>", where the verb comes from this
# table and never from the title's own framing.
# INSTRUMENT TYPE COMES FROM THE SOURCE'S OWN TAXONOMY, never from the title.
# Measured 2026-08-25: White House presidential-action titles do not name their
# instrument — "Delivering Gold Standard Childhood Vaccine Recommendations" is an
# EXECUTIVE ORDER and "Temporary Suspension of Additional Duties…" is a PROCLAMATION,
# and neither says so. Title-pattern typing was tried and was wrong. The site files
# each action under a typed subcategory, so each is harvested as its own channel and
# the type is known by construction.
#
# fr_publication_expected drives the Federal Register join. It matters because
# "unmatched" only means something for instruments 44 U.S.C. § 1505 requires to be
# published (executive orders, proclamations). Memoranda and nominations are
# routinely absent, so their absence asserts nothing.
CHANNELS: List[Dict[str, Any]] = [
    {"key": "releases", "path": "/releases/",
     "doc_type": "official_release", "instrument": "release",
     "verb": "published a release titled", "fr_expected": False},

    {"key": "fact_sheets", "path": "/fact-sheets/",
     "doc_type": "official_fact_sheet", "instrument": "fact_sheet",
     "verb": "issued a fact sheet titled", "fr_expected": False},

    {"key": "executive_orders", "path": "/presidential-actions/executive-orders/",
     "doc_type": "presidential_action", "instrument": "executive_order",
     "verb": "announced an executive order titled", "fr_expected": True},

    {"key": "proclamations", "path": "/presidential-actions/proclamations/",
     "doc_type": "presidential_action", "instrument": "proclamation",
     "verb": "announced a proclamation titled", "fr_expected": True},

    {"key": "presidential_memoranda", "path": "/presidential-actions/presidential-memoranda/",
     "doc_type": "presidential_action", "instrument": "memorandum",
     "verb": "announced a presidential memorandum titled", "fr_expected": None},

    {"key": "nominations_appointments", "path": "/presidential-actions/nominations-appointments/",
     "doc_type": "presidential_action", "instrument": "nominations",
     "verb": "announced nominations sent to the Senate:", "fr_expected": False},

    {"key": "briefings_statements", "path": "/briefings-statements/",
     "doc_type": "official_statement", "instrument": "statement",
     "verb": "issued a statement titled", "fr_expected": False},
]

# Default set. `releases` and `fact_sheets` carry the substantive statement acts;
# `executive_orders`, `proclamations` and `presidential_memoranda` are harvested so they
# can be JOINED to the Federal Register — matched, the FR record is the citation of
# record and this supplies the signing/announcement stage (SED-045); unmatched, the
# item remains a speech act, and for an FR-expected instrument that absence is itself
# reportable after a grace period. `nominations_appointments` is bulk routine and
# `briefings_statements` is almost entirely ceremonial (National Park Week, anniversary
# messages), so both are off by default but available.
# Override with e.g. WH_CHANNELS=releases,executive_orders,briefings_statements
DEFAULT_CHANNELS = ("releases", "fact_sheets", "executive_orders",
                    "proclamations", "presidential_memoranda")

MAX_PAGES = 40          # runaway guard; a 1-week window normally needs 1–2 pages
PAGE_SIZE_HINT = 10     # observed items per listing page

# Machine-readable constraints, mirrored from source_tiering_rulings_v1_draft.json.
# Recorded on every entity so the constraint travels with the data rather than
# living only in prose (Ruling A: a prose-only constraint is a false promise).
ACTOR_RECORD_CONSTRAINTS = {
    "trust_basis": "official_actor_record",
    "never_corroborates": True,
    "proposition_scope": "own_acts_and_statements",
}

# One <li class="... wp-block-post ..."> per article on the listing page.
_POST_BLOCK = re.compile(r'<li[^>]+class="[^"]*wp-block-post[^"]*"[^>]*>(.*?)</li>', re.S | re.I)
_TITLE_LINK = re.compile(
    r'<h2[^>]+class="[^"]*wp-block-post-title[^"]*"[^>]*>\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
    re.S | re.I,
)
_DATETIME = re.compile(r'datetime="(\d{4}-\d{2}-\d{2})', re.I)
_CATEGORY = re.compile(r'class="[^"]*taxonomy-category[^"]*"[^>]*>\s*<a[^>]*>(.*?)</a>', re.S | re.I)
_URL_DATE = re.compile(r"/(\d{4})/(\d{2})/")


def _strip_tags(s: str) -> str:
    return normalize_ws(_html.unescape(re.sub(r"<[^>]+>", " ", s or "")))


def _date_from_url(url: str) -> str:
    """Fallback date: the listing URLs embed /YYYY/MM/ — day is unknown, so use the 1st."""
    m = _URL_DATE.search(url or "")
    return f"{m.group(1)}-{m.group(2)}-01" if m else ""


def _parse_listing(page_html: str, chan: Dict[str, str], logger) -> List[Dict[str, Any]]:
    """
    Parse one listing page into entity records (pre-window).

    The listing carries the OFFICIAL title in the post-title <h2>, so no per-article
    fetch is needed — and the official title is what the Federal Register join matches
    on (ruling 1.5 method 3: signing date + normalized title). URL slugs are lossy and
    must not be used for the join.
    """
    channel = chan["key"]
    out: List[Dict[str, Any]] = []
    blocks = _POST_BLOCK.findall(page_html or "")
    if not blocks:
        logger.debug("WH[%s]: no post blocks matched on this page", channel)
    for blk in blocks:
        m = _TITLE_LINK.search(blk)
        if not m:
            continue
        url = canonicalize_url(m.group(1).strip(), base=BASE + "/")
        title = _strip_tags(m.group(2))
        dm = _DATETIME.search(blk)
        post_date = dm.group(1) if dm else _date_from_url(url)
        cm = _CATEGORY.search(blk)
        category = _strip_tags(cm.group(1)) if cm else ""

        if not title or not url:
            continue

        entity = {
            "source": "White House",
            "doc_type": chan["doc_type"],
            "title": title,
            "url": url,
            "canonical_url": url,
            "summary_url": "",
            # Deliberately empty, per the deterministic Federal Register precedent.
            # A summary of a release titled "President Trump Delivers Largest Drop in
            # Violent Crime in American History" would be that CLAIM restated in our
            # voice. The title is retained verbatim as the thing that was published;
            # the builder wraps it in a speech act and never asserts it.
            "summary": "",
            "summary_origin": "",
            "summary_timestamp": "",
            "post_date": post_date,
            "raw_line": f"=== {post_date} — {title}",
            # White House extras
            "wh_channel": channel,
            "wh_category": category,
            # The verb the builder MUST use. The title is quoted, never paraphrased,
            # and never supplies the verb.
            "speech_act_verb": chan["verb"],
            # Typed by the source's own taxonomy (the subcategory it was filed under),
            # never inferred from the title — WH titles do not name their instrument.
            "wh_instrument_type": chan["instrument"],
            # True  -> 44 U.S.C. § 1505 expects Federal Register publication, so an
            #          unmatched instrument is a reportable absence (after a grace period).
            # False -> routinely absent from the FR; absence asserts nothing.
            # None  -> unknown; assert nothing either way.
            "fr_publication_expected": chan["fr_expected"],
            "discovery": "listing",
            **ACTOR_RECORD_CONSTRAINTS,
        }
        out.append(entity)
    return out


def _listing_url(path: str, page: int) -> str:
    return f"{BASE}{path}" if page <= 1 else f"{BASE}{path}page/{page}/"


# ── sitemap gap-fill ─────────────────────────────────────────────────────────
# The listing pages do not reach the whole archive: /releases/ stops at page 45
# (2025-05-08), so the first ~15 weeks of the administration are unreachable that
# way. The sitemaps carry everything — ~2,060 URLs, with the publication date in
# the path (/releases/2025/03/slug) — so any window the listings miss is filled
# from there instead.
#
# This is automatic and needs no flag: recent windows are satisfied entirely by
# the listings and cost no extra requests; older windows fall through to the
# sitemap. Article pages are fetched ONLY for gap URLs, because the sitemap gives
# no title and the URL slug is truncated — and a truncated title would degrade the
# Federal Register title match that presidential-actions depends on.
SITEMAP_INDEX = f"{BASE}/sitemap_index.xml"
_SITEMAP_POST = re.compile(r"<loc>([^<]*post-sitemap\d*\.xml)</loc>", re.I)
_SITEMAP_LOC = re.compile(r"<loc>([^<]+)</loc>", re.I)
_PATH_YM = re.compile(r"/(\d{4})/(\d{2})/")
_OG_TITLE = re.compile(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', re.I)
_H1 = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)


def _sitemap_urls(session, logger) -> List[str]:
    """Every post URL the site publishes, from the sitemap index."""
    status, text = http_get(session, SITEMAP_INDEX, logger)
    if status != 200 or not text:
        logger.warning("WH: sitemap index unavailable (status=%s); gap-fill disabled", status)
        return []
    out: List[str] = []
    for sm in _SITEMAP_POST.findall(text):
        st, xml = http_get(session, sm, logger)
        if st != 200 or not xml:
            continue
        out.extend(_SITEMAP_LOC.findall(xml))
        polite_sleep()
    logger.debug("WH: sitemap enumerated %d urls", len(out))
    return out


def _fetch_article(session, url: str, chan: Dict[str, Any], logger) -> Optional[Dict[str, Any]]:
    """Resolve one gap URL to a full entity by fetching its page for the real title."""
    status, text = http_get(session, url, logger)
    if status != 200 or not text:
        logger.debug("WH: gap fetch failed status=%s url=%s", status, url)
        return None
    m = _OG_TITLE.search(text) or _H1.search(text)
    title = _strip_tags(m.group(1)) if m else ""
    # strip the site suffix WordPress appends to og:title
    title = re.sub(r"\s*[–—-]\s*The White House\s*$", "", title).strip()
    dm = _DATETIME.search(text)
    post_date = dm.group(1) if dm else _date_from_url(url)
    if not title:
        return None
    return {
        "source": "White House",
        "doc_type": chan["doc_type"],
        "title": title,
        "url": url,
        "canonical_url": url,
        "summary_url": "",
        "summary": "",
        "summary_origin": "",
        "summary_timestamp": "",
        "post_date": post_date,
        "raw_line": f"=== {post_date} — {title}",
        "wh_channel": chan["key"],
        "wh_category": "",
        "speech_act_verb": chan["verb"],
        "wh_instrument_type": chan["instrument"],
        "fr_publication_expected": chan["fr_expected"],
        "discovery": "sitemap",
        **ACTOR_RECORD_CONSTRAINTS,
    }


def _gap_fill(session, wanted: set, have_urls: set, start_iso: str, end_iso: str,
              logger) -> List[Dict[str, Any]]:
    """
    Find in-window URLs the listing walk did not return, and resolve them.

    Matched by the date in the URL path at month granularity (the day is not in the
    path), so the exact date comes from the fetched page and the window filter is
    applied afterwards as usual.
    """
    by_path = {c["path"].strip("/"): c for c in CHANNELS if c["key"] in wanted}
    if not by_path:
        return []
    urls = _sitemap_urls(session, logger)
    if not urls:
        return []

    start_ym, end_ym = start_iso[:7], end_iso[:7]
    gaps: List[Tuple[str, Dict[str, Any]]] = []
    for u in urls:
        if u in have_urls:
            continue
        ym = _PATH_YM.search(u)
        if not ym or not (start_ym <= f"{ym.group(1)}-{ym.group(2)}" <= end_ym):
            continue
        rel = u.replace(BASE + "/", "")
        for cpath, chan in by_path.items():
            if rel.startswith(cpath + "/"):
                gaps.append((u, chan))
                break

    if not gaps:
        logger.debug("WH: no sitemap gaps in window")
        return []
    logger.info("WH: %d in-window URL(s) absent from listings; resolving from sitemap", len(gaps))

    out: List[Dict[str, Any]] = []
    for u, chan in gaps:
        ent = _fetch_article(session, u, chan, logger)
        if ent:
            out.append(ent)
        polite_sleep()
    logger.info("WH: gap-fill resolved %d/%d", len(out), len(gaps))
    return out


def _discover_channel(session, chan: Dict[str, str],
                      start_iso: str, end_iso: str, logger
                      ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Walk a listing channel newest-first, stopping once a whole page falls before
    the window. Listing order is reverse-chronological, so this bounds the crawl
    to roughly the window size rather than the full archive.
    """
    channel, path = chan["key"], chan["path"]
    snapshot: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []
    logger.info("WH: discovering channel=%s path=%s", channel, path)

    for page in range(1, MAX_PAGES + 1):
        url = _listing_url(path, page)
        status, text = http_get(session, url, logger)
        audit.append({"channel": channel, "page": page, "url": url, "status": status,
                      "bytes": len(text or "")})
        if status != 200 or not text:
            logger.debug("WH[%s]: stop at page=%s (status=%s)", channel, page, status)
            break

        items = _parse_listing(text, chan, logger)
        if not items:
            logger.debug("WH[%s]: no items parsed on page=%s; stopping", channel, page)
            break

        snapshot.extend(items)
        dates = [i["post_date"] for i in items if i.get("post_date")]
        newest, oldest = (max(dates), min(dates)) if dates else ("", "")
        logger.debug("WH[%s]: page=%s items=%d span=%s..%s",
                     channel, page, len(items), oldest, newest)

        # Whole page older than the window start -> nothing further can qualify.
        if oldest and oldest < start_iso:
            logger.debug("WH[%s]: page=%s fully before window start (%s < %s); stopping",
                         channel, page, oldest, start_iso)
            break
        polite_sleep()

    logger.info("WH[%s]: snapshot items=%d", channel, len(snapshot))
    return snapshot, audit


def _filter_window_and_dedupe(items: List[Dict[str, Any]], start_iso: str, end_iso: str,
                              logger) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Window filter + stable dedupe by canonical_url, with a per-skip audit."""
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_title": 0, "no_url": 0, "dupe": 0}
    kept: List[Dict[str, Any]] = []
    seen = set()

    for it in items:
        title = (it.get("title") or "").strip()
        url = (it.get("canonical_url") or it.get("url") or "").strip()
        iso = (it.get("post_date") or "").strip()

        reason = None
        if not title:
            stats["no_title"] += 1; reason = "no_title"
        elif not url:
            stats["no_url"] += 1; reason = "no_url"
        elif not iso:
            stats["nodate"] += 1; reason = "nodate"
        elif not within_window(iso, start_iso, end_iso):
            stats["outside"] += 1; reason = "outside"

        if reason:
            logger.debug("Window: %s SKIPT reason=%s | title=%r url=%r",
                         iso or "''", reason, title[:80], url)
            continue

        if url in seen:
            stats["dupe"] += 1
            logger.debug("Window: %s SKIPT reason=dupe | url=%r", iso, url)
            continue

        seen.add(url)
        stats["inside"] += 1
        kept.append(it)

    return kept, stats


def run_harvester(
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    session=None,
) -> Dict[str, Any]:
    """
    Harvest White House statement acts for [start, end].

    Channels are controlled by WH_CHANNELS (comma-separated); the default is
    briefings_statements only. Writes RAW (pre-window) and FILTERED
    (window + dedupe) artifacts under the standard V4 paths.
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    logger.info("Session ready. Harvesting %s → %s", start, end)

    wanted = {c.strip() for c in (os.getenv("WH_CHANNELS") or ",".join(DEFAULT_CHANNELS)).split(",") if c.strip()}
    logger.info("WH channels: %s", ", ".join(sorted(wanted)) or "(none)")

    full_snapshot: List[Dict[str, Any]] = []
    full_audit: List[Dict[str, Any]] = []

    for chan in CHANNELS:
        if chan["key"] not in wanted:
            logger.debug("WH: channel=%s not selected; skipping", chan["key"])
            continue
        snap, audit = _discover_channel(sess, chan, start, end, logger)
        full_snapshot.extend(snap)
        full_audit.extend(audit)

    # Listing pages do not reach the whole archive (/releases/ stops at 2025-05-08).
    # Anything in-window they missed is resolved from the sitemap. No flag: recent
    # windows find no gaps and pay nothing.
    have = {i.get("canonical_url") for i in full_snapshot}
    full_snapshot.extend(_gap_fill(sess, wanted, have, start, end, logger))

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    filtered_items, win_stats = _filter_window_and_dedupe(full_snapshot, start, end, logger)

    raw_payload = {
        "generated_at": now_utc,
        "schema": "dc.v4.raw",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "channels": sorted(wanted),
        "parsed_total": len(full_snapshot),
        "audit": full_audit,
        "items_snapshot": [
            {
                "url": it.get("url", ""),
                "title": it.get("title", ""),
                "post_date": it.get("post_date", ""),
                "doc_type": it.get("doc_type", ""),
                "raw_line": (it.get("raw_line", "") or "")[:500],
                "wh_channel": it.get("wh_channel", ""),
                "wh_category": it.get("wh_category", ""),
                "wh_instrument_type": it.get("wh_instrument_type", ""),
                "fr_publication_expected": it.get("fr_publication_expected"),
                "discovery": it.get("discovery", ""),
            }
            for it in full_snapshot
        ],
    }
    write_json(raw_path, raw_payload)
    logger.info("Wrote raw JSON: %s", raw_path)

    filtered_payload = {
        "generated_at": now_utc,
        "schema": "dc.v4.filtered",
        "source": HARVESTER_ID,
        "entity_type": "white_house_statement_act",
        "window": {"start": start, "end": end},
        "count": len(filtered_items),
        "entities": filtered_items,
        "window_stats": win_stats,
        # Travels with the artifact so no downstream step can lose it.
        "source_constraints": ACTOR_RECORD_CONSTRAINTS,
        "ruling": "AUTHOR RULING 2026-08-25 — supersedes the 2026-08-13 TOXIC ruling. "
                  "Official actor record: documents its own acts and statements only; "
                  "never corroborates; never validates an underlying claim; executive "
                  "orders, proclamations and memoranda are cited from the Federal Register.",
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(filtered_items))

    return {
        "source": HARVESTER_ID,
        "entity_count": len(filtered_items),
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock — White House statement-act harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="Artifacts root directory")
    p.add_argument("--level", default="INFO", help="Logging level")
    p.add_argument("--log", default=None, help="Optional log file path")
    args = p.parse_args()

    res = run_harvester(
        start=args.start,
        end=args.end,
        artifacts_root=args.artifacts,
        level=args.level,
        log_path=args.log,
    )
    print(res)
