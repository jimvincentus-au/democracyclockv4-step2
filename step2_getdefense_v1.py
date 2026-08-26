# step2_getdefense_v1.py — Defense Press Products harvester (Democracy Clock V4 contract)
"""
Defense Press Products harvester — STATEMENT ACTS from the central department press office.

SCOPE
    This is collector 1 of 3 in the Defense source family. It covers CENTRAL press
    products only: releases, advisories, transcripts, speeches, publications and
    contract announcements.

    Deliberately NOT in this collector, because they carry different evidentiary
    weight and must not be flattened together merely for sharing a .mil domain:
      * Defense Authoritative Actions — DoD/DoW issuances (directives, instructions,
        manuals), Federal Register rules, Inspector General reports. An operative
        directive is not a press release.
      * Defense Component Activity — DVIDS, service newsrooms, combatant commands.
        Component public affairs is the same government actor republished, never
        independent corroboration.

EVIDENTIARY TREATMENT (Event Trust Contract v0.3 §4.5, author ruling 2026-08-25)
    These are OFFICIAL ACTOR RECORDS. A release documents that the department issued
    the announcement, in those words, on that date. It does not establish the truth
    of any operational, numerical or outcome claim inside it.

        "The department announced it would deploy 500 personnel"   -> documented
        "500 personnel were deployed and achieved the objective"   -> NOT documented here

    Constraints, carried on every entity:
      * never_corroborates — contributes ZERO to any desk count.
      * proposition_scope = own_acts_and_statements.

THE DEPARTMENT OF WAR RENAME
    The department now publishes as the "Department of War" from war.gov; the feed is
    titled "Department of War News Feed" and refers to "Secretary of War Pete Hegseth".
    Both the harvested URL and the domain observed are recorded, because a rename is
    itself an archive-relevant fact and historical URLs must stay resolvable.

WHY RSS
    Measured 2026-08-25. The HTML listing pages return 403 to a plain client and 200
    only with a full browser header set; the CMS RSS endpoint is stable, structured and
    reaches 500 items — far enough back to cover the administration from 2025-01-20 in
    a single request per channel. RSS is therefore the discovery layer. HTML enrichment
    (body text, attachments, issuing office) is a separate optional pass.

    robots.txt (war.gov, checked 2026-08-25) disallows only DNN system paths
    (/Admin/, /Config/, /bin/, *.axd …). Neither /News/ nor RSS.ashx is disallowed.

OUTPUT (standard V4 contract)
    {artifacts}/json/defense_raw_{start}_{end}.json
    {artifacts}/json/defense_filtered_{start}_{end}.json
"""
from __future__ import annotations

import html as _html
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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

HARVESTER_ID = "defense"

__all__ = ["run_harvester"]

BASE = "https://www.war.gov"
RSS_PATH = "/DesktopModules/ArticleCS/RSS.ashx"
SITE_ID = 945                 # the central department site
FEED_MAX = 500                # server-side cap; 500 reaches 2024-12-19 on Releases

# The edge (Akamai) rejects plain clients with 403. These headers are required for
# every request, RSS included.
BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

# ContentType IDs discovered by probing the CMS on 2026-08-25; each maps to a
# section whose channel <link> confirms the identity.
#
# The VERB is the safety mechanism, exactly as in the White House harvester: press
# titles routinely assert outcomes ("Department of War Announces a $750 Million
# Investment…"), so the builder emits <actor> <verb> "<title>" and never a bare title.
CHANNELS: List[Dict[str, Any]] = [
    {"key": "releases",     "content_type": 9,
     "doc_type": "department_press_release", "instrument": "release",
     "verb": "issued a press release titled",       "default": True},

    {"key": "advisories",   "content_type": 2,
     "doc_type": "department_advisory", "instrument": "advisory",
     "verb": "issued a media advisory titled",      "default": True},

    {"key": "transcripts",  "content_type": 13,
     "doc_type": "department_transcript", "instrument": "transcript",
     "verb": "published a transcript titled",       "default": True},

    {"key": "speeches",     "content_type": 11,
     "doc_type": "department_speech", "instrument": "speech",
     "verb": "published remarks titled",            "default": True},

    {"key": "publications", "content_type": 5,
     "doc_type": "department_publication", "instrument": "publication",
     "verb": "published a departmental document titled", "default": False},

    # Off by default. A contract announcement states a CEILING or potential total
    # value, which is not money obligated. Treating the announced figure as spend
    # would be exactly the over-certification this source family guards against;
    # USAspending is the check. Harvested only when explicitly requested.
    {"key": "contracts",    "content_type": 400,
     "doc_type": "department_contract_announcement", "instrument": "contract_announcement",
     "verb": "announced contract awards in",        "default": False},

    # Departmental feature journalism — the department's own storytelling, not an
    # announcement. Off by default.
    {"key": "news_stories", "content_type": 1,
     "doc_type": "department_news_story", "instrument": "news_story",
     "verb": "published a news story titled",       "default": False},
]

DEFAULT_CHANNELS = tuple(c["key"] for c in CHANNELS if c["default"])

# Mirrored from source_tiering_rulings_v1_draft.json so the constraint travels with
# the data rather than living only in prose (Ruling A).
ACTOR_RECORD_CONSTRAINTS = {
    "trust_basis": "official_actor_record",
    "never_corroborates": True,
    "proposition_scope": "own_acts_and_statements",
}

_ITEM = re.compile(r"<item>(.*?)</item>", re.S | re.I)

# ── sub-typing within a channel ───────────────────────────────────────────────
# The Releases feed mixes instruments that are not alike. Two need separating:
#
#   CASUALTY IDENTIFICATION — "DOW Identifies Army Casualty". The department
#   confirming the death of its own service member is an authoritative record about
#   its own personnel, not a claim about the world. It is kept and typed so it is
#   never flattened into ordinary press output. Note the title alone is uninformative
#   ("DOW Identifies Army Casualties"); the name, rank and location live in the
#   standfirst, which is retained verbatim.
#
#   OFFICER NOMINATION — "Secretary of War General Officer Announcement for Aug. 3,
#   2026". Routine personnel routing to the Senate. Ruled out of scope by the author
#   2026-08-25: not useful downstream. Dropped from the FILTERED artifact but retained
#   in RAW, so the discovery record persists even though its standing does not
#   (ruling 1.6 — evidentiary standing may fall; the discovery record need not vanish).
_SUBTYPES: List[Tuple[str, str, str, bool]] = [
    # (regex on title, subtype, verb, keep_in_filtered)
    (r"\bidentifies?\b.*\bcasualt", "casualty_identification",
     "announced a casualty identification:", True),
    (r"\b(flag|general)\s+officer\s+announcement", "officer_nomination",
     "announced an officer nomination:", False),
]


def _subtype(title: str, chan: Dict[str, Any]) -> Tuple[str, str, bool]:
    """Return (subtype, verb, keep_in_filtered) for one item."""
    t = (title or "").lower()
    for pat, sub, verb, keep in _SUBTYPES:
        if re.search(pat, t):
            return sub, verb, keep
    return chan["instrument"], chan["verb"], True


def _tag(block: str, name: str) -> str:
    m = re.search(rf"<{name}[^>]*>(.*?)</{name}>", block or "", re.S | re.I)
    if not m:
        return ""
    v = re.sub(r"<!\[CDATA\[|\]\]>", "", m.group(1))
    return normalize_ws(_html.unescape(re.sub(r"<[^>]+>", " ", v)))


def _iso_from_rfc822(s: str) -> str:
    """RSS pubDate is RFC-822; the pipeline works in ISO dates."""
    s = (s or "").strip()
    if not s:
        return ""
    try:
        return parsedate_to_datetime(s).date().isoformat()
    except Exception:
        m = re.search(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", s)
        if m:
            try:
                return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}",
                                         "%d %b %Y").date().isoformat()
            except Exception:
                return ""
        return ""


def _feed_url(content_type: int) -> str:
    return f"{BASE}{RSS_PATH}?ContentType={content_type}&Site={SITE_ID}&max={FEED_MAX}"


def _parse_feed(xml: str, chan: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    blocks = _ITEM.findall(xml or "")
    if not blocks:
        logger.debug("DEF[%s]: no <item> blocks in feed", chan["key"])
    for blk in blocks:
        title = _tag(blk, "title")
        link = _tag(blk, "link")
        if not title or not link:
            continue
        url = canonicalize_url(link, base=BASE + "/")
        post_date = _iso_from_rfc822(_tag(blk, "pubDate"))
        desc = _tag(blk, "description")
        creator = _tag(blk, "dc:creator") or _tag(blk, "creator")
        guid = _tag(blk, "guid")

        # Record which domain actually served the item. The department renamed from
        # Defense to War mid-corpus; historical URLs must stay resolvable and the
        # rename itself is archive-relevant.
        m = re.match(r"https?://([^/]+)", url or "")
        domain = (m.group(1).lower().replace("www.", "") if m else "")

        subtype, verb, keep = _subtype(title, chan)

        out.append({
            "source": "Department of War",
            "doc_type": chan["doc_type"],
            "title": title,
            "url": url,
            "canonical_url": url,
            "summary_url": "",
            # The RSS description is the department's own standfirst — the CONTENT of
            # the announcement, not an established fact. Kept verbatim under a name
            # that cannot be mistaken for our prose; `summary` stays empty so no
            # downstream step renders it as event prose.
            "summary": "",
            "summary_origin": "",
            "summary_timestamp": "",
            "post_date": post_date,
            "raw_line": f"=== {post_date} — {title}",
            # Defense extras
            "def_channel": chan["key"],
            "def_instrument_type": chan["instrument"],
            # Sub-type within the channel; equals instrument_type unless a special
            # instrument was detected (casualty identification, officer nomination).
            "def_subtype": subtype,
            "in_scope": keep,
            "def_standfirst_verbatim": desc,
            "def_byline": creator,
            "def_guid": guid,
            "observed_domain": domain,
            "speech_act_verb": verb,
            # Press products are announcements, never published instruments; the
            # Federal Register join does not apply to this collector.
            "fr_publication_expected": False,
            **ACTOR_RECORD_CONSTRAINTS,
        })
    return out


def _filter_window_and_dedupe(items: List[Dict[str, Any]], start_iso: str, end_iso: str,
                              logger) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Window filter + stable dedupe by canonical_url, with a per-skip audit."""
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_title": 0, "no_url": 0,
             "dupe": 0, "out_of_scope": 0}
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
        elif not it.get("in_scope", True):
            # Ruled out of scope by sub-type. Present in RAW, excluded here.
            stats["out_of_scope"] += 1
            reason = f"out_of_scope:{it.get('def_subtype', '')}"

        if reason:
            logger.debug("Window: %s SKIPT reason=%s | title=%r", iso or "''", reason, title[:80])
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
    Harvest central Defense press products for [start, end].

    One request per channel: the RSS feed returns up to 500 items (reaching well past
    2025-01-20), and the window filter is applied client-side because the feed offers
    no date parameter. Channels are selected with DEF_CHANNELS (comma-separated).
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    # The edge rejects plain clients with 403; these headers are mandatory.
    try:
        sess.headers.update(BROWSER_HEADERS)
    except Exception:
        logger.warning("Could not set browser headers on the session; 403s are likely.")
    logger.info("Session ready. Harvesting %s → %s", start, end)

    wanted = {c.strip() for c in
              (os.getenv("DEF_CHANNELS") or ",".join(DEFAULT_CHANNELS)).split(",") if c.strip()}
    logger.info("DEF channels: %s", ", ".join(sorted(wanted)) or "(none)")

    full_snapshot: List[Dict[str, Any]] = []
    full_audit: List[Dict[str, Any]] = []

    for chan in CHANNELS:
        if chan["key"] not in wanted:
            continue
        url = _feed_url(chan["content_type"])
        status, text = http_get(sess, url, logger)
        full_audit.append({"channel": chan["key"], "content_type": chan["content_type"],
                           "url": url, "status": status, "bytes": len(text or "")})
        if status != 200 or not text:
            logger.warning("DEF[%s]: feed fetch failed (status=%s)", chan["key"], status)
            continue

        items = _parse_feed(text, chan, logger)
        dates = [i["post_date"] for i in items if i.get("post_date")]
        logger.info("DEF[%s]: %d items%s", chan["key"], len(items),
                    f" span={min(dates)}..{max(dates)}" if dates else "")
        # The feed caps at FEED_MAX — measured 2026-08-26, the server ignores any
        # larger max and returns exactly 500. Items come newest-first, so the harvest
        # is COMPLETE for the window precisely while the feed's oldest item predates
        # the window start; there is no way to page past the cap.
        #
        # Two conditions, because the first one arrives too late to act on.
        if dates and len(items) >= FEED_MAX:
            oldest = min(dates)
            if oldest > start:
                # Coverage has ALREADY been lost.
                logger.warning(
                    "DEF[%s]: feed hit the %d-item cap and its oldest item (%s) is still "
                    "inside the window — earlier items are NOT reachable via RSS.",
                    chan["key"], FEED_MAX, oldest)
            else:
                # Still complete, but the cap is a sliding window: every item the
                # department publishes pushes the oldest one off the end. Measured
                # 2026-08-26, Releases had 35 items of slack at ~27/month — roughly
                # six weeks before it could no longer reproduce 2025-01-20. Warn while
                # there is still time to re-harvest, not after the fact.
                headroom = sum(1 for d in dates if d < start)
                if headroom <= FEED_MAX * 0.10:
                    logger.warning(
                        "DEF[%s]: feed is complete for this window but has only %d of "
                        "%d items of slack before the cap slides past %s. Re-harvest "
                        "soon; once it slides, the early weeks are unrecoverable from "
                        "RSS and only the committed weekly indexes hold them.",
                        chan["key"], headroom, FEED_MAX, start)
                else:
                    logger.info("DEF[%s]: cap headroom %d item(s) before %s",
                                chan["key"], headroom, start)
        full_snapshot.extend(items)
        polite_sleep()

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    filtered_items, win_stats = _filter_window_and_dedupe(full_snapshot, start, end, logger)

    write_json(raw_path, {
        "generated_at": now_utc,
        "schema": "dc.v4.raw",
        "source": HARVESTER_ID,
        "window": {"start": start, "end": end},
        "channels": sorted(wanted),
        "feed_max": FEED_MAX,
        "parsed_total": len(full_snapshot),
        "audit": full_audit,
        "items_snapshot": [
            {
                "url": it.get("url", ""),
                "title": it.get("title", ""),
                "post_date": it.get("post_date", ""),
                "doc_type": it.get("doc_type", ""),
                "def_channel": it.get("def_channel", ""),
                "def_instrument_type": it.get("def_instrument_type", ""),
                "def_subtype": it.get("def_subtype", ""),
                "in_scope": it.get("in_scope", True),
                "observed_domain": it.get("observed_domain", ""),
                "raw_line": (it.get("raw_line", "") or "")[:500],
            }
            for it in full_snapshot
        ],
    })
    logger.info("Wrote raw JSON: %s", raw_path)

    write_json(filtered_path, {
        "generated_at": now_utc,
        "schema": "dc.v4.filtered",
        "source": HARVESTER_ID,
        "entity_type": "defense_press_product",
        "window": {"start": start, "end": end},
        "count": len(filtered_items),
        "entities": filtered_items,
        "window_stats": win_stats,
        "source_constraints": ACTOR_RECORD_CONSTRAINTS,
        "ruling": "Official actor record (Event Trust Contract v0.3 §4.5). Documents that "
                  "the department issued the announcement, in those words, on that date. "
                  "Does not establish the truth of any operational, numerical or outcome "
                  "claim within it. Never corroborates.",
        "collector": "defense_press_products",
        "note_rename": "The department publishes as the Department of War from war.gov. "
                       "observed_domain is recorded per item.",
    })
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

    p = argparse.ArgumentParser(description="Democracy Clock — Defense Press Products harvester")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--artifacts", default=str(ARTIFACTS_ROOT), help="Artifacts root directory")
    p.add_argument("--level", default="INFO", help="Logging level")
    p.add_argument("--log", default=None, help="Optional log file path")
    args = p.parse_args()

    print(run_harvester(start=args.start, end=args.end, artifacts_root=args.artifacts,
                        level=args.level, log_path=args.log))
