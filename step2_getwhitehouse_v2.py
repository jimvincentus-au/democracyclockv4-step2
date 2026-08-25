# step2_getwhitehouse_v2.py — White House statement-act harvester (Democracy Clock V4 contract)
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

DOWNSTREAM DISCIPLINE (enforced by the builder and the trust grader, not here)
    The event recorded must be the SPEECH ACT — actor = the speaker, action = the
    saying. Never the content of the statement as a fact about the world.

═══════════════════════════════════════════════════════════════════════════════
VERSION HISTORY
═══════════════════════════════════════════════════════════════════════════════
v1 (2026-08-25)
    Listing-first discovery. Each presidential action was harvested from its typed
    subcategory listing (/presidential-actions/executive-orders/ etc.) so that the
    instrument type was "known by construction" — never inferred from the title,
    which v1 correctly established is not a reliable typing signal ("Delivering Gold
    Standard Childhood Vaccine Recommendations" is an EXECUTIVE ORDER; the title
    does not say so). A sitemap gap-fill existed as a backstop.

v2 (2026-08-26) — SITEMAP-FIRST DISCOVERY
    v1's premise held for recent items and silently failed for early ones.

    MEASURED 2026-08-26. whitehouse.gov's typed subcategory listings are truncated
    at the source. /presidential-actions/executive-orders/ terminates at page 19
    (page 20 → HTTP 404) with its oldest item at 2025-01-29 and nothing else before
    2025-03-19. Proclamations begin 2025-03-24; presidential memoranda 2025-03-20.
    The sitemap, by contrast, carries 611 presidential actions including 77 in
    January 2025 and 49 in February 2025.

        family                 sitemap   reachable via listings   MISSING
        presidential-actions       611                      413       198
        releases                   628                      427       201
        fact-sheets                344                      321        23

    So a v1 harvest of week 5 (2025-02-15..21) returned 8 fact sheets, 6 releases
    and ZERO instruments — while that week's EOs 14215–14219 (Ensuring
    Accountability for All Agencies; Ending Taxpayer Subsidization of Open Borders;
    Ensuring Lawful Governance / DOGE) sat in the sitemap unharvested. The result
    was an inverted record for exactly the weeks Volume 1 covers: the promotional
    channel present, the instrument channel absent.

    The gap-fill did not catch it. It matched sitemap URLs against each channel's
    LISTING path prefix, and the site uses two permalink shapes for one document:

        listing shape    /presidential-actions/executive-orders/<slug>/
        sitemap shape    /presidential-actions/2025/02/<slug>/     <- declares itself
                                                                     rel=canonical

    `/presidential-actions/2025/02/...` does not start with
    `presidential-actions/executive-orders/`, so every presidential action fell
    through to no channel and was dropped. Fact sheets and releases matched on
    their bare family prefix and came through — which is why the failure presented
    as a plausible-looking harvest rather than an error.

    v2 therefore:
      • discovers from the SITEMAP (complete) and uses listings only for typing;
      • joins the two routes on SLUG, not URL, because one document has two URLs
        and v1's URL dedupe would have double-counted every recent item;
      • emits the site's declared canonical (the date permalink) as canonical_url;
      • types presidential actions by cascade — see INSTRUMENT TYPING below.

INSTRUMENT TYPING — three tiers, never the title
    The type drives `speech_act_verb`, and the wrapper wording IS the event's
    identity downstream (act_key = normalize(actor)|normalize(action)|date), so a
    type guessed now cannot be quietly corrected later.

      1. LISTING SUBCATEGORY. If the slug also appeared in a typed subcategory
         listing, that is the source's own taxonomy. Authoritative. Free.

      2. FEDERAL REGISTER. Article pages do NOT carry the subcategory — measured
         2026-08-26, ld+json articleSection is "Presidential Actions" for every
         instrument alike, and the page's taxonomy-category blocks belong to the
         related-posts rail, not to the article. The FR API does carry it, exactly
         and structurally: subtype ∈ {Executive Order, Proclamation, Memorandum,
         Notice}, plus executive_order_number and a separate signing_date. Matching
         on signing_date ≈ post_date + normalized title is ruling 1.5 method 3, the
         already-ratified join. One batched request per window.

         This is also the standing ruling's own answer: these instruments are
         "cited from the FEDERAL REGISTER, not from here."

      3. FEDERAL REGISTER RESIDUAL (ratified 2026-08-26). Within a single signing
         date, if exactly one White House presidential action and exactly one FR
         presidential document remain unclaimed after tier 2, they are the same
         instrument. No title similarity is consulted — only the date and the fact
         that one candidate remains on each side. The uniqueness requirement is the
         whole safety guarantee.

         This exists because the two publishers do not always agree on an official
         title: EO 14219 is "…Regulatory Initiative" at the White House and
         "…Deregulatory Initiative" in the Federal Register. Stamped
         wh_typed_by="fr_date_residual" so the weaker provenance stays visible and
         reversible as a class.

      4. UNTYPED. No match → wh_instrument_type="presidential_action",
         fr_publication_expected=None, verb "announced a presidential action
         titled". An honest unknown. NEVER a title-pattern guess.

CHANNEL SELECTION IS COARSER THAN IT LOOKS (ratified 2026-08-26)
    The canonical permalink exposes only the family (/presidential-actions/), so
    selecting any presidential-action channel sweeps in ALL of them — including
    nominations and one-off announcements that WH_CHANNELS does not name. This is
    deliberate: an announcement the White House itself filed under presidential
    actions belongs in the record, and wh_typed_by already reports exactly how much
    is known about each one.

WHY SCRAPING
    whitehouse.gov exposes no usable machine surface: wp-json REST returns 403 and
    /feed/ returns 404. robots.txt is "User-agent: * / Disallow:" — i.e. no
    restriction — re-checked 2026-08-26.

OUTPUT (standard V4 contract)
    {artifacts}/json/whitehouse_raw_{start}_{end}.json
    {artifacts}/json/whitehouse_filtered_{start}_{end}.json
"""
from __future__ import annotations

import html as _html
import json
import os
import re
import urllib.parse
from datetime import datetime, timedelta, timezone
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
HARVESTER_VERSION = "whitehouse_v2"

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
#
# `family` is the FIRST path segment, which is the only thing the canonical (sitemap)
# permalink exposes. `path` is the typed subcategory listing, used for typing only.
#
# fr_publication_expected drives the Federal Register join. It matters because
# "unmatched" only means something for instruments 44 U.S.C. § 1505 requires to be
# published (executive orders, proclamations). Memoranda and nominations are
# routinely absent, so their absence asserts nothing.
CHANNELS: List[Dict[str, Any]] = [
    {"key": "releases", "family": "releases", "path": "/releases/",
     "doc_type": "official_release", "instrument": "release",
     "verb": "published a release titled", "fr_expected": False},

    {"key": "fact_sheets", "family": "fact-sheets", "path": "/fact-sheets/",
     "doc_type": "official_fact_sheet", "instrument": "fact_sheet",
     "verb": "issued a fact sheet titled", "fr_expected": False},

    {"key": "executive_orders", "family": "presidential-actions",
     "path": "/presidential-actions/executive-orders/",
     "doc_type": "presidential_action", "instrument": "executive_order",
     "verb": "announced an executive order titled", "fr_expected": True},

    {"key": "proclamations", "family": "presidential-actions",
     "path": "/presidential-actions/proclamations/",
     "doc_type": "presidential_action", "instrument": "proclamation",
     "verb": "announced a proclamation titled", "fr_expected": True},

    {"key": "presidential_memoranda", "family": "presidential-actions",
     "path": "/presidential-actions/presidential-memoranda/",
     "doc_type": "presidential_action", "instrument": "memorandum",
     "verb": "announced a presidential memorandum titled", "fr_expected": None},

    {"key": "nominations_appointments", "family": "presidential-actions",
     "path": "/presidential-actions/nominations-appointments/",
     "doc_type": "presidential_action", "instrument": "nominations",
     "verb": "announced nominations sent to the Senate:", "fr_expected": False},

    {"key": "briefings_statements", "family": "briefings-statements",
     "path": "/briefings-statements/",
     "doc_type": "official_statement", "instrument": "statement",
     "verb": "issued a statement titled", "fr_expected": False},
]

# The fallback record for a presidential action whose instrument could not be
# established from the source's own taxonomy or from the Federal Register.
# Deliberately vague in the verb: it says only what is known — that the White House
# announced a presidential action under this title on this date.
UNTYPED_PRESIDENTIAL = {
    "key": "presidential_actions_untyped", "family": "presidential-actions",
    "path": "/presidential-actions/",
    "doc_type": "presidential_action", "instrument": "presidential_action",
    "verb": "announced a presidential action titled", "fr_expected": None,
}

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
FR_GRACE_DAYS = 3       # signing_date vs WH post_date tolerance for the FR join

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

SITEMAP_INDEX = f"{BASE}/wp-sitemap.xml"
_SITEMAP_POST = re.compile(r"<loc>([^<]*post-sitemap\d*\.xml)</loc>", re.I)
_SITEMAP_LOC = re.compile(r"<loc>([^<]+)</loc>", re.I)
_OG_TITLE = re.compile(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', re.I)
_H1 = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)

FR_API = "https://www.federalregister.gov/api/v1/documents.json"
# FR subtype -> our channel key. "Notice" (continuations of national emergencies) is
# a presidential document but not one of our typed instruments; it stays untyped
# rather than being forced into a category it does not belong to.
_FR_SUBTYPE_CHANNEL = {
    "Executive Order": "executive_orders",
    "Proclamation": "proclamations",
    "Memorandum": "presidential_memoranda",
}

_CHANNEL_BY_KEY = {c["key"]: c for c in CHANNELS}
_FAMILIES = {c["family"] for c in CHANNELS}


def _strip_tags(s: str) -> str:
    return normalize_ws(_html.unescape(re.sub(r"<[^>]+>", " ", s or "")))


def _date_from_url(url: str) -> str:
    """Fallback date: permalinks embed /YYYY/MM/ — day is unknown, so use the 1st."""
    m = _URL_DATE.search(url or "")
    return f"{m.group(1)}-{m.group(2)}-01" if m else ""


def _slug(url: str) -> str:
    """
    The join key between the two permalink shapes.

    One document is published at both /presidential-actions/executive-orders/<slug>/
    and /presidential-actions/<yyyy>/<mm>/<slug>/. Deduping on URL (v1) would keep
    both; deduping on slug keeps one.
    """
    return (url or "").rstrip("/").rsplit("/", 1)[-1].lower()


def _family_of(url: str) -> str:
    """First path segment — the only classification the canonical permalink carries."""
    parts = [p for p in (url or "").replace(BASE, "").split("/") if p]
    return parts[0] if parts else ""


def _norm_title(s: str) -> str:
    """
    Normalization for the Federal Register title join (ruling 1.5 method 3).

    Curly quotes, ampersands and case vary between the two publishers for the same
    instrument; nothing else is allowed to vary, so nothing else is stripped.
    """
    s = _html.unescape(s or "").lower()
    s = s.replace("’", "'").replace("‘", "'")
    s = s.replace("“", '"').replace("”", '"')
    s = s.replace("—", "-").replace("–", "-")
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ── Federal Register typing ──────────────────────────────────────────────────
def _fr_presidential_index(session, start_iso: str, end_iso: str,
                           logger) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """
    One batched FR query for presidential documents SIGNED in the window (± grace).

    Returns ({normalized_title: record}, [all records]). Keyed on title because that
    is what both publishers usually render identically; the date is used as a
    corroborating bound, not as the key. The full list is returned as well because
    the residual pass needs to reason about what remains unclaimed on a given date.

    Failure is non-fatal: an empty index simply means presidential actions fall
    through to tier 4 (untyped), which is the honest outcome rather than a guess.
    """
    try:
        lo = (datetime.strptime(start_iso, "%Y-%m-%d") - timedelta(days=FR_GRACE_DAYS)).date()
        hi = (datetime.strptime(end_iso, "%Y-%m-%d") + timedelta(days=FR_GRACE_DAYS)).date()
    except ValueError:
        logger.warning("WH: bad window for FR typing (%s..%s); skipping", start_iso, end_iso)
        return {}, []

    params = [
        ("conditions[type][]", "PRESDOCU"),
        ("conditions[signing_date][gte]", lo.isoformat()),
        ("conditions[signing_date][lte]", hi.isoformat()),
        ("per_page", "1000"),
        ("order", "oldest"),
    ]
    for f in ("document_number", "publication_date", "signing_date", "title",
              "subtype", "executive_order_number", "html_url"):
        params.append(("fields[]", f))

    url = f"{FR_API}?{urllib.parse.urlencode(params)}"
    status, text = http_get(session, url, logger)
    if status != 200 or not text:
        logger.warning("WH: FR typing query failed (status=%s); presidential actions "
                       "will remain untyped", status)
        return {}, []
    try:
        results = json.loads(text).get("results") or []
    except (ValueError, TypeError) as exc:
        logger.warning("WH: FR typing response unparseable (%s); presidential actions "
                       "will remain untyped", exc)
        return {}, []

    idx: Dict[str, Dict[str, Any]] = {}
    for r in results:
        key = _norm_title(r.get("title") or "")
        if key:
            idx.setdefault(key, r)
    logger.info("WH: FR presidential index %s..%s -> %d document(s)",
                lo, hi, len(idx))
    return idx, results


def _type_from_fr(title: str, post_date: str, fr_idx: Dict[str, Dict[str, Any]],
                  logger) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Tier 2 typing. Returns (channel_spec_or_None, provenance).

    The title must match exactly after normalization AND the signing date must fall
    within the grace window of the White House post date. Both, because a title match
    alone would collide across years for recurring proclamations ("American Heart
    Month, 2025" vs "…, 2026" differ, but many ceremonial titles do not).
    """
    rec = fr_idx.get(_norm_title(title))
    if not rec:
        return None, {}
    signed = (rec.get("signing_date") or "")[:10]
    if signed and post_date:
        try:
            delta = abs((datetime.strptime(signed, "%Y-%m-%d")
                         - datetime.strptime(post_date, "%Y-%m-%d")).days)
        except ValueError:
            delta = 999
        if delta > FR_GRACE_DAYS:
            logger.debug("WH: FR title match rejected on date (%s vs %s) for %r",
                         signed, post_date, title[:60])
            return None, {}
    key = _FR_SUBTYPE_CHANNEL.get(rec.get("subtype") or "")
    return (_CHANNEL_BY_KEY.get(key) if key else None), _fr_prov(rec)


# ── entity construction ──────────────────────────────────────────────────────
def _entity(title: str, url: str, post_date: str, chan: Dict[str, Any],
            discovery: str, category: str = "",
            typed_by: str = "", fr_prov: Optional[Dict[str, Any]] = None
            ) -> Dict[str, Any]:
    return {
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
        "wh_channel": chan["key"],
        "wh_category": category,
        # The verb the builder MUST use. The title is quoted, never paraphrased,
        # and never supplies the verb.
        "speech_act_verb": chan["verb"],
        # Typed by the source's own taxonomy or by the Federal Register — never
        # inferred from the title. `wh_typed_by` records which, so a downstream
        # reader can tell an authoritative type from an unresolved one.
        "wh_instrument_type": chan["instrument"],
        "wh_typed_by": typed_by,
        # True  -> 44 U.S.C. § 1505 expects Federal Register publication, so an
        #          unmatched instrument is a reportable absence (after a grace period).
        # False -> routinely absent from the FR; absence asserts nothing.
        # None  -> unknown; assert nothing either way.
        "fr_publication_expected": chan["fr_expected"],
        "discovery": discovery,
        "harvester_version": HARVESTER_VERSION,
        **(fr_prov or {}),
        **ACTOR_RECORD_CONSTRAINTS,
    }


def _extract_listing_rows(page_html: str) -> List[Dict[str, str]]:
    """
    Extract one listing page to plain rows — the cacheable unit.

    The listing carries the OFFICIAL title in the post-title <h2>, which is what the
    Federal Register join matches on. URL slugs are lossy and must not be used for
    the join.
    """
    rows: List[Dict[str, str]] = []
    for blk in _POST_BLOCK.findall(page_html or ""):
        m = _TITLE_LINK.search(blk)
        if not m:
            continue
        url = canonicalize_url(m.group(1).strip(), base=BASE + "/")
        title = _strip_tags(m.group(2))
        if not title or not url:
            continue
        dm = _DATETIME.search(blk)
        cm = _CATEGORY.search(blk)
        rows.append({
            "url": url,
            "title": title,
            "post_date": dm.group(1) if dm else _date_from_url(url),
            "category": _strip_tags(cm.group(1)) if cm else "",
        })
    return rows


def _rows_to_entities(rows: List[Dict[str, str]], chan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build entities from cached rows.

    In v2 the listing is a TYPING source, not the discovery spine: it establishes
    the instrument type by the source's own taxonomy for the items it still carries.
    """
    return [_entity(r["title"], r["url"], r["post_date"], chan, "listing",
                    category=r.get("category", ""), typed_by="listing_subcategory")
            for r in rows]


def _listing_url(path: str, page: int) -> str:
    return f"{BASE}{path}" if page <= 1 else f"{BASE}{path}page/{page}/"


def _discover_channel(session, chan: Dict[str, Any], start_iso: str,
                      cache: Optional[_ListingCache], logger
                      ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Walk a typed subcategory listing newest-first, stopping once a whole page falls
    before the window. Listing order is reverse-chronological, so this bounds the
    crawl to roughly the window size rather than the full archive.
    """
    channel, path = chan["key"], chan["path"]
    snapshot: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []
    logger.info("WH: typing scan channel=%s path=%s", channel, path)

    for page in range(1, MAX_PAGES + 1):
        url = _listing_url(path, page)

        cached = cache.get(url) if cache is not None else None
        if cached is not None:
            status, rows = cached
            from_network = False
        else:
            status, text = http_get(session, url, logger)
            rows = _extract_listing_rows(text) if (status == 200 and text) else []
            if cache is not None:
                cache.put(url, status, rows)
            from_network = True

        audit.append({"channel": channel, "page": page, "url": url, "status": status,
                      "cached": not from_network})
        if status != 200:
            logger.debug("WH[%s]: stop at page=%s (status=%s)", channel, page, status)
            break
        if not rows:
            logger.debug("WH[%s]: no items parsed on page=%s; stopping", channel, page)
            break

        snapshot.extend(_rows_to_entities(rows, chan))
        dates = [r["post_date"] for r in rows if r.get("post_date")]
        oldest = min(dates) if dates else ""
        if oldest and oldest < start_iso:
            logger.debug("WH[%s]: page=%s fully before window start (%s < %s); stopping",
                         channel, page, oldest, start_iso)
            break
        if from_network:
            polite_sleep()

    logger.info("WH[%s]: listing items=%d", channel, len(snapshot))
    return snapshot, audit


# ── sitemap discovery (the spine) ────────────────────────────────────────────
def _sitemap_urls(session, cache: Optional[_ListingCache], logger) -> List[str]:
    """
    Every post URL the site publishes, from the sitemap index.

    Shares the listing cache's TTL: the sitemap grows during normal operation but is
    stable for the duration of a backfill, and it is ~700 KB per page fetched three
    times per week otherwise.
    """
    if cache is not None:
        hit = cache.get(SITEMAP_INDEX)
        if hit is not None and hit[0] == 200:
            return [r["url"] for r in hit[1]]

    status, text = http_get(session, SITEMAP_INDEX, logger)
    if status != 200 or not text:
        logger.error("WH: sitemap index unavailable (status=%s) — discovery would be "
                     "incomplete; refusing to pretend otherwise", status)
        return []
    out: List[str] = []
    for sm in _SITEMAP_POST.findall(text):
        st, xml = http_get(session, sm, logger)
        if st != 200 or not xml:
            logger.warning("WH: sitemap page unavailable (status=%s) %s", st, sm)
            continue
        out.extend(_SITEMAP_LOC.findall(xml))
        polite_sleep()
    logger.info("WH: sitemap enumerated %d post url(s)", len(out))
    if cache is not None and out:
        cache.put(SITEMAP_INDEX, 200, [{"url": u} for u in out])
    return out


# ── caches ───────────────────────────────────────────────────────────────────
# Two distinct caches, for two distinct reasons.
#
# The ARTICLE cache is permanent. A published post's title and datetime do not
# change, and the sitemap permalink carries only /YYYY/MM/, so a one-week window
# must consider every article in that month and resolve each one's exact date by
# fetching it. Over an 82-week backfill each article would otherwise be fetched
# roughly four times (once per week whose month it touches) — ~6,800 requests for
# ~1,600 documents.
#
# The LISTING cache is time-limited. Listings DO change as new items publish, but
# not within one backfill run. Each week re-walks all five listings from page 1,
# and the walk only stops when a page falls entirely before the window — so week 1
# walks ~114 pages while week 82 stops after ~10. Measured on week 5, the listing
# walk was 79 of 167 seconds. Across 82 weeks that is roughly 5,000 fetches of
# pages whose content is identical for the duration of the run.
#
# Both are speed and politeness measures only: a miss costs a request, and deleting
# either file changes no output.
WH_LISTING_TTL_SECONDS = int(os.getenv("WH_LISTING_TTL_SECONDS") or 6 * 3600)


class _JsonCache:
    """Shared load/flush plumbing. Subclasses define the record shape."""

    filename = "cache.json"

    def __init__(self, artifacts: Path, logger):
        self.path = Path(artifacts) / "cache" / self.filename
        self.logger = logger
        self.data: Dict[str, Any] = {}
        self.dirty = False
        self.hits = 0
        self.misses = 0
        try:
            if self.path.exists():
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning("WH: %s unreadable (%s); starting empty", self.filename, exc)
            self.data = {}

    def flush(self) -> None:
        if not self.dirty:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=1),
                                 encoding="utf-8")
        except OSError as exc:
            self.logger.warning("WH: could not persist %s (%s)", self.filename, exc)


class _ListingCache(_JsonCache):
    """
    Cached listing-page extractions, keyed by listing URL.

    Stores the EXTRACTED ROWS rather than the HTML — 260 KB per page across ~5,000
    pages is not worth writing to disk, and rows survive changes to the entity
    schema. The HTTP status is cached too, so a 404 marking the end of a listing is
    not refetched on every subsequent week.
    """

    filename = "whitehouse_listings.json"

    def get(self, url: str) -> Optional[Tuple[int, List[Dict[str, str]]]]:
        rec = self.data.get(url)
        if not rec:
            return None
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(rec["fetched_at"])).total_seconds()
        except (KeyError, ValueError):
            return None
        if age > WH_LISTING_TTL_SECONDS:
            return None
        self.hits += 1
        return int(rec.get("status", 0)), rec.get("rows") or []

    def put(self, url: str, status: int, rows: List[Dict[str, str]]) -> None:
        self.misses += 1
        self.data[url] = {
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "status": status,
            "rows": rows,
        }
        self.dirty = True


# ── article cache ────────────────────────────────────────────────────────────
# The sitemap permalink carries only /YYYY/MM/, so a one-week window must consider
# every article in that month and resolve each one's exact date by fetching it. Over
# an 82-week backfill each article would otherwise be fetched roughly four times
# (once per week whose month it touches) — ~6,800 requests for ~1,600 documents.
#
# A published White House post's title and datetime do not change, so the cache is
# keyed on URL with no expiry. It is a speed and politeness measure only: a cache
# miss simply costs a request, and deleting the file changes no output.
class _ArticleCache(_JsonCache):
    filename = "whitehouse_articles.json"

    def get(self, url: str) -> Optional[Tuple[str, str]]:
        rec = self.data.get(url)
        if not rec:
            return None
        self.hits += 1
        return rec.get("title", ""), rec.get("post_date", "")

    def put(self, url: str, title: str, post_date: str) -> None:
        self.misses += 1
        if title:
            self.data[url] = {"title": title, "post_date": post_date}
            self.dirty = True


def _fetch_article(session, url: str, cache: Optional["_ArticleCache"],
                   logger) -> Tuple[str, str, bool]:
    """
    Resolve one sitemap URL to (title, post_date, came_from_network).

    The caller uses the third element to decide whether to sleep — a cache hit
    should not pay the politeness delay.
    """
    if cache is not None:
        hit = cache.get(url)
        if hit is not None:
            return hit[0], hit[1], False

    status, text = http_get(session, url, logger)
    if status != 200 or not text:
        logger.debug("WH: article fetch failed status=%s url=%s", status, url)
        return "", "", True
    m = _OG_TITLE.search(text) or _H1.search(text)
    title = _strip_tags(m.group(1)) if m else ""
    # strip the site suffix WordPress appends to og:title
    title = re.sub(r"\s*[–—-]\s*The White House\s*$", "", title).strip()
    dm = _DATETIME.search(text)
    post_date = dm.group(1) if dm else _date_from_url(url)
    if cache is not None:
        cache.put(url, title, post_date)
    return title, post_date, True


def _fr_prov(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fr_document_number": rec.get("document_number") or "",
        "fr_subtype": rec.get("subtype") or "",
        "fr_signing_date": (rec.get("signing_date") or "")[:10],
        "fr_publication_date": (rec.get("publication_date") or "")[:10],
        "fr_url": rec.get("html_url") or "",
        "executive_order_number": rec.get("executive_order_number"),
    }


def _residual_pass(unmatched_wh: List[Dict[str, Any]], fr_all: List[Dict[str, Any]],
                   consumed: set, logger) -> Dict[str, Dict[str, Any]]:
    """
    Tier 3 typing — RATIFIED 2026-08-26, date-and-exhaustion.

    Within a single signing date, if exactly one White House presidential action and
    exactly one Federal Register presidential document remain unclaimed after exact
    title matching, they are the same instrument.

    This exists because the two publishers do not always agree on an instrument's
    official title. Measured 2026-08-26 on February 2025: of 49 presidential actions,
    37 matched the FR title exactly and exactly one differed —

        WH  …"Department of Government Efficiency"   Regulatory Initiative
        FR  …"Department of Government Efficiency" Deregulatory Initiative

    which is EO 14219. A fuzzy title match would have papered over a real discrepancy
    between two official records; ruling 1.5 forbids that and is right to. This rule
    uses no title similarity at all — only the signing date and the fact that one
    candidate remains on each side. The uniqueness requirement is the entire safety
    guarantee: two unclaimed items on either side means ambiguity, and ambiguity
    means we decline.

    Matches are stamped wh_typed_by="fr_date_residual" so the weaker provenance stays
    visible downstream and can be reviewed or reversed as a class.

    Returns {wh_url: fr_record}.
    """
    wh_by_date: Dict[str, List[Dict[str, Any]]] = {}
    for w in unmatched_wh:
        if w.get("post_date"):
            wh_by_date.setdefault(w["post_date"], []).append(w)

    fr_by_date: Dict[str, List[Dict[str, Any]]] = {}
    for r in fr_all:
        if r.get("document_number") in consumed:
            continue
        d = (r.get("signing_date") or "")[:10]
        if d:
            fr_by_date.setdefault(d, []).append(r)

    resolved: Dict[str, Dict[str, Any]] = {}
    for d, whs in wh_by_date.items():
        frs = fr_by_date.get(d) or []
        if len(whs) == 1 and len(frs) == 1:
            resolved[whs[0]["url"]] = frs[0]
            logger.info("WH: residual match on %s — %r <- FR %s %s", d,
                        whs[0]["title"][:60], frs[0].get("subtype"),
                        frs[0].get("executive_order_number") or "")
        elif whs and frs:
            logger.debug("WH: residual declined on %s (%d WH / %d FR unclaimed)",
                         d, len(whs), len(frs))
    return resolved


def _discover_sitemap(session, wanted_families: set, start_iso: str, end_iso: str,
                      known_by_slug: Dict[str, Dict[str, Any]],
                      fr_idx: Dict[str, Dict[str, Any]], fr_all: List[Dict[str, Any]],
                      art_cache: Optional[_ArticleCache],
                      list_cache: Optional[_ListingCache], logger
                      ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Discover every in-window document from the sitemap and type it.

    The sitemap is the COMPLETE surface; the typed listings are not (measured
    2026-08-26: 198 of 611 presidential actions appear in no listing at all). So the
    sitemap drives discovery, and anything a listing already typed is reused rather
    than refetched.

    Windowing is done at MONTH granularity from the permalink, because the path
    carries /YYYY/MM/ but not the day. The exact date comes from the fetched page and
    the real window filter is applied afterwards as usual.

    Typing runs in passes rather than inline, because the residual rule cannot be
    evaluated for one item in isolation — it needs to know what else remains
    unclaimed on the same date.
    """
    stats = {"candidates": 0, "already_typed": 0, "fetched": 0, "fetch_failed": 0,
             "typed_fr": 0, "typed_residual": 0, "untyped": 0, "cache_hits": 0}
    urls = _sitemap_urls(session, list_cache, logger)
    if not urls:
        return [], stats

    start_ym, end_ym = start_iso[:7], end_iso[:7]
    out: List[Dict[str, Any]] = []
    pending: List[Dict[str, Any]] = []      # presidential actions awaiting typing

    # ── pass A: discover and resolve ─────────────────────────────────────────
    for u in urls:
        fam = _family_of(u)
        if fam not in wanted_families:
            continue
        ym = _URL_DATE.search(u)
        if not ym or not (start_ym <= f"{ym.group(1)}-{ym.group(2)}" <= end_ym):
            continue
        stats["candidates"] += 1

        prior = known_by_slug.get(_slug(u))
        if prior:
            # Already discovered and authoritatively typed by its subcategory
            # listing. Adopt the site's declared canonical URL (the date permalink
            # this sitemap entry uses) and keep the listing's typing.
            stats["already_typed"] += 1
            adopted = dict(prior)
            adopted["url"] = adopted["canonical_url"] = u.rstrip("/") + "/"
            adopted["discovery"] = "listing+sitemap"
            out.append(adopted)
            continue

        title, post_date, from_network = _fetch_article(session, u, art_cache, logger)
        if from_network:
            polite_sleep()
        if not title:
            stats["fetch_failed"] += 1
            continue
        stats["fetched"] += 1

        if fam == "presidential-actions":
            pending.append({"url": u, "title": title, "post_date": post_date})
        else:
            chan = next(c for c in CHANNELS if c["family"] == fam)
            out.append(_entity(title, u, post_date, chan, "sitemap",
                               typed_by="url_family"))

    # ── pass B: exact Federal Register title match ───────────────────────────
    # Items already typed by a listing still CLAIM their FR document, so the
    # residual pass cannot hand it to something else.
    consumed: set = set()
    for ent in out:
        rec = fr_idx.get(_norm_title(ent.get("title") or ""))
        if rec and rec.get("document_number"):
            consumed.add(rec["document_number"])

    typed: Dict[str, Tuple[Dict[str, Any], Dict[str, Any], str]] = {}
    still_open: List[Dict[str, Any]] = []
    for p in pending:
        chan, prov = _type_from_fr(p["title"], p["post_date"], fr_idx, logger)
        if chan:
            typed[p["url"]] = (chan, prov, "federal_register")
            if prov.get("fr_document_number"):
                consumed.add(prov["fr_document_number"])
        else:
            still_open.append(p)

    # ── pass C: residual, date-and-exhaustion ────────────────────────────────
    for wh_url, rec in _residual_pass(still_open, fr_all, consumed, logger).items():
        chan = _CHANNEL_BY_KEY.get(_FR_SUBTYPE_CHANNEL.get(rec.get("subtype") or "", ""))
        if chan:
            typed[wh_url] = (chan, _fr_prov(rec), "fr_date_residual")
            consumed.add(rec.get("document_number") or "")

    # ── pass D: build ────────────────────────────────────────────────────────
    for p in pending:
        hit = typed.get(p["url"])
        if hit:
            chan, prov, how = hit
            stats["typed_residual" if how == "fr_date_residual" else "typed_fr"] += 1
            out.append(_entity(p["title"], p["url"], p["post_date"], chan,
                               "sitemap", typed_by=how, fr_prov=prov))
        else:
            stats["untyped"] += 1
            out.append(_entity(p["title"], p["url"], p["post_date"],
                               UNTYPED_PRESIDENTIAL, "sitemap", typed_by="unresolved"))

    if art_cache is not None:
        stats["cache_hits"] = art_cache.hits
        art_cache.flush()
    logger.info("WH: sitemap discovery %s", stats)
    return out, stats


def _merge_by_slug(items: List[Dict[str, Any]], logger) -> List[Dict[str, Any]]:
    """
    Collapse the two discovery routes onto one record per document.

    One document has two permalinks, so URL dedupe (v1) keeps both. Slug is the
    identity. When both routes produced a record, the one with the stronger typing
    wins: listing subcategory > federal register > url family > unresolved.
    """
    rank = {"listing_subcategory": 4, "federal_register": 3, "fr_date_residual": 2,
            "url_family": 1, "unresolved": 0, "": 0}
    best: Dict[str, Dict[str, Any]] = {}
    collisions = 0
    for it in items:
        s = _slug(it.get("canonical_url") or it.get("url") or "")
        if not s:
            continue
        cur = best.get(s)
        if cur is None:
            best[s] = it
            continue
        collisions += 1
        if rank.get(it.get("wh_typed_by", ""), 0) > rank.get(cur.get("wh_typed_by", ""), 0):
            best[s] = it
    if collisions:
        logger.info("WH: merged %d duplicate permalink record(s) by slug", collisions)
    return list(best.values())


def _filter_window(items: List[Dict[str, Any]], start_iso: str, end_iso: str,
                   logger) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Window filter, with a per-skip audit. Identity dedupe already happened."""
    stats = {"inside": 0, "outside": 0, "nodate": 0, "no_title": 0, "no_url": 0}
    kept: List[Dict[str, Any]] = []

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

    Channels are controlled by WH_CHANNELS (comma-separated). Writes RAW (pre-window)
    and FILTERED (window + identity dedupe) artifacts under the standard V4 paths.
    """
    logger = setup_logger(f"dc.{HARVESTER_ID}", level, Path(log_path) if log_path else None)

    artifacts = Path(artifacts_root)
    raw_path, filtered_path = create_artifact_paths(artifacts, HARVESTER_ID, start, end)

    sess = session or build_session()
    logger.info("Session ready (%s). Harvesting %s → %s", HARVESTER_VERSION, start, end)

    wanted = {c.strip() for c in (os.getenv("WH_CHANNELS") or ",".join(DEFAULT_CHANNELS)).split(",") if c.strip()}
    logger.info("WH channels: %s", ", ".join(sorted(wanted)) or "(none)")
    wanted_families = {c["family"] for c in CHANNELS if c["key"] in wanted}

    list_cache = _ListingCache(artifacts, logger)
    art_cache = _ArticleCache(artifacts, logger)

    # 1. Typed subcategory listings — authoritative typing for what they still carry.
    listing_items: List[Dict[str, Any]] = []
    full_audit: List[Dict[str, Any]] = []
    for chan in CHANNELS:
        if chan["key"] not in wanted:
            logger.debug("WH: channel=%s not selected; skipping", chan["key"])
            continue
        snap, audit = _discover_channel(sess, chan, start, list_cache, logger)
        listing_items.extend(snap)
        full_audit.extend(audit)

    known_by_slug = {_slug(i["canonical_url"]): i for i in listing_items}

    # 2. Federal Register index — tier-2 and tier-3 typing for what listings dropped.
    need_fr = "presidential-actions" in wanted_families
    fr_idx, fr_all = _fr_presidential_index(sess, start, end, logger) if need_fr else ({}, [])

    # 3. Sitemap — the complete discovery surface.
    sitemap_items, sm_stats = _discover_sitemap(
        sess, wanted_families, start, end, known_by_slug, fr_idx, fr_all,
        art_cache, list_cache, logger)
    list_cache.flush()
    logger.info("WH: cache hits — listings=%d articles=%d",
                list_cache.hits, art_cache.hits)

    full_snapshot = _merge_by_slug(listing_items + sitemap_items, logger)

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    filtered_items, win_stats = _filter_window(full_snapshot, start, end, logger)

    typed_counts: Dict[str, int] = {}
    for it in filtered_items:
        k = it.get("wh_typed_by") or "?"
        typed_counts[k] = typed_counts.get(k, 0) + 1
    unresolved = typed_counts.get("unresolved", 0)
    if unresolved:
        logger.warning("WH: %d in-window presidential action(s) could not be typed from "
                       "the source taxonomy or the Federal Register; recorded as "
                       "untyped presidential actions (never guessed from the title)",
                       unresolved)
    logger.info("WH: typing provenance %s", typed_counts)

    raw_payload = {
        "generated_at": now_utc,
        "schema": "dc.v4.raw",
        "source": HARVESTER_ID,
        "harvester_version": HARVESTER_VERSION,
        "window": {"start": start, "end": end},
        "channels": sorted(wanted),
        "parsed_total": len(full_snapshot),
        "discovery_stats": sm_stats,
        "typing_provenance": typed_counts,
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
                "wh_typed_by": it.get("wh_typed_by", ""),
                "fr_publication_expected": it.get("fr_publication_expected"),
                "executive_order_number": it.get("executive_order_number"),
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
        "harvester_version": HARVESTER_VERSION,
        "entity_type": "white_house_statement_act",
        "window": {"start": start, "end": end},
        "count": len(filtered_items),
        "entities": filtered_items,
        "window_stats": win_stats,
        "typing_provenance": typed_counts,
        # Travels with the artifact so no downstream step can lose it.
        "source_constraints": ACTOR_RECORD_CONSTRAINTS,
        "ruling": "AUTHOR RULING 2026-08-25 — supersedes the 2026-08-13 TOXIC ruling. "
                  "Official actor record: documents its own acts and statements only; "
                  "never corroborates; never validates an underlying claim; executive "
                  "orders, proclamations and memoranda are cited from the Federal Register.",
        "typing_note": "wh_instrument_type is established by the source's own "
                       "subcategory taxonomy, or by the Federal Register subtype "
                       "(signing_date + normalized title, ruling 1.5 method 3), or "
                       "left as an untyped presidential_action. It is NEVER inferred "
                       "from the title. wh_typed_by records which route applied.",
    }
    write_json(filtered_path, filtered_payload)
    logger.info("Wrote filtered entities: %s (count=%d)", filtered_path, len(filtered_items))

    return {
        "source": HARVESTER_ID,
        "harvester_version": HARVESTER_VERSION,
        "entity_count": len(filtered_items),
        "typing_provenance": typed_counts,
        "entities_path": str(filtered_path),
        "raw_path": str(raw_path),
        "log_path": str(log_path or ""),
    }


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Democracy Clock — White House statement-act harvester (v2)")
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
