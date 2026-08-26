#!/usr/bin/env python3
"""
DOJ pre-extraction scope filter.

WHY (2026-08-26)
    justice harvests 541 releases across 83 weeks. Most are ordinary federal
    prosecutions of private individuals: a jail administrator charged with
    assaulting a detainee, a police officer convicted of sexual assault, a man
    who threatened a Sikh nonprofit. Those are the Civil Rights Division working
    CORRECTLY. They are not democracy events, and paying an extractor to read
    each one so the protocol can discard it is waste.

    This filter decides scope BEFORE the LLM is called. It is deliberately
    over-inclusive: the Canonical Extraction Protocol still applies its own
    relevance test downstream, so a marginal keep costs one call, while a wrong
    drop costs an event that never existed.

WHAT IT KEEPS
    Acts bearing on how legal power itself is DIRECTED, per PREFACE_JUSTICE:
      • institutional components (AG / Deputy AG / Associate AG / OLC / Pardon
        Attorney) -- policy, personnel, departmental positions on the law
      • voting and elections matters at any stage
      • national security and civil-liberties enforcement
      • cases ENDING or being redirected -- dismissals, consent decrees, appeals
      • title signals: policy, guidance, nominations, resignations, removals,
        suits against states or officials, and statements by named leadership

WHAT IT DROPS
    Ordinary criminal enforcement by a litigating division with no institutional
    or political dimension.

NOTHING IS SILENTLY DROPPED. Every excluded record is returned with the rule
that excluded it, and the caller is expected to persist that list.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

# Components whose output is institutional by nature: policy, personnel, the
# department's positions on the law. Matched as substrings, case-insensitive.
INSTITUTIONAL_COMPONENTS = (
    "Office of the Attorney General",
    "Office of the Deputy Attorney General",
    "Office of the Associate Attorney General",
    "Office of Legal Counsel",
    "Office of the Pardon Attorney",
    "Office of Public Affairs",
    "Office of Legislative Affairs",
    "National Security Division",
    "Civil Rights - Voting",
    "Voting Section",
)

# Stages that mark a case ENDING or being redirected. A dropped case is a
# decision about how legal power is used; a routine conviction is not.
DIRECTION_STAGES = {
    "case_dismissed",
    "consent_decree_proposed",
    "consent_decree_entered",
    "appeal_filed",
    "investigation_announced",
}

# Title signals. Kept tight -- each must indicate an institutional act rather
# than an ordinary prosecution.
TITLE_KEEP = re.compile(
    r"intends? to nominate|nominat|appoint(?:s|ed|ment)|sworn in|resign|steps? down|"
    r"removed from|dismiss(?:es|ed|al) (?:the )?(?:case|charges|lawsuit|complaint)|"
    r"drops? (?:the )?(?:case|charges|lawsuit)|withdraw|consent decree|"
    r"statement (?:from|by) (?:attorney general|acting attorney general|deputy|"
    r"associate|principal|the justice department|justice department)|"
    r"policy|guidance|memorandum|directive|announces? (?:new|the) (?:policy|initiative|task force)|"
    r"sues? (?:the )?(?:state|city|county|commonwealth)|lawsuit against (?:the )?(?:state|city|county)|"
    r"election|voting rights|redistrict|ballot|census|"
    r"files? statement of interest|amicus|"
    r"civil rights division|office of the attorney general",
    re.I,
)

# Explicit chaff: ordinary criminal matter, no institutional dimension. Only
# consulted when nothing above matched.
TITLE_ROUTINE = re.compile(
    r"sentenced|pleads? guilty|convicted|indicted|charged with|arrested|"
    r"found guilty|admits|agrees to pay|to pay \$|settles? false claims|"
    r"fraud scheme|conspiracy to|money launder|drug traffick|child (?:porn|exploit)|"
    r"firearms|methamphetamine|fentanyl|racketeer",
    re.I,
)


def classify(e: Dict[str, Any]) -> Tuple[bool, str]:
    """Return (in_scope, rule) for one filtered DOJ entity."""
    title = (e.get("title") or "").strip()
    comps = e.get("doj_components") or []
    stage = (e.get("legal_stage") or "").strip()

    for c in comps:
        for needle in INSTITUTIONAL_COMPONENTS:
            if needle.lower() in str(c).lower():
                return True, f"institutional_component:{needle}"

    if stage in DIRECTION_STAGES:
        return True, f"direction_stage:{stage}"

    if TITLE_KEEP.search(title):
        return True, "title_signal"

    if TITLE_ROUTINE.search(title):
        return False, "routine_enforcement"

    # Unmatched: keep. An extractor call is cheaper than a missing event, and the
    # protocol's own relevance test is the second gate.
    return True, "unmatched_default_keep"


def partition(entities: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split entities into (in_scope, excluded). Excluded carry `_scope_rule`."""
    keep, drop = [], []
    for e in entities:
        ok, rule = classify(e)
        e = dict(e)
        e["_scope_rule"] = rule
        (keep if ok else drop).append(e)
    return keep, drop
