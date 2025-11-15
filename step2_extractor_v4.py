#!/usr/bin/env python3
"""
extractor_v4.py — unified, builder-agnostic event extractor for Democracy Clock V4

Design goals:
- The builder supplies the system_prompt (PREFACE + CANONICAL protocol).
- This module NEVER edits or reformats the LLM output; it returns it verbatim.
- Verbest logging of exactly-what-we-sent and exactly-what-we-got.
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import logging
import random
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import math
from step2_helper_v4 import setup_logger

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

import random

LOGGER = setup_logger("dc.extractor", level="DEBUG")

# Single place where we define the fallback LLM for ALL builders
_DEFAULT_EXTRACT_MODEL = "builder_default"
_DEFAULT_TEMPERATURE = 0.2

def _ensure_console_logger(logger: logging.Logger) -> None:
    # always have a console handler for extractor
    has_console = False
    for h in logger.handlers:
        if isinstance(h, logging.StreamHandler):
            has_console = True
            break
    if not has_console:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s"))
        logger.addHandler(h)
    # extractor is always loud
    logger.setLevel(logging.DEBUG)

_ensure_console_logger(LOGGER)

# ──────────────────────────────────────────────────────────────────────────────
# Policy-aware debug writer (quiet by default)
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_log_policy(explicit: Optional[str] = None, sample_explicit: Optional[float] = None) -> tuple[str, float]:
    """
    Decide logging policy:
      - never     : never write per-item artifacts
      - failures  : write only on fetch/schema failure (DEFAULT)
      - sample    : write failures + sample of successes (rate via DC_LOG_SAMPLE, default 0.05)
      - always    : write for every item
    Allow per-call overrides or env vars: DC_LOG_POLICY / DC_LOG_SAMPLE
    """
    p = (explicit or os.getenv("DC_LOG_POLICY") or "failures").strip().lower()
    if p not in ("never", "failures", "sample", "always"):
        p = "failures"
    try:
        s = float(sample_explicit if sample_explicit is not None else (os.getenv("DC_LOG_SAMPLE") or 0.05))
    except Exception:
        s = 0.05
    s = max(0.0, min(1.0, s))
    return p, s

class _DebugWriter:
    """
    Policy-aware artifact writer. All writes go through here.
    """
    def __init__(self, base_path: Path, *, policy: str, sample_rate: float):
        self.base = base_path
        self.policy = policy
        self.sample_rate = sample_rate
        self.sample_hit = (random.random() < sample_rate) if policy == "sample" else False

    def _should(self, *, is_failure: bool, force: bool = False) -> bool:
        if self.policy == "never":
            return False
        if force:
            return True
        if self.policy == "always":
            return True
        if self.policy == "failures":
            return is_failure
        if self.policy == "sample":
            return is_failure or self.sample_hit
        return False

    def text(self, suffix: str, txt: str, *, is_failure: bool = False, force: bool = False):
        if not self._should(is_failure=is_failure, force=force):
            return
        p = self.base.with_suffix(suffix)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(txt or "", encoding="utf-8")
        except Exception:
            pass

    def json(self, suffix: str, obj: dict, *, is_failure: bool = False, force: bool = False):
        if not self._should(is_failure=is_failure, force=force):
            return
        p = self.base.with_suffix(suffix)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

# ---------- NEW: schema retry helpers ----------

def _needs_schema_retry(out_text: str) -> bool:
    if not out_text or not out_text.strip():
        return True
    comp = _preparse_compliance_scan(out_text)
    if not comp.get("has_footer_end_of_log", False):
        return True
    if comp.get("count_category_lines", 0) == 0:
        return True
    if comp.get("count_why_lines", 0) == 0:
        return True
    # NEW: we now always ask the model to emit "Attacks: [...]" for every event
    if comp.get("count_attacks_lines", 0) == 0:
        return True
    return False

def _retry_append_instruction(messages: List[Dict]) -> List[Dict]:
    """
    Append a single, strict user instruction to re-emit output exactly per schema.
    We do NOT alter the original system prompt or original user ARTICLE_TEXT payload.
    """
    retry_user = (
        "Your previous output failed the Canonical Extraction Protocol.\n"
        "Re-emit the ENTIRE output now EXACTLY per schema with:\n"
        "1) BEGIN LOG / END OF LOG delimiters\n"
        "2) For every event: a 'Category:' line and a 'Why Relevant:' line\n"
        "3) Plain TEXT only (no JSON, no code fences, no commentary)\n"
        "Do not summarize. Do not explain. Output the corrected log only."
    )
    return [*messages, {"role": "user", "content": retry_user}]

def _infer_issue(pre: Dict[str, Any], comp: Dict[str, Any]) -> str:
    approx_room = pre.get("length_estimates", {}).get("approx_room_for_output_tokens")
    has_footer  = comp.get("has_footer_end_of_log", False)
    cat         = comp.get("count_category_lines", 0)
    why         = comp.get("count_why_lines", 0)

    # If we’re missing footer and have almost no room left, assume truncation
    if not has_footer and approx_room is not None and approx_room < 200:
        return "likely_truncation_or_token_budget"
    # If footer present but schema lines are missing, it’s schema drift
    if has_footer and (cat == 0 or why == 0):
        return "schema_drift_missing_fields"
    # If nothing schema-like was detected at all
    if (cat + why) == 0 and not has_footer:
        return "no_schema_fields_detected"
    return "unknown_or_ok"

def _approx_tokens_from_chars(n_chars: int) -> int:
    # Conservative heuristic; adjust if you know your model’s tokenizer
    return max(1, math.ceil(n_chars / 4))

def _safe_head_tail(txt: str, head: int = 800, tail: int = 600) -> Dict[str, str]:
    return {
        "head": txt[:head],
        "tail": txt[-tail:] if len(txt) > tail else txt,
    }

_EVENT_TITLE_RE = re.compile(r"^\s*\d{4}-\d{2}-\d{2}\s+—\s+.+$", re.M)

def _preparse_compliance_scan(text: str) -> Dict[str, Any]:
    if not text:
        return {
            "has_footer_end_of_log": False,
            "has_total_events": False,
            "count_category_lines": 0,
            "count_why_lines": 0,
            "count_attacks_lines": 0,
            "detected_event_blocks": 0,
        }
    has_footer = "[END OF LOG]" in text
    has_total  = bool(re.search(r"Total events found:\s*\[\d+\]", text))
    cat_count  = len(re.findall(r"^Category:\s*.+$", text, re.M))
    why_count  = len(re.findall(r"^Why Relevant:\s*.+$", text, re.M))
    attacks_count = len(re.findall(r"^Attacks:\s*.+$", text, re.M))
    block_est  = len(_EVENT_TITLE_RE.findall(text))
    return {
        "has_footer_end_of_log": has_footer,
        "has_total_events": has_total,
        "count_category_lines": cat_count,
        "count_why_lines": why_count,
        "count_attacks_lines": attacks_count,
        "detected_event_blocks": block_est,
    }

def _write_json_debug(path: Optional[Path], payload: Dict[str, Any]) -> None:
    if not path:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # don’t let debug writing crash the flow
        pass

def _sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _write_text(path: Path, text: str) -> None:
    _ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text if text is not None else "")

def _write_json(path: Path, obj: dict) -> None:
    _ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _now_stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


# -----------------------------------------------------------------------------
# Fetch helpers (HTML → clean text)
# -----------------------------------------------------------------------------

def _fetch_substack_transcript_text(transcript_url: str, timeout: int = 30) -> str:
    """
    Best-effort: fetch a Substack transcript JSON and flatten all 'text' fields to plain text.
    Returns '' on failure.
    """
    try:
        r = requests.get(transcript_url, timeout=timeout)
        if r.status_code != 200:
            return ""
        try:
            j = r.json()
        except Exception:
            return ""
        parts: List[str] = []
        def _collect(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, str) and k.lower() == "text":
                        parts.append(v.strip())
                    else:
                        _collect(v)
            elif isinstance(obj, list):
                for it in obj:
                    _collect(it)
        _collect(j)
        return "\n".join(p for p in parts if p)
    except Exception:
        return ""

def fetch_article_text(url: str, timeout: int = 30) -> Tuple[str, int]:
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    raw_html = ""
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        status = r.status_code
        raw_html = r.text or ""
    except Exception as e:
        LOGGER.error("fetch_article_text: EXCEPTION for url=%s err=%s", url, e)
        return "", 0

    LOGGER.debug("fetch_article_text: url=%s status=%s raw_bytes=%s", url, status, len(raw_html))

    text = ""
    winning_selector = None
    if status == 200 and raw_html:
        soup = BeautifulSoup(raw_html, "html.parser")

        # DemocracyDocket first
        selectors = [
            "div.single-post-content-main",     # main DD body
            "div.single-post__content-main",
            "div.single-post__content-center",
            "div.single-post__content",
            "article.post",                     # generic WP
            "article.prose",
            "div.post-body",
            "div.newsletter-body",
            "article",
            "div.body",
            "div#content",
        ]

        for sel in selectors:
            node = soup.select_one(sel)
            if node:
                cand = node.get_text("\n", strip=True)
                if len(cand) > len(text):
                    text = cand
                    winning_selector = sel

        # fallback to whole page if we still have a tiny body
        if len(text) < 500:
            full = soup.get_text("\n", strip=True)
            if len(full) > len(text):
                text = full
                winning_selector = winning_selector or "<full-page>"

        LOGGER.debug(
            "fetch_article_text: selected_len=%s selector=%s url=%s",
            len(text), winning_selector, url
        )

        # if body still looks suspicious, dump HTML head
        if len(text) < 500:
            dbg_dir = Path(os.getenv("DC_LOG_DIR") or "artifacts/log")
            dbg_dir.mkdir(parents=True, exist_ok=True)
            dbg_file = dbg_dir / "fetch_debug_democracydocket.txt"
            dbg_file.write_text(
                f"URL: {url}\nSTATUS: {status}\nSELECTOR: {winning_selector}\nCLEAN_LEN: {len(text)}\n\nHTML_HEAD:\n{raw_html[:2000]}\n",
                encoding="utf-8"
            )
            LOGGER.warning(
                "fetch_article_text: short body (%s chars) for url=%s — wrote head to %s",
                len(text), url, dbg_file
            )

    return text, status


# -----------------------------------------------------------------------------
# OpenAI call
# -----------------------------------------------------------------------------

def call_openai(messages: List[Dict], *, model: str, temperature: float, max_tokens: int) -> Tuple[str, Optional[str]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=messages,
    )
    choice = resp.choices[0]
    text = (choice.message.content or "").strip()
    # Some SDKs expose finish_reason directly; keep defensive fallback
    finish_reason = getattr(choice, "finish_reason", None)
    return text, finish_reason


# -----------------------------------------------------------------------------
# Message construction
# -----------------------------------------------------------------------------

def build_messages(
    *,
    article_url: str,
    article_text: str,
    system_prompt: Optional[str],
    article_title: Optional[str],
    article_date: Optional[str],
) -> List[Dict]:
    """
    Build the (system, user) messages. The builder provides `system_prompt` (PREFACE + CANONICAL).
    We attach URL/title/date and the full ARTICLE_TEXT (truncated at a high cap).
    """
    MAX_CHARS = 200_000  # generous; builders can pre-chunk if needed
    body = (article_text or "")[:MAX_CHARS]

    system_msg = (system_prompt or "").strip()
    if not system_msg:
        system_msg = "You will extract democracy-affecting events from the provided ARTICLE_TEXT. Follow the user message's explicit schema exactly."

    header_lines = []
    if article_date:
        header_lines.append(f"ARTICLE_DATE: {article_date}")
    if article_title:
        header_lines.append(f"ARTICLE_TITLE: {article_title}")
    header_lines.append(f"ARTICLE_URL: {article_url}")
    header = "\n".join(header_lines)

    # 🚫 Stop JSON / code-fence replies cold.
    format_reminder = (
        "FORMAT REMINDER:\n"
        "Return plain TEXT using the exact schema in the Canonical Extraction Protocol.\n"
        "Do NOT use JSON, markdown code fences, or alternative formats.\n"
    )

    user_msg = f"{header}\n\n{format_reminder}\nARTICLE_TEXT:\n{body}"

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


# -----------------------------------------------------------------------------
# Compliance detector (non-blocking; logs only)
# -----------------------------------------------------------------------------

_END_RE = re.compile(r"\[END OF LOG\]\s*$")
_CAT_RE = re.compile(r"^Category:\s*\S", re.IGNORECASE | re.MULTILINE)
_WHY_RE = re.compile(r"^Why Relevant:\s*\S", re.IGNORECASE | re.MULTILINE)

def _log_compliance_warnings(logger: logging.Logger, out_text: str) -> None:
    if not out_text.strip():
        logger.warning("LLM returned empty output.")
        return

    if not _END_RE.search(out_text):
        logger.warning("LLM output is missing the [END OF LOG] footer.")

    if not _CAT_RE.search(out_text):
        logger.warning("LLM output seems to be missing 'Category:' lines.")

    if not _WHY_RE.search(out_text):
        logger.warning("LLM output seems to be missing 'Why Relevant:' lines.")

    # NEW: warn if we didn't get any attacks line(s)
    if not re.search(r'^"?attacks"?\s*:\s*\[?.+?\]?', out_text, re.IGNORECASE | re.M):
        logger.warning("LLM output seems to be missing 'attacks' lines.")


# -----------------------------------------------------------------------------
# Extractors (URL or ready text)
# -----------------------------------------------------------------------------

def extract_events_from_url(
    url: str,
    *,
    system_prompt: Optional[str] = None,
    article_title: Optional[str] = None,
    article_date: Optional[str] = None,
    source_hint: Optional[str] = None,   # retained for compatibility; unused here
    artifacts_root: Optional[str] = None,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    debug_dir: Optional[Path] = None,
    idx: Optional[int] = None,
    # NEW (optional per-call overrides):
    log_policy: Optional[str] = None,
    log_sample_rate: Optional[float] = None,
) -> str:
    """
    1) Fetch article text
    2) Build messages
    3) Call OpenAI (with single schema retry if needed)
    4) Write artifacts based on policy (defaults to failures-only)
    5) Return raw LLM text (unaltered)
    """
    model = model or os.getenv("OPENAI_MODEL_EVENTS", "gpt-4o-mini")
    temperature = 0.0 if temperature is None else temperature
    max_tokens = max_tokens or 6000

    default_log_dir = Path(artifacts_root or "artifacts") / "log"
    out_dir = Path(debug_dir) if debug_dir else default_log_dir
    _ensure_dir(out_dir)

    # 1) fetch
    art_text, rc = fetch_article_text(url)
    LOGGER.debug("extract_events_from_url: after fetch len=%s rc=%s url=%s", len(art_text), rc, url)

    # 2) decide log policy
    _pol, _samp = _resolve_log_policy(log_policy, log_sample_rate)

    # 3) build per-item base
    stamp = _now_stamp()
    ix = idx if idx is not None else 0
    base = out_dir / f"extract_{stamp}_idx{ix}"

    LOGGER.debug(
        "extract_events_from_url: debug artifacts base=%s policy=%s sample=%s",
        base, _pol, _samp
    )

    dbg = _DebugWriter(base, policy=_pol, sample_rate=_samp)

    logger = LOGGER
    logger.info(
        "Fetched article: url=%s rc=%s chars=%s sha1=%s",
        url, rc, len(art_text), _sha1(art_text)
    )
    LOGGER.debug("extract_events_from_url: log_policy=%s sample=%s", _pol, _samp)
    dbg = _DebugWriter(base, policy=_pol, sample_rate=_samp)

    # Persist minimal fetch meta (policy-gated)
    fetch_meta = {
        "url": url, "article_title": article_title, "article_date": article_date,
        "system_prompt_present": bool(system_prompt), "model": model,
        "temperature": temperature, "max_tokens": max_tokens,
        "fetch_rc": rc, "article_text_chars": len(art_text),
        "article_text_sha1": _sha1(art_text),
    }
    dbg.json(".llm_request.json", fetch_meta, is_failure=(rc != 200 or not art_text.strip()))
    dbg.text(".system_prompt.txt", system_prompt or "", is_failure=(rc != 200 or not art_text.strip()))
    dbg.text(".article_text.txt", art_text, is_failure=(rc != 200 or not art_text.strip()))

    if rc != 200 or not art_text.strip():
        fail_msg = f"(Fetch failed for {url} with RC={rc})"
        user_msg = f"ARTICLE_DATE: {article_date}\nARTICLE_TITLE: {article_title}\nARTICLE_URL: {url}\n\nARTICLE_TEXT:\n"
        dbg.text(".user_message.txt", user_msg, is_failure=True, force=True)
        dbg.text(".llm_response.txt", fail_msg, is_failure=True, force=True)
        dbg.json(".debug.json", {
            "ts": stamp,
            "phase": "precall_fetch_failed",
            "request": {
                "system_prompt_chars": len(system_prompt or ""),
                **{f"system_prompt_{k}": v for k, v in _safe_head_tail(system_prompt or "", 400, 400).items()},
                "user_message_head": user_msg[:800],
            },
            "fetch": {"rc": rc, "article_text_chars": len(art_text)},
            "response": {"text_head": fail_msg[:600], "chars": len(fail_msg)},
        }, is_failure=True, force=True)
        return fail_msg

    messages = build_messages(
        article_url=url,
        article_text=art_text,
        system_prompt=system_prompt,
        article_title=article_title,
        article_date=article_date,
    )
    user_msg = next((m.get("content") or "" for m in messages if m.get("role") == "user"), "")

    LOGGER.debug(
        "extract_events_from_text: built messages for url=%s | user_chars=%s | system_chars=%s",
        url, len(user_msg), len((system_prompt or "")),
    )

    system_chars = len((system_prompt or "").strip())
    user_chars   = len(user_msg)
    approx_in    = _approx_tokens_from_chars(system_chars + user_chars)
    provider_cap = 16000
    approx_room  = max(0, provider_cap - approx_in - (max_tokens or 0))

    pre = {
        "ts_start": time.time(),
        "model_call": {"model": model, "temperature": temperature, "max_tokens": max_tokens},
        "prompt_payload": {
            "system_prompt_chars": system_chars,
            "user_prompt_chars": user_chars,
            **{f"system_prompt_{k}": v for k, v in _safe_head_tail(system_prompt or "", 400, 400).items()},
            **{f"user_prompt_{k}": v for k, v in _safe_head_tail(user_msg, 800, 400).items()},
        },
        "length_estimates": {
            "approx_input_tokens": approx_in,
            "approx_room_for_output_tokens": approx_room,
        },
        "meta": {"article_title": article_title, "article_date": article_date, "url": url},
    }
    dbg.json(".debug.json", {"phase": "precall", **pre}, is_failure=False)

    out_text, finish_reason = call_openai(
        messages=messages,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    dbg.text(".user_message.txt", user_msg, is_failure=False)
    dbg.text(".llm_response.txt", out_text, is_failure=False)

    elapsed = time.time() - pre["ts_start"]
    compliance = _preparse_compliance_scan(out_text or "")
    post = {
        "phase": "postcall",
        "elapsed_sec": round(elapsed, 3),
        "response_raw": {
            "finish_reason": finish_reason,
            "response_chars": len(out_text or ""),
            **{f"response_{k}": v for k, v in _safe_head_tail(out_text or "", 600, 400).items()},
        },
        "compliance_scan_preparse": compliance,
        "automatic_inference": _infer_issue(pre, compliance),
        "attempt": 1,
    }
    is_schema_fail = _needs_schema_retry(out_text or "")
    dbg.json(".debug.json", {**pre, **post}, is_failure=is_schema_fail)

    logger = LOGGER
    _log_compliance_warnings(logger, out_text or "")

    # Retry once on schema failure
    need_retry = (out_text or "").strip() == "RETRY_SCHEMA" or is_schema_fail
    if need_retry:
        messages_retry = _retry_append_instruction(messages)
        out_text_retry, finish_reason_retry = call_openai(
            messages=messages_retry,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        dbg.text(".llm_response_retry1.txt", out_text_retry or "", is_failure=True)

        compliance_r = _preparse_compliance_scan(out_text_retry or "")
        post_r = {
            "phase": "postcall",
            "elapsed_sec": round(time.time() - pre["ts_start"], 3),
            "response_raw_retry1": {
                "finish_reason": finish_reason_retry,
                "response_chars": len(out_text_retry or ""),
                **{f"response_retry1_{k}": v for k, v in _safe_head_tail(out_text_retry or "", 600, 400).items()},
            },
            "compliance_scan_preparse_retry1": compliance_r,
            "automatic_inference_retry1": _infer_issue(pre, compliance_r),
            "attempt": 2,
        }
        dbg.json(".debug.json", {**pre, **post, **post_r}, is_failure=_needs_schema_retry(out_text_retry or ""))

        if not _needs_schema_retry(out_text_retry or ""):
            out_text = (out_text_retry or "").strip()
            finish_reason = finish_reason_retry
            _log_compliance_warnings(logger, out_text or "")

    return (out_text or "").strip()


def extract_events_from_text(
    text: str,
    *,
    system_prompt: Optional[str] = None,
    meta: Optional[dict] = None,
    article_title: Optional[str] = None,
    article_date: Optional[str] = None,
    artifacts_root: Optional[str] = None,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    debug_dir: Optional[Path] = None,
    idx: Optional[int] = None,
    # NEW (optional per-call overrides):
    log_policy: Optional[str] = None,
    log_sample_rate: Optional[float] = None,
) -> str:
    """
    Extract events from provided TEXT. Writes artifacts per logging policy.
    Returns LLM output verbatim.
    """
    if not (text or "").strip():
        return "(no text to extract)"

    model = model or os.getenv("OPENAI_MODEL_EVENTS", "gpt-4o-mini")
    temperature = 0.0 if temperature is None else temperature
    max_tokens = max_tokens or 6000

    url = (meta or {}).get("url") or (meta or {}).get("canonical_url") or "(text source)"
    a_title = article_title or (meta or {}).get("title")
    a_date  = article_date or (meta or {}).get("post_date")

    default_log_dir = Path(artifacts_root or "artifacts") / "log"
    out_dir = Path(debug_dir) if debug_dir else default_log_dir
    _ensure_dir(out_dir)
    LOGGER.debug("extract_events_from_text: writing to %s", out_dir)

    messages = build_messages(
        article_url=url,
        article_text=text,
        system_prompt=system_prompt,
        article_title=a_title,
        article_date=a_date,
    )
    user_msg = next((m.get("content") or "" for m in messages if m.get("role") == "user"), "")

    stamp = _now_stamp()
    ix = idx if idx is not None else (meta or {}).get("_idx") or 0
    base = out_dir / f"extract_{stamp}_idx{ix}"
    _pol, _samp = _resolve_log_policy(log_policy, log_sample_rate)
    LOGGER.debug("extract_events_from_text: log_policy=%s sample=%s base=%s", _pol, _samp, base)
    dbg = _DebugWriter(base, policy=_pol, sample_rate=_samp)

    system_chars = len((system_prompt or "").strip())
    user_chars   = len(user_msg)
    approx_in    = _approx_tokens_from_chars(system_chars + user_chars)
    provider_cap = 16000
    approx_room  = max(0, provider_cap - approx_in - (max_tokens or 0))

    pre = {
        "ts_start": time.time(),
        "model_call": {"model": model, "temperature": temperature, "max_tokens": max_tokens},
        "prompt_payload": {
            "system_prompt_chars": system_chars,
            "user_prompt_chars": user_chars,
            **{f"system_prompt_{k}": v for k, v in _safe_head_tail(system_prompt or "", 400, 400).items()},
            **{f"user_prompt_{k}": v for k, v in _safe_head_tail(user_msg, 800, 400).items()},
        },
        "length_estimates": {
            "approx_input_tokens": approx_in,
            "approx_room_for_output_tokens": approx_room,
        },
        "meta": {"article_title": a_title, "article_date": a_date, "url": url},
    }
    dbg.json(".debug.json", {"phase": "precall", **pre}, is_failure=False)

    out_text, finish_reason = call_openai(
        messages=messages,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    dbg.json(".llm_request.json", {
        "url": url,
        "article_title": a_title,
        "article_date": a_date,
        "system_prompt_present": bool(system_prompt),
        "system_prompt_sha1": _sha1(system_prompt or ""),
        "system_prompt_chars": system_chars,
        "article_text_sha1": _sha1(text),
        "article_text_chars": len(text),
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }, is_failure=False)
    dbg.text(".system_prompt.txt", system_prompt or "", is_failure=False)
    dbg.text(".article_text.txt", text, is_failure=False)
    dbg.text(".user_message.txt", user_msg, is_failure=False)
    dbg.text(".llm_response.txt", out_text or "", is_failure=False)

    elapsed = time.time() - pre["ts_start"]
    compliance = _preparse_compliance_scan(out_text or "")
    post = {
        "phase": "postcall",
        "elapsed_sec": round(elapsed, 3),
        "response_raw": {
            "finish_reason": finish_reason,
            "response_chars": len(out_text or ""),
            **{f"response_{k}": v for k, v in _safe_head_tail(out_text or "", 600, 400).items()},
        },
        "compliance_scan_preparse": compliance,
        "automatic_inference": _infer_issue(pre, compliance),
        "attempt": 1,
    }
    is_schema_fail = _needs_schema_retry(out_text or "")
    dbg.json(".debug.json", {**pre, **post}, is_failure=is_schema_fail)

    logger = LOGGER
    _log_compliance_warnings(logger, out_text or "")

    if (out_text or "").strip() == "RETRY_SCHEMA" or is_schema_fail:
        messages_retry = _retry_append_instruction(messages)
        out_text_retry, finish_reason_retry = call_openai(
            messages=messages_retry,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        dbg.text(".llm_response_retry1.txt", out_text_retry or "", is_failure=True)

        compliance_r = _preparse_compliance_scan(out_text_retry or "")
        post_r = {
            "phase": "postcall",
            "elapsed_sec": round(time.time() - pre["ts_start"], 3),
            "response_raw_retry1": {
                "finish_reason": finish_reason_retry,
                "response_chars": len(out_text_retry or ""),
                **{f"response_retry1_{k}": v for k, v in _safe_head_tail(out_text_retry or "", 600, 400).items()},
            },
            "compliance_scan_preparse_retry1": compliance_r,
            "automatic_inference_retry1": _infer_issue(pre, compliance_r),
            "attempt": 2,
        }
        dbg.json(".debug.json", {**pre, **post, **post_r}, is_failure=_needs_schema_retry(out_text_retry or ""))

        if not _needs_schema_retry(out_text_retry or ""):
            out_text = (out_text_retry or "").strip()
            finish_reason = finish_reason_retry
            _log_compliance_warnings(logger, out_text or "")

    return (out_text or "").strip()