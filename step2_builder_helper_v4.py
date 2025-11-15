#!/usr/bin/env python3
# builder_helper_v4.py — shared utilities for Step-2 “builders”
from __future__ import annotations

import json
import re
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# ---------- paths / IO ----------

def paths_for_window(
    artifacts_root: Path | str, source: str, start: str, end: str
) -> Tuple[Path, Path, Path]:
    """
    Returns (filtered_json_in, events_json_out, events_txt_out) and ensures
    output directories exist.
    """
    root = Path(artifacts_root)
    jdir  = root / "json"
    ejdir = root / "eventjson"
    edir  = root / "events"
    ejdir.mkdir(parents=True, exist_ok=True)
    edir.mkdir(parents=True, exist_ok=True)

    filtered_in      = jdir  / f"{source}_filtered_{start}_{end}.json"
    events_json_out  = ejdir / f"{source}_events_{start}_{end}.json"
    events_txt_out   = edir  / f"{source}_events_{start}_{end}.txt"
    return filtered_in, events_json_out, events_txt_out


def _ensure_dirs(artifacts_root: Path | str) -> None:
    root = Path(artifacts_root)
    (root / "json").mkdir(parents=True, exist_ok=True)
    (root / "eventjson").mkdir(parents=True, exist_ok=True)
    (root / "events").mkdir(parents=True, exist_ok=True)
    (root / "log").mkdir(parents=True, exist_ok=True)


def _load_filtered(artifacts_root: Path | str, source: str, start: str, end: str, logger=None) -> Dict[str, Any]:
    path = Path(artifacts_root) / "json" / f"{source}_filtered_{start}_{end}.json"
    if not path.exists():
        raise FileNotFoundError(f"Filtered JSON not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if logger:
        n = len(data.get("entities", [])) if isinstance(data, dict) else 0
        logger.info("Loaded filtered payload: %s (entities=%d)", path.name, n)
    # attach path hint so callers can report it
    if isinstance(data, dict):
        data["path"] = str(path)
    return data


def _debug_write_json(path: Path, obj: Any) -> None:
    """
    Debug helper: write JSON if desired. Currently no-op to avoid large artifacts.
    Toggle by removing the early return.
    """
    return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _debug_write_text(path: Path, title: str, body: str) -> None:
    """
    Debug helper: write text if desired. Currently no-op to avoid large artifacts.
    Toggle by removing the early return.
    """
    return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"=== {title} ===\n{body}", encoding="utf-8")


def write_events_json(path: Path, payload: Dict[str, Any], logger=None) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if logger:
        logger.info("Wrote events JSON: %s (events=%d)", path, len(payload.get("events", [])))


def write_events_txt(path: Path, items: List[Dict[str, Any]], logger=None) -> None:
    """
    Human-readable preview. Not the Master Event Log.
    """
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            title = it.get("title", "")
            url = it.get("canonical_url") or it.get("url") or ""
            pd = (it.get("post_date") or it.get("date") or "")[:10]
            src = it.get("publication") or it.get("source") or ""
            summary = (it.get("summary") or "").strip()
            body = (it.get("events_text") or "").strip()

            if title or pd:
                f.write(f"=== {pd} — {title} [Source: {src}]\n")
            if url:
                f.write(url + "\n")
            if summary:
                f.write(f"Summary: {summary}\n\n")
            if body:
                f.write(body.rstrip() + "\n")
            f.write("\n")
    if logger:
        logger.info("Wrote events TXT preview: %s", path)


# ---------- selection / prompt plumbing ----------

def _pick_indices(n_total: int, ids: Optional[List[int]], limit: Optional[int]) -> List[int]:
    """
    Return the zero-based indices to process.
    - If ids provided: clamp to [0, n_total-1].
    - Else: 0..limit-1 (or all if limit is None).
    """
    if ids:
        out = [i for i in ids if isinstance(i, int) and 0 <= i < n_total]
        return out
    if limit is None or limit >= n_total:
        return list(range(n_total))
    return list(range(limit))

# ---------- normalization ----------

def split_summary_and_body(extracted_text: str) -> tuple[str, str]:
    """
    Our extractor prefixes with 'Summary:' line. Return (summary, body).
    """
    if not extracted_text:
        return "", ""
    if extracted_text.startswith("Summary:"):
        lines = extracted_text.splitlines()
        first = lines[0][len("Summary:"):].strip() if lines else ""
        rest = "\n".join(lines[1:]).lstrip()
        return first, rest
    return "", extracted_text


def make_event_record(entity: Dict[str, Any], extracted_text: str) -> Dict[str, Any]:
    """
    Stable, minimal schema every builder can emit.
    """
    summary, body = split_summary_and_body(extracted_text)
    return {
        "source": entity.get("source") or "",
        "publication": entity.get("source") or "",
        "title": entity.get("title") or "",
        "url": entity.get("url") or entity.get("canonical_url") or "",
        "canonical_url": entity.get("canonical_url") or entity.get("url") or "",
        "post_date": entity.get("post_date") or "",
        "doc_type": entity.get("doc_type") or "news_article",
        "raw_line": entity.get("raw_line") or "",
        "summary": summary,
        "events_text": body,
    }


# ---------- LLM output parsing (canonical blocks) ----------

# Match an event header line, with or without leading "==="
# Examples:
#   === 2025-10-20 — Title
#   2025-10-20 — Title
_HEADER_RE = re.compile(r"^(?:===\s*)?(\d{4}-\d{2}-\d{2})\s+—\s+(.+)$", re.M)
_ATK_RE     = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)

def _parse_llm_events_canonical(text: str, *, article_url: str = "", logger=None) -> List[Dict[str, Any]]:
    """
    Parse canonical blocks of the form:

      [optional '==='] YYYY-MM-DD — <Event title>
      [optional direct URL line]
      Summary: ...
      Source: ...
      Category: ...
      Why Relevant: ...
      attacks: [a, b, c]       # optional but preferred
    """
    if not (text or "").strip():
        return []

    events: List[Dict[str, Any]] = []
    headers = list(_HEADER_RE.finditer(text))
    if not headers:
        if logger:
            logger.debug("Parser: no header lines matched. First 200 chars: %r", (text or "")[:200])
        return []

    for idx, m in enumerate(headers):
        start = m.start()
        end   = headers[idx + 1].start() if (idx + 1) < len(headers) else len(text)
        block = text[start:end].strip()

        lines = [ln.rstrip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue

        date_iso = m.group(1).strip()
        title    = m.group(2).strip()

        # Optional direct URL line immediately after header
        i = 1
        url = ""
        if i < len(lines) and (lines[i].startswith("http://") or lines[i].startswith("https://")):
            url = lines[i].strip()
            i += 1

        # Labeled fields
        summary = category = why = ""
        alt_source_line = ""
        attacks_line = ""
        for j in range(i, len(lines)):
            ln = lines[j]
            if ln.startswith("Summary:"):
                summary = ln[len("Summary:"):].strip()
            elif ln.startswith("Category:"):
                category = ln[len("Category:"):].strip()
            elif ln.startswith("Why Relevant:"):
                why = ln[len("Why Relevant:"):].strip()
            elif ln.startswith("Source:"):
                alt_source_line = ln[len("Source:"):].strip()
            elif _ATK_RE.match(ln):
                attacks_line = _ATK_RE.match(ln).group(1).strip()

        if not url and alt_source_line:
            m2 = re.search(r"(https?://\S+)", alt_source_line)
            if m2:
                url = m2.group(1)

        # Normalize attacks to a list of handles
        attacks_list: List[str] = []
        if attacks_line:
            cleaned = attacks_line.strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1].strip()
            for part in re.split(r"[;,]", cleaned):
                h = part.strip().strip('"').strip("'")
                if h:
                    attacks_list.append(h.lower().replace(" ", "_"))

        events.append({
            "date": date_iso,
            "title": title,
            "url": url or article_url,
            "summary": summary,
            "category": category,
            "why_relevant": why,
            "attacks": attacks_list,
        })

        if logger:
            logger.debug(
                "Parser: block ok | date=%s title=%s url=%s summary=%s category=%s why=%s attacks=%s",
                date_iso, title[:120], (url or article_url), bool(summary), category, bool(why), bool(attacks_list)
            )

    if logger:
        logger.debug("Parser: total blocks parsed=%d", len(events))
    return events


# ---------- Final serializer (Step-2 structured JSON) ----------

def serialize_events_structured(
    *,
    source: str,
    start_date_iso: str,
    end_date_iso: str,
    tz: str,
    events_in: List[Dict[str, Any]],
    artifacts_root: str | Path,
) -> str:
    """
    Write the Step-2 structured events payload for a builder and return the path.
    Ensures every event has an 'attacks' list (possibly empty) and normalizes tokens.
    """
    root = Path(artifacts_root)
    out_path = root / "eventjson" / f"{source}_events_{start_date_iso}_{end_date_iso}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    events_out: List[Dict[str, Any]] = []
    for ev in events_in:
        ev2 = dict(ev)  # shallow copy
        raw_attacks = ev2.get("attacks", [])
        parts: List[str] = []
        if isinstance(raw_attacks, str):
            parts = [p.strip() for p in re.split(r"[;,]", raw_attacks) if p.strip()]
        elif isinstance(raw_attacks, (list, tuple)):
            parts = [str(p).strip() for p in raw_attacks if str(p).strip()]
        else:
            parts = []
        # normalize: lowercase + underscores, drop surrounding quotes
        attacks_norm = []
        for token in parts:
            t = token.strip().strip('"').strip("'")
            if not t:
                continue
            attacks_norm.append(t.lower().replace(" ", "_"))
        ev2["attacks"] = attacks_norm

        events_out.append(ev2)

    payload = {
        "source": source,
        "window": {"start": start_date_iso, "end": end_date_iso, "tz": tz},
        "events": events_out,
        "count": len(events_out),
        "version": "v4-step2",
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out_path)

# ---------- logging ----------

def setup_logger(name: str, level: str | int = "INFO", logfile: Path | str | None = None, propagate: bool = False) -> logging.Logger:
    """
    Original verbose logger: always writes to both console and file (if given),
    and shows full DEBUG output when requested.  No environment policy logic.
    """
    if isinstance(level, str):
        lvl = getattr(logging, level.upper(), logging.INFO)
    else:
        lvl = int(level)

    logger = logging.getLogger(name)
    logger.setLevel(lvl)
    logger.propagate = propagate

    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")

    # Console handler
    has_console = any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
                      for h in logger.handlers)
    if not has_console:
        console = logging.StreamHandler()
        console.setLevel(lvl)
        console.setFormatter(fmt)
        logger.addHandler(console)

    # File handler
    if logfile:
        if isinstance(logfile, str):
            logfile = Path(logfile)
        logfile.parent.mkdir(parents=True, exist_ok=True)
        has_file = any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(logfile)
                       for h in logger.handlers)
        if not has_file:
            fh = logging.FileHandler(logfile, encoding="utf-8")
            fh.setLevel(lvl)
            fh.setFormatter(fmt)
            logger.addHandler(fh)

    logger.debug("Logger configured (original verbose mode): name=%s level=%s logfile=%s handlers=%s",
                 name, logging.getLevelName(lvl), logfile,
                 [type(h).__name__ for h in logger.handlers])
    return logger