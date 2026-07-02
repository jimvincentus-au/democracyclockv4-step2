

#!/usr/bin/env python3
"""
step2_buildscotusorders_v4.py — official SCOTUS orders builder for Democracy Clock V4

Purpose:
- Load filtered official SCOTUS order-list entities from artifacts/json
- Download/read each official order PDF
- Use the canonical extractor to produce case-specific order events
- Write artifacts/eventjson/scotusorders_events_START_END.json

Design:
- The SCOTUS orders index is not enough. It usually says only "Order List" or
  "Miscellaneous Order". The case names, docket numbers, dispositions, and useful
  relevance live inside the PDF. This builder therefore uses the official index as
  discovery/provenance, then extracts structured events from the official PDF text.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_v4 import ARTIFACTS_ROOT
from step2_extractor_v4 import extract_events_from_text
from step2_helper_v4 import setup_logger
from step2_prompts_v4 import compose_system_prompt

TZ_DEFAULT = "Australia/Brisbane"
DEFAULT_SOURCE = "scotusorders"
DEFAULT_CATEGORY = "Courts / Supreme Court / Orders"

_HDR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+—\s+(.*)$")
_SUM_RE = re.compile(r"^Summary:\s*(.+)$", re.IGNORECASE)
_SRC_RE = re.compile(r"^Source:\s*(.+)$", re.IGNORECASE)
_CAT_RE = re.compile(r"^Category:\s*(.+)$", re.IGNORECASE)
_WHY_RE = re.compile(r"^Why Relevant:\s*(.+)$", re.IGNORECASE)
_ATK_RE = re.compile(r'^"?attacks"?\s*:\s*(.+)$', re.IGNORECASE)
_URL_EX = re.compile(r"https?://\S+")

_INFERENTIAL_SUMMARY_STARTS = (
    "this case involves",
    "these cases involve",
    "the case involves",
    "the cases involve",
    "the outcome of this case",
    "the outcome could",
    "this could",
    "these cases concern",
    "this case concerns",
    "the case concerns",
    "which may have implications",
    "which could have implications",
    "potentially affecting",
)

_INFERENTIAL_WHY_PHRASES = (
    "rights of workers",
    "workers' rights",
    "international relations",
    "business relations",
    "institutional accountability",
    "democratic governance",
    "state power",
    "policy implications",
    "could impact",
    "could affect",
    "may impact",
    "may affect",
    "significant implications",
    "ongoing legal battles",
)


def make_event_id(source: str, title: str, url: Optional[str], source_date: str) -> str:
    base = f"{source}|{title.strip()}|{(url or '').strip()}|{source_date}"
    return "sha1:" + hashlib.sha1(base.encode("utf-8")).hexdigest()


def compute_post_date_str(source_date_str: str) -> str:
    d = datetime.strptime(source_date_str, "%Y-%m-%d").date()
    return (d + timedelta(days=1)).isoformat()


def _coerce_iso_date(v: Any) -> Optional[str]:
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v or "").strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _first_text_value(ent: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        value = ent.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _clean_title_piece(value: str) -> str:
    return " ".join(str(value or "").replace("\n", " ").split())


def _strip_revision_marker(value: str) -> str:
    return _REVISION_MARKER_RE.sub("", str(value or "")).strip()


def _event_url(ent: Dict[str, Any]) -> str:
    return _first_text_value(ent, ["canonical_url", "url", "pdf_url", "link", "source_url", "permalink"])


def _event_date(ent: Dict[str, Any], fallback: str) -> str:
    for key in ["post_date", "order_date", "date", "published", "published_at", "source_date"]:
        iso = _coerce_iso_date(ent.get(key))
        if iso:
            return iso
    return fallback[:10]


def _order_type(ent: Dict[str, Any]) -> str:
    raw = _first_text_value(ent, ["order_type", "type", "section", "doc_type", "category"])
    clean = raw.lower().strip().replace("-", "_").replace(" ", "_")
    if "misc" in clean:
        return "miscellaneous_order"
    if "court" in clean or "orders_of_the_court" in clean:
        return "orders_of_the_court"
    if clean:
        return clean

    title = _first_text_value(ent, ["title", "label", "name", "row_text"])
    title_lower = title.lower()
    if "miscellaneous" in title_lower:
        return "miscellaneous_order"
    if "orders of the court" in title_lower or "order list" in title_lower:
        return "orders_of_the_court"
    return "supreme_court_order"


def _display_order_label(ent: Dict[str, Any]) -> str:
    order_type = _order_type(ent)
    if order_type == "miscellaneous_order":
        return "Miscellaneous Order"
    if order_type == "orders_of_the_court":
        return "Orders of the Court"
    return "Supreme Court Order"


def _source_title(ent: Dict[str, Any], source_date: str) -> str:
    raw = _clean_title_piece(_strip_revision_marker(_first_text_value(ent, ["title", "label", "name", "row_text"])))
    if raw.startswith("Miscellaneous Order: Miscellaneous Order"):
        return f"Miscellaneous Order ({source_date})"
    if raw.startswith("Orders of the Court: Orders of the Court"):
        return f"Orders of the Court ({source_date})"
    return raw or f"{_display_order_label(ent)} ({source_date})"


def _load_filtered_list(source: str, start: str, end: str, artifacts: Path) -> List[Dict[str, Any]]:
    json_dir = artifacts / "json"
    exact_path = json_dir / f"{source}_filtered_{start}_{end}.json"

    def _extract_items(path: Path) -> List[Dict[str, Any]]:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            data = payload.get("entities") or payload.get("items") or payload.get("events") or []
            return data if isinstance(data, list) else []
        return payload if isinstance(payload, list) else []

    def _item_date(ent: Dict[str, Any]) -> str:
        for key in ["post_date", "order_date", "date", "published", "published_at", "source_date"]:
            iso = _coerce_iso_date(ent.get(key))
            if iso:
                return iso
        return ""

    def _in_window(ent: Dict[str, Any]) -> bool:
        iso = _item_date(ent)
        return bool(iso and start <= iso <= end)

    if exact_path.exists():
        exact_items = _extract_items(exact_path)
        if exact_items:
            return exact_items

    combined: List[Dict[str, Any]] = []
    seen = set()
    for path in sorted(json_dir.glob(f"{source}_filtered_*.json")):
        for ent in _extract_items(path):
            if not isinstance(ent, dict):
                continue
            if not _in_window(ent):
                continue
            key = (_event_url(ent), _first_text_value(ent, ["title", "label", "name"]), _item_date(ent))
            if key in seen:
                continue
            seen.add(key)
            combined.append(ent)

    return combined


def _download_pdf(url: str, artifacts: Path, source: str) -> Path:
    cache_dir = artifacts / "pdfcache" / source
    cache_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    path = cache_dir / f"{digest}.pdf"
    if path.exists() and path.stat().st_size > 0:
        return path

    req = urllib.request.Request(url, headers={"User-Agent": "DemocracyClock/1.0"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        data = resp.read()
    if not data:
        raise RuntimeError("empty_pdf_download")
    path.write_bytes(data)
    return path


def _extract_pdf_text(pdf_path: Path, max_pages: int) -> str:
    errors: List[str] = []

    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        chunks = []
        for page in reader.pages[:max_pages]:
            chunks.append(page.extract_text() or "")
        text = "\n".join(chunks).strip()
        if text:
            return text
    except Exception as e:
        errors.append(f"pypdf:{e}")

    try:
        from PyPDF2 import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        chunks = []
        for page in reader.pages[:max_pages]:
            chunks.append(page.extract_text() or "")
        text = "\n".join(chunks).strip()
        if text:
            return text
    except Exception as e:
        errors.append(f"PyPDF2:{e}")

    raise RuntimeError("pdf_text_unavailable: " + " | ".join(errors))


def _build_extraction_text(ent: Dict[str, Any], *, source_date: str, pdf_url: str, pdf_text: str, max_chars: int) -> str:
    order_label = _display_order_label(ent)
    source_title = _source_title(ent, source_date)
    pdf_text = pdf_text.strip()[:max_chars]

    return (
        f"Official source: Supreme Court of the United States\n"
        f"Official document type: {order_label}\n"
        f"Official document title: {source_title}\n"
        f"Official document date: {source_date}\n"
        f"Official PDF URL: {pdf_url}\n\n"
        "Task: Extract useful, case-specific Democracy Clock events from the official Supreme Court order text below.\n"
        "Do not create a generic event saying merely that the Court released an order list.\n"
        "For each case-specific order, identify the case name, docket number if present, the disposition or procedural action, "
        "and the narrow procedural significance of that order. Keep summaries procedural; do not add background about the legal issue "
        "unless the PDF text itself states it.\n"
        "If the text contains routine denials or administrative items with no Democracy Clock relevance, omit them.\n\n"
        "Official order text:\n"
        f"{pdf_text}"
    )


# --- Helper functions for inferential summary/why cleaning ---
def _split_sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[.!?])\s+", str(text or "").strip())
    return [p.strip() for p in parts if p.strip()]


def _looks_inferential_sentence(sentence: str) -> bool:
    s = sentence.strip().lower()
    return any(s.startswith(prefix) or prefix in s for prefix in _INFERENTIAL_SUMMARY_STARTS)


def _clean_order_summary(summary: str) -> str:
    """Keep SCOTUS-order summaries procedural and PDF-visible.

    The extractor often appends background such as "this case involves..." or
    "could have implications...". That belongs outside the source layer unless
    the PDF itself states it. Keep the disposition/action sentence(s), drop the
    inferential tail.
    """
    sentences = _split_sentences(summary)
    if not sentences:
        return ""

    kept: List[str] = []
    for sentence in sentences:
        if _looks_inferential_sentence(sentence):
            continue
        kept.append(sentence)

    if not kept:
        kept = sentences[:1]

    return " ".join(kept).strip()


def _clean_order_why(why: str, summary: str) -> str:
    """Make Why Relevant conservative for official SCOTUS orders.

    Avoid broad policy characterization unless the extracted text supports it
    directly. Prefer procedural significance: execution allowed to proceed,
    federal government participation, briefing schedule paused, divided argument
    granted, emergency relief granted/denied, or review denied.
    """
    why_clean = str(why or "").strip()
    why_lower = why_clean.lower()
    if why_clean and not any(phrase in why_lower for phrase in _INFERENTIAL_WHY_PHRASES):
        return why_clean

    summary_lower = str(summary or "").lower()
    if "stay of execution" in summary_lower or "death sentence" in summary_lower or "execution" in summary_lower:
        return "The order is relevant because the Court declined to intervene in a capital case, allowing the execution-related ruling to stand."
    if "acting solicitor general" in summary_lower or "solicitor general" in summary_lower:
        return "The order is relevant because it records the federal government's formal participation in Supreme Court argument."
    if "briefing schedule" in summary_lower and "abeyance" in summary_lower:
        return "The order is relevant because it changes the procedural timeline in a case involving a federal agency or federal official."
    if "divided argument" in summary_lower:
        return "The order is relevant because it changes how argument time is allocated in a pending Supreme Court case."
    if "certiorari" in summary_lower or "writ of certiorari" in summary_lower:
        return "The order is relevant because it records the Court's decision not to review the lower-court judgment."
    if "stay" in summary_lower:
        return "The order is relevant because it grants or denies interim relief while litigation continues."
    if "motion" in summary_lower:
        return "The order is relevant because it resolves a procedural motion in a pending Supreme Court matter."
    return "The order is relevant as an official Supreme Court procedural action in a pending case."


def _parse_llm_events_canonical(text: str, *, article_url: str, logger=None) -> List[Dict[str, Any]]:
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    blocks: List[List[str]] = []
    cur: List[str] = []

    for ln in lines:
        if _HDR_RE.match(ln):
            if cur:
                blocks.append(cur)
            cur = [ln]
        elif cur:
            cur.append(ln)
    if cur:
        blocks.append(cur)

    events: List[Dict[str, Any]] = []
    for bidx, block in enumerate(blocks, 1):
        date_s = title = summary = cat = why = ""
        attacks_line = ""
        url = ""

        header_match = _HDR_RE.match(block[0])
        if header_match:
            date_s = header_match.group(1).strip()
            title = header_match.group(2).strip()

        for ln in block[1:]:
            if not ln.strip():
                continue

            summary_match = _SUM_RE.match(ln)
            if summary_match:
                summary = summary_match.group(1).strip()
                continue

            source_match = _SRC_RE.match(ln)
            if source_match:
                src_line = source_match.group(1).strip()
                u = _URL_EX.search(src_line)
                url = u.group(0) if u else ""
                continue

            category_match = _CAT_RE.match(ln)
            if category_match:
                cat = category_match.group(1).strip()
                continue

            why_match = _WHY_RE.match(ln)
            if why_match:
                why = why_match.group(1).strip()
                continue

            attacks_match = _ATK_RE.match(ln)
            if attacks_match:
                attacks_line = attacks_match.group(1).strip()
                continue

        if not url:
            url = article_url or ""

        sources = [url] if url else []
        if article_url and article_url not in sources:
            sources.append(article_url)

        if logger:
            if not summary:
                logger.warning("Block %d missing Summary", bidx)
            if not why:
                logger.warning("Block %d missing Why Relevant", bidx)

        attacks_list: List[str] = []
        if attacks_line:
            cleaned = attacks_line.strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1].strip()
            for part in re.split(r"[;,]", cleaned):
                h = part.strip().strip('"').strip("'")
                if h:
                    attacks_list.append(h.lower().replace(" ", "_"))

        clean_summary = _clean_order_summary(summary)
        clean_why = _clean_order_why(why, clean_summary)

        events.append({
            "source_date": date_s,
            "title": title,
            "url": url,
            "summary": clean_summary,
            "why_relevant": clean_why,
            "category": DEFAULT_CATEGORY,
            "sources": sources,
            "tags": [],
            "attacks": [],
            "llm_attacks_suggested": attacks_list,
        })

    return events


def run_builder(
    *,
    source: str,
    start: str,
    end: str,
    artifacts_root: str | Path = ARTIFACTS_ROOT,
    level: str = "INFO",
    log_path: Optional[str] = None,
    limit: Optional[int] = None,
    ids: Optional[List[int]] = None,
    skip_existing: bool = False,
    max_pages: int = 20,
    max_chars: int = 50000,
) -> Dict[str, Any]:
    artifacts = Path(artifacts_root)
    (artifacts / "eventjson").mkdir(parents=True, exist_ok=True)
    (artifacts / "log").mkdir(parents=True, exist_ok=True)

    logger = setup_logger(f"dc.build.{source}", level, Path(log_path) if log_path else None)

    out_json = artifacts / "eventjson" / f"{source}_events_{start}_{end}.json"
    if skip_existing and out_json.exists():
        logger.info("Skipping existing output: %s", out_json)
        return {"source": source, "events": -1, "path": str(out_json), "skipped": True}

    filtered_path = artifacts / "json" / f"{source}_filtered_{start}_{end}.json"
    items = _load_filtered_list(source, start, end, artifacts)
    if filtered_path.exists():
        logger.info("Loaded %d official order entities for requested window; exact file exists at %s", len(items), filtered_path)
    else:
        logger.info(
            "Loaded %d official order entities for requested window via fallback scan; exact file absent at %s",
            len(items),
            filtered_path,
        )

    idxs = list(range(len(items))) if not ids else [i - 1 for i in ids if 1 <= i <= len(items)]
    if limit:
        idxs = idxs[:limit]

    system_prompt = compose_system_prompt(source, include_attacks=True)
    system_prompt += (
        "\n\nSCOTUS ORDERS SPECIAL RULES:\n"
        "Use the official order PDF text, not merely the order-list metadata.\n"
        "Extract case-specific events only. Do not emit a generic order-list event.\n"
        "Each event title must name the case or proceeding when the PDF provides one.\n"
        "Each Summary must state the disposition/action shown in the PDF.\n"
        "Each Why Relevant must be narrow and procedural: e.g. review denied, execution stay denied, briefing paused, argument participation granted, divided argument granted, or emergency relief granted/denied.\n"
        "Do not add external issue background such as employment law, international arbitration, environmental policy, education policy, workers' rights, state power, or institutional accountability unless those words or ideas are explicit in the PDF text.\n"
        "Prefer a short Why Relevant over a speculative one.\n"
        "Do not infer factual background, legal issues, parties' motivations, or policy consequences not visible in the PDF text.\n"
        "Do not assign attacks tags for SCOTUS orders; leave attacks empty.\n"
        f"Use this category exactly: {DEFAULT_CATEGORY}.\n"
    )

    all_events: List[Dict[str, Any]] = []
    noncompliant: List[Dict[str, Any]] = []

    if not idxs:
        noncompliant.append({
            "idx": None,
            "url": "",
            "reason": "no_input_entities_for_requested_window",
            "message": f"No {source} filtered entities were available for {start} → {end}.",
        })

    for run_idx, item_idx in enumerate(idxs, 1):
        ent = items[item_idx]
        pdf_url = _event_url(ent)
        source_date = _event_date(ent, start)
        source_title = _source_title(ent, source_date)
        logger.info("[%s] extracting %d/%d idx=%d title=%s", source, run_idx, len(idxs), item_idx, source_title[:120])

        if not pdf_url:
            noncompliant.append({"idx": item_idx, "url": "", "reason": "missing_pdf_url", "source_record": ent})
            continue

        try:
            pdf_path = _download_pdf(pdf_url, artifacts, source)
            pdf_text = _extract_pdf_text(pdf_path, max_pages=max_pages)
            extraction_text = _build_extraction_text(
                ent,
                source_date=source_date,
                pdf_url=pdf_url,
                pdf_text=pdf_text,
                max_chars=max_chars,
            )
            llm_text = extract_events_from_text(
                extraction_text,
                system_prompt=system_prompt,
                artifacts_root=str(artifacts),
                idx=item_idx,
            )
        except Exception as e:
            logger.exception("Extraction failed idx=%d url=%s", item_idx, pdf_url)
            noncompliant.append({
                "idx": item_idx,
                "url": pdf_url,
                "reason": "pdf_or_llm_extraction_failed",
                "error": str(e),
                "source_record": ent,
            })
            continue

        llm_out_path = artifacts / "log" / f"{source}_llm_out_idx{item_idx}_{start}_{end}.txt"
        with open(llm_out_path, "w", encoding="utf-8") as f:
            f.write(llm_text)

        parsed_events = _parse_llm_events_canonical(llm_text, article_url=pdf_url, logger=logger)
        if not parsed_events:
            noncompliant.append({
                "idx": item_idx,
                "url": pdf_url,
                "reason": "no_case_specific_events_extracted",
                "source_title": source_title,
                "source_record": ent,
            })
            continue

        for ev in parsed_events:
            ev["source"] = source
            ev["category"] = DEFAULT_CATEGORY
            ev.setdefault("tags", [])
            for tag in [source, "supreme_court", "official_record", "order", _order_type(ent)]:
                if tag and tag not in ev["tags"]:
                    ev["tags"].append(tag)
            ev["attacks"] = []
            ev["source_pdf_url"] = pdf_url
            ev["source_order_title"] = source_title
            ev["order_type"] = _order_type(ent)
            ev["source_record"] = ent
            ev["event_id"] = make_event_id(source, ev.get("title", ""), ev.get("url") or pdf_url, ev.get("source_date", source_date))
            all_events.append(ev)

    out_payload = {
        "source": source,
        "window": {"start": start, "end": end, "tz": TZ_DEFAULT},
        "events": all_events,
        "noncompliant": noncompliant,
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, ensure_ascii=False, indent=2)

    logger.info("Wrote %s (events=%d noncompliant=%d)", out_json, len(all_events), len(noncompliant))
    return {"source": source, "events": len(all_events), "path": str(out_json), "noncompliant": len(noncompliant)}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Democracy Clock V4 — official SCOTUS orders PDF extractor")
    ap.add_argument("--source", default=DEFAULT_SOURCE, help=f"Source name (default: {DEFAULT_SOURCE})")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--level", default="INFO")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--ids", type=int, nargs="+")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--max-chars", type=int, default=50000)
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    run_builder(
        source=args.source,
        start=args.start,
        end=args.end,
        artifacts_root=ARTIFACTS_ROOT,
        level=args.level,
        limit=args.limit,
        ids=args.ids,
        skip_existing=args.skip_existing,
        max_pages=args.max_pages,
        max_chars=args.max_chars,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.", flush=True)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        raise