# schema_v4.py
# schema_v4.py
"""
Canonical JSON schemas and lightweight validators for Democracy Clock V4.

There are two primary JSON shapes used across the pipeline:

1) FILTERED PACK (harvester output; saved as *_filtered_YYYY-MM-DD_YYYY-MM-DD.json)
----------------------------------------------------------------------
{
  "generated_at": "2025-10-28T19:15:03Z",           # ISO-8601 in UTC
  "window": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"},
  "source": "econ",                                  # harvester key (e.g., 'econ', 'cbo', 'congress', ...)
  "entities": [ { ... normalized item ... }, ... ],  # list of normalized source items
  "meta": { ... optional source-specific details ... }  # OPTIONAL
}

2) EVENTS PACK (builder output; saved as *_events_YYYY-MM-DD_YYYY-MM-DD.json and *_events_*.md)
----------------------------------------------------------------------
{
  "generated_at": "2025-10-28T19:18:44Z",           # ISO-8601 in UTC
  "window": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"},
  "source": "econ",                                  # same source key as input
  "builder": "buildecon_v4",                         # module or logical builder name
  "events": [ { ... normalized event ... }, ... ],   # list of extracted/constructed events
  "meta": { ... optional builder notes (counters, warnings) ... }  # OPTIONAL
}

NOTE: During migration we may still encounter legacy filtered files that are a *bare list* of items.
To avoid breaking, builders should call `coerce_filtered_pack(...)` before validation.
Once all harvesters emit canonical dicts, we can remove the coercion step.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

# -------------------------
# Canonical keys/constants
# -------------------------

FILTERED_REQUIRED_KEYS = ("generated_at", "window", "source", "entities")
EVENTS_REQUIRED_KEYS = ("generated_at", "window", "source", "builder", "events")

WINDOW_REQUIRED_KEYS = ("start", "end")

# Acceptable stage names for validation
STAGE_FILTERED = "filtered"
STAGE_EVENTS = "events"
VALID_STAGES = (STAGE_FILTERED, STAGE_EVENTS)

ISO_DATE_FMT = "%Y-%m-%d"


# -------------------------
# Helpers
# -------------------------

def _utc_now_iso() -> str:
    """Return current time in RFC3339/ISO-8601 (UTC, with 'Z')."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_iso_date_yyyy_mm_dd(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, ISO_DATE_FMT)
        return True
    except Exception:
        return False


def _validate_window(window: Mapping[str, Any]) -> List[str]:
    errs: List[str] = []
    for k in WINDOW_REQUIRED_KEYS:
        if k not in window:
            errs.append(f"window missing key '{k}'")
    if "start" in window and not _is_iso_date_yyyy_mm_dd(window["start"]):
        errs.append("window.start must be 'YYYY-MM-DD'")
    if "end" in window and not _is_iso_date_yyyy_mm_dd(window["end"]):
        errs.append("window.end must be 'YYYY-MM-DD'")
    return errs


def _require_keys(data: Mapping[str, Any], required_keys: Iterable[str]) -> List[str]:
    return [f"missing key '{k}'" for k in required_keys if k not in data]


def _is_list(value: Any) -> bool:
    return isinstance(value, list)


def _is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


# -------------------------
# Public factory helpers
# -------------------------

def new_filtered_pack(
    window: Mapping[str, str],
    source: str,
    entities: Iterable[Mapping[str, Any]],
    meta: Optional[Mapping[str, Any]] = None,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a canonical FILTERED pack.
    """
    pack: Dict[str, Any] = {
        "generated_at": generated_at or _utc_now_iso(),
        "window": {"start": window["start"], "end": window["end"]},
        "source": source,
        "entities": list(entities),
    }
    if meta:
        pack["meta"] = dict(meta)
    return pack


def new_events_pack(
    window: Mapping[str, str],
    source: str,
    builder: str,
    events: Iterable[Mapping[str, Any]],
    meta: Optional[Mapping[str, Any]] = None,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a canonical EVENTS pack.
    """
    pack: Dict[str, Any] = {
        "generated_at": generated_at or _utc_now_iso(),
        "window": {"start": window["start"], "end": window["end"]},
        "source": source,
        "builder": builder,
        "events": list(events),
    }
    if meta:
        pack["meta"] = dict(meta)
    return pack


# -------------------------
# Coercion for legacy files
# -------------------------

def coerce_filtered_pack(
    data: Union[List[Any], Mapping[str, Any]],
    window: Optional[Mapping[str, str]] = None,
    source: Optional[str] = None,
    logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    If `data` is already a canonical filtered pack (dict with entities),
    return it unchanged. If it's a *list*, wrap it into a canonical pack
    using provided `window` and `source` (required for wrapping).
    """
    if _is_mapping(data) and "entities" in data:
        if logger:
            logger.debug("schema_v4: detected canonical filtered pack (dict).")
        return dict(data)

    if _is_list(data):
        if window is None or source is None:
            raise ValueError("coerce_filtered_pack requires `window` and `source` when wrapping a list.")
        if logger:
            logger.debug("schema_v4: wrapping legacy list into canonical filtered pack.")
        return new_filtered_pack(window=window, source=source, entities=data)

    # Unknown shape
    raise TypeError("coerce_filtered_pack expected dict-with-entities or list-of-entities.")


# -------------------------
# Validation
# -------------------------

def validate_schema(
    pack: Mapping[str, Any],
    stage: str,
    logger: Optional[Any] = None,
    raise_on_error: bool = False,
) -> Tuple[bool, List[str]]:
    """
    Validate a pack against the canonical schema.

    Returns (ok, errors). If `raise_on_error=True`, raises ValueError on failure.
    """
    errs: List[str] = []

    if stage not in VALID_STAGES:
        errs.append(f"Unknown stage '{stage}'. Expected one of {VALID_STAGES}.")

    if not _is_mapping(pack):
        errs.append("Top-level JSON must be an object/dict.")
    else:
        # Check required keys for the stage
        required = FILTERED_REQUIRED_KEYS if stage == STAGE_FILTERED else EVENTS_REQUIRED_KEYS
        errs.extend(_require_keys(pack, required))

        # Validate window
        if "window" in pack and _is_mapping(pack["window"]):
            errs.extend(_validate_window(pack["window"]))
        else:
            errs.append("`window` must be an object with 'start' and 'end'.")

        # Validate list containers
        list_key = "entities" if stage == STAGE_FILTERED else "events"
        if list_key in pack and not _is_list(pack[list_key]):
            errs.append(f"`{list_key}` must be a list.")

        # generated_at existence/type (format is best-effort)
        if "generated_at" in pack and not isinstance(pack["generated_at"], str):
            errs.append("`generated_at` must be a string (ISO-8601 UTC).")

        # basic type checks
        if "source" in pack and not isinstance(pack["source"], str):
            errs.append("`source` must be a string.")
        if stage == STAGE_EVENTS and "builder" in pack and not isinstance(pack["builder"], str):
            errs.append("`builder` must be a string.")

    ok = len(errs) == 0
    if not ok and logger:
        logger.debug("schema_v4.validate_schema: FAIL -> %s", "; ".join(errs))
    if not ok and raise_on_error:
        raise ValueError("; ".join(errs))
    return ok, errs


__all__ = [
    # constants
    "STAGE_FILTERED",
    "STAGE_EVENTS",
    "FILTERED_REQUIRED_KEYS",
    "EVENTS_REQUIRED_KEYS",
    "WINDOW_REQUIRED_KEYS",
    # factories
    "new_filtered_pack",
    "new_events_pack",
    # coercion
    "coerce_filtered_pack",
    # validation
    "validate_schema",
]