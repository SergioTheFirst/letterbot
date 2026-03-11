from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Final

logger = logging.getLogger(__name__)

TRUTH_HIERARCHY: Final[tuple[str, str, str, str]] = (
    "events_v1",
    "persisted_runtime_state",
    "projections",
    "ui_summaries",
)

_PROJECTION_SOURCES: Final[frozenset[str]] = frozenset(
    {
        "projection",
        "projections",
        "read_model",
        "read_models",
        "snapshot",
        "snapshots",
        "status_strip",
        "processing_spans",
        "cache",
        "ui_summary",
        "ui_summaries",
    }
)


def _normalize_source(source: object) -> str:
    return str(source or "").strip().lower()


def _coerce_timestamp(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        pass
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def truth_rank(source: str) -> int:
    normalized = _normalize_source(source)
    try:
        return TRUTH_HIERARCHY.index(normalized)
    except ValueError:
        return len(TRUTH_HIERARCHY)


def assert_not_projection_source(
    *,
    source: str,
    context: str = "",
    hint: str = "canonical events should be used instead",
    logger_: logging.Logger | None = None,
) -> bool:
    normalized = _normalize_source(source)
    if normalized not in _PROJECTION_SOURCES:
        return True
    (logger_ or logger).warning(
        "projection_used_as_truth",
        extra={
            "source": normalized,
            "context": context,
            "hint": hint,
        },
    )
    return False


def mark_as_stale(
    *,
    source: str,
    snapshot_ts: object,
    now_ts: float,
    threshold_seconds: float,
    context: str = "",
    logger_: logging.Logger | None = None,
) -> dict[str, object]:
    parsed_ts = _coerce_timestamp(snapshot_ts)
    threshold = max(float(threshold_seconds or 0.0), 0.0)
    if parsed_ts is None:
        return {
            "source": _normalize_source(source),
            "stale": False,
            "age_seconds": None,
            "threshold_seconds": threshold,
        }
    age_seconds = max(float(now_ts) - parsed_ts, 0.0)
    stale = age_seconds > threshold if threshold > 0 else False
    if stale:
        (logger_ or logger).warning(
            "snapshot_marked_stale",
            extra={
                "source": _normalize_source(source),
                "context": context,
                "age_seconds": round(age_seconds, 3),
                "threshold_seconds": threshold,
            },
        )
    return {
        "source": _normalize_source(source),
        "stale": stale,
        "age_seconds": age_seconds,
        "threshold_seconds": threshold,
    }


__all__ = [
    "TRUTH_HIERARCHY",
    "assert_not_projection_source",
    "mark_as_stale",
    "truth_rank",
]
