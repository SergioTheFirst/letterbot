from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_INDEX_DEFS: tuple[tuple[str, str], ...] = (
    (
        "idx_events_v1_account_entity_event_ts",
        "CREATE INDEX IF NOT EXISTS idx_events_v1_account_entity_event_ts "
        "ON events_v1(account_id, entity_id, event_type, ts_utc)",
    ),
    (
        "idx_events_v1_account_event_ts",
        "CREATE INDEX IF NOT EXISTS idx_events_v1_account_event_ts "
        "ON events_v1(account_id, event_type, ts_utc)",
    ),
)


@dataclass(slots=True)
class IndexResult:
    created: list[str]
    verified: list[str]
    errors: list[dict[str, str]]
    timings_ms: dict[str, int]


def _sanitize_error(text: object, *, limit: int = 160) -> str:
    raw = str(text or "")
    cleaned = re.sub(r"[\w.+'-]+@[\w.-]+", "[redacted]", raw)
    return cleaned[:limit]


def _load_existing_indexes(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA index_list('{table}')").fetchall()
    names: set[str] = set()
    for row in rows:
        if len(row) > 1 and row[1]:
            names.add(str(row[1]))
    return names


def _run_index_statements(
    conn: sqlite3.Connection,
    index_defs: Iterable[tuple[str, str]],
) -> IndexResult:
    created: list[str] = []
    verified: list[str] = []
    errors: list[dict[str, str]] = []
    timings_ms: dict[str, int] = {}
    existing = _load_existing_indexes(conn, "events_v1")
    for name, statement in index_defs:
        start = time.monotonic()
        try:
            conn.execute(statement)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            timings_ms[name] = elapsed_ms
            if name in existing:
                verified.append(name)
            else:
                created.append(name)
                existing.add(name)
        except Exception as exc:
            timings_ms[name] = int((time.monotonic() - start) * 1000)
            errors.append(
                {
                    "index": name,
                    "error_type": type(exc).__name__,
                    "message": _sanitize_error(exc),
                }
            )
    return IndexResult(
        created=created,
        verified=verified,
        errors=errors,
        timings_ms=timings_ms,
    )


def ensure_indexes(db_path: str) -> dict[str, object]:
    """Create maintenance indexes for events_v1.

    Returns a dict with created/verified index names, errors, and timings.
    """

    resolved_path = Path(db_path)
    result = IndexResult(created=[], verified=[], errors=[], timings_ms={})
    try:
        with sqlite3.connect(resolved_path) as conn:
            conn.execute("BEGIN")
            result = _run_index_statements(conn, _INDEX_DEFS)
            if result.errors:
                conn.rollback()
            else:
                conn.commit()
    except Exception as exc:
        result.errors.append(
            {
                "index": "__transaction__",
                "error_type": type(exc).__name__,
                "message": _sanitize_error(exc),
            }
        )
    return {
        "created": sorted(result.created),
        "verified": sorted(result.verified),
        "errors": result.errors,
        "timings_ms": result.timings_ms,
    }


__all__ = ["ensure_indexes"]
