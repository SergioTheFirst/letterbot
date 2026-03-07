from __future__ import annotations

import sqlite3
from pathlib import Path

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability.decision_trace_v1 import DecisionTraceV1, from_canonical_json


def load_latest_decision_traces(
    *,
    db_path: Path,
    email_id: int,
    limit: int = 10,
    read_only: bool = False,
) -> list[DecisionTraceV1]:
    if email_id <= 0:
        return []
    if read_only:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM events_v1
            WHERE event_type = ?
              AND email_id = ?
            ORDER BY ts_utc DESC
            LIMIT ?
            """,
            (EventType.DECISION_TRACE_RECORDED.value, int(email_id), int(limit)),
        ).fetchall()
    finally:
        conn.close()
    seen: set[str] = set()
    traces: list[DecisionTraceV1] = []
    for (payload_json,) in rows:
        trace = from_canonical_json(payload_json)
        if not trace or not trace.decision_kind:
            continue
        if trace.decision_kind in seen:
            continue
        seen.add(trace.decision_kind)
        traces.append(trace)
    return traces


__all__ = ["load_latest_decision_traces"]
