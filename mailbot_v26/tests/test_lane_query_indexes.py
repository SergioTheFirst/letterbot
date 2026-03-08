import sqlite3
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _explain_plan(
    conn: sqlite3.Connection, query: str, params: tuple[object, ...]
) -> str:
    rows = conn.execute(query, params).fetchall()
    return " ".join(str(row[3]) for row in rows if len(row) > 3)


def test_lane_queries_use_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "lane_indexes.sqlite"
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        base_params = ("2026-01-01T00:00:00+00:00", "acct@example.com")
        event_plan = _explain_plan(
            conn,
            """
            EXPLAIN QUERY PLAN
            SELECT e.id
            FROM emails e
            WHERE e.received_at >= ?
              AND e.account_email = ?
              AND EXISTS (
                SELECT 1 FROM events_v1 ev
                WHERE ev.email_id = e.id AND ev.event_type = ? AND ev.ts_utc >= ?
              )
            ORDER BY e.received_at DESC, e.id DESC
            LIMIT 5
            """,
            (*base_params, "telegram_failed", 0.0),
        )
        assert "idx_emails_account_received_at_id" in event_plan
        assert "idx_events_v1_email_event_ts" in event_plan

        commitment_plan = _explain_plan(
            conn,
            """
            EXPLAIN QUERY PLAN
            SELECT e.id
            FROM emails e
            WHERE e.received_at >= ?
              AND e.account_email = ?
              AND EXISTS (
                SELECT 1 FROM commitments c
                WHERE c.email_row_id = e.id AND c.status IN (?, ?)
              )
            ORDER BY e.received_at DESC, e.id DESC
            LIMIT 5
            """,
            (*base_params, "pending", "expired"),
        )
        assert "idx_commitments_email_status_created_id" in commitment_plan

        span_plan = _explain_plan(
            conn,
            """
            EXPLAIN QUERY PLAN
            SELECT e.id
            FROM emails e
            WHERE e.received_at >= ?
              AND e.account_email = ?
              AND EXISTS (
                SELECT 1 FROM processing_spans ps
                WHERE ps.email_id = e.id AND ps.ts_start_utc >= ? AND ps.outcome != ?
              )
            ORDER BY e.received_at DESC, e.id DESC
            LIMIT 5
            """,
            (*base_params, 0.0, "ok"),
        )
        assert "idx_processing_spans_email_ts" in span_plan
