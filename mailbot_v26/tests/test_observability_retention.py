import sqlite3
import time
from pathlib import Path

from mailbot_v26.observability.processing_span import (
    ProcessingSpanRecorder,
    _prune_observability,
)


def _insert_span(conn: sqlite3.Connection, span_id: str, ts_value: float) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO processing_spans (
            span_id,
            ts_start_utc,
            ts_end_utc,
            total_duration_ms,
            account_id,
            email_id,
            stage_durations_json,
            llm_provider,
            llm_model,
            llm_latency_ms,
            llm_quality_score,
            fallback_used,
            outcome,
            error_code,
            health_snapshot_id
        ) VALUES (?, ?, ?, ?, ?, ?, '{}', NULL, NULL, NULL, NULL, 0, 'ok', '', '')
        """,
        (span_id, ts_value, ts_value, 10, "acc", None),
    )


def _insert_snapshot(
    conn: sqlite3.Connection, snapshot_id: str, ts_value: float
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO system_health_snapshots (
            snapshot_id,
            ts_utc,
            payload_json,
            gates_state,
            metrics_brief
        ) VALUES (?, ?, '{}', '', '')
        """,
        (snapshot_id, ts_value),
    )


def test_prune_by_age(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    old_ts = time.time() - 100 * 86400
    recent_ts = time.time() - 10 * 86400

    with sqlite3.connect(db_path) as conn:
        _insert_span(conn, "old", old_ts)
        _insert_span(conn, "new", recent_ts)
        _insert_snapshot(conn, "s_old", old_ts)
        _insert_snapshot(conn, "s_new", recent_ts)
        conn.commit()
        _prune_observability(
            conn,
            spans_retention_days=90,
            spans_max_rows=10,
            health_retention_days=30,
            health_max_rows=10,
        )
        remaining_spans = {
            row[0] for row in conn.execute("SELECT span_id FROM processing_spans")
        }
        remaining_snapshots = {
            row[0]
            for row in conn.execute("SELECT snapshot_id FROM system_health_snapshots")
        }

    assert remaining_spans == {"new"}
    assert remaining_snapshots == {"s_new"}


def test_prune_by_row_cap(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    base = time.time()
    with sqlite3.connect(db_path) as conn:
        for idx in range(5):
            ts_value = base + idx
            _insert_span(conn, f"span_{idx}", ts_value)
            _insert_snapshot(conn, f"snap_{idx}", ts_value)
        conn.commit()
        _prune_observability(
            conn,
            spans_retention_days=365,
            spans_max_rows=3,
            health_retention_days=365,
            health_max_rows=2,
        )
        remaining_spans = {
            row[0] for row in conn.execute("SELECT span_id FROM processing_spans")
        }
        remaining_snapshots = {
            row[0]
            for row in conn.execute("SELECT snapshot_id FROM system_health_snapshots")
        }

    assert remaining_spans == {"span_2", "span_3", "span_4"}
    assert remaining_snapshots == {"snap_3", "snap_4"}
