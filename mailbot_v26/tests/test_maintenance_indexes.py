import sqlite3
from pathlib import Path

from mailbot_v26.maintenance.indexes import ensure_indexes


def _create_events_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE events_v1 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            ts_utc REAL NOT NULL,
            ts TEXT,
            account_id TEXT NOT NULL,
            entity_id TEXT,
            email_id INTEGER,
            payload JSON,
            payload_json JSON,
            schema_version INTEGER NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE
        )
        """)
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type,
            ts_utc,
            ts,
            account_id,
            entity_id,
            email_id,
            payload,
            payload_json,
            schema_version,
            fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "decision_trace_recorded",
            1.0,
            "2026-01-01T00:00:00+00:00",
            "acc",
            "entity",
            1,
            "{}",
            "{}",
            1,
            "fp",
        ),
    )
    conn.commit()


def _index_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA index_list('events_v1')").fetchall()
    return {str(row[1]) for row in rows if len(row) > 1}


def test_ensure_indexes_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "maintenance.sqlite"
    with sqlite3.connect(db_path) as conn:
        _create_events_schema(conn)
        count_before = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]

    first = ensure_indexes(str(db_path))
    second = ensure_indexes(str(db_path))
    assert not first["errors"]
    assert not second["errors"]

    with sqlite3.connect(db_path) as conn:
        indexes = _index_names(conn)
        count_after = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]

    assert "idx_events_v1_account_entity_event_ts" in indexes
    assert "idx_events_v1_account_event_ts" in indexes
    assert count_before == count_after
