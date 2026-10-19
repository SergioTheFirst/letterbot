from __future__ import annotations

import sqlite3
from pathlib import Path

from mailbot_v26.events.emitter import EventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB

_EMAILS_PRIORITY = ("emails", ("priority",))
_EVENTS_TYPE_TS = ("events_v1", ("event_type", "ts_utc"))


def _indexes_on(
    conn: sqlite3.Connection, table: str, columns: tuple[str, ...]
) -> list[str]:
    names: list[str] = []
    for row in conn.execute(f"PRAGMA index_list('{table}')").fetchall():
        index_name = str(row[1])
        index_columns = tuple(
            str(info[2])
            for info in conn.execute(f"PRAGMA index_info('{index_name}')").fetchall()
        )
        if index_columns == columns:
            names.append(index_name)
    return sorted(names)


def _assert_single_indexes(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        assert _indexes_on(conn, *_EMAILS_PRIORITY) == ["idx_emails_priority"]
        assert _indexes_on(conn, *_EVENTS_TYPE_TS) == ["idx_events_v1_event_type_ts"]


def test_fresh_schema_has_one_index_per_duplicated_column_set(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fresh.sqlite"

    KnowledgeDB(db_path)

    _assert_single_indexes(db_path)


def test_schema_init_drops_legacy_duplicate_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_email_priority ON emails(priority)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_type_ts "
            "ON events_v1(event_type, ts_utc)"
        )

    KnowledgeDB(db_path)

    _assert_single_indexes(db_path)


def test_event_emitter_init_drops_legacy_duplicate_events_index(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "events.sqlite"
    EventEmitter(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_type_ts "
            "ON events_v1(event_type, ts_utc)"
        )

    EventEmitter(db_path)

    with sqlite3.connect(db_path) as conn:
        assert _indexes_on(conn, *_EVENTS_TYPE_TS) == ["idx_events_v1_event_type_ts"]
