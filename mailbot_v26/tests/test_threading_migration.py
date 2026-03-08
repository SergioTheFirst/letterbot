from __future__ import annotations

import sqlite3
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _column_names(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {row[1] for row in conn.execute("PRAGMA table_info(emails)")}


def test_thread_columns_added_on_new_db(tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    KnowledgeDB(db_path)

    columns = _column_names(db_path)
    for column in ("rfc_message_id", "in_reply_to", "references", "thread_key"):
        assert column in columns


def test_thread_columns_added_on_existing_db(tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT,
                from_email TEXT,
                subject TEXT,
                received_at TEXT,
                priority TEXT,
                action_line TEXT,
                body_summary TEXT,
                raw_body_hash TEXT
            );
            """)
        conn.commit()

    KnowledgeDB(db_path)

    columns = _column_names(db_path)
    for column in ("rfc_message_id", "in_reply_to", "references", "thread_key"):
        assert column in columns
