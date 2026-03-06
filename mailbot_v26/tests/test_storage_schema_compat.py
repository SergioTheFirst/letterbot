from __future__ import annotations

import sqlite3
from pathlib import Path

from mailbot_v26.bot_core.storage import Storage


def _create_legacy_emails_table(db_path: Path, *, include_telegram_delivered_at: bool) -> None:
    telegram_col = ",\n                telegram_delivered_at TEXT" if include_telegram_delivered_at else ""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT NOT NULL,
                uid INTEGER NOT NULL,
                message_id TEXT,
                from_email TEXT,
                from_name TEXT,
                subject TEXT,
                received_at TEXT,
                attachments_count INTEGER DEFAULT 0,
                error_last TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
                {telegram_col},
                UNIQUE(account_email, uid)
            );
            """
        )
        conn.commit()


def _email_columns(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("PRAGMA table_info(emails);").fetchall()
    return {str(row[1]) for row in rows}


def test_storage_migrates_missing_status_column(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    _create_legacy_emails_table(db_path, include_telegram_delivered_at=False)

    storage = Storage(db_path)
    storage.close()

    columns = _email_columns(db_path)
    assert "status" in columns
    assert "telegram_delivered_at" in columns

    with sqlite3.connect(db_path) as conn:
        indexes = {str(row[1]) for row in conn.execute("PRAGMA index_list(emails);").fetchall()}
    assert "idx_emails_status" in indexes


def test_storage_init_works_with_old_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_with_telegram.sqlite"
    _create_legacy_emails_table(db_path, include_telegram_delivered_at=True)

    storage = Storage(db_path)
    storage.close()

    storage = Storage(db_path)
    try:
        email_id = storage.upsert_email(
            account_email="acc@example.com",
            uid=101,
            message_id="mid-101",
            from_email="sender@example.com",
            from_name="Sender",
            subject="Subject",
            received_at="2026-03-06T10:00:00",
            attachments_count=0,
        )
        assert email_id > 0

        row = storage.conn.execute("SELECT status FROM emails WHERE id = ?;", (email_id,)).fetchone()
        assert row is not None
        assert row[0] == "NEW"
    finally:
        storage.close()