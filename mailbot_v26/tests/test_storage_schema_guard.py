"""B24: Storage never drops a populated table to fix a missing emails.uid.

A legacy KnowledgeDB file (emails without Storage's uid column) used to be
wiped silently: Storage dropped emails, queue, the delivery log and snoozes,
then recreated them empty. It now refuses with StorageSchemaError and leaves
every row in place; an empty legacy table is still recreated.
"""

from __future__ import annotations

import logging
import sqlite3
import zipfile
from contextlib import closing
from pathlib import Path

import pytest

from mailbot_v26.bot_core.storage import Storage, StorageSchemaError
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tools import restore

CANARY_SUBJECT = "Legacy canary-subject"


def _legacy_db(path: Path, *, rows: int) -> Path:
    kdb = KnowledgeDB(path)  # KnowledgeDB schema: emails without uid
    for index in range(rows):
        saved = kdb.save_email(
            account_email="account@example.com",
            from_email=f"sender{index}@example.com",
            subject=f"{CANARY_SUBJECT} {index}",
            received_at="2025-01-01T09:00:00+00:00",
            priority="🟡",
            action_line="Reply",
            body_summary="Summary",
            raw_body="Body",
            attachment_summaries=[],
        )
        assert saved is not None
    return path


def _email_columns(db_path: Path) -> set[str]:
    with closing(sqlite3.connect(db_path)) as conn:
        return {str(row[1]) for row in conn.execute("PRAGMA table_info(emails);")}


def _count(db_path: Path, table: str) -> int:
    with closing(sqlite3.connect(db_path)) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_storage_refuses_populated_emails_table_without_uid(tmp_path: Path) -> None:
    db_path = _legacy_db(tmp_path / "legacy.sqlite", rows=3)

    with pytest.raises(StorageSchemaError) as info:
        Storage(db_path)

    error = info.value
    assert error.db_path == db_path
    assert error.row_counts == {"emails": 3}
    message = str(error)
    assert str(db_path) in message
    assert "uid" in message
    assert CANARY_SUBJECT not in message
    assert _count(db_path, "emails") == 3
    assert "uid" not in _email_columns(db_path)
    # The refused Storage closed its connection (Windows keeps open files).
    db_path.unlink()


def test_storage_refuses_when_a_dependent_table_has_rows(tmp_path: Path) -> None:
    db_path = _legacy_db(tmp_path / "legacy.sqlite", rows=0)
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute(
            "CREATE TABLE telegram_snooze (id INTEGER PRIMARY KEY, email_id INTEGER)"
        )
        conn.execute("INSERT INTO telegram_snooze (email_id) VALUES (1)")
        conn.commit()

    with pytest.raises(StorageSchemaError) as info:
        Storage(db_path)

    assert info.value.row_counts == {"telegram_snooze": 1}
    assert _count(db_path, "telegram_snooze") == 1


def test_storage_recreates_empty_emails_table_without_uid(
    tmp_path: Path, caplog
) -> None:
    db_path = _legacy_db(tmp_path / "fresh.sqlite", rows=0)
    caplog.set_level(logging.INFO)

    Storage(db_path).close()

    assert "uid" in _email_columns(db_path)
    assert "storage_schema_recreated" in caplog.text


def test_restore_refuses_backup_database_without_uid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(restore, "_IS_WINDOWS", False)
    root = tmp_path / "root"
    current_db = root / "data" / "mailbot.sqlite"
    current_db.parent.mkdir(parents=True)
    Storage(current_db).close()
    (root / "state.json").write_text('{"state": "current"}', encoding="utf-8")
    before = current_db.read_bytes()

    legacy = _legacy_db(tmp_path / "legacy.sqlite", rows=2)
    archive = tmp_path / "backup.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(legacy, "data/mailbot.sqlite")
        handle.writestr("state.json", '{"state": "from-backup"}')

    with pytest.raises(restore.RestoreRefusedError) as info:
        restore.restore_from_backup(archive, base_dir=root)

    assert "uid" in str(info.value)
    assert CANARY_SUBJECT not in str(info.value)
    assert current_db.read_bytes() == before
    assert (root / "state.json").read_text(encoding="utf-8") == '{"state": "current"}'
    assert not list(current_db.parent.glob("*.restore-tmp"))
