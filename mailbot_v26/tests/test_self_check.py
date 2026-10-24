from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from pytest import LogCaptureFixture

from mailbot_v26.storage.self_check import run_self_check


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "database.sqlite"
    schema_src = Path(__file__).resolve().parents[1] / "storage" / "schema.sql"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_src.read_text(encoding="utf-8"))
    return db_path


def test_self_check_logs_and_preserves_data(
    tmp_path: Path, caplog: LogCaptureFixture
) -> None:
    db_path = _init_db(tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (
                account_email,
                from_email,
                subject,
                received_at,
                priority,
                priority_reason,
                action_line,
                body_summary,
                raw_body_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "acc@example.com",
                "from@example.com",
                "Initial",
                datetime.utcnow().isoformat(),
                "🟡",
                "Escalated due to history",
                "Do something",
                "Summary",
                "hash1",
            ),
        )
        conn.execute(
            """
            INSERT INTO emails (
                account_email,
                from_email,
                subject,
                received_at,
                priority,
                priority_reason,
                action_line,
                body_summary,
                raw_body_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "acc@example.com",
                "from@example.com",
                "Secondary",
                datetime.utcnow().isoformat(),
                "🔵",
                None,
                "Action",
                "Body",
                "hash2",
            ),
        )
        conn.commit()
        initial_count = conn.execute("SELECT COUNT(*) FROM emails;").fetchone()[0]

    caplog.set_level(logging.INFO)
    run_self_check(db_path=db_path, project_root=Path(__file__).resolve().parents[1])

    joined = " | ".join(caplog.messages)
    assert "[SELF-CHECK] emails schema OK" in joined
    assert "priority_reason not exposed in Telegram/user output" in joined
    assert "[CRM] total=2;" in joined

    with sqlite3.connect(db_path) as conn:
        final_count = conn.execute("SELECT COUNT(*) FROM emails;").fetchone()[0]

    assert final_count == initial_count


def _storage_schema_db(tmp_path: Path) -> Path:
    from mailbot_v26.bot_core.storage import Storage
    from mailbot_v26.storage.knowledge_db import KnowledgeDB

    db_path = tmp_path / "data" / "mailbot.sqlite"
    storage = Storage(db_path)
    try:
        storage.persist_email_and_enqueue(
            account_email="acc@example.com",
            uid=1,
            message_id="<m1@example.com>",
            from_email="from@example.com",
            from_name="From",
            subject="Subject",
            received_at="2026-10-01T09:00:00+00:00",
            attachments_count=0,
        )
    finally:
        storage.close()
    KnowledgeDB(db_path)
    return db_path


def test_self_check_never_writes_and_uses_read_only_connections(
    tmp_path: Path, caplog: LogCaptureFixture, monkeypatch
) -> None:
    from mailbot_v26.storage import self_check

    db_path = _storage_schema_db(tmp_path)
    before = (db_path.read_bytes(), db_path.stat().st_mtime_ns)
    opened: list[str] = []
    real_connect = sqlite3.connect

    def _spy_connect(database, *args, **kwargs):
        opened.append(str(database))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(self_check.sqlite3, "connect", _spy_connect)
    caplog.set_level(logging.INFO)

    run_self_check(db_path=db_path, project_root=Path(__file__).resolve().parents[1])

    assert opened, "the self-check reads the database"
    assert all(target.endswith("?mode=ro") for target in opened), opened
    assert (db_path.read_bytes(), db_path.stat().st_mtime_ns) == before
    joined = " | ".join(caplog.messages)
    assert "[SELF-CHECK] emails schema OK" in joined
    assert "[CRM] total=1;" in joined
    assert "priority_reason check" not in joined


def test_self_check_reports_missing_database_and_never_creates_it(
    tmp_path: Path, caplog: LogCaptureFixture
) -> None:
    db_path = tmp_path / "data" / "mailbot.sqlite"
    caplog.set_level(logging.INFO)

    run_self_check(db_path=db_path, project_root=tmp_path)

    assert "[SELF-CHECK] CRM DB not found" in caplog.text
    assert not db_path.exists()
    assert not db_path.parent.exists()
