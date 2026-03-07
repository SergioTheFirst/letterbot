from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from mailbot_v26.intelligence.priority_engine import PriorityEngine
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _prepare_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "database.sqlite"
    schema_copy = tmp_path / "schema.sql"

    storage_schema = Path(__file__).resolve().parents[1] / "storage" / "schema.sql"
    schema_copy.write_text(storage_schema.read_text(encoding="utf-8"), encoding="utf-8")

    return db_path


def test_priority_reason_added_on_escalation(tmp_path: Path) -> None:
    db_path = _prepare_db(tmp_path)
    KnowledgeDB(db_path)

    received_at = datetime(2024, 5, 30, 12, 0)
    with sqlite3.connect(db_path) as conn:
        for days_ago in (5, 10, 20):
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
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "account@example.com",
                    "sender@example.com",
                    f"Subject {days_ago}",
                    (received_at - timedelta(days=days_ago)).isoformat(),
                    "🔴",
                    None,
                    "",
                    "",
                    "",
                ),
            )
        conn.commit()

    engine = PriorityEngine(db_path)
    priority, reason = engine.adjust_priority(
        llm_priority="🟡",
        from_email="sender@example.com",
        received_at=received_at,
    )

    assert priority == "🔴"
    assert reason is not None
    assert "30 дней" in reason


def test_priority_reason_absent_when_priority_unchanged(tmp_path: Path) -> None:
    db_path = _prepare_db(tmp_path)
    KnowledgeDB(db_path)

    engine = PriorityEngine(db_path)
    priority, reason = engine.adjust_priority(
        llm_priority="🔵",
        from_email="new@example.com",
        received_at=datetime(2024, 6, 1, 10, 0),
    )

    assert priority == "🔵"
    assert reason is None


def test_save_email_persists_priority_reason(tmp_path: Path) -> None:
    db_path = _prepare_db(tmp_path)
    db = KnowledgeDB(db_path)

    db.save_email(
        account_email="audit@example.com",
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 6, 2, 9, 0).isoformat(),
        priority="🟡",
        priority_reason="Повышен до 🟡: тестовая причина",
        action_line="Action",
        body_summary="Краткое описание письма.",
        raw_body="Body",
        attachment_summaries=[],
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT priority_reason FROM emails ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert row is not None
    assert row[0] == "Повышен до 🟡: тестовая причина"
