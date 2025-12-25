from __future__ import annotations

import sqlite3

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_sqlite_busy_logs_and_suppresses(tmp_path, caplog) -> None:
    db = KnowledgeDB(tmp_path / "crm.sqlite")
    db._WRITE_BASE_DELAY = 0.0
    db._WRITE_MAX_TOTAL_WAIT = 1.0
    db._WRITE_RETRIES = 2

    def _locked_connect():
        raise sqlite3.OperationalError("database is locked")

    db._connect = _locked_connect  # type: ignore[assignment]

    with caplog.at_level("ERROR"):
        result = db.save_email(
            account_email="account@example.com",
            from_email="sender@example.com",
            subject="Subject",
            received_at="2024-01-01T00:00:00",
            priority="🔵",
            action_line="Проверить письмо",
            body_summary="Summary",
            raw_body="Body",
            attachment_summaries=[],
        )

    assert result is None
    assert "crm_write_failed" in caplog.text
