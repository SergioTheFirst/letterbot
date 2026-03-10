from __future__ import annotations

import logging

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_knowledge_db_init_does_not_log_missing_emails_on_fresh_db(
    tmp_path, caplog
) -> None:
    db_path = tmp_path / "fresh.sqlite"

    caplog.set_level(logging.ERROR)
    KnowledgeDB(db_path)

    assert "KnowledgeDB migration failed: no such table: emails" not in caplog.text
