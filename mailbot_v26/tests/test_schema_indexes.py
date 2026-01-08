import sqlite3
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_schema_indexes_include_cockpit_support(tmp_path: Path) -> None:
    db_path = tmp_path / "schema.sqlite"
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
            ("idx_emails_account_received_at",),
        ).fetchone()
    assert row is not None
