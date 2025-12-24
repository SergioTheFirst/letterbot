from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_deferred_for_digest_persisted(tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    db = KnowledgeDB(db_path)

    email_row_id = db.save_email(
        account_email="account@example.com",
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 6, 1, 10, 0).isoformat(),
        priority="🔵",
        action_line="Action",
        body_summary="Summary",
        raw_body="Body",
        attachment_summaries=[],
    )

    assert email_row_id is not None
    assert db.mark_deferred_for_digest(email_row_id=email_row_id, deferred=True)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT deferred_for_digest FROM emails WHERE id = ?",
            (email_row_id,),
        ).fetchone()

    assert row is not None
    assert row[0] == 1
