from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_weekly_digest_uses_relationship_top_senders(tmp_path: Path) -> None:
    db_path = tmp_path / "weekly-relationship.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Invoice", "invoice payment", now.isoformat(), now.isoformat()),
        )
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Contract", "contract update", now.isoformat(), now.isoformat()),
        )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    data = weekly_digest._collect_weekly_data(
        analytics=analytics,
        account_email="user@example.com",
        account_emails=["user@example.com"],
        week_key="2026-W10",
        now=now,
    )

    assert data.relationship_top_senders
    assert data.relationship_top_senders[0]["sender_email"] == "vendor@example.com"

