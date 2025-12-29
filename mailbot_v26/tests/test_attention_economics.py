from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from mailbot_v26.insights.attention_economics import compute_attention_economics
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _insert_email(
    conn: sqlite3.Connection,
    *,
    account_email: str,
    from_email: str,
    body_words: int,
    created_at: str,
    attachments: int = 0,
) -> None:
    body_summary = " ".join(["w"] * body_words)
    cur = conn.execute(
        """
        INSERT INTO emails (
            account_email,
            from_email,
            subject,
            deferred_for_digest,
            body_summary,
            created_at
        )
        VALUES (?, ?, 'subject', 0, ?, ?)
        """,
        (account_email, from_email, body_summary, created_at),
    )
    email_id = cur.lastrowid
    for idx in range(attachments):
        conn.execute(
            "INSERT INTO attachments (email_id, filename, summary) VALUES (?, ?, ?)",
            (email_id, f"file{idx}.txt", "summary"),
        )


def _ensure_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trust_snapshots (
            id TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            entity_id TEXT NOT NULL,
            trust_score REAL
        );
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS relationship_health_snapshots (
            id TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            entity_id TEXT NOT NULL,
            health_score REAL
        );
        """,
    )


def test_at_risk_only_uses_health_or_anomalies(tmp_path: Path) -> None:
    db_path = tmp_path / "attention.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)

    now = datetime.utcnow()
    earlier = (now - timedelta(days=3)).isoformat()
    now_iso = now.isoformat()

    with sqlite3.connect(db_path) as conn:
        _ensure_snapshot_tables(conn)
        for _ in range(3):
            _insert_email(
                conn,
                account_email="acc@example.com",
                from_email="risk@example.com",
                body_words=400,
                attachments=1,
                created_at=now_iso,
            )
        for _ in range(2):
            _insert_email(
                conn,
                account_email="acc@example.com",
                from_email="trust-only@example.com",
                body_words=120,
                created_at=now_iso,
            )

        conn.execute(
            "INSERT INTO relationship_health_snapshots (id, entity_id, health_score, created_at) VALUES (?, ?, ?, ?)",
            ("h1", "risk@example.com", 80.0, earlier),
        )
        conn.execute(
            "INSERT INTO relationship_health_snapshots (id, entity_id, health_score, created_at) VALUES (?, ?, ?, ?)",
            ("h2", "risk@example.com", 60.0, now_iso),
        )

        conn.execute(
            "INSERT INTO trust_snapshots (id, entity_id, trust_score, created_at) VALUES (?, ?, ?, ?)",
            ("t1", "trust-only@example.com", 0.8, earlier),
        )
        conn.execute(
            "INSERT INTO trust_snapshots (id, entity_id, trust_score, created_at) VALUES (?, ?, ?, ?)",
            ("t2", "trust-only@example.com", 0.6, now_iso),
        )

        conn.commit()

    result = compute_attention_economics(
        analytics=analytics,
        account_email="acc@example.com",
        window_days=7,
        now=now,
    )

    assert result is not None
    assert result.sample_size == 5

    at_risk_ids = {entity.entity_id for entity in result.at_risk}
    assert at_risk_ids == {"risk@example.com"}

