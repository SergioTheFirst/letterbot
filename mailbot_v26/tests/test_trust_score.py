from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from mailbot_v26.insights.trust_score import TrustScoreCalculator
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_entity(db_path, from_email: str) -> str:
    store = ContextStore(db_path)
    resolution = store.resolve_sender_entity(
        from_email=from_email,
        from_name="Sender",
    )
    assert resolution is not None
    return resolution.entity_id


def _insert_email(conn: sqlite3.Connection, *, from_email: str, received_at: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO emails (
            account_email,
            from_email,
            subject,
            received_at,
            priority
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("account@example.com", from_email, "Subject", received_at, "🔵"),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_commitment(conn: sqlite3.Connection, *, email_row_id: int, status: str) -> None:
    conn.execute(
        """
        INSERT INTO commitments (
            email_row_id,
            source,
            commitment_text,
            deadline_iso,
            status,
            confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            email_row_id,
            "email",
            "Send report",
            None,
            status,
            1.0,
        ),
    )
    conn.commit()


def _seed_interactions(db_path, entity_id: str, now: datetime) -> None:
    store = ContextStore(db_path)
    for offset in (5, 10, 15, 35, 40):
        store.record_interaction_event(
            entity_id=entity_id,
            event_type="email_received",
            event_time=now - timedelta(days=offset),
            metadata={"source": "test"},
        )


def test_trust_score_insufficient_data(tmp_path) -> None:
    db_path = tmp_path / "trust.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")

    analytics = KnowledgeAnalytics(db_path)
    calculator = TrustScoreCalculator(analytics)
    result = calculator.compute(
        entity_id=entity_id,
        from_email="sender@example.com",
    )

    assert result.snapshot.score is None
    assert result.snapshot.reason == "insufficient_data"


def test_trust_score_commitment_reliability_influences_score(tmp_path) -> None:
    now = datetime.utcnow()

    def _compute_score(status: str) -> float:
        db_path = tmp_path / f"trust_{status}.sqlite"
        KnowledgeDB(db_path)
        entity_id = _seed_entity(db_path, "sender@example.com")
        _seed_interactions(db_path, entity_id, now)
        with sqlite3.connect(db_path) as conn:
            for _ in range(2):
                email_row_id = _insert_email(
                    conn,
                    from_email="sender@example.com",
                    received_at=now.isoformat(),
                )
                _insert_commitment(conn, email_row_id=email_row_id, status=status)
        analytics = KnowledgeAnalytics(db_path)
        calculator = TrustScoreCalculator(analytics)
        result = calculator.compute(
            entity_id=entity_id,
            from_email="sender@example.com",
        )
        assert result.snapshot.score is not None
        return float(result.snapshot.score)

    fulfilled_score = _compute_score("fulfilled")
    expired_score = _compute_score("expired")

    assert fulfilled_score > expired_score
