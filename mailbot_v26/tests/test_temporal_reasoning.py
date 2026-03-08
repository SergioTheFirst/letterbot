from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from mailbot_v26.insights.temporal_reasoning import TemporalReasoningEngine
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _init_db(tmp_path) -> tuple[KnowledgeDB, ContextStore, KnowledgeAnalytics]:
    db_path = tmp_path / "knowledge.sqlite"
    db = KnowledgeDB(db_path)
    store = ContextStore(db_path)
    analytics = KnowledgeAnalytics(db_path)
    return db, store, analytics


def _insert_email(
    conn: sqlite3.Connection, *, from_email: str, received_at: str
) -> int:
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


def _insert_commitment(
    conn: sqlite3.Connection,
    *,
    email_row_id: int,
    deadline_iso: str,
    status: str = "created",
) -> None:
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
        (email_row_id, "email", "Prepare report", deadline_iso, status, 1.0),
    )
    conn.commit()


def test_temporal_reasoning_detects_commitment_deadline_risk(tmp_path) -> None:
    _, store, analytics = _init_db(tmp_path)
    now = datetime.now(timezone.utc)

    resolution = store.resolve_sender_entity(
        from_email="sender@example.com",
        from_name="Sender",
    )
    assert resolution is not None

    with sqlite3.connect(analytics.path) as conn:
        email_row_id = _insert_email(
            conn,
            from_email="sender@example.com",
            received_at=now.isoformat(),
        )
        _insert_commitment(
            conn,
            email_row_id=email_row_id,
            deadline_iso=(now + timedelta(hours=12)).isoformat(),
        )

    engine = TemporalReasoningEngine(analytics)
    states = engine.evaluate(
        entity_id=resolution.entity_id,
        from_email="sender@example.com",
        now=now,
    )

    assert any(state.state_type == "commitment_deadline_risk" for state in states)


def test_temporal_reasoning_detects_response_overdue_and_silence_break(
    tmp_path,
) -> None:
    _, store, analytics = _init_db(tmp_path)
    now = datetime.now(timezone.utc)

    resolution = store.resolve_sender_entity(
        from_email="sender@example.com",
        from_name="Sender",
    )
    assert resolution is not None

    last_received = now - timedelta(days=20)
    for offset in range(21, 27):
        store.record_interaction_event(
            entity_id=resolution.entity_id,
            event_type="email_received",
            event_time=now - timedelta(days=offset),
            metadata={"email_id": offset},
        )
    store.record_interaction_event(
        entity_id=resolution.entity_id,
        event_type="email_received",
        event_time=last_received,
        metadata={"email_id": 999},
    )
    store.recompute_email_frequency(entity_id=resolution.entity_id, now=now)

    for hours in (4.0, 6.0, 5.0):
        store.record_interaction_event(
            entity_id=resolution.entity_id,
            event_type="response_time",
            event_time=now - timedelta(days=5),
            metadata={"response_time_hours": hours},
        )

    engine = TemporalReasoningEngine(analytics)
    states = engine.evaluate(
        entity_id=resolution.entity_id,
        from_email="sender@example.com",
        now=now,
    )

    state_types = {state.state_type for state in states}
    assert "response_overdue" in state_types
    assert "silence_break" in state_types
