from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from mailbot_v26.insights.narrative_composer import compose_narrative
from mailbot_v26.storage.analytics import KnowledgeAnalytics


def _analytics(tmp_path) -> KnowledgeAnalytics:
    db_path = tmp_path / "events.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE interaction_events (
                entity_id TEXT,
                event_type TEXT,
                event_time TEXT,
                metadata TEXT
            )
            """)
    return KnowledgeAnalytics(db_path)


def _insert_event(
    conn, *, entity_id: str, event_type: str, event_time: datetime
) -> None:
    conn.execute(
        """
        INSERT INTO interaction_events (entity_id, event_type, event_time, metadata)
        VALUES (?, ?, ?, ?)
        """,
        (entity_id, event_type, event_time.isoformat(), "{}"),
    )


def test_fact_only_amount_deadline_type(tmp_path) -> None:
    narrative = compose_narrative(
        email_id=101,
        subject="Счет на оплату",
        body_text="Сумма 120 000 ₽, оплатить до 15.01.2024.",
        from_email="finance@example.com",
        mail_type="INVOICE_FINAL",
        received_at=datetime(2024, 1, 10, 9, 0, tzinfo=timezone.utc),
        attachments=[{"filename": "invoice.pdf"}],
        entity_id=None,
        analytics=None,
        enable_patterns=False,
    )

    assert narrative is not None
    assert "Тип: INVOICE_FINAL" in narrative.fact
    assert "Сумма: 120 000 ₽" in narrative.fact
    assert "Дедлайн: 2024-01-15" in narrative.fact


def test_pattern_suppressed_without_samples(tmp_path) -> None:
    analytics = _analytics(tmp_path)
    entity_id = "entity-1"
    now = datetime.now(timezone.utc)
    with sqlite3.connect(analytics.path) as conn:
        _insert_event(
            conn,
            entity_id=entity_id,
            event_type="email_received",
            event_time=now - timedelta(days=2),
        )
        _insert_event(
            conn,
            entity_id=entity_id,
            event_type="email_received",
            event_time=now - timedelta(days=10),
        )
        conn.commit()

    narrative = compose_narrative(
        email_id=102,
        subject="Reminder",
        body_text="Просим оплатить.",
        from_email="billing@example.com",
        mail_type="PAYMENT_REMINDER",
        received_at=now,
        attachments=[],
        entity_id=entity_id,
        analytics=analytics,
        enable_patterns=True,
    )

    assert narrative is not None
    assert narrative.pattern is None


def test_pattern_emitted_with_samples(tmp_path) -> None:
    analytics = _analytics(tmp_path)
    entity_id = "entity-2"
    now = datetime.now(timezone.utc)
    with sqlite3.connect(analytics.path) as conn:
        for offset in range(10):
            _insert_event(
                conn,
                entity_id=entity_id,
                event_type="email_received",
                event_time=now - timedelta(days=(offset % 7) + 1),
            )
        for offset in range(8, 11):
            _insert_event(
                conn,
                entity_id=entity_id,
                event_type="email_received",
                event_time=now - timedelta(days=offset),
            )
        conn.commit()

    narrative = compose_narrative(
        email_id=103,
        subject="Update",
        body_text="Новый статус",
        from_email="ops@example.com",
        mail_type="STATUS_UPDATE",
        received_at=now,
        attachments=[],
        entity_id=entity_id,
        analytics=analytics,
        enable_patterns=True,
    )

    assert narrative is not None
    assert narrative.pattern == "Обычно 3/нед, сейчас 10/нед."


def test_recommendation_rules(tmp_path) -> None:
    narrative = compose_narrative(
        email_id=104,
        subject="Contract",
        body_text="Расторжение договора.",
        from_email="legal@example.com",
        mail_type="CONTRACT_TERMINATION",
        received_at=datetime(2024, 1, 10, 10, 0, tzinfo=timezone.utc),
        attachments=[],
        entity_id=None,
        analytics=None,
        enable_patterns=False,
    )

    assert narrative is not None
    assert narrative.action == "Проверить условия и ответить юридической команде."


def test_length_trimming_is_deterministic(tmp_path) -> None:
    long_sender = "a" * 220 + "@example.com"
    narrative = compose_narrative(
        email_id=105,
        subject="Invoice",
        body_text="Счет на оплату до 11.01.2024.",
        from_email=long_sender,
        mail_type="INVOICE",
        received_at=datetime(2024, 1, 10, 10, 0, tzinfo=timezone.utc),
        attachments=[{"filename": "file.pdf"}],
        entity_id=None,
        analytics=None,
        enable_patterns=False,
    )

    assert narrative is not None
    assert len(narrative.fact) == 180
    assert narrative.fact.endswith("…")
