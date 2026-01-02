from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_email(
    db: KnowledgeDB,
    *,
    account_email: str,
    from_email: str,
    subject: str,
    received_at: datetime,
    thread_key: str,
) -> None:
    email_id = db.save_email(
        account_email=account_email,
        from_email=from_email,
        subject=subject,
        received_at=received_at.isoformat(),
        priority="P0",
        action_line="",
        body_summary="",
        raw_body="",
        thread_key=thread_key,
        attachment_summaries=[],
    )
    assert email_id is not None


def test_deadlock_insights_dedupe_and_fields(tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)

    now = datetime.now(timezone.utc)
    account_email = "account@example.com"

    thread_one = "thread-1"
    _seed_email(
        db,
        account_email=account_email,
        from_email="old@example.com",
        subject="Сделка",
        received_at=now - timedelta(days=2),
        thread_key=thread_one,
    )
    _seed_email(
        db,
        account_email=account_email,
        from_email="new@example.com",
        subject="",
        received_at=now - timedelta(days=1),
        thread_key=thread_one,
    )

    thread_two = "thread-2"
    _seed_email(
        db,
        account_email=account_email,
        from_email="owner@example.com",
        subject="Контракт",
        received_at=now - timedelta(hours=2),
        thread_key=thread_two,
    )

    emitter.emit(
        EventV1(
            event_type=EventType.DEADLOCK_DETECTED,
            ts_utc=(now - timedelta(days=2)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={"thread_key": thread_one},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.DEADLOCK_DETECTED,
            ts_utc=(now - timedelta(days=1)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={"thread_key": thread_one},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.DEADLOCK_DETECTED,
            ts_utc=(now - timedelta(hours=1)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={"thread_key": thread_two},
        )
    )

    insights = analytics.get_deadlock_insights(
        account_email=account_email,
        window_days=7,
        limit=5,
    )
    item = next(
        insight for insight in insights if insight["thread_key"] == thread_one
    )
    assert item["subject"] == "Сделка"
    assert item["from_email"] == "new@example.com"

    limited = analytics.get_deadlock_insights(
        account_email=account_email,
        window_days=7,
        limit=1,
    )
    assert len(limited) == 1
    assert limited[0]["thread_key"] == thread_two


def test_silence_insights_dedupe_and_limit(tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)

    now = datetime.now(timezone.utc)
    account_email = "account@example.com"

    emitter.emit(
        EventV1(
            event_type=EventType.SILENCE_SIGNAL_DETECTED,
            ts_utc=(now - timedelta(days=1)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={
                "contact": "client@example.com",
                "days_silent": 3.6,
                "count_window": 4,
                "last_seen_ts": 100,
            },
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.SILENCE_SIGNAL_DETECTED,
            ts_utc=(now - timedelta(hours=1)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={
                "contact": "client@example.com",
                "days_silent": 5.2,
                "count_window": 6,
                "last_seen_ts": 200,
            },
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.SILENCE_SIGNAL_DETECTED,
            ts_utc=(now - timedelta(hours=2)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={
                "contact": "vendor@example.com",
                "days_silent": 2.1,
                "count_window": 3,
                "last_seen_ts": 150,
            },
        )
    )

    insights = analytics.get_silence_insights(
        account_email=account_email,
        window_days=7,
        limit=2,
    )
    assert len(insights) == 2
    by_contact = {item["contact"]: item for item in insights}
    assert by_contact["client@example.com"]["days_silent"] == 5
