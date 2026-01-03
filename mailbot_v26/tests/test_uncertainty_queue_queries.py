from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics


def test_uncertainty_queue_items_filters_and_orders(tmp_path) -> None:
    db_path = tmp_path / "events.sqlite"
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)

    now = datetime.now(timezone.utc)
    account_email = "account@example.com"

    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_DECISION_RECORDED,
            ts_utc=(now - timedelta(hours=3)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=1,
            payload={
                "priority": "high",
                "confidence": 65,
                "sender": "older@example.com",
                "subject": "Old",
                "engine": "rules",
            },
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_DECISION_RECORDED,
            ts_utc=(now - timedelta(hours=1)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=2,
            payload={
                "priority": "high",
                "confidence": 50,
                "sender": "new@example.com",
                "subject": "New",
                "engine": "priority_v2",
            },
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_DECISION_RECORDED,
            ts_utc=(now - timedelta(hours=2)).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=3,
            payload={
                "priority": "low",
                "confidence": 80,
                "sender": "skip@example.com",
                "subject": "Skip",
                "engine": "shadow",
            },
        )
    )

    items = analytics.uncertainty_queue_items(
        account_email,
        since_ts=(now - timedelta(days=1)).timestamp(),
        min_confidence=70,
        limit=2,
    )
    assert [item["sender"] for item in items] == [
        "new@example.com",
        "older@example.com",
    ]
    assert [item["confidence"] for item in items] == [50, 65]

    recent_items = analytics.uncertainty_queue_items(
        account_email,
        since_ts=(now - timedelta(hours=2)).timestamp(),
        min_confidence=70,
        limit=2,
    )
    assert [item["sender"] for item in recent_items] == ["new@example.com"]
