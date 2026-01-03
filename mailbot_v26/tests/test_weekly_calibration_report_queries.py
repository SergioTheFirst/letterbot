from __future__ import annotations

from datetime import datetime, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_event(
    emitter: ContractEventEmitter,
    *,
    event_type: EventType,
    ts_utc: float,
    account_email: str,
    entity_id: str | None,
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_email,
            entity_id=entity_id,
            email_id=None,
            payload=payload,
        )
    )


def test_weekly_surprise_breakdown_returns_none_when_below_min(tmp_path) -> None:
    db_path = tmp_path / "calibration.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(2):
        _emit_event(
            emitter,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            entity_id="entity-a",
            payload={"old_priority": "low", "new_priority": "high"},
        )

    _emit_event(
        emitter,
        event_type=EventType.SURPRISE_DETECTED,
        ts_utc=now.timestamp() + 10,
        account_email="acc@example.com",
        entity_id="entity-a",
        payload={},
    )

    report = analytics.weekly_surprise_breakdown(
        "acc@example.com",
        since_ts=now.timestamp() - (7 * 86400),
        top_n=3,
        min_corrections=3,
    )
    assert report is None


def test_weekly_surprise_breakdown_counts_accuracy_and_top(tmp_path) -> None:
    db_path = tmp_path / "calibration.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(12):
        _emit_event(
            emitter,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            entity_id="entity-a",
            payload={"old_priority": "low", "new_priority": "high"},
        )

    for offset in range(3):
        _emit_event(
            emitter,
            event_type=EventType.SURPRISE_DETECTED,
            ts_utc=now.timestamp() + 20 + offset,
            account_email="acc@example.com",
            entity_id="entity-a",
            payload={},
        )

    for offset in range(2):
        _emit_event(
            emitter,
            event_type=EventType.SURPRISE_DETECTED,
            ts_utc=now.timestamp() + 30 + offset,
            account_email="acc@example.com",
            entity_id="entity-b",
            payload={},
        )

    _emit_event(
        emitter,
        event_type=EventType.SURPRISE_DETECTED,
        ts_utc=now.timestamp() + 40,
        account_email="acc@example.com",
        entity_id=None,
        payload={},
    )

    report = analytics.weekly_surprise_breakdown(
        "acc@example.com",
        since_ts=now.timestamp() - (7 * 86400),
        top_n=3,
        min_corrections=10,
    )

    assert report is not None
    assert report["corrections"] == 12
    assert report["surprises"] == 6
    assert report["accuracy_pct"] == 50
    assert report["top"] == [
        {"label": "entity-a", "count": 3},
        {"label": "entity-b", "count": 2},
        {"label": "контакт", "count": 1},
    ]
