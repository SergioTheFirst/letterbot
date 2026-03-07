from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics


def _emit_event(
    emitter: ContractEventEmitter,
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
    entity_id: str | None,
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=entity_id,
            email_id=None,
            payload=payload,
        )
    )


def test_commitment_chain_digest_orders_and_limits(tmp_path: Path) -> None:
    db_path = tmp_path / "commitment_chain.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)
    account_id = "acc@example.com"

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_STATUS_CHANGED,
        ts_utc=(now - timedelta(days=2)).timestamp(),
        account_id=account_id,
        entity_id="entity-expired",
        payload={
            "commitment_id": 1,
            "old_status": "pending",
            "new_status": "expired",
            "reason": "deadline_passed",
            "deadline_iso": "2024-07-01",
            "commitment_text": "Согласовать бюджет",
            "from_email": "exp@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_CREATED,
        ts_utc=(now - timedelta(days=1)).timestamp(),
        account_id=account_id,
        entity_id="entity-pending",
        payload={
            "commitment_text": "Отправить договор",
            "deadline_iso": "2024-07-12",
            "status": "pending",
            "source": "test",
            "confidence": 0.9,
            "from_email": "pending@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_CREATED,
        ts_utc=(now - timedelta(days=3)).timestamp(),
        account_id=account_id,
        entity_id="entity-old",
        payload={
            "commitment_text": "Уточнить сроки",
            "deadline_iso": "2024-07-05",
            "status": "pending",
            "source": "test",
            "confidence": 0.8,
            "from_email": "old@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_STATUS_CHANGED,
        ts_utc=(now - timedelta(days=1)).timestamp(),
        account_id=account_id,
        entity_id="entity-done",
        payload={
            "commitment_id": 4,
            "old_status": "pending",
            "new_status": "fulfilled",
            "reason": "confirmation_text",
            "deadline_iso": "2024-07-08",
            "commitment_text": "Подготовить отчёт",
            "from_email": "done@example.com",
        },
    )

    items = analytics.commitment_chain_digest_items(
        account_id,
        since_ts=(now - timedelta(days=30)).timestamp(),
        max_entities=2,
        max_items_per_entity=1,
    )

    assert [item["entity_label"] for item in items] == [
        "entity-expired",
        "entity-pending",
    ]
    assert items[0]["items"] == [
        {"text": "Согласовать бюджет", "status": "просрочено", "due": "2024-07-01"}
    ]
    assert items[1]["items"] == [
        {"text": "Отправить договор", "status": "ожидает", "due": "2024-07-12"}
    ]


def test_commitment_chain_digest_scopes_account_emails(tmp_path: Path) -> None:
    db_path = tmp_path / "commitment_chain_scope.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_STATUS_CHANGED,
        ts_utc=(now - timedelta(days=2)).timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={
            "commitment_id": 1,
            "old_status": "pending",
            "new_status": "expired",
            "deadline_iso": "2024-07-01",
            "commitment_text": "Согласовать бюджет",
            "from_email": "a@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_CREATED,
        ts_utc=(now - timedelta(days=1)).timestamp(),
        account_id="acc-b@example.com",
        entity_id="entity-b",
        payload={
            "commitment_text": "Отправить договор",
            "deadline_iso": "2024-07-12",
            "status": "pending",
            "source": "test",
            "confidence": 0.9,
            "from_email": "b@example.com",
        },
    )

    items = analytics.commitment_chain_digest_items(
        "acc-a@example.com",
        account_emails=["acc-a@example.com", "acc-b@example.com"],
        since_ts=(now - timedelta(days=30)).timestamp(),
        max_entities=3,
        max_items_per_entity=1,
    )

    assert [item["entity_label"] for item in items] == ["entity-a", "entity-b"]


def test_commitment_chain_digest_empty_scope_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "commitment_chain_empty.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_STATUS_CHANGED,
        ts_utc=(now - timedelta(days=2)).timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={
            "commitment_id": 1,
            "old_status": "pending",
            "new_status": "expired",
            "deadline_iso": "2024-07-01",
            "commitment_text": "Согласовать бюджет",
            "from_email": "a@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_CREATED,
        ts_utc=(now - timedelta(days=1)).timestamp(),
        account_id="acc-b@example.com",
        entity_id="entity-b",
        payload={
            "commitment_text": "Отправить договор",
            "deadline_iso": "2024-07-12",
            "status": "pending",
            "source": "test",
            "confidence": 0.9,
            "from_email": "b@example.com",
        },
    )

    items = analytics.commitment_chain_digest_items(
        "acc-a@example.com",
        account_emails=[],
        since_ts=(now - timedelta(days=30)).timestamp(),
        max_entities=3,
        max_items_per_entity=1,
    )

    assert [item["entity_label"] for item in items] == ["entity-a"]
