from __future__ import annotations

from datetime import datetime, timezone

import pytest

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
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload=payload,
        )
    )


def test_surprise_rate_requires_denominator(tmp_path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(2):
        _emit_event(
            emitter,
            event_type=EventType.SURPRISE_DETECTED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={},
        )

    metrics = analytics.behavior_metrics_digest(
        account_email="acc@example.com",
        window_days=7,
    )
    assert "surprise_rate" not in metrics

    for offset in range(4):
        _emit_event(
            emitter,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=now.timestamp() + 10 + offset,
            account_email="acc@example.com",
            payload={
                "old_priority": "🔵",
                "new_priority": "🔴",
            },
        )

    metrics = analytics.behavior_metrics_digest(
        account_email="acc@example.com",
        window_days=7,
    )
    assert metrics["surprise_rate"] == pytest.approx(0.5)


def test_compression_rate_counts_suppression_modes(tmp_path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset, mode in enumerate(["BATCH_TODAY", "DEFER_TO_MORNING", "IMMEDIATE"]):
        _emit_event(
            emitter,
            event_type=EventType.DELIVERY_POLICY_APPLIED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={"mode": mode},
        )

    metrics = analytics.behavior_metrics_digest(
        account_email="acc@example.com",
        window_days=7,
    )
    assert metrics["compression_rate"] == pytest.approx(2 / 3)


def test_attention_debt_distribution_counts_buckets(tmp_path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset, bucket in enumerate(["low", "low", "medium", "high"]):
        _emit_event(
            emitter,
            event_type=EventType.ATTENTION_DEBT_UPDATED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={"bucket": bucket},
        )

    metrics = analytics.behavior_metrics_digest(
        account_email="acc@example.com",
        window_days=7,
    )
    assert metrics["attention_debt_distribution"] == {
        "low": 2,
        "medium": 1,
        "high": 1,
    }


def test_signal_counts_include_deadlock_and_silence(tmp_path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(2):
        _emit_event(
            emitter,
            event_type=EventType.DEADLOCK_DETECTED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={"thread_key": "thread-1"},
        )
    _emit_event(
        emitter,
        event_type=EventType.SILENCE_SIGNAL_DETECTED,
        ts_utc=now.timestamp() + 10,
        account_email="acc@example.com",
        payload={"contact": "client@example.com"},
    )

    metrics = analytics.behavior_metrics_digest(
        account_email="acc@example.com",
        window_days=7,
    )
    assert metrics["signal_counts"] == {
        "deadlock_count": 2,
        "silence_count": 1,
    }


def test_behavior_metrics_digest_aggregates_account_scope(tmp_path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset, mode in enumerate(["BATCH_TODAY", "IMMEDIATE"]):
        _emit_event(
            emitter,
            event_type=EventType.DELIVERY_POLICY_APPLIED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={"mode": mode},
        )
    for offset, mode in enumerate(["DEFER_TO_MORNING", "IMMEDIATE"]):
        _emit_event(
            emitter,
            event_type=EventType.DELIVERY_POLICY_APPLIED,
            ts_utc=now.timestamp() + 10 + offset,
            account_email="alt@example.com",
            payload={"mode": mode},
        )

    metrics = analytics.behavior_metrics_digest(
        account_email="acc@example.com",
        account_emails=["acc@example.com", "alt@example.com"],
        window_days=7,
    )
    assert metrics["compression_rate"] == pytest.approx(0.5)
