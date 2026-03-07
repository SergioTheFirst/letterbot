from __future__ import annotations

from datetime import datetime, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.insights.quality_metrics import compute_quality_metrics
from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_received(
    emitter: ContractEventEmitter,
    *,
    ts: datetime,
    account_id: str,
    email_id: int,
    priority: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=ts.timestamp(),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={"priority": priority, "from_email": "a@example.com"},
        )
    )


def _emit_correction(
    emitter: ContractEventEmitter,
    *,
    ts: datetime,
    account_id: str,
    email_id: int,
    old_priority: str,
    new_priority: str,
    engine: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=ts.timestamp(),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "old_priority": old_priority,
                "new_priority": new_priority,
                "engine": engine,
                "source": "preview_buttons",
                "account_email": account_id,
                "sender_email": "a@example.com",
                "system_mode": "FULL",
            },
        )
    )


def test_quality_metrics_weekly_summary_deterministic(tmp_path) -> None:
    db_path = tmp_path / "quality.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 1, 7, tzinfo=timezone.utc)

    _emit_received(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=1,
        priority="🟡",
    )
    _emit_received(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=2,
        priority="🔵",
    )
    _emit_received(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=3,
        priority="🔴",
    )
    _emit_correction(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=1,
        old_priority="🟡",
        new_priority="🔴",
        engine="priority_v2",
    )
    _emit_correction(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=2,
        old_priority="🔵",
        new_priority="🔴",
        engine="auto",
    )

    metrics = compute_quality_metrics(
        analytics=analytics,
        account_email="acc",
        window_days=7,
        now=now,
    )

    assert metrics is not None
    assert metrics.corrections_total == 2
    assert metrics.emails_received == 3
    assert metrics.correction_rate == 2 / 3
    assert metrics.by_new_priority[0].key == "🔴"
    assert metrics.by_new_priority[0].count == 2
    assert metrics.by_engine[0].key == "auto"
    assert metrics.by_engine[0].count == 1


def test_compute_quality_metrics_aggregates_account_scope(tmp_path) -> None:
    db_path = tmp_path / "quality_scope.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    anchor = datetime(2024, 1, 7, tzinfo=timezone.utc)

    _emit_received(
        contract_emitter,
        ts=anchor,
        account_id="acc",
        email_id=1,
        priority="🟡",
    )
    _emit_received(
        contract_emitter,
        ts=anchor,
        account_id="alt",
        email_id=2,
        priority="🔵",
    )
    _emit_correction(
        contract_emitter,
        ts=anchor,
        account_id="acc",
        email_id=1,
        old_priority="🟡",
        new_priority="🔴",
        engine="priority_v2",
    )
    _emit_correction(
        contract_emitter,
        ts=anchor,
        account_id="alt",
        email_id=2,
        old_priority="🔵",
        new_priority="🟡",
        engine="auto",
    )

    metrics = compute_quality_metrics(
        analytics=analytics,
        account_email="acc",
        account_emails=["acc", "alt"],
        window_days=1,
        now=anchor,
    )

    assert metrics is not None
    assert metrics.corrections_total == 2
    assert metrics.emails_received == 2
    assert metrics.correction_rate == 1.0


def test_weekly_digest_includes_quality_block_when_flag_on(tmp_path) -> None:
    db_path = tmp_path / "digest_quality.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 1, 7, tzinfo=timezone.utc)
    week_key = weekly_digest._iso_week_key(now)

    _emit_received(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=10,
        priority="🟡",
    )
    _emit_received(
        contract_emitter,
        ts=now,
        account_id="alt",
        email_id=11,
        priority="🔵",
    )
    _emit_correction(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=10,
        old_priority="🟡",
        new_priority="🔴",
        engine="priority_v2",
    )
    _emit_correction(
        contract_emitter,
        ts=now,
        account_id="alt",
        email_id=11,
        old_priority="🔵",
        new_priority="🟡",
        engine="auto",
    )

    data_with_block = weekly_digest._collect_weekly_data(
        analytics=analytics,
        account_email="acc",
        account_emails=["acc", "alt"],
        week_key=week_key,
        include_quality_metrics=True,
        now=now,
    )
    text_with = weekly_digest._build_weekly_digest_text(data_with_block)

    data_without_block = weekly_digest._collect_weekly_data(
        analytics=analytics,
        account_email="acc",
        week_key=week_key,
        include_quality_metrics=False,
        now=now,
    )
    text_without = weekly_digest._build_weekly_digest_text(data_without_block)

    assert "Качество" not in text_with
    assert "Исправления по приоритету" not in text_with
    assert "Качество" not in text_without
