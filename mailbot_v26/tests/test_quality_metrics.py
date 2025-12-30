from __future__ import annotations

from datetime import datetime, timezone
import sqlite3

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.insights.quality_metrics import compute_quality_metrics
from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_delivered(
    emitter: ContractEventEmitter,
    *,
    ts: datetime,
    account_id: str,
    email_id: int,
    priority: str,
    mail_type: str,
    from_email: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.TELEGRAM_DELIVERED,
            ts_utc=ts.timestamp(),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "priority": priority,
                "mail_type": mail_type,
                "from_email": from_email,
                "render_mode": "FULL",
            },
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
                "engine": "priority_v2_shadow",
                "model_version": "v2.0",
                "reason_codes": ["rule"],
            },
        )
    )


def test_quality_metrics_weekly_summary_deterministic(tmp_path) -> None:
    db_path = tmp_path / "quality.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 1, 7, tzinfo=timezone.utc)

    _emit_delivered(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=1,
        priority="🟡",
        mail_type="invoice.final",
        from_email="a@example.com",
    )
    _emit_delivered(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=2,
        priority="🔵",
        mail_type="update",
        from_email="b@example.com",
    )
    _emit_correction(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=1,
        old_priority="🟡",
        new_priority="🔴",
    )

    metrics = compute_quality_metrics(
        analytics=analytics,
        account_email="acc",
        window_days=7,
        now=now,
    )

    assert metrics.corrections_total == 1
    assert metrics.evaluated_total == 2
    assert metrics.accuracy == 0.5
    assert metrics.by_mail_type[0].key == "invoice.final"
    assert metrics.by_mail_type[0].corrections == 1
    assert metrics.top_errors[0]["mail_type"] == "invoice.final"


def test_weekly_digest_includes_quality_block_when_flag_on(tmp_path) -> None:
    db_path = tmp_path / "digest_quality.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 1, 7, tzinfo=timezone.utc)
    week_key = weekly_digest._iso_week_key(now)

    _emit_delivered(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=10,
        priority="🟡",
        mail_type="invoice.final",
        from_email="a@example.com",
    )
    _emit_correction(
        contract_emitter,
        ts=now,
        account_id="acc",
        email_id=10,
        old_priority="🟡",
        new_priority="🔴",
    )

    data_with_block = weekly_digest._collect_weekly_data(
        analytics=analytics,
        account_email="acc",
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

    assert "Качество" in text_with
    assert "Топ-ошибки" in text_with
    assert "Качество" not in text_without
    assert "Топ-ошибки" not in text_without
