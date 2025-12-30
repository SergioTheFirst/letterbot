from datetime import datetime, timedelta, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.notification_sla import (
    ErrorBreakdown,
    NotificationAlertStore,
    NotificationSLAResult,
    compute_notification_sla,
)
from mailbot_v26.pipeline import daily_digest
from mailbot_v26.pipeline.weekly_digest import (
    WeeklyDigestData,
    _build_weekly_digest_text,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system.orchestrator import SystemOrchestrator
from mailbot_v26.system_health import OperationalMode


def _emit_event(emitter: ContractEventEmitter, event: EventV1) -> None:
    emitter.emit(event)


def test_notification_sla_percentiles_and_rates(tmp_path) -> None:
    db_path = tmp_path / "events.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime(2024, 1, 2, tzinfo=timezone.utc)

    detected = now - timedelta(hours=1)
    delivered_at = detected + timedelta(seconds=30)
    _emit_event(
        emitter,
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=detected.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=1,
            payload={"occurred_at_utc": detected.timestamp()},
        ),
    )
    _emit_event(
        emitter,
        EventV1(
            event_type=EventType.TELEGRAM_DELIVERED,
            ts_utc=delivered_at.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=1,
            payload={
                "delivered": True,
                "occurred_at_utc": delivered_at.timestamp(),
                "mode": "html",
                "retry_count": 0,
            },
        ),
    )

    salvage_detected = now - timedelta(minutes=30)
    salvage_delivered = salvage_detected + timedelta(seconds=10)
    _emit_event(
        emitter,
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=salvage_detected.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=2,
            payload={"occurred_at_utc": salvage_detected.timestamp()},
        ),
    )
    _emit_event(
        emitter,
        EventV1(
            event_type=EventType.TELEGRAM_DELIVERED,
            ts_utc=salvage_delivered.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=2,
            payload={
                "delivered": True,
                "occurred_at_utc": salvage_delivered.timestamp(),
                "mode": "plain_salvage",
                "retry_count": 1,
            },
        ),
    )

    stale_detected = now - timedelta(hours=4)
    _emit_event(
        emitter,
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=stale_detected.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=3,
            payload={"occurred_at_utc": stale_detected.timestamp()},
        ),
    )

    sla = compute_notification_sla(analytics=analytics, now=now)

    assert sla.delivery_rate_24h == 2 / 3
    assert sla.salvage_rate_24h == 0.5
    assert sla.p50_latency_24h == 20.0
    assert sla.p90_latency_24h == 28.0
    assert sla.undelivered_24h == 1


def test_policy_marks_notification_degraded_when_sla_bad() -> None:
    orchestrator = SystemOrchestrator()
    sla = NotificationSLAResult(
        delivery_rate_24h=0.5,
        delivery_rate_7d=0.9,
        salvage_rate_24h=0.0,
        p50_latency_24h=10.0,
        p90_latency_24h=130.0,
        p99_latency_24h=150.0,
        p50_latency_7d=12.0,
        p90_latency_7d=20.0,
        p99_latency_7d=40.0,
        top_error_reasons_24h=[],
        error_rate_24h=0.5,
        undelivered_24h=5,
        delivered_24h=5,
        total_24h=10,
    )
    decision = orchestrator.evaluate(
        system_mode=OperationalMode.FULL,
        metrics=None,
        gates=None,
        runtime_flags=None,
        feature_flags=None,
        telegram_ok=True,
        has_daily_digest_content=False,
        has_weekly_digest_content=False,
        notification_sla=sla,
    )
    assert decision.telegram_health_degraded is True
    assert any(reason.startswith("notification_sla") for reason in decision.reasons)


def test_alert_store_dedupe_and_cooldown(tmp_path) -> None:
    store = NotificationAlertStore(tmp_path / "alerts.sqlite")
    fingerprint = "rate_low"
    assert store.should_alert(fingerprint=fingerprint) is True
    store.save_alert(fingerprint)
    assert store.should_alert(fingerprint=fingerprint) is False
    later = datetime.now(timezone.utc) + timedelta(hours=7)
    assert store.should_alert(fingerprint=fingerprint, now=later) is True
    assert store.record_failure() == 1
    store.reset_failures()
    assert store.record_failure() == 1


def test_digest_blocks_include_notification_sla() -> None:
    sla = NotificationSLAResult(
        delivery_rate_24h=0.98,
        delivery_rate_7d=0.99,
        salvage_rate_24h=0.03,
        p50_latency_24h=5.0,
        p90_latency_24h=14.0,
        p99_latency_24h=30.0,
        p50_latency_7d=6.0,
        p90_latency_7d=16.0,
        p99_latency_7d=32.0,
        top_error_reasons_24h=[ErrorBreakdown(reason="parse", count=2, share=0.02)],
        error_rate_24h=0.02,
        undelivered_24h=1,
        delivered_24h=50,
        total_24h=51,
    )
    digest_data = daily_digest.DigestData(
        deferred_total=0,
        deferred_attachments_only=0,
        deferred_informational=0,
        commitments_pending=0,
        commitments_expired=0,
        trust_delta=None,
        health_delta=None,
        anomaly_alerts=[],
        attention_economics=None,
        quality_metrics=None,
        notification_sla=sla,
    )
    text = daily_digest._build_digest_text(digest_data)
    assert "Надёжность уведомлений" in text

    weekly_data = _build_weekly_digest_text(
        WeeklyDigestData(
            week_key="2024-W01",
            total_emails=0,
            deferred_emails=0,
            attention_entities=[],
            commitment_counts={},
            overdue_commitments=[],
            trust_deltas={},
            anomaly_alerts=[],
            notification_sla=sla,
            previous_week_sla=sla,
        )
    )
    assert "Надёжность уведомлений" in weekly_data

