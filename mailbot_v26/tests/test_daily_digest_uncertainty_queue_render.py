from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from mailbot_v26.config.uncertainty_queue import UncertaintyQueueConfig
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import daily_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics


def _base_digest_kwargs() -> dict[str, object]:
    return dict(
        deferred_total=0,
        deferred_attachments_only=0,
        deferred_informational=0,
        deferred_items=[],
        uncertainty_queue_items=[],
        commitments_pending=0,
        commitments_expired=0,
        trust_delta=None,
        health_delta=None,
        anomaly_alerts=[],
        attention_economics=None,
        quality_metrics=None,
        notification_sla=None,
        deadlock_insights=[],
        silence_insights=[],
        digest_insights_enabled=False,
        digest_insights_max_items=0,
        digest_action_templates_enabled=False,
        trust_bootstrap_snapshot=None,
        trust_bootstrap_min_samples=0,
        trust_bootstrap_hide_action_templates=False,
        regret_minimization_stats=None,
    )


def _seed_uncertainty_events(emitter: ContractEventEmitter, account_email: str) -> None:
    now = datetime.now(timezone.utc)
    payloads = [
        {
            "priority": "high",
            "confidence": 60,
            "sender": "first@example.com",
            "subject": "Первое",
            "engine": "rules",
            "offset": timedelta(hours=1),
        },
        {
            "priority": "high",
            "confidence": 65,
            "sender": "second@example.com",
            "subject": "Второе",
            "engine": "priority_v2",
            "offset": timedelta(hours=2),
        },
        {
            "priority": "low",
            "confidence": 80,
            "sender": "skip@example.com",
            "subject": "Пропуск",
            "engine": "shadow",
            "offset": timedelta(hours=3),
        },
        {
            "priority": "low",
            "confidence": 40,
            "sender": "",
            "subject": "",
            "engine": "shadow",
            "offset": timedelta(hours=4),
        },
    ]
    for idx, payload in enumerate(payloads, start=1):
        ts = (now - payload.pop("offset")).timestamp()
        emitter.emit(
            EventV1(
                event_type=EventType.PRIORITY_DECISION_RECORDED,
                ts_utc=ts,
                account_id=account_email,
                entity_id=None,
                email_id=idx,
                payload=payload,
            )
        )


def test_daily_digest_uncertainty_queue_absent_when_flag_off(tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    _seed_uncertainty_events(emitter, "account@example.com")

    data = daily_digest._collect_digest_data(
        analytics=analytics,
        account_email="account@example.com",
        include_uncertainty_queue=False,
        now=datetime.now(timezone.utc),
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "Требуют уточнения" not in text


def test_daily_digest_uncertainty_queue_rendered_and_limited(tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    _seed_uncertainty_events(emitter, "account@example.com")

    data = daily_digest._collect_digest_data(
        analytics=analytics,
        account_email="account@example.com",
        include_uncertainty_queue=True,
        uncertainty_queue_config=UncertaintyQueueConfig(
            window_days=1,
            min_confidence=70,
            max_items=2,
        ),
        now=datetime.now(timezone.utc),
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "<b>Требуют уточнения</b>" in text
    assert "Низкая уверенность по приоритету: 2" in text
    assert "first@example.com — Первое (60%)" in text
    assert "second@example.com — Второе (65%)" in text
    assert "Пропуск" not in text
    assert "40%" not in text


def test_daily_digest_uncertainty_queue_aggregates_scope_accounts(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "digest.sqlite"
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    def emit(
        *,
        account_email: str,
        hours: float,
        sender: str,
        subject: str,
        confidence: int,
    ) -> None:
        emitter.emit(
            EventV1(
                event_type=EventType.PRIORITY_DECISION_RECORDED,
                ts_utc=(now - timedelta(hours=hours)).timestamp(),
                account_id=account_email,
                entity_id=None,
                email_id=int(hours * 10),
                payload={
                    "priority": "high",
                    "confidence": confidence,
                    "sender": sender,
                    "subject": subject,
                    "engine": "rules",
                },
            )
        )

    emit(
        account_email="account@example.com",
        hours=1,
        sender="alpha@example.com",
        subject="Альфа",
        confidence=60,
    )
    emit(
        account_email="account@example.com",
        hours=4,
        sender="delta@example.com",
        subject="Дельта",
        confidence=65,
    )
    emit(
        account_email="alt@example.com",
        hours=2,
        sender="beta@example.com",
        subject="Бета",
        confidence=55,
    )
    emit(
        account_email="alt@example.com",
        hours=3,
        sender="gamma@example.com",
        subject="Гамма",
        confidence=68,
    )
    emit(
        account_email="alt@example.com",
        hours=0.5,
        sender="skip@example.com",
        subject="Высокая",
        confidence=85,
    )

    monkeypatch.setattr(
        daily_digest,
        "resolve_account_scope",
        lambda _: SimpleNamespace(
            account_emails=["account@example.com", "alt@example.com"]
        ),
    )

    data = daily_digest._collect_digest_data(
        analytics=analytics,
        account_email="account@example.com",
        include_uncertainty_queue=True,
        uncertainty_queue_config=UncertaintyQueueConfig(
            window_days=1,
            min_confidence=70,
            max_items=3,
        ),
        now=now,
    )
    text = daily_digest._build_digest_text(data, locale="ru")

    assert "Низкая уверенность по приоритету: 3" in text
    assert "alpha@example.com — Альфа (60%)" in text
    assert "beta@example.com — Бета (55%)" in text
    assert "gamma@example.com — Гамма (68%)" in text
    assert "delta@example.com — Дельта (65%)" not in text
    assert "Высокая" not in text


def test_bootstrap_keeps_action_templates_suppressed() -> None:
    snapshot = daily_digest.TrustBootstrapSnapshot(
        start_ts=1.0,
        days_since_start=1.0,
        samples_count=1,
        corrections_count=0,
        surprises_count=0,
        surprise_rate=None,
        active=True,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "trust_bootstrap_snapshot": snapshot,
            "trust_bootstrap_min_samples": 10,
            "digest_insights_enabled": True,
            "digest_insights_max_items": 1,
            "digest_action_templates_enabled": True,
            "deadlock_insights": [
                {"from_email": "boss@example.com", "subject": "Счёт"}
            ],
            "uncertainty_queue_items": [
                {"sender": "check@example.com", "subject": "Проверка", "confidence": 40}
            ],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "Текст:" not in text
