from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mailbot_v26.config_loader import AccountScope
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import daily_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB

_TARGET_EMOJI = "\U0001f3af"


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
        digest_insights_enabled=True,
        digest_insights_max_items=3,
        digest_action_templates_enabled=False,
        trust_bootstrap_snapshot=None,
        trust_bootstrap_min_samples=0,
        trust_bootstrap_hide_action_templates=False,
    )


def test_daily_digest_insights_section_absent_when_empty() -> None:
    data = daily_digest.DigestData(**_base_digest_kwargs())
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "ТРЕБУЕТ ВНИМАНИЯ" not in text


def test_daily_digest_insights_section_present_with_items() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "deadlock_insights": [
                {
                    "from_email": "boss@example.com",
                    "subject": "Счёт",
                }
            ],
            "silence_insights": [
                {
                    "contact": "client@example.com",
                    "days_silent": 5,
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "\u26a0\ufe0f <b>ТРЕБУЕТ ВНИМАНИЯ</b>" in text
    assert "Застой в переписке" in text
    assert "Нет ответа" in text
    assert _TARGET_EMOJI in text
    assert "пинговать" not in text
    assert "Deadlock" not in text
    assert "Silence" not in text


def test_daily_digest_insights_order_and_limit() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "digest_insights_max_items": 3,
            "deadlock_insights": [
                {"from_email": "a@example.com", "subject": "A1"},
                {"from_email": "b@example.com", "subject": "B1"},
            ],
            "silence_insights": [
                {"contact": "c@example.com", "days_silent": 3},
                {"contact": "d@example.com", "days_silent": 4},
            ],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    lines = text.splitlines()
    header_index = lines.index("\u26a0\ufe0f <b>ТРЕБУЕТ ВНИМАНИЯ</b>")
    insight_lines = lines[header_index + 1 : header_index + 4]
    assert insight_lines == [
        f"• Застой в переписке: a@example.com — A1 → {_TARGET_EMOJI} Предложить созвон (15 мин)",
        f"• Застой в переписке: b@example.com — B1 → {_TARGET_EMOJI} Предложить созвон (15 мин)",
        f"• Нет ответа: c@example.com — 3 дня → {_TARGET_EMOJI} Вежливо напомнить сегодня",
    ]


def test_daily_digest_insights_action_templates_present_when_enabled() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "digest_action_templates_enabled": True,
            "deadlock_insights": [
                {
                    "from_email": "boss@example.com",
                    "subject": "Счёт",
                }
            ],
            "silence_insights": [
                {
                    "contact": "client@example.com",
                    "days_silent": 5,
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    lines = [line for line in text.splitlines() if line.strip()]
    assert any(
        line.startswith("  <i>Текст:") and line.endswith("</i>") for line in lines
    )
    assert (
        lines.count(
            "  <i>Текст: Предлагаю созвониться на 15 минут сегодня или завтра — так быстрее решим вопрос.</i>"
        )
        == 1
    )
    assert (
        lines.count(
            "  <i>Текст: Напомню про наш вопрос. Удобно вернуться к нему сегодня?</i>"
        )
        == 1
    )
    assert "пинговать" not in text
    assert "Deadlock" not in text
    assert "Silence" not in text


def test_daily_digest_insights_action_templates_absent_when_disabled() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "deadlock_insights": [
                {
                    "from_email": "boss@example.com",
                    "subject": "Счёт",
                }
            ],
            "silence_insights": [
                {
                    "contact": "client@example.com",
                    "days_silent": 5,
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "Текст:" not in text
    assert "пинговать" not in text
    assert "Deadlock" not in text
    assert "Silence" not in text


def test_daily_digest_bootstrap_block_and_templates_hidden() -> None:
    snapshot = daily_digest.TrustBootstrapSnapshot(
        start_ts=1.0,
        days_since_start=1.0,
        samples_count=12,
        corrections_count=0,
        surprises_count=0,
        surprise_rate=None,
        active=True,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "trust_bootstrap_snapshot": snapshot,
            "trust_bootstrap_min_samples": 50,
            "digest_action_templates_enabled": True,
            "deadlock_insights": [
                {"from_email": "boss@example.com", "subject": "Счёт"}
            ],
            "silence_insights": [{"contact": "client@example.com", "days_silent": 5}],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "\U0001f393 <b>Режим обучения</b>" in text
    assert "Прогресс: 12/50" in text
    assert "Текст:" not in text
    assert "→" not in text


def test_daily_digest_bootstrap_inactive_keeps_templates() -> None:
    snapshot = daily_digest.TrustBootstrapSnapshot(
        start_ts=1.0,
        days_since_start=20.0,
        samples_count=60,
        corrections_count=2,
        surprises_count=0,
        surprise_rate=0.0,
        active=False,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "trust_bootstrap_snapshot": snapshot,
            "trust_bootstrap_min_samples": 50,
            "digest_action_templates_enabled": True,
            "deadlock_insights": [
                {"from_email": "boss@example.com", "subject": "Счёт"}
            ],
            "silence_insights": [{"contact": "client@example.com", "days_silent": 5}],
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "\U0001f393 <b>Режим обучения</b>" not in text
    assert "Текст:" in text


def test_daily_digest_insights_scope_aggregation(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)

    now = datetime.now(timezone.utc)
    primary = "account@example.com"
    secondary = "alt@example.com"

    _seed_email(
        db,
        account_email=primary,
        from_email="primary@example.com",
        subject="Первичный",
        received_at=now - timedelta(days=1),
        thread_key="thread-primary",
    )
    _seed_email(
        db,
        account_email=secondary,
        from_email="secondary@example.com",
        subject="Вторичный",
        received_at=now - timedelta(hours=4),
        thread_key="thread-secondary",
    )

    emitter.emit(
        EventV1(
            event_type=EventType.DEADLOCK_DETECTED,
            ts_utc=(now - timedelta(hours=2)).timestamp(),
            account_id=primary,
            entity_id=None,
            email_id=None,
            payload={"thread_key": "thread-primary"},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.DEADLOCK_DETECTED,
            ts_utc=(now - timedelta(hours=1)).timestamp(),
            account_id=secondary,
            entity_id=None,
            email_id=None,
            payload={"thread_key": "thread-secondary"},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.SILENCE_SIGNAL_DETECTED,
            ts_utc=(now - timedelta(hours=2)).timestamp(),
            account_id=primary,
            entity_id=None,
            email_id=None,
            payload={"contact": "client@example.com", "days_silent": 3},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.SILENCE_SIGNAL_DETECTED,
            ts_utc=(now - timedelta(hours=1)).timestamp(),
            account_id=secondary,
            entity_id=None,
            email_id=None,
            payload={"contact": "vendor@example.com", "days_silent": 4},
        )
    )

    monkeypatch.setattr(
        daily_digest,
        "resolve_account_scope",
        lambda *_args, **_kwargs: AccountScope(
            chat_id="chat",
            account_emails=[primary, secondary],
        ),
    )

    data = daily_digest._collect_digest_data(
        analytics=analytics,
        account_email=primary,
        include_digest_insights=True,
        digest_insights_window_days=7,
        digest_insights_max_items=5,
        now=now,
    )

    text = daily_digest._build_digest_text(data, locale="ru")
    assert (
        f"• Застой в переписке: primary@example.com — Первичный → {_TARGET_EMOJI} "
        "Предложить созвон (15 мин)" in text
    )
    assert (
        f"• Застой в переписке: secondary@example.com — Вторичный → {_TARGET_EMOJI} "
        "Предложить созвон (15 мин)" in text
    )
    assert (
        f"• Нет ответа: client@example.com — 3 дня → {_TARGET_EMOJI} "
        "Вежливо напомнить сегодня" in text
    )
    assert (
        f"• Нет ответа: vendor@example.com — 4 дня → {_TARGET_EMOJI} "
        "Вежливо напомнить сегодня" in text
    )
