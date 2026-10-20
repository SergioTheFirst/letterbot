"""WP1c / B15: notification SLA accounting, SLA alert and the system-notice stub.

- A delivery wins over an earlier failure for the same email.
- Every undelivered email counts one reason; error_rate_24h = undelivered/total.
- The SLA alert carries the bot token and is remembered (cooldown) only when it
  actually reached Telegram.
- send_system_notice is an unconfigured stub: it reports "not sent", never logs
  the notice text, and the mode-change notice is logged as skipped.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.notification_sla import (
    NotificationSLAResult,
    compute_notification_sla,
)
from mailbot_v26.pipeline import processor, stage_telegram
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import system_health
from mailbot_v26.tests.test_delivery_invariants import (  # noqa: F401 - fixtures
    _isolated_runtime,
    _patch_premium,
    _seed_email,
    _setup,
)
from mailbot_v26.tests import test_delivery_invariants as _invariants

# Shared fixtures, re-exported by assignment so pytest discovers them here.
events = _invariants.events
telegram = _invariants.telegram
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _emit(emitter, event_type: EventType, email_id: int, at: datetime, **payload):
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=at.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=email_id,
            payload=payload,
        )
    )


def _sla_db(tmp_path):
    db_path = tmp_path / "events.sqlite"
    KnowledgeDB(db_path)
    return ContractEventEmitter(db_path), KnowledgeAnalytics(db_path)


# --- accounting -----------------------------------------------------------------


def test_sla_delivery_wins_over_an_earlier_failure(tmp_path) -> None:
    emitter, analytics = _sla_db(tmp_path)
    now = datetime(2024, 1, 2, tzinfo=timezone.utc)
    detected = now - timedelta(hours=3)
    _emit(emitter, EventType.EMAIL_RECEIVED, 1, detected)
    _emit(
        emitter,
        EventType.TELEGRAM_FAILED,
        1,
        detected + timedelta(seconds=10),
        delivered=False,
        error="server error",
    )
    _emit(
        emitter,
        EventType.TELEGRAM_DELIVERED,
        1,
        detected + timedelta(seconds=60),
        delivered=True,
    )

    sla = compute_notification_sla(analytics=analytics, now=now)

    assert sla.delivered_24h == 1
    assert sla.undelivered_24h == 0
    assert sla.delivery_rate_24h == 1.0
    assert sla.top_error_reasons_24h == []
    assert sla.error_rate_24h == 0.0
    assert sla.p50_latency_24h == 60.0


def test_sla_counts_one_reason_per_undelivered_email(tmp_path) -> None:
    emitter, analytics = _sla_db(tmp_path)
    now = datetime(2024, 1, 2, tzinfo=timezone.utc)
    detected = now - timedelta(hours=3)
    for email_id in (1, 2, 3, 4):
        _emit(emitter, EventType.EMAIL_RECEIVED, email_id, detected)
    _emit(emitter, EventType.TELEGRAM_DELIVERED, 1, detected + timedelta(seconds=5))
    _emit(
        emitter,
        EventType.TELEGRAM_FAILED,
        2,
        detected + timedelta(seconds=5),
        delivered=False,
        error="chat not found",
    )
    _emit(
        emitter,
        EventType.TELEGRAM_FAILED,
        3,
        detected + timedelta(seconds=5),
        delivered=False,
        error="delivery_unknown",
        delivery_state="unknown",
        error_class="ReadTimeout",
    )
    # Email 4 never got a delivery event: "missing_delivery".

    sla = compute_notification_sla(analytics=analytics, now=now)

    assert sla.total_24h == 4
    assert sla.delivered_24h == 1
    assert sla.undelivered_24h == 3
    assert sla.unknown_24h == 1
    reasons = {item.reason: item.count for item in sla.top_error_reasons_24h}
    assert reasons == {"chat not found": 1, "delivery_unknown": 1, "missing_delivery": 1}
    assert sum(reasons.values()) == sla.undelivered_24h
    assert all(item.share == 0.25 for item in sla.top_error_reasons_24h)
    assert sla.error_rate_24h == 3 / 4


# --- SLA alert ------------------------------------------------------------------


def _degraded_sla() -> NotificationSLAResult:
    return NotificationSLAResult(
        delivery_rate_24h=0.5,
        delivery_rate_7d=0.9,
        salvage_rate_24h=0.0,
        p50_latency_24h=10.0,
        p90_latency_24h=20.0,
        p99_latency_24h=30.0,
        p50_latency_7d=10.0,
        p90_latency_7d=20.0,
        p99_latency_7d=30.0,
        top_error_reasons_24h=[],
        error_rate_24h=0.5,
        undelivered_24h=5,
        delivered_24h=5,
        total_24h=10,
    )


@pytest.mark.parametrize("delivered", [True, False], ids=["delivered", "not-delivered"])
def test_sla_alert_carries_bot_token_and_is_saved_only_when_delivered(
    monkeypatch, caplog, delivered
) -> None:
    sent: list[TelegramPayload] = []
    saved: list[str] = []

    def fake_enqueue_tg(*, email_id, payload):
        sent.append(payload)
        return DeliveryResult(
            delivered=delivered,
            retryable=False,
            error=None if delivered else "Forbidden: bot was blocked by the user",
        )

    monkeypatch.setattr(
        processor, "feature_flags", SimpleNamespace(ENABLE_NOTIFICATION_SLA=True)
    )
    monkeypatch.setattr(processor, "enqueue_tg", fake_enqueue_tg)
    monkeypatch.setattr(
        processor,
        "notification_alert_store",
        SimpleNamespace(
            should_alert=lambda **_kwargs: True,
            save_alert=lambda fingerprint, now=None: saved.append(fingerprint),
        ),
    )

    with caplog.at_level("INFO"):
        processor._maybe_alert_notification_sla(
            account_email="account@example.com",
            telegram_chat_id="chat",
            telegram_bot_token="bot-token",
            sla_result=_degraded_sla(),
            consecutive_failures=0,
        )

    assert len(sent) == 1
    assert sent[0].metadata["bot_token"] == "bot-token"
    assert sent[0].metadata["chat_id"] == "chat"
    if delivered:
        assert len(saved) == 1
    else:
        assert saved == []
        assert "notification_sla_alert_send_failed" in caplog.text


def test_premium_processor_passes_the_bot_token_to_the_sla_alert(
    tmp_path, monkeypatch, telegram, events
) -> None:
    _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=True
    )
    captured: list[dict] = []
    monkeypatch.setattr(
        processor,
        "_maybe_alert_notification_sla",
        lambda **kwargs: captured.append(kwargs),
    )
    _seed_email(storage, stage="PARSE")

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(telegram.accepted) == 1
    assert len(captured) == 1
    assert captured[0]["telegram_bot_token"] == config.keys.telegram_bot_token


# --- system notice stub ---------------------------------------------------------


def test_system_notice_stub_reports_not_sent_without_logging_the_text(caplog) -> None:
    with caplog.at_level("INFO", logger="mailbot"):
        sent = stage_telegram.send_system_notice(
            chat_id="chat",
            notice_text="Notice body 12345",
            account_email="account@example.com",
        )

    assert sent is False
    assert "telegram_system_notice_unconfigured" in caplog.text
    assert "Notice body 12345" not in caplog.text


def test_mode_change_notice_is_logged_as_skipped_not_sent(caplog) -> None:
    change = system_health.update_component("Telegram", False, reason="test")
    assert change is not None

    with caplog.at_level("INFO", logger="mailbot"):
        processor._notify_system_mode_change(
            change=change, chat_id="chat", account_email="account@example.com"
        )

    assert "system_mode_notice_sent" not in caplog.text
    assert "system_mode_notice_skipped" in caplog.text
    assert '"reason":"unconfigured"' in caplog.text
    assert "Telegram временно недоступен" not in caplog.text
