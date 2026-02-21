from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.pipeline import daily_digest
from mailbot_v26.pipeline import digest_scheduler


def _base_digest_data() -> daily_digest.DigestData:
    return daily_digest.DigestData(
        deferred_total=1,
        deferred_attachments_only=0,
        deferred_informational=0,
        deferred_items=[{"from_email": "a@example.com", "subject": "S"}],
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
    )


def test_support_telegram_not_added_when_disabled() -> None:
    payload = digest_scheduler._build_daily_payload(
        account_email="account@example.com",
        chat_id="chat",
        bot_token="token",
        data=_base_digest_data(),
        support_ps="",
    )

    assert "P.S." not in payload.html_text


def test_support_telegram_added_when_due(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    state_path = tmp_path / "support_state.json"

    due = digest_scheduler._support_due(now=now, state_path=state_path, frequency_days=30)
    payload = digest_scheduler._build_daily_payload(
        account_email="account@example.com",
        chat_id="chat",
        bot_token="token",
        data=_base_digest_data(),
        support_ps="MailBot бесплатный. Поддержать разработку: /support",
    )

    assert due is True
    assert "P.S." in payload.html_text
    assert "Поддержать разработку" in payload.html_text


def test_support_telegram_not_repeated_before_frequency(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    state_path = tmp_path / "support_state.json"
    state_path.write_text(
        json.dumps({"last_shown_utc": (now - timedelta(days=5)).isoformat()}, ensure_ascii=False),
        encoding="utf-8",
    )

    due = digest_scheduler._support_due(now=now, state_path=state_path, frequency_days=30)

    assert due is False


def test_support_telegram_config_disabled_when_feature_flag_off(monkeypatch) -> None:
    monkeypatch.setattr(digest_scheduler, "_resolve_yaml_config_path", lambda: Path("config.yaml"))
    monkeypatch.setattr(
        digest_scheduler,
        "load_yaml_config",
        lambda _path: {
            "features": {"donate_enabled": False},
            "support": {
                "telegram": {"enabled": True, "frequency_days": 30, "text": "Letterbot бесплатный. Поддержка: /support"}
            },
        },
    )

    cfg = digest_scheduler._load_support_telegram_config()

    assert cfg.enabled is False
    assert cfg.text == ""


def test_support_telegram_config_enabled_when_feature_flag_on(monkeypatch) -> None:
    monkeypatch.setattr(digest_scheduler, "_resolve_yaml_config_path", lambda: Path("config.yaml"))
    monkeypatch.setattr(
        digest_scheduler,
        "load_yaml_config",
        lambda _path: {
            "features": {"donate_enabled": True},
            "support": {
                "telegram": {"enabled": True, "frequency_days": 30, "text": "Letterbot бесплатный. Поддержка: /support"}
            },
        },
    )

    cfg = digest_scheduler._load_support_telegram_config()

    assert cfg.enabled is True
    assert cfg.text == "Letterbot бесплатный. Поддержка: /support"


def test_support_telegram_config_enabled_via_support_enabled(monkeypatch) -> None:
    monkeypatch.setattr(digest_scheduler, "_resolve_yaml_config_path", lambda: Path("config.yaml"))
    monkeypatch.setattr(
        digest_scheduler,
        "load_yaml_config",
        lambda _path: {
            "support": {
                "enabled": True,
                "telegram": {"enabled": True, "frequency_days": 30, "text": "Letterbot бесплатный. Поддержка: /support"},
            },
        },
    )

    cfg = digest_scheduler._load_support_telegram_config()

    assert cfg.enabled is True
    assert cfg.text == "Letterbot бесплатный. Поддержка: /support"


def test_support_telegram_config_support_enabled_overrides_legacy_feature(monkeypatch) -> None:
    monkeypatch.setattr(digest_scheduler, "_resolve_yaml_config_path", lambda: Path("config.yaml"))
    monkeypatch.setattr(
        digest_scheduler,
        "load_yaml_config",
        lambda _path: {
            "features": {"donate_enabled": True},
            "support": {
                "enabled": False,
                "telegram": {"enabled": True, "frequency_days": 30, "text": "Letterbot бесплатный. Поддержка: /support"},
            },
        },
    )

    cfg = digest_scheduler._load_support_telegram_config()

    assert cfg.enabled is False
    assert cfg.text == ""
