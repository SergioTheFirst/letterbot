from __future__ import annotations

import configparser
from pathlib import Path
from datetime import datetime, timedelta, timezone

from mailbot_v26.pipeline import daily_digest
from mailbot_v26.pipeline import digest_scheduler
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore


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


def test_support_config_from_settings_section() -> None:
    parser = configparser.ConfigParser()
    parser.read_string(
        """[support]
enabled = true
telegram = true
min_days_between_asks = 30
url = https://example.com/insider
message = Поддержать Letterbot → {url}
"""
    )

    cfg = digest_scheduler._load_support_config_from_settings(parser)

    assert cfg is not None
    assert cfg.enabled is True
    assert cfg.telegram is True
    assert cfg.min_days_between_asks == 30


def test_support_telegram_not_added_to_daily_payload() -> None:
    payload = digest_scheduler._build_daily_payload(
        account_email="account@example.com",
        chat_id="chat",
        bot_token="token",
        data=_base_digest_data(),
    )

    assert "💛" not in payload.html_text


def test_weekly_support_footer_rate_limited_via_runtime_overrides(tmp_path) -> None:
    now = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)
    store = RuntimeOverrideStore(tmp_path / "runtime.sqlite")
    cfg = digest_scheduler.SupportTelegramConfig(
        enabled=True,
        telegram=True,
        min_days_between_asks=30,
        url="https://example.com/insider",
        message="Поддержать Letterbot → {url}",
    )

    first = digest_scheduler._resolve_weekly_support_footer(
        now_utc=now,
        account_email="user@example.com",
        chat_id="chat-1",
        override_store=store,
        support_config=cfg,
    )
    last_ask_key = "support:last_ask_utc:chat-1"
    first_saved = store.get_value(last_ask_key)

    second = digest_scheduler._resolve_weekly_support_footer(
        now_utc=now,
        account_email="user@example.com",
        chat_id="chat-1",
        override_store=store,
        support_config=cfg,
    )

    old = now - timedelta(days=31)
    store.set_value(last_ask_key, old.isoformat())
    third = digest_scheduler._resolve_weekly_support_footer(
        now_utc=now,
        account_email="user@example.com",
        chat_id="chat-1",
        override_store=store,
        support_config=cfg,
    )

    assert first.startswith("💛 Поддержать Letterbot")
    assert first_saved is not None
    assert second == ""
    assert third.startswith("💛 Поддержать Letterbot")


def test_support_telegram_config_enabled_when_feature_flag_on(monkeypatch) -> None:
    monkeypatch.setattr(digest_scheduler, "_resolve_yaml_config_path", lambda: Path("config.yaml"))
    monkeypatch.setattr(
        digest_scheduler,
        "load_yaml_config",
        lambda _path: {
            "features": {"donate_enabled": True},
            "support": {
                "telegram": {
                    "enabled": True,
                    "frequency_days": 30,
                    "text": "Letterbot бесплатный. Поддержка: /support",
                }
            },
        },
    )
    monkeypatch.setattr(
        digest_scheduler,
        "resolve_config_paths",
        lambda _base_dir: type("Paths", (), {"two_file_mode": False})(),
    )

    cfg = digest_scheduler._load_support_telegram_config()

    assert cfg.enabled is True
    assert cfg.message == "Letterbot бесплатный. Поддержка: /support"
