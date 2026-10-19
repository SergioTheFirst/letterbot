from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mailbot_v26.pipeline import digest_scheduler, processor
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.tests.test_digest_scheduler import (
    DummyLogger,
    _build_config,
    _build_storage,
    _seed_deferred_email,
    _write_config,
)
from mailbot_v26.worker.telegram_sender import DeliveryResult


@pytest.fixture
def ru_locale_override(monkeypatch, tmp_path):
    store = RuntimeOverrideStore(tmp_path / "runtime_overrides.sqlite")
    store.set_value("ui_locale", "ru")
    monkeypatch.setattr(processor, "runtime_override_store", store)
    return store


def _run_tick(monkeypatch, tmp_path, *, daily: bool, weekly: bool, now: datetime):
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily_enabled=daily, weekly_enabled=weekly)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)
    storage = _build_storage(tmp_path)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)
    sent: list[object] = []

    def _sender(payload) -> DeliveryResult:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    digest_scheduler.run_digest_tick(
        now=now,
        config=_build_config(tmp_path),
        storage=storage,
        telegram_sender=_sender,
        logger=DummyLogger(),
    )
    return sent


def test_scheduled_daily_digest_uses_runtime_ui_locale(
    monkeypatch, tmp_path, ru_locale_override
) -> None:
    sent = _run_tick(
        monkeypatch,
        tmp_path,
        daily=True,
        weekly=False,
        now=datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc),
    )

    assert len(sent) == 1
    assert "<b>Дайджест дня</b>" in sent[0].html_text
    assert "Daily Digest" not in sent[0].html_text


def test_scheduled_weekly_digest_uses_runtime_ui_locale(
    monkeypatch, tmp_path, ru_locale_override
) -> None:
    sent = _run_tick(
        monkeypatch,
        tmp_path,
        daily=False,
        weekly=True,
        now=datetime(2025, 12, 29, 9, 0, tzinfo=timezone.utc),
    )

    assert len(sent) == 1
    assert "За неделю" in sent[0].html_text
    assert "Over the week" not in sent[0].html_text
