"""Terminal queue paths must release the per-email pipeline caches (A16 / QW9)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mailbot_v26.bot_core import pipeline as core_pipeline
from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    remember_raw_email,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.start import _process_queue
from mailbot_v26.tests.test_telegram_delivery_pipeline import (
    _configure_pipeline,
    _make_config,
    _seed_pipeline_context,
    _seed_queue,
)
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _clear_caches() -> None:
    PIPELINE_CACHE.clear()
    PIPELINE_INBOUND_CACHE.clear()
    PIPELINE_RAW_CACHE.clear()


@pytest.fixture(autouse=True)
def _isolated_caches():
    _clear_caches()
    yield
    _clear_caches()


def _seed_all_caches(email_id: int, account_email: str) -> None:
    _seed_pipeline_context(email_id, account_email)
    remember_raw_email(email_id, b"raw email bytes")


def _assert_caches_released(email_id: int) -> None:
    assert email_id not in PIPELINE_CACHE
    assert email_id not in PIPELINE_INBOUND_CACHE
    assert email_id not in PIPELINE_RAW_CACHE


def test_duplicate_delivery_skip_releases_caches(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_all_caches(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)

    def fake_send(_payload):
        return DeliveryResult(delivered=True, retryable=False, message_id=1)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)
    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    storage.enqueue_stage(email_id, "TG")
    _seed_all_caches(email_id, config.accounts[0].login)
    _process_queue(storage, config, processor, flags)

    _assert_caches_released(email_id)


def test_telegram_max_attempts_releases_caches(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_all_caches(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)
    with storage.conn:
        storage.conn.execute(
            "UPDATE queue SET attempts = ? WHERE email_id = ?", (2, email_id)
        )
    calls: list[object] = []

    def fake_send(payload):
        calls.append(payload)
        if len(calls) == 1:
            return DeliveryResult(delivered=False, retryable=True, error="server error")
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    _process_queue(storage, config, processor, FeatureFlags(base_dir=tmp_path))

    with storage.conn:
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    assert remaining == 0
    _assert_caches_released(email_id)


class _UnknownStageStorage:
    def __init__(self, email_id: int) -> None:
        self._items = [
            {"queue_id": 1, "email_id": email_id, "stage": "BOGUS", "attempts": 1}
        ]
        self.done: list[int] = []

    def list_due_snoozes(self, now_iso, limit):
        return []

    def claim_next(self, stages):
        return self._items.pop(0) if self._items else None

    def mark_done(self, queue_id):
        self.done.append(queue_id)


def test_unknown_stage_releases_caches(tmp_path) -> None:
    config = _make_config(tmp_path)
    email_id = 4242
    _seed_all_caches(email_id, config.accounts[0].login)
    storage = _UnknownStageStorage(email_id)

    _process_queue(
        storage,
        config,
        SimpleNamespace(),
        SimpleNamespace(ENABLE_PREMIUM_PROCESSOR=False),
    )

    assert storage.done == [1]
    _assert_caches_released(email_id)
