"""Queue dispatcher failures must not fan out into fail-open sends (A1 / QW3)."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.pipeline import PipelineContext
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.pipeline.processor import InboundMessage
from mailbot_v26.tests.test_polling_loop import (
    DummyProcessor,
    _build_imap,
    _config,
    _install_common_patches,
)


def _account() -> AccountConfig:
    return AccountConfig(
        account_id="acc1",
        login="acc1@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat1",
    )


def _clear_pipeline_caches() -> None:
    start_module.PIPELINE_CACHE.clear()
    start_module.PIPELINE_INBOUND_CACHE.clear()
    start_module.PIPELINE_RAW_CACHE.clear()


@pytest.fixture(autouse=True)
def _isolated_pipeline_caches():
    _clear_pipeline_caches()
    yield
    _clear_pipeline_caches()


def _seed_cached_email(email_id: int, account: AccountConfig) -> None:
    start_module.PIPELINE_CACHE[email_id] = PipelineContext(
        email_id=email_id, account_email=account.login, uid=email_id
    )
    start_module.PIPELINE_INBOUND_CACHE[email_id] = InboundMessage(
        subject="cached subject", body="cached body", sender="s@example.com"
    )


def test_queue_dispatcher_failure_does_not_fail_open_cached_emails(
    monkeypatch, tmp_path, caplog
) -> None:
    account = _account()
    monkeypatch.setattr(
        start_module, "ResilientIMAP", _build_imap({account.login: [[]]})
    )
    _install_common_patches(monkeypatch, [account], tmp_path)

    processed: list[str] = []

    class RecordingProcessor(DummyProcessor):
        def process(self, login, inbound):
            processed.append(login)
            return "fail-open text"

    monkeypatch.setattr(start_module, "MessageProcessor", RecordingProcessor)
    sent_texts: list[str] = []

    def _record_send(payload, *_args, **_kwargs):
        sent_texts.append(str(getattr(payload, "html_text", "")))
        return SimpleNamespace(delivered=True, error=None, message_id=1)

    monkeypatch.setattr(start_module, "send_telegram", _record_send)
    fail_open_calls: list[object] = []
    original_fail_open = start_module._fail_open_process

    def _spy_fail_open(*args, **kwargs):
        fail_open_calls.append(args)
        return original_fail_open(*args, **kwargs)

    monkeypatch.setattr(start_module, "_fail_open_process", _spy_fail_open)

    def _raise_queue(*_args, **_kwargs):
        raise RuntimeError("queue boom")

    monkeypatch.setattr(start_module, "_process_queue", _raise_queue)
    _seed_cached_email(101, account)
    _seed_cached_email(102, account)

    caplog.set_level(logging.INFO, logger="mailbot")
    start_module.main(max_cycles=1)

    assert "Queue dispatcher failure" in caplog.text
    assert fail_open_calls == []
    assert processed == []
    assert not any("fail-open text" in text for text in sent_texts)


class _SnoozeFailingStorage:
    def __init__(self) -> None:
        self.claim_calls = 0

    def list_due_snoozes(self, now_iso, limit):
        raise RuntimeError("bad snooze row")

    def claim_next(self, stages):
        self.claim_calls += 1
        return None


def test_snooze_failure_does_not_abort_queue_processing(
    monkeypatch, tmp_path, caplog
) -> None:
    storage = _SnoozeFailingStorage()
    config = _config([_account()], tmp_path)
    caplog.set_level(logging.INFO, logger="mailbot")

    start_module._process_queue(
        storage,
        config,
        SimpleNamespace(),
        SimpleNamespace(ENABLE_PREMIUM_PROCESSOR=False),
    )

    assert storage.claim_calls == 1
    assert "bad snooze row" in caplog.text
