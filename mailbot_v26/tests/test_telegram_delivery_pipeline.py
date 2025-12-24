from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from mailbot_v26.bot_core import pipeline as core_pipeline
from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PipelineContext,
    configure_pipeline,
    store_inbound,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor
from mailbot_v26.start import _process_queue
from mailbot_v26.worker.telegram_sender import TelegramSendResult


def _make_config(tmp_path) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=10,
            max_attachment_mb=10,
            admin_chat_id="",
        ),
        accounts=[
            AccountConfig(
                account_id="acc-1",
                login="account@example.com",
                password="pw",
                host="imap.example.com",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            )
        ],
        keys=KeysConfig(
            telegram_bot_token="token",
            cf_account_id="cf",
            cf_api_token="api",
        ),
        storage=StorageConfig(db_path=tmp_path / "queue.sqlite"),
    )


def _seed_queue(storage: Storage, account_email: str) -> int:
    email_id = storage.upsert_email(
        account_email=account_email,
        uid=1,
        message_id="msg-1",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.utcnow().isoformat(),
        attachments_count=0,
    )
    storage.enqueue_stage(email_id, "TG")
    return email_id


def _seed_pipeline_context(email_id: int, account_email: str) -> PipelineContext:
    ctx = PipelineContext(email_id=email_id, account_email=account_email, uid=1)
    ctx.llm_result = {"text": "Telegram message"}
    PIPELINE_CACHE[email_id] = ctx
    store_inbound(
        email_id,
        InboundMessage(
            subject="Subject",
            body="Body",
            sender="sender@example.com",
        ),
    )
    return ctx


def _configure_pipeline(config: BotConfig) -> MessageProcessor:
    processor = MessageProcessor(SimpleNamespace(), SimpleNamespace())
    configure_pipeline(config, processor)
    return processor


def _cleanup_pipeline(email_id: int) -> None:
    PIPELINE_CACHE.pop(email_id, None)
    PIPELINE_INBOUND_CACHE.pop(email_id, None)


def test_telegram_retry_on_http_failure(monkeypatch, tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)

    def fake_send(_payload):
        return TelegramSendResult(success=False, error="bad request", status_code=400)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    with caplog.at_level("INFO"):
        _process_queue(storage, config, processor)

    with storage.conn:
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    assert remaining == 1
    assert "telegram_delivery_retry" in caplog.text
    _cleanup_pipeline(email_id)


def test_telegram_delivery_failed_after_max_attempts(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)

    with storage.conn:
        storage.conn.execute(
            "UPDATE queue SET attempts = ? WHERE email_id = ?",
            (2, email_id),
        )

    calls: list[dict[str, object]] = []

    def fake_send(payload):
        calls.append({"payload": payload})
        if len(calls) == 1:
            return TelegramSendResult(success=False, error="bad request", status_code=400)
        return TelegramSendResult(success=True)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    _process_queue(storage, config, processor)

    with storage.conn:
        status = storage.conn.execute(
            "SELECT status FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()[0]
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    assert status == "DELIVERY_FAILED"
    assert remaining == 0
    assert any(
        "TELEGRAM DELIVERY FAILED" in entry["payload"].html_text for entry in calls[1:]
    )
    _cleanup_pipeline(email_id)
