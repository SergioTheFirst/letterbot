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
from mailbot_v26.start import (
    _build_telegram_delivery_key,
    _persist_inbound_and_enqueue_parse,
    _process_queue,
)
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _make_config(tmp_path) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=10,
            max_email_mb=15,
            max_attachment_mb=10,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
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


def _configure_pipeline(config: BotConfig, *, enable_premium_processor: bool = False) -> MessageProcessor:
    processor = MessageProcessor(SimpleNamespace(), SimpleNamespace())
    configure_pipeline(
        config,
        processor,
        enable_premium_processor=enable_premium_processor,
    )
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
        return DeliveryResult(delivered=False, retryable=True, error="server error")

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    with caplog.at_level("INFO"):
        _process_queue(storage, config, processor, flags)

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
            return DeliveryResult(delivered=False, retryable=True, error="server error")
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    with storage.conn:
        status = storage.conn.execute(
            "SELECT status FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()[0]
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    assert status == "DELIVERY_FAILED"
    assert remaining == 0
    assert any(
        "TELEGRAM DELIVERY FAILED" in entry["payload"].html_text
        for entry in calls[1:]
    )
    _cleanup_pipeline(email_id)


def test_duplicate_uid_ingest_enqueues_once_and_sends_once(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    processor = _configure_pipeline(config)

    calls: list[object] = []

    def fake_send(payload):
        calls.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    email_id, enqueued = _persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email=config.accounts[0].login,
        uid=950,
        message_id="msg-950",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.utcnow().isoformat(),
        attachments_count=0,
        raw_email=b"raw",
        inbound=InboundMessage(subject="Subject", body="Body", sender="sender@example.com"),
    )
    assert enqueued is True
    ctx = PIPELINE_CACHE[email_id]
    ctx.llm_result = {"text": "Telegram message"}

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    _, second_enqueued = _persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email=config.accounts[0].login,
        uid=950,
        message_id="msg-950",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.utcnow().isoformat(),
        attachments_count=0,
        raw_email=b"raw",
        inbound=InboundMessage(subject="Subject", body="Body", sender="sender@example.com"),
    )

    assert second_enqueued is False
    _process_queue(storage, config, processor, flags)

    with storage.conn:
        queue_size = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    assert queue_size == 0
    assert len(calls) == 1


def test_tg_stage_idempotency_skips_duplicate_delivery(monkeypatch, tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)
    calls: list[object] = []

    def fake_send(payload):
        calls.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    storage.enqueue_stage(email_id, "TG")
    _seed_pipeline_context(email_id, config.accounts[0].login)
    with caplog.at_level("INFO"):
        _process_queue(storage, config, processor, flags)

    with storage.conn:
        queue_size = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    assert queue_size == 0
    assert len(calls) == 1
    assert "telegram_delivery_skipped_duplicate" in caplog.text




def test_stage_tg_payload_carries_bot_token_and_chat_id(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)

    captured: dict[str, object] = {}

    def fake_send(payload):
        captured["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    payload = captured["payload"]
    assert payload.metadata["bot_token"] == config.keys.telegram_bot_token
    assert payload.metadata["chat_id"] == config.accounts[0].telegram_chat_id
    _cleanup_pipeline(email_id)
def test_stage_tg_adds_inline_keyboard_when_premium_enabled(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config, enable_premium_processor=True)

    captured = {}

    def fake_send(payload):
        captured["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = False
    _process_queue(storage, config, processor, flags)

    reply_markup = captured["payload"].reply_markup
    assert isinstance(reply_markup, dict)
    assert "inline_keyboard" in reply_markup
    _cleanup_pipeline(email_id)



def test_stage_tg_adds_inline_keyboard_when_premium_disabled(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config, enable_premium_processor=False)

    captured = {}

    def fake_send(payload):
        captured["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = False
    _process_queue(storage, config, processor, flags)

    reply_markup = captured["payload"].reply_markup
    assert isinstance(reply_markup, dict)
    assert "inline_keyboard" in reply_markup
    _cleanup_pipeline(email_id)

def test_tg_stage_rerun_after_success_still_sends_once(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config)

    calls: list[object] = []

    def fake_send(payload):
        calls.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=42)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    # simulate queue reclaim/rerun for already delivered email
    storage.enqueue_stage(email_id, "TG")
    _seed_pipeline_context(email_id, config.accounts[0].login)
    _process_queue(storage, config, processor, flags)

    with storage.conn:
        delivery_rows = storage.conn.execute(
            "SELECT COUNT(*) FROM telegram_delivery_log WHERE email_id = ? AND kind = 'email'",
            (email_id,),
        ).fetchone()[0]
        message_id_row = storage.conn.execute(
            "SELECT telegram_message_id FROM telegram_delivery_log WHERE email_id = ? AND kind = 'email'",
            (email_id,),
        ).fetchone()

    assert len(calls) == 1
    assert delivery_rows == 1
    assert message_id_row is not None
    assert message_id_row[0] == "42"


def test_duplicate_uid_ingest_is_case_insensitive(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    processor = _configure_pipeline(config)

    calls: list[object] = []

    def fake_send(payload):
        calls.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    email_id, enqueued = _persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email="Account@Example.com",
        uid=950,
        message_id="msg-950",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.utcnow().isoformat(),
        attachments_count=0,
        raw_email=b"raw",
        inbound=InboundMessage(subject="Subject", body="Body", sender="sender@example.com"),
    )
    assert enqueued is True
    ctx = PIPELINE_CACHE[email_id]
    ctx.llm_result = {"text": "Telegram message"}

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    _, second_enqueued = _persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email="account@example.com",
        uid=950,
        message_id="msg-950",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.utcnow().isoformat(),
        attachments_count=0,
        raw_email=b"raw",
        inbound=InboundMessage(subject="Subject", body="Body", sender="sender@example.com"),
    )

    assert second_enqueued is False
    assert len(calls) == 1


def test_delivery_key_snooze_kind_separation() -> None:
    email_key = _build_telegram_delivery_key(email_id=101, kind="email")
    snooze_key = _build_telegram_delivery_key(email_id=101, kind="snooze", snooze_ts="2026-03-01T10:00:00Z")

    assert email_key == "email:101"
    assert snooze_key == "snooze:email:101:2026-03-01T10:00:00Z"
    assert snooze_key != email_key


def test_due_snooze_delivered_once(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    with storage.conn:
        storage.conn.execute("DELETE FROM queue")
        storage.conn.execute(
            """
            INSERT INTO telegram_snooze (email_id, deliver_at_utc, status, reminder_text, attempts, created_at, updated_at)
            VALUES (?, datetime('now','-1 minute'), 'pending', 'orig', 0, datetime('now'), datetime('now'))
            """,
            (email_id,),
        )

    processor = _configure_pipeline(config)
    calls: list[object] = []

    def fake_send(payload):
        calls.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=77)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)
    flags = FeatureFlags(base_dir=tmp_path)

    _process_queue(storage, config, processor, flags)
    _process_queue(storage, config, processor, flags)

    with storage.conn:
        row = storage.conn.execute(
            "SELECT status FROM telegram_snooze WHERE email_id = ?",
            (email_id,),
        ).fetchone()
        log_count = storage.conn.execute(
            "SELECT COUNT(*) FROM telegram_delivery_log WHERE email_id = ? AND kind = 'snooze'",
            (email_id,),
        ).fetchone()[0]

    assert len(calls) == 1
    assert row is not None and row[0] == "delivered"
    assert log_count == 1


def test_snooze_retry_on_failure_not_stuck(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    with storage.conn:
        storage.conn.execute("DELETE FROM queue")
        storage.conn.execute(
            """
            INSERT INTO telegram_snooze (email_id, deliver_at_utc, status, reminder_text, attempts, created_at, updated_at)
            VALUES (?, datetime('now','-1 minute'), 'pending', 'orig', 0, datetime('now'), datetime('now'))
            """,
            (email_id,),
        )

    processor = _configure_pipeline(config)

    def fake_send(_payload):
        return DeliveryResult(delivered=False, retryable=True, error="tg down")

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)
    flags = FeatureFlags(base_dir=tmp_path)

    _process_queue(storage, config, processor, flags)

    with storage.conn:
        row = storage.conn.execute(
            "SELECT status, attempts, last_error FROM telegram_snooze WHERE email_id = ?",
            (email_id,),
        ).fetchone()

    assert row is not None
    assert row[0] == "pending"
    assert int(row[1]) >= 1
    assert "tg down" in str(row[2])


def test_stage_tg_uses_real_priority_and_attachment_insight(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    ctx = _seed_pipeline_context(email_id, config.accounts[0].login)
    ctx.llm_result = {
        "text": "Базовый текст",
        "priority": "🔴",
        "body_text": "Счет на оплату во вложении.",
        "attachments": [
            {
                "filename": "invoice.xlsx",
                "text": "Счет №123 от 01.03.2026 сумма 120 000 руб оплатить до 10.03.2026",
            }
        ],
    }
    store_inbound(
        email_id,
        InboundMessage(
            subject="Счет на оплату",
            body="Счет на оплату во вложении.",
            sender="sender@example.com",
            mail_type="INVOICE",
            attachments=[],
        ),
    )
    processor = _configure_pipeline(config)

    captured: dict[str, object] = {}

    def fake_send(payload):
        captured["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    _process_queue(storage, config, processor, flags)

    payload = captured["payload"]
    assert payload.priority == "🔴"
    assert "📎 Счет" in payload.html_text
    assert payload.reply_markup
    assert "inline_keyboard" in payload.reply_markup
    _cleanup_pipeline(email_id)



def test_priority_keyboard_uses_initial_prio_true_in_user_path(monkeypatch, tmp_path) -> None:
    config = _make_config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, config.accounts[0].login)
    _seed_pipeline_context(email_id, config.accounts[0].login)
    processor = _configure_pipeline(config, enable_premium_processor=False)

    captured: dict[str, object] = {}

    def fake_send(payload):
        captured["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = False
    _process_queue(storage, config, processor, flags)

    keyboard = captured["payload"].reply_markup["inline_keyboard"]
    labels = [button["text"] for button in keyboard[0]]
    assert labels == ["🔴 Срочно", "🟡 Важно", "🔵 Низкий"]
    _cleanup_pipeline(email_id)
