from __future__ import annotations

from datetime import datetime, timezone
from email.message import EmailMessage
from types import SimpleNamespace

from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    PipelineContext,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor
from mailbot_v26.start import _process_queue
from mailbot_v26.worker.telegram_sender import DeliveryResult
from mailbot_v26.tests.integration.harness import build_config
from mailbot_v26.config_loader import AccountConfig, BotConfig


def _seed_queue(
    *,
    tmp_path,
    account_email: str,
    inbound: InboundMessage,
    raw: bytes,
    config: BotConfig,
) -> tuple[Storage, int]:
    storage = Storage(config.storage.db_path)
    email_id = storage.upsert_email(
        account_email=account_email,
        uid=1,
        message_id="msg-1",
        from_email="sender@example.com",
        from_name="Sender",
        subject=inbound.subject,
        received_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).isoformat(),
        attachments_count=0,
    )
    storage.enqueue_stage(email_id, "PARSE")
    PIPELINE_CACHE[email_id] = PipelineContext(
        email_id=email_id,
        account_email=account_email,
        uid=1,
    )
    PIPELINE_INBOUND_CACHE[email_id] = inbound
    PIPELINE_RAW_CACHE[email_id] = raw
    return storage, email_id


def _cleanup(email_id: int) -> None:
    PIPELINE_CACHE.pop(email_id, None)
    PIPELINE_INBOUND_CACHE.pop(email_id, None)
    PIPELINE_RAW_CACHE.pop(email_id, None)


def _build_raw_email() -> bytes:
    message = EmailMessage()
    message["Subject"] = "Test subject"
    message["From"] = "Sender <sender@example.com>"
    message["To"] = "receiver@example.com"
    message["Date"] = "Tue, 02 Jan 2024 10:00:00 +0000"
    message.set_content("Hello")
    return message.as_bytes()


def test_premium_flag_off_skips_premium_processor(monkeypatch, tmp_path) -> None:
    account = AccountConfig(
        account_id="acc",
        login="account@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    inbound = InboundMessage(subject="Test subject", body="Hello", sender="sender@example.com")
    storage, email_id = _seed_queue(
        tmp_path=tmp_path,
        account_email="account@example.com",
        inbound=inbound,
        raw=_build_raw_email(),
        config=config,
    )
    processor = MessageProcessor(SimpleNamespace(), SimpleNamespace())

    monkeypatch.setattr(
        "mailbot_v26.start.processor_module.process_message",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("premium called")),
    )

    calls = {"parse": 0, "llm": 0, "tg": 0}

    def fake_parse(_ctx):
        calls["parse"] += 1

    def fake_llm(_ctx):
        calls["llm"] += 1

    def fake_tg(_ctx):
        calls["tg"] += 1
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr("mailbot_v26.start.stage_parse", fake_parse)
    monkeypatch.setattr("mailbot_v26.start.stage_llm", fake_llm)
    monkeypatch.setattr("mailbot_v26.start.stage_tg", fake_tg)

    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = False

    _process_queue(storage, config, processor, flags)

    assert calls == {"parse": 1, "llm": 1, "tg": 1}
    _cleanup(email_id)


def test_premium_flag_on_routes_to_premium_processor(monkeypatch, tmp_path) -> None:
    account = AccountConfig(
        account_id="acc",
        login="account@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    inbound = InboundMessage(subject="Test subject", body="Hello", sender="sender@example.com")
    storage, email_id = _seed_queue(
        tmp_path=tmp_path,
        account_email="account@example.com",
        inbound=inbound,
        raw=_build_raw_email(),
        config=config,
    )
    processor = MessageProcessor(SimpleNamespace(), SimpleNamespace())

    called = {}

    def fake_premium(**kwargs):
        called["account_email"] = kwargs.get("account_email")

    monkeypatch.setattr("mailbot_v26.start.processor_module.process_message", fake_premium)

    monkeypatch.setattr(
        "mailbot_v26.start.stage_parse",
        lambda _ctx: (_ for _ in ()).throw(AssertionError("legacy parse called")),
    )

    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = True

    _process_queue(storage, config, processor, flags)

    assert called["account_email"] == "account@example.com"
    with storage.conn:
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    assert remaining == 0
    _cleanup(email_id)


def test_premium_failure_falls_back(monkeypatch, tmp_path) -> None:
    account = AccountConfig(
        account_id="acc",
        login="account@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    inbound = InboundMessage(subject="Test subject", body="Hello", sender="sender@example.com")
    storage, email_id = _seed_queue(
        tmp_path=tmp_path,
        account_email="account@example.com",
        inbound=inbound,
        raw=_build_raw_email(),
        config=config,
    )
    processor = MessageProcessor(SimpleNamespace(), SimpleNamespace())

    monkeypatch.setattr(
        "mailbot_v26.start.processor_module.process_message",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    calls = {"parse": 0, "llm": 0, "tg": 0}

    def fake_parse(_ctx):
        calls["parse"] += 1

    def fake_llm(_ctx):
        calls["llm"] += 1

    def fake_tg(_ctx):
        calls["tg"] += 1
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr("mailbot_v26.start.stage_parse", fake_parse)
    monkeypatch.setattr("mailbot_v26.start.stage_llm", fake_llm)
    monkeypatch.setattr("mailbot_v26.start.stage_tg", fake_tg)

    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = True

    _process_queue(storage, config, processor, flags)

    assert calls == {"parse": 1, "llm": 1, "tg": 1}
    _cleanup(email_id)
