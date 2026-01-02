from __future__ import annotations

import logging
import socket
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.bot_core import pipeline as core_pipeline
from mailbot_v26.bot_core.pipeline import PIPELINE_CACHE, PipelineContext, configure_pipeline, store_inbound
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.llm.providers import LLMProvider
from mailbot_v26.llm.router import LLMRouter, LLMRouterConfig
from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor, process_message
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.start import _process_queue
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.worker import telegram_sender
from mailbot_v26.worker.telegram_sender import DeliveryResult

from mailbot_v26.tests.integration.harness import (
    build_config,
    build_raw_email,
    cleanup_pipeline_cache,
    run_single_cycle,
)


class StubProvider(LLMProvider):
    def __init__(self, response: str = "", healthy: bool = True) -> None:
        self.response = response
        self.healthy = healthy
        self.calls = 0

    def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
        self.calls += 1
        return self.response

    def healthcheck(self) -> bool:
        return self.healthy


def _llm_result(*, provider: str = "cloudflare") -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔵",
        action_line="Проверить",
        body_summary="Summary",
        attachment_summaries=[],
        llm_provider=provider,
    )


def _patch_processor_basics(monkeypatch, *, llm_result: SimpleNamespace) -> None:
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: llm_result)
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_COMMITMENT_TRACKER=False,
            ENABLE_ANOMALY_ALERTS=False,
            ENABLE_DAILY_DIGEST=False,
            ENABLE_WEEKLY_DIGEST=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.8,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ),
    )
    monkeypatch.setattr(
        processor,
        "runtime_flag_store",
        SimpleNamespace(get_flags=lambda **_kwargs: (RuntimeFlags(enable_auto_priority=False), False)),
    )
    monkeypatch.setattr(processor, "metrics_aggregator", SimpleNamespace(snapshot=lambda: {}))
    monkeypatch.setattr(processor, "system_gates", SimpleNamespace(evaluate=lambda _metrics: None))
    monkeypatch.setattr(
        processor,
        "context_store",
        SimpleNamespace(
            resolve_sender_entity=lambda **_kwargs: None,
            record_interaction_event=lambda **_kwargs: None,
            recompute_email_frequency=lambda **_kwargs: (0.0, 0),
        ),
    )
    monkeypatch.setattr(
        processor,
        "analytics",
        SimpleNamespace(sender_stats=lambda: [], priority_escalations=lambda **_kwargs: []),
    )
    monkeypatch.setattr(
        processor,
        "shadow_priority_engine",
        SimpleNamespace(compute=lambda **_kwargs: (llm_result.priority, None)),
    )
    monkeypatch.setattr(processor, "shadow_action_engine", SimpleNamespace(compute=lambda **_kwargs: []))
    monkeypatch.setattr(processor, "auto_action_engine", SimpleNamespace(propose=lambda **_kwargs: None))
    monkeypatch.setattr(processor, "decision_trace_writer", SimpleNamespace(write=lambda **_kwargs: None))
    monkeypatch.setattr(processor, "trust_snapshot_writer", SimpleNamespace(write=lambda *_args, **_kwargs: None))
    monkeypatch.setattr(
        processor, "relationship_health_snapshot_writer", SimpleNamespace(write=lambda *_args, **_kwargs: None)
    )
    monkeypatch.setattr(processor, "event_emitter", SimpleNamespace(emit=lambda **_kwargs: None))
    monkeypatch.setattr(processor, "contract_event_emitter", SimpleNamespace(emit=lambda *_args, **_kwargs: None))
    monkeypatch.setattr(processor, "system_snapshotter", SimpleNamespace(maybe_log=lambda **_kwargs: None))
    monkeypatch.setattr(processor, "send_system_notice", lambda **_kwargs: None)


class DummyResponse:
    def __init__(self, status_code: int = 200, text: str = "ok") -> None:
        self.status_code = status_code
        self.text = text
        self.content = text.encode()


def _with_bot_token(payload, *, bot_token: str) -> object:
    payload.metadata.setdefault("bot_token", bot_token)
    payload.metadata.setdefault("chat_id", payload.metadata.get("chat_id") or "chat")
    return payload


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


def _reset_queue_ready(storage: Storage) -> None:
    with storage.conn:
        storage.conn.execute("UPDATE queue SET not_before = NULL")


def test_gigachat_unavailable_falls_back(caplog, tmp_path: Path) -> None:
    gigachat = StubProvider(response="giga", healthy=False)
    cloudflare = StubProvider(response="cf", healthy=True)
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    with caplog.at_level(logging.INFO):
        result = router.complete([{"role": "user", "content": "Hello"}])

    assert result == "cf"
    assert gigachat.calls == 0
    assert cloudflare.calls == 1
    assert any("[LLM-FALLBACK]" in record.message for record in caplog.records)


def test_telegram_parse_error_salvage_sends_plain(monkeypatch, caplog) -> None:
    system_health.reset()
    llm_result = _llm_result(provider="gigachat")
    _patch_processor_basics(monkeypatch, llm_result=llm_result)

    calls: list[dict[str, object]] = []

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        calls.append(json)
        if len(calls) == 1:
            return DummyResponse(status_code=400, text="Bad Request: can't parse entities")
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda **kwargs: telegram_sender.send_telegram(
            _with_bot_token(kwargs["payload"], bot_token="token")
        ),
    )

    with caplog.at_level(logging.INFO):
        process_message(
            account_email="account@example.com",
            message_id=10,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body",
            attachments=[],
            telegram_chat_id="chat",
        )

    assert calls[0]["parse_mode"] == "HTML"
    assert "parse_mode" not in calls[1]
    assert any("[TG-SALVAGE]" in record.message for record in caplog.records)
    assert any("\"event\":\"telegram_sent\"" in record.message for record in caplog.records)


def test_telegram_retry_then_success(monkeypatch, tmp_path, caplog) -> None:
    account = AccountConfig(
        account_id="acc-1",
        login="account@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, account.login)
    _seed_pipeline_context(email_id, account.login)
    processor_instance = MessageProcessor(SimpleNamespace(), SimpleNamespace())
    configure_pipeline(config, processor_instance)

    calls = []

    def fake_send(_payload):
        calls.append(True)
        if len(calls) <= 2:
            return DeliveryResult(delivered=False, retryable=True, error="timeout")
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    with caplog.at_level(logging.INFO):
        _process_queue(storage, config, processor_instance, flags)
        _reset_queue_ready(storage)
        _process_queue(storage, config, processor_instance, flags)
        _reset_queue_ready(storage)
        _process_queue(storage, config, processor_instance, flags)

    with storage.conn:
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    assert remaining == 0
    assert caplog.text.count("telegram_delivery_retry") >= 2
    cleanup_pipeline_cache([email_id])


def test_telegram_retry_exhausted_marks_failed(monkeypatch, tmp_path, caplog) -> None:
    account = AccountConfig(
        account_id="acc-1",
        login="account@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    storage = Storage(config.storage.db_path)
    email_id = _seed_queue(storage, account.login)
    _seed_pipeline_context(email_id, account.login)
    processor_instance = MessageProcessor(SimpleNamespace(), SimpleNamespace())
    configure_pipeline(config, processor_instance)

    def fake_send(_payload):
        return DeliveryResult(delivered=False, retryable=True, error="timeout")

    monkeypatch.setattr(core_pipeline, "send_telegram", fake_send)
    monkeypatch.setattr("mailbot_v26.start.send_telegram", fake_send)

    flags = FeatureFlags(base_dir=tmp_path)
    with caplog.at_level(logging.INFO):
        _process_queue(storage, config, processor_instance, flags)
        _reset_queue_ready(storage)
        _process_queue(storage, config, processor_instance, flags)
        _reset_queue_ready(storage)
        _process_queue(storage, config, processor_instance, flags)

    with storage.conn:
        status = storage.conn.execute(
            "SELECT status FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()[0]
        remaining = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

    assert status == "DELIVERY_FAILED"
    assert remaining == 0
    assert "telegram_delivery_failed" in caplog.text
    cleanup_pipeline_cache([email_id])


def test_imap_error_backoff_and_other_accounts_continue(monkeypatch, tmp_path, caplog) -> None:
    bad = AccountConfig(
        account_id="bad",
        login="bad@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat1",
    )
    good = AccountConfig(
        account_id="good",
        login="good@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat2",
    )
    config = build_config(tmp_path, [bad, good])
    storage = Storage(config.storage.db_path)
    runtime_health = AccountRuntimeHealthManager(tmp_path / "runtime.json")
    runtime_health.register_account(bad)
    runtime_health.register_account(good)

    call_log: list[str] = []

    class FakeIMAP:
        def __init__(self, account, *_args):
            self.account = account

        def fetch_new_messages(self):
            call_log.append(self.account.login)
            if self.account.login == bad.login:
                raise socket.gaierror("dns fail")
            return []

    alerts: list[str] = []

    def fake_send(payload):
        alerts.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False)

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with caplog.at_level(logging.INFO):
        run_single_cycle(
            config=config,
            accounts_to_poll=[bad, good],
            imap_factory=lambda account, state, ts: FakeIMAP(account, state, ts),
            processor=MessageProcessor(SimpleNamespace(), SimpleNamespace()),
            runtime_health=runtime_health,
            storage=storage,
            now=now,
            telegram_sender=fake_send,
        )
        run_single_cycle(
            config=config,
            accounts_to_poll=[bad, good],
            imap_factory=lambda account, state, ts: FakeIMAP(account, state, ts),
            processor=MessageProcessor(SimpleNamespace(), SimpleNamespace()),
            runtime_health=runtime_health,
            storage=storage,
            now=now + timedelta(minutes=2),
            telegram_sender=fake_send,
        )

    bad_state = runtime_health.get_state("bad")

    assert call_log.count(good.login) == 2
    assert bad_state.consecutive_failures == 2
    assert bad_state.next_retry_at_utc is not None
    assert len(alerts) == 1


def test_sqlite_busy_side_effect_does_not_block_telegram(monkeypatch, tmp_path, caplog) -> None:
    from mailbot_v26.storage.knowledge_db import KnowledgeDB

    system_health.reset()
    llm_result = _llm_result(provider="cloudflare")
    _patch_processor_basics(monkeypatch, llm_result=llm_result)

    db = KnowledgeDB(tmp_path / "crm.sqlite")
    db._WRITE_RETRIES = 2
    db._WRITE_BASE_DELAY = 0.0
    db._WRITE_MAX_TOTAL_WAIT = 0.1

    def _locked_connect():
        raise sqlite3.OperationalError("database is locked")

    calls: list[bool] = []

    monkeypatch.setattr(processor, "knowledge_db", db)
    monkeypatch.setattr(db, "_connect", _locked_connect)
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda **_kwargs: calls.append(True) or DeliveryResult(True, False),
    )

    with caplog.at_level(logging.ERROR):
        process_message(
            account_email="account@example.com",
            message_id=11,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body",
            attachments=[],
            telegram_chat_id="chat",
        )

    assert "crm_write_failed" in caplog.text
    assert calls


def test_llm_recovery_returns_full(monkeypatch, caplog) -> None:
    system_health.reset()
    _patch_processor_basics(monkeypatch, llm_result=_llm_result(provider="cloudflare"))
    monkeypatch.setattr(processor, "enqueue_tg", lambda **_kwargs: DeliveryResult(True, False))

    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: None)

    with caplog.at_level(logging.INFO):
        process_message(
            account_email="account@example.com",
            message_id=12,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body",
            attachments=[],
            telegram_chat_id="chat",
        )

    assert system_health.mode == OperationalMode.DEGRADED_NO_LLM

    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: _llm_result(provider="cloudflare"))

    with caplog.at_level(logging.INFO):
        process_message(
            account_email="account@example.com",
            message_id=13,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body",
            attachments=[],
            telegram_chat_id="chat",
        )

    assert system_health.mode == OperationalMode.FULL
    assert any("\"event\":\"system_mode_changed\"" in record.message for record in caplog.records)


def test_imap_account_recovery_after_backoff(tmp_path) -> None:
    account = AccountConfig(
        account_id="acc",
        login="acc@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    storage = Storage(config.storage.db_path)
    runtime_health = AccountRuntimeHealthManager(tmp_path / "runtime.json")
    runtime_health.register_account(account)

    class FlakyIMAP:
        def __init__(self, account, *_args):
            self.account = account
            self.calls = getattr(self.account, "calls", 0)
            self.account.calls = self.calls + 1

        def fetch_new_messages(self):
            if self.account.calls <= 1:
                raise socket.gaierror("dns fail")
            return []

    alerts: list[str] = []

    def fake_send(payload):
        alerts.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False)

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    run_single_cycle(
        config=config,
        accounts_to_poll=[account],
        imap_factory=lambda acc, state, ts: FlakyIMAP(acc, state, ts),
        processor=MessageProcessor(SimpleNamespace(), SimpleNamespace()),
        runtime_health=runtime_health,
        storage=storage,
        now=now,
        telegram_sender=fake_send,
    )
    run_single_cycle(
        config=config,
        accounts_to_poll=[account],
        imap_factory=lambda acc, state, ts: FlakyIMAP(acc, state, ts),
        processor=MessageProcessor(SimpleNamespace(), SimpleNamespace()),
        runtime_health=runtime_health,
        storage=storage,
        now=now + timedelta(minutes=2),
        telegram_sender=fake_send,
    )

    state = runtime_health.get_state(account.account_id)
    assert state.consecutive_failures == 0
    assert state.next_retry_at_utc is None
    assert len(alerts) == 1
