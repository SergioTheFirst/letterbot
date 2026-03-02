from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.processor import MessageProcessor
from mailbot_v26.tests.integration.harness import DummyState, build_config, build_raw_email, run_single_cycle
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _run_cycle_with_email(tmp_path, monkeypatch, *, subject: str, body: str, llm_priority: str):
    account = AccountConfig(
        account_id="test_account",
        login="user@example.com",
        password="pass",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="123456",
    )
    config = build_config(tmp_path, accounts=[account])

    llm_result = SimpleNamespace(
        priority=llm_priority,
        action_line="Ответить",
        body_summary="Краткое резюме",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_: llm_result)

    delivered_payloads = []

    def fake_send(payload):
        delivered_payloads.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr("mailbot_v26.bot_core.pipeline.send_telegram", fake_send)

    raw_email = build_raw_email(subject=subject, body=body, sender="boss@company.ru")

    class MockIMAP:
        def fetch_new_messages(self):
            return [(1, raw_email)]

    storage = Storage(tmp_path / "mailbot.sqlite")
    processor_instance = MessageProcessor(config=config, state=DummyState())
    runtime_health = AccountRuntimeHealthManager(tmp_path / "runtime_health.json")

    run_single_cycle(
        config=config,
        accounts_to_poll=[account],
        imap_factory=lambda acc, state, now: MockIMAP(),
        processor=processor_instance,
        runtime_health=runtime_health,
        storage=storage,
        now=datetime.now(timezone.utc),
        telegram_sender=fake_send,
    )

    conn = sqlite3.connect(tmp_path / "mailbot.sqlite")
    rows = conn.execute("SELECT id FROM emails").fetchall()
    conn.close()

    return delivered_payloads, rows


def test_happy_path_simple_email(tmp_path, monkeypatch) -> None:
    payloads, rows = _run_cycle_with_email(
        tmp_path,
        monkeypatch,
        subject="Тестовое письмо",
        body="Привет, это test happy-path.",
        llm_priority="🔵",
    )

    assert len(payloads) >= 1
    assert payloads[-1].metadata.get("bot_token") == "token"
    assert payloads[-1].metadata.get("chat_id") == "123456"
    assert len(rows) >= 1


def test_happy_path_high_priority_email(tmp_path, monkeypatch) -> None:
    payloads, rows = _run_cycle_with_email(
        tmp_path,
        monkeypatch,
        subject="СРОЧНО: нужно согласование",
        body="Просьба подтвердить до конца дня.",
        llm_priority="🔴",
    )

    assert len(payloads) >= 1
    assert "🔴" in payloads[-1].html_text or payloads[-1].metadata.get("priority") == "🔴"
    assert len(rows) >= 1


def test_happy_path_delivery_has_required_metadata(tmp_path, monkeypatch) -> None:
    payloads, _rows = _run_cycle_with_email(
        tmp_path,
        monkeypatch,
        subject="Контрактные метаданные",
        body="Проверка payload metadata.",
        llm_priority="🟡",
    )

    assert len(payloads) >= 1
    for payload in payloads:
        assert payload.metadata.get("bot_token")
        assert payload.metadata.get("chat_id")
