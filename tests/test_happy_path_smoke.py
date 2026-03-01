from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from email.message import EmailMessage
from types import SimpleNamespace

from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.processor import MessageProcessor
from mailbot_v26.tests.integration.harness import DummyState, build_config, build_raw_email, run_single_cycle
from mailbot_v26.worker.telegram_sender import DeliveryResult


def test_happy_path_invoice_with_excel_attachment(tmp_path, monkeypatch) -> None:
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
        priority="🔵",
        action_line="Оплатить счёт",
        body_summary="Счёт на оплату за услуги",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_: llm_result)

    delivered_payloads = []

    def fake_send(payload):
        delivered_payloads.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr("mailbot_v26.bot_core.pipeline.send_telegram", fake_send)

    msg = EmailMessage()
    msg["Subject"] = "FW: Счет на оплату от 29.12.25г."
    msg["From"] = "sender@company.ru"
    msg["To"] = "user@example.com"
    msg.set_content("Во вложении счёт на оплату.")
    xlsx_content = b"fake xlsx content with amount 15000"
    msg.add_attachment(
        xlsx_content,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="Счет_на_оплату.xlsx",
    )
    raw_email = msg.as_bytes()

    class MockIMAP:
        def fetch_new_messages(self):
            return [(1, raw_email)]

    storage = Storage(tmp_path / "mailbot.sqlite")
    processor_instance = MessageProcessor(config=config, state=DummyState())
    runtime_health = AccountRuntimeHealthManager(tmp_path / "runtime_health.json")

    monkeypatch.setattr(
        "mailbot_v26.bot_core.pipeline.extract_excel_text",
        lambda content, filename: "Итого к оплате 15 000 руб.",
    )

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

    assert len(delivered_payloads) >= 1, "TG delivery не вызван"
    final_payload = delivered_payloads[-1]
    assert "SAFE_FALLBACK" not in final_payload.html_text
    assert "Счет_на_оплату.xlsx" in final_payload.html_text
    assert "15 000" in final_payload.html_text
    assert final_payload.metadata.get("bot_token") == "token"
    assert final_payload.metadata.get("chat_id") == "123456"

    conn = sqlite3.connect(tmp_path / "mailbot.sqlite")
    rows = conn.execute("SELECT id FROM emails").fetchall()
    conn.close()
    assert len(rows) >= 1, "Письмо не сохранено в БД"


def test_happy_path_simple_email_no_attachments(tmp_path, monkeypatch) -> None:
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
        priority="🔵",
        action_line="Ответить",
        body_summary="Простое письмо",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_: llm_result)

    delivered = []
    monkeypatch.setattr(
        "mailbot_v26.bot_core.pipeline.send_telegram",
        lambda p: delivered.append(p) or DeliveryResult(delivered=True, retryable=False),
    )

    raw_email = build_raw_email(
        subject="Тестовое письмо",
        body="Привет, это тест.",
        sender="boss@company.ru",
    )

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
        telegram_sender=lambda p: (delivered.append(p), DeliveryResult(delivered=True, retryable=False))[1],
    )

    assert len(delivered) >= 1
    assert "SAFE_FALLBACK" not in delivered[-1].html_text
    assert delivered[-1].metadata.get("bot_token") == "token"
    assert delivered[-1].metadata.get("chat_id") == "123456"
