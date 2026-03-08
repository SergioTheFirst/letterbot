from __future__ import annotations

import sqlite3
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import processor
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Позвонить клиенту",
        body_summary="Краткое описание письма.",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
        llm_provider="cloudflare",
        llm_model="test-model",
        prompt_full="FULL PROMPT\nline2",
        prompt_vars={"subject": "Subject"},
        crm_context={"account": "account@example.com"},
        llm_request='{"input":"data"}',
        llm_response='{"output":"result"}',
    )


def _common_monkeypatches(monkeypatch, db_path) -> None:
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(
        processor, "decision_trace_writer", DecisionTraceWriter(db_path)
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: (llm_priority, None),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [],
    )


def test_decision_trace_persists_prompt_and_response(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "decision_trace.sqlite"
    _common_monkeypatches(monkeypatch, db_path)
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=10,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("""
            SELECT prompt_full, response_full
            FROM decision_traces
            ORDER BY created_at DESC
            LIMIT 1
            """).fetchone()

    assert row == ("FULL PROMPT\nline2", '{"output":"result"}')


def test_decision_trace_does_not_change_telegram_payload(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "decision_trace_payload.sqlite"
    _common_monkeypatches(monkeypatch, db_path)

    def _enqueue_tg(*, email_id: int, payload) -> None:
        payload_store["payload"] = payload

    payload_store: dict[str, object] = {}
    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=11,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    payload = payload_store["payload"]
    assert payload.metadata["chat_id"] == "chat"
    assert payload.metadata["account_email"] == "account@example.com"
    assert payload.metadata["action_line"] == "Позвонить клиенту"
    assert payload.metadata["body_summary"] == "Краткое описание письма."
    assert payload.metadata["attachment_summaries"] == [
        {"filename": "file.txt", "summary": "summary"}
    ]


def test_decision_trace_write_failure_does_not_stop_pipeline(
    monkeypatch, tmp_path
) -> None:
    db_path = tmp_path / "decision_trace_fail.sqlite"
    _common_monkeypatches(monkeypatch, db_path)

    def _raise_write(**kwargs) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        processor, "decision_trace_writer", SimpleNamespace(write=_raise_write)
    )

    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=12,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent.get("payload").metadata.get("chat_id") == "chat"


def test_decision_trace_records_signal_fallback(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "decision_trace_signal.sqlite"
    _common_monkeypatches(monkeypatch, db_path)
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=13,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="a" * 120,
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("""
            SELECT signal_fallback_used
            FROM decision_traces
            ORDER BY created_at DESC
            LIMIT 1
            """).fetchone()

    assert row == (1,)


def test_message_interpretation_created(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "message_interpretation.sqlite"
    _common_monkeypatches(monkeypatch, db_path)
    monkeypatch.setattr(
        processor, "contract_event_emitter", ContractEventEmitter(db_path)
    )
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=21,
        from_email="vendor@example.com",
        subject="Счет №123",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Просим оплатить счет 87500 руб до 15.04.2026",
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("""
            SELECT event_type, payload
            FROM events_v1
            WHERE event_type = 'message_interpretation'
            ORDER BY ts_utc DESC
            LIMIT 1
            """).fetchone()

    assert row is not None
    assert row[0] == "message_interpretation"


def test_interpretation_matches_decision() -> None:
    facts = processor._collect_message_facts(  # noqa: SLF001
        subject="Счет №445",
        body_text="К оплате 87 500 руб до 15.04.2026",
        attachments=[],
        mail_type="INVOICE",
    )
    facts = processor._validate_message_facts(
        facts, evidence_text="Счет №445 87 500 руб до 15.04.2026"
    )  # noqa: SLF001
    context = processor._detect_conversation_context(  # noqa: SLF001
        subject="Счет №445",
        body_text="К оплате 87 500 руб до 15.04.2026",
        message_facts=facts,
    )
    decision = processor._build_message_decision(  # noqa: SLF001
        priority="🟡",
        action_line="Оплатить",
        summary="Оплатить счет",
        message_facts=facts,
        subject="Счет №445",
        body_text="К оплате 87 500 руб до 15.04.2026",
        attachments=[],
        context=context,
    )

    interpretation = processor._build_message_interpretation(  # noqa: SLF001
        email_id=55,
        sender_email="vendor@example.com",
        message_facts=facts,
        decision=decision,
        document_id="invoice_445_vendor",
    )

    assert interpretation.action == decision.action
    assert interpretation.priority == decision.priority
    assert interpretation.context == decision.context
    assert interpretation.doc_kind == decision.doc_kind


def test_no_behavior_change_in_telegram_payload(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "telegram_payload_stable.sqlite"
    _common_monkeypatches(monkeypatch, db_path)
    monkeypatch.setattr(
        processor, "contract_event_emitter", ContractEventEmitter(db_path)
    )

    payload_store: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        payload_store["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=22,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    payload = payload_store["payload"]
    assert payload.metadata["chat_id"] == "chat"
    assert payload.metadata["account_email"] == "account@example.com"
    assert payload.metadata["action_line"] == "Позвонить клиенту"
    assert payload.metadata["body_summary"] == "Краткое описание письма."
