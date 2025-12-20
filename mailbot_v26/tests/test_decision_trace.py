from __future__ import annotations

import sqlite3
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.pipeline import processor
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Позвонить клиенту",
        body_summary="Summary",
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
    monkeypatch.setattr(processor, "decision_trace_writer", DecisionTraceWriter(db_path))
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
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: None)

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
        row = conn.execute(
            """
            SELECT prompt_full, llm_response
            FROM decision_traces
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()

    assert row == ("FULL PROMPT\nline2", '{"output":"result"}')


def test_decision_trace_does_not_change_telegram_payload(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "decision_trace_payload.sqlite"
    _common_monkeypatches(monkeypatch, db_path)

    payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: payload.update(kwargs))

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

    assert set(payload.keys()) == {
        "chat_id",
        "priority",
        "from_email",
        "subject",
        "action_line",
        "body_summary",
        "attachment_summaries",
        "account_email",
    }


def test_decision_trace_write_failure_does_not_stop_pipeline(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "decision_trace_fail.sqlite"
    _common_monkeypatches(monkeypatch, db_path)

    def _raise_write(**kwargs) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(processor, "decision_trace_writer", SimpleNamespace(write=_raise_write))

    sent: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: sent.update(kwargs))

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

    assert sent.get("chat_id") == "chat"
