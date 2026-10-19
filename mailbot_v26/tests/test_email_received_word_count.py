from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.pipeline import processor
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔵",
        action_line="Ответить",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="cloudflare",
        llm_model="test-model",
        prompt_full="PROMPT",
        prompt_vars={},
        crm_context={},
        llm_request="{}",
        llm_response="{}",
    )


def test_email_received_payload_counts_words(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "word_count.sqlite"
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(
        processor, "decision_trace_writer", DecisionTraceWriter(db_path)
    )
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)
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
    captured: list[dict[str, object]] = []

    def _capture(event_type, **kwargs) -> None:
        if event_type == EventType.EMAIL_RECEIVED:
            captured.append(kwargs["payload"])

    monkeypatch.setattr(processor, "_emit_contract_event", _capture)

    processor.process_message(
        account_email="account@example.com",
        message_id=501,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="hello brave world",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert captured, "EMAIL_RECEIVED contract event was not emitted"
    assert captured[-1]["word_count"] == 3
