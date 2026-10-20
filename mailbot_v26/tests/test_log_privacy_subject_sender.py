"""WP3b: pipeline log lines never carry raw subjects or sender addresses.

The "LLM stage is not configured" error and the process_message start line
(email_received) log the email id, lengths and counts only.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from types import SimpleNamespace

import pytest

from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.pipeline import processor, stage_llm
from mailbot_v26.storage.knowledge_db import KnowledgeDB

SECRET_SUBJECT = "Invoice 42 SECRET-SUBJ-9Z"
SECRET_SENDER = "secret.sender.9z@example.com"


def _json_records(caplog: pytest.LogCaptureFixture, event: str) -> list[dict]:
    records = []
    for record in caplog.records:
        try:
            payload = json.loads(record.getMessage())
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("event") == event:
            records.append(payload)
    return records


def test_unconfigured_llm_stage_logs_no_subject_or_sender(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    result = stage_llm.run_llm_stage(
        subject=SECRET_SUBJECT,
        from_email=SECRET_SENDER,
        body_text="body",
        attachments=[{"filename": "a.pdf"}, {"filename": "b.pdf"}],
    )
    assert result is None
    assert SECRET_SUBJECT not in caplog.text
    assert SECRET_SENDER not in caplog.text
    records = _json_records(caplog, "processing_error")
    assert records, caplog.text
    error = records[-1]["error"]
    assert "LLM stage is not configured" in error
    assert f"subject_len={len(SECRET_SUBJECT)}" in error
    assert "attachments=2" in error


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


def test_email_received_log_has_counts_not_subject_or_sender(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "log_privacy.sqlite"
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(processor, "decision_trace_writer", DecisionTraceWriter(db_path))
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
    monkeypatch.setattr(processor, "_emit_contract_event", lambda *a, **k: None)
    caplog.set_level(logging.INFO)

    body = "hello brave world"
    processor.process_message(
        account_email="account@example.com",
        message_id=777,
        from_email=SECRET_SENDER,
        subject=SECRET_SUBJECT,
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text=body,
        attachments=[{"filename": "a.pdf", "content_type": "application/pdf"}],
        telegram_chat_id="chat",
    )

    records = _json_records(caplog, "email_received")
    assert records, "email_received was not logged"
    record = records[-1]
    assert "subject" not in record
    assert "from_email" not in record
    raw = json.dumps(record, ensure_ascii=False)
    assert SECRET_SUBJECT not in raw
    assert SECRET_SENDER not in raw
    assert record["email_id"] == 777
    assert record["subject_len"] == len(SECRET_SUBJECT)
    assert record["body_len"] == len(body)
    assert record["attachments"] == 1
