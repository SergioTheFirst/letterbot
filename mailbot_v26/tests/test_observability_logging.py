from __future__ import annotations

import builtins
import importlib
import io
import json
import logging
import sys
from datetime import datetime
from types import SimpleNamespace

import pytest

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.observability import logger as observability_logger
from mailbot_v26.pipeline import processor
from mailbot_v26.priority.auto_gates import CircuitBreakerStatus, GateDecision


def _capture_plain_json_logs() -> tuple[io.StringIO, logging.Handler, logging.Logger]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    return stream, handler, root_logger


def _setup_processor(monkeypatch, processor_module) -> None:
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Ответить клиенту",
        body_summary="Summary",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor_module, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor_module, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor_module, "shadow_priority_engine", SimpleNamespace(compute=lambda **kwargs: ("🔴", "shadow"))
    )
    monkeypatch.setattr(processor_module, "shadow_action_engine", SimpleNamespace(compute=lambda **kwargs: []))
    monkeypatch.setattr(processor_module, "priority_confidence_engine", SimpleNamespace(score=lambda **kwargs: 0.95))
    monkeypatch.setattr(
        processor_module, "auto_priority_gates", SimpleNamespace(evaluate=lambda **kwargs: GateDecision(open=True, reasons=()))
    )
    monkeypatch.setattr(
        processor_module,
        "auto_priority_breaker",
        SimpleNamespace(check=lambda: CircuitBreakerStatus(tripped=False, reason=None, reject_rate=None, confidence_p50=None)),
    )
    monkeypatch.setattr(
        processor_module,
        "runtime_flag_store",
        SimpleNamespace(get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=True), False)),
    )
    monkeypatch.setattr(
        processor_module,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.8,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor_module, "send_to_telegram", lambda **kwargs: None)


def test_structured_logging_events_emitted(monkeypatch) -> None:
    structlog = pytest.importorskip("structlog")
    observability_logger._CONFIGURED = False
    observability_logger.STRUCTLOG_AVAILABLE = True
    observability_logger.structlog = structlog

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
        ],
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    _setup_processor(monkeypatch, processor)
    try:
        processor.process_message(
            account_email="account@example.com",
            message_id=42,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body",
            attachments=[],
            telegram_chat_id="chat",
        )
    finally:
        root_logger.removeHandler(handler)

    events = [
        json.loads(line)["event"]
        for line in stream.getvalue().splitlines()
        if line.strip()
    ]
    assert "email_received" in events
    assert "llm_decision" in events
    assert "auto_priority_evaluated" in events
    assert "telegram_sent" in events


def test_fallback_logging_without_structlog(monkeypatch) -> None:
    original_import = builtins.__import__

    def _blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "structlog":
            raise ImportError("structlog unavailable")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)
    sys.modules.pop("mailbot_v26.observability.logger", None)
    sys.modules.pop("mailbot_v26.observability", None)
    sys.modules.pop("mailbot_v26.pipeline.processor", None)
    reloaded_logger = importlib.import_module("mailbot_v26.observability.logger")
    fallback_processor = importlib.import_module("mailbot_v26.pipeline.processor")
    stream, handler, root_logger = _capture_plain_json_logs()
    _setup_processor(monkeypatch, fallback_processor)
    try:
        logger = reloaded_logger.get_logger("mailbot")
        logger.info("email_received", email_id=123)
    finally:
        root_logger.removeHandler(handler)

    payload = json.loads(stream.getvalue().strip())
    assert payload["event"] == "email_received"
    assert payload["email_id"] == 123


def test_telegram_payload_stability(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🟡",
        action_line="Проверить документы",
        body_summary="Summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
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

    payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: payload.update(kwargs))

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

    assert payload == {
        "chat_id": "chat",
        "priority": "🟡",
        "from_email": "sender@example.com",
        "subject": "Subject",
        "action_line": "Проверить документы",
        "body_summary": "Summary",
        "attachment_summaries": [{"filename": "file.txt", "summary": "summary"}],
        "account_email": "account@example.com",
    }


def test_auto_priority_behavior_unchanged(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🟡",
        action_line="Проверить документы",
        body_summary="Summary",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor, "shadow_priority_engine", SimpleNamespace(compute=lambda **kwargs: ("🔴", "shadow")))
    monkeypatch.setattr(processor, "shadow_action_engine", SimpleNamespace(compute=lambda **kwargs: []))
    monkeypatch.setattr(processor, "priority_confidence_engine", SimpleNamespace(score=lambda **kwargs: 0.95))
    monkeypatch.setattr(processor, "auto_priority_gates", SimpleNamespace(evaluate=lambda **kwargs: GateDecision(open=True, reasons=())))
    monkeypatch.setattr(
        processor,
        "auto_priority_breaker",
        SimpleNamespace(check=lambda: CircuitBreakerStatus(tripped=False, reason=None, reject_rate=None, confidence_p50=None)),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", SimpleNamespace(get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=True), False)))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.8,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )

    saved_payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: saved_payload.update(kwargs)))
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

    assert payload["priority"] == "🔴"
    assert saved_payload["priority"] == "🔴"
