from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.observability import logger as observability_logger
from mailbot_v26.pipeline import processor
from mailbot_v26.priority.auto_engine import AutoPriorityEngine
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
        processor_module, "shadow_priority_engine", SimpleNamespace(compute=lambda **kwargs: ("🟡", "shadow"))
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
        SimpleNamespace(get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=True), False), set_enable_auto_priority=lambda **kwargs: None),
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
    monkeypatch.setattr(
        processor_module,
        "auto_priority_engine",
        AutoPriorityEngine(
            processor_module.auto_priority_gates,
            processor_module.auto_priority_breaker,
            processor_module.runtime_flag_store,
            processor_module.system_health,
            enabled_flag=lambda: processor_module.feature_flags.ENABLE_AUTO_PRIORITY,
        ),
    )


def test_structured_logging_events_emitted(monkeypatch) -> None:
    observability_logger._CONFIGURED = False
    stream, handler, root_logger = _capture_plain_json_logs()

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

    events = []
    for line in stream.getvalue().splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        events.append(payload["event"])
    assert "email_received" in events
    assert "llm_decision" in events
    assert "auto_priority_applied" in events
    assert "telegram_sent" in events
    assert "signal_evaluated" in events
    assert "decision_traced" in events


def test_observability_logger_outputs_json() -> None:
    observability_logger._CONFIGURED = False
    stream, handler, root_logger = _capture_plain_json_logs()
    try:
        logger = observability_logger.get_logger("mailbot")
        logger.info("email_received", email_id=123)
    finally:
        root_logger.removeHandler(handler)

    payload = json.loads(stream.getvalue().strip())
    assert payload["event"] == "email_received"
    assert payload["email_id"] == 123
    assert payload["level"] == "INFO"
    assert payload["timestamp"].endswith("Z")


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


def test_signal_fallback_logging(monkeypatch) -> None:
    observability_logger._CONFIGURED = False
    stream, handler, root_logger = _capture_plain_json_logs()

    _setup_processor(monkeypatch, processor)
    try:
        processor.process_message(
            account_email="account@example.com",
            message_id=99,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="a" * 100,
            attachments=[],
            telegram_chat_id="chat",
        )
    finally:
        root_logger.removeHandler(handler)

    payloads = [
        json.loads(line)
        for line in stream.getvalue().splitlines()
        if line.strip()
    ]
    evaluated = [entry for entry in payloads if entry["event"] == "signal_evaluated"]
    assert evaluated
    evaluated_payload = evaluated[0]
    assert "entropy" in evaluated_payload
    assert "printable_ratio" in evaluated_payload
    assert "quality_score" in evaluated_payload
    assert evaluated_payload["fallback_used"] is True

    fallback_events = [
        entry for entry in payloads if entry["event"] == "signal_fallback_used"
    ]
    assert fallback_events
    assert fallback_events[0]["reason"] == "entropy_below_threshold"


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
    monkeypatch.setattr(
        processor,
        "auto_priority_engine",
        AutoPriorityEngine(
            processor.auto_priority_gates,
            processor.auto_priority_breaker,
            processor.runtime_flag_store,
            processor.system_health,
            enabled_flag=lambda: processor.feature_flags.ENABLE_AUTO_PRIORITY,
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


def test_system_health_snapshot_logging_does_not_break(monkeypatch) -> None:
    _setup_processor(monkeypatch, processor)
    monkeypatch.setattr(
        processor,
        "system_snapshotter",
        SimpleNamespace(
            maybe_log=lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        ),
    )

    payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: payload.update(kwargs))

    processor.process_message(
        account_email="account@example.com",
        message_id=55,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert payload.get("chat_id") == "chat"
