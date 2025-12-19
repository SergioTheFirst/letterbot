from __future__ import annotations

import sqlite3
import sys
import types
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.priority.auto_gates import CircuitBreakerStatus, GateDecision
from mailbot_v26.storage.knowledge_db import KnowledgeDB


# Stub missing pipeline dependencies before importing the processor
if "mailbot_v26.pipeline.stage_llm" not in sys.modules:
    stage_llm = types.ModuleType("mailbot_v26.pipeline.stage_llm")
    stage_llm.run_llm_stage = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_llm"] = stage_llm

if "mailbot_v26.pipeline.stage_telegram" not in sys.modules:
    stage_telegram = types.ModuleType("mailbot_v26.pipeline.stage_telegram")
    stage_telegram.send_to_telegram = lambda **kwargs: None
    stage_telegram.send_preview_to_telegram = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_telegram"] = stage_telegram

from mailbot_v26.pipeline import processor


class StubRuntimeFlagStore:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled
        self.disable_calls = 0

    def get_flags(self, *, force: bool = False):
        return RuntimeFlags(enable_gigachat=False, enable_auto_priority=self.enabled), False

    def set_enable_auto_priority(self, enabled: bool) -> None:
        self.enabled = enabled
        self.disable_calls += 1


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔵",
        action_line="Action line",
        body_summary="Body summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
    )


def test_flag_off_priority_stays_llm(monkeypatch):
    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )

    sent: dict[str, object] = {}
    monkeypatch.setattr(
        processor,
        "send_to_telegram",
        lambda **kwargs: sent.update(kwargs),
    )
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None),
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(False))

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent["priority"] == "🔵"


def test_flag_on_applies_shadow_priority(monkeypatch):
    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )

    sent: dict[str, object] = {}
    monkeypatch.setattr(
        processor,
        "send_to_telegram",
        lambda **kwargs: sent.update(kwargs),
    )
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None),
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(True))
    monkeypatch.setattr(
        processor.auto_priority_gates,
        "evaluate",
        lambda **kwargs: GateDecision(open=True, reasons=()),
    )
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=2,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 2, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent["priority"] == "🟡"
    assert sent["action_line"] == llm_result.action_line
    assert sent["body_summary"] == llm_result.body_summary
    assert sent["attachment_summaries"] == llm_result.attachment_summaries
    assert sent["account_email"] == "account@example.com"


def test_db_persistence_records_original_priority(monkeypatch, tmp_path):
    db_path = tmp_path / "auto_priority.sqlite"

    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )

    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: None)
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        KnowledgeDB(db_path),
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=True,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(True))
    monkeypatch.setattr(
        processor.auto_priority_gates,
        "evaluate",
        lambda **kwargs: GateDecision(open=True, reasons=()),
    )
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=3,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 3, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT priority, original_priority, priority_reason FROM emails ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert row == ("🟡", "🔵", "shadow reason")


def test_gate_closed_skips_auto_priority(monkeypatch):
    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(True))
    monkeypatch.setattr(
        processor.auto_priority_gates,
        "evaluate",
        lambda **kwargs: GateDecision(open=False, reasons=("shadow_accuracy",)),
    )
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
    )

    sent: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: sent.update(kwargs))

    processor.process_message(
        account_email="account@example.com",
        message_id=4,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 4, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent["priority"] == "🔵"


def test_runtime_flag_off_disables_auto_priority(monkeypatch):
    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(False))
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
    )

    sent: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: sent.update(kwargs))

    processor.process_message(
        account_email="account@example.com",
        message_id=5,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 5, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent["priority"] == "🔵"


def test_circuit_breaker_disables_auto_priority(monkeypatch):
    llm_result = _llm_result()
    runtime_store = StubRuntimeFlagStore(True)

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", runtime_store)
    monkeypatch.setattr(
        processor.auto_priority_breaker,
        "check",
        lambda: CircuitBreakerStatus(
            tripped=True,
            reason="reject_rate_1h",
            reject_rate=0.3,
            confidence_p50=0.6,
        ),
    )
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
    )

    sent: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: sent.update(kwargs))

    processor.process_message(
        account_email="account@example.com",
        message_id=6,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 6, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert runtime_store.enabled is False
    assert runtime_store.disable_calls == 1
    assert sent["priority"] == "🔵"
