from __future__ import annotations

import sys
import types
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.priority.auto_engine import AutoPriorityEngine
from mailbot_v26.priority.auto_gates import GateDecision
from mailbot_v26.priority.confidence_engine import PriorityConfidenceEngine


# Stub missing pipeline dependencies before importing the processor
if "mailbot_v26.pipeline.stage_llm" not in sys.modules:
    stage_llm = types.ModuleType("mailbot_v26.pipeline.stage_llm")
    stage_llm.run_llm_stage = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_llm"] = stage_llm

if "mailbot_v26.pipeline.stage_telegram" not in sys.modules:
    stage_telegram = types.ModuleType("mailbot_v26.pipeline.stage_telegram")
    stage_telegram.send_to_telegram = lambda **kwargs: None
    stage_telegram.send_preview_to_telegram = lambda **kwargs: None
    stage_telegram.send_system_notice = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_telegram"] = stage_telegram

from mailbot_v26.pipeline import processor
from mailbot_v26.telegram_utils import telegram_safe


class StubRuntimeFlagStore:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def get_flags(self, *, force: bool = False):
        return RuntimeFlags(enable_gigachat=False, enable_auto_priority=self.enabled), False

    def set_enable_auto_priority(self, enabled: bool) -> None:
        self.enabled = enabled


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔵",
        action_line="Action line",
        body_summary="Body summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
    )


def _reset_auto_priority_engine(monkeypatch, runtime_store) -> None:
    monkeypatch.setattr(
        processor,
        "auto_priority_engine",
        AutoPriorityEngine(
            processor.auto_priority_gates,
            processor.auto_priority_breaker,
            runtime_store,
            processor.system_health,
            enabled_flag=lambda: processor.feature_flags.ENABLE_AUTO_PRIORITY,
        ),
    )


def test_confidence_zero_when_shadow_not_higher():
    engine = PriorityConfidenceEngine()

    score = engine.score(
        llm_priority="🔴",
        shadow_priority="🟡",
        sender_stats={},
        recent_history={},
    )

    assert score == 0.0


def test_confidence_high_history_exceeds_threshold():
    engine = PriorityConfidenceEngine()

    score = engine.score(
        llm_priority="🔵",
        shadow_priority="🔴",
        sender_stats={
            "red_count": 5,
            "emails_total": 6,
            "llm_underestimates_often": True,
        },
        recent_history={"escalations": 3, "is_trending_up": True},
    )

    assert score >= 0.6


def test_confidence_low_history_below_threshold():
    engine = PriorityConfidenceEngine()

    score = engine.score(
        llm_priority="🔵",
        shadow_priority="🟡",
        sender_stats={"red_count": 1, "emails_total": 10},
        recent_history={},
    )

    assert score < 0.6


def test_flag_off_bypasses_auto_priority(monkeypatch):
    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
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
        message_id=10,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 10, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent["priority"] == llm_result.priority


def test_telegram_payload_unchanged(monkeypatch):
    llm_result = _llm_result()

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 1.0,
    )

    sent: dict[str, object] = {}
    monkeypatch.setattr(
        processor,
        "send_to_telegram",
        lambda **kwargs: sent.update(kwargs),
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
    runtime_store = StubRuntimeFlagStore(True)
    monkeypatch.setattr(processor, "runtime_flag_store", runtime_store)
    _reset_auto_priority_engine(monkeypatch, runtime_store)
    monkeypatch.setattr(
        processor.auto_priority_gates,
        "evaluate",
        lambda **kwargs: GateDecision(open=True, reasons=()),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=11,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 11, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    telegram_text = telegram_safe(
        processor._build_telegram_text(
            priority="🟡",
            from_email="sender@example.com",
            subject="Subject",
            action_line=llm_result.action_line,
            body_summary=llm_result.body_summary,
            body_text="Body",
            attachment_summary="",
        )
    )

    expected_payload = {
        "chat_id": "chat",
        "priority": "🟡",
        "from_email": "sender@example.com",
        "subject": "Subject",
        "action_line": llm_result.action_line,
        "body_summary": llm_result.body_summary,
        "attachment_summaries": llm_result.attachment_summaries,
        "telegram_text": telegram_text,
        "account_email": "account@example.com",
    }

    assert sent == expected_payload
