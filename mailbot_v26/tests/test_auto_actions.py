import sqlite3
import sys
import types
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.actions import AutoActionEngine
from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.priority.auto_engine import AutoPriorityEngine
from mailbot_v26.priority.auto_gates import GateDecision
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.storage.knowledge_db import KnowledgeDB

# Stub missing pipeline dependencies before importing the processor
if "mailbot_v26.pipeline.stage_llm" not in sys.modules:
    stage_llm = types.ModuleType("mailbot_v26.pipeline.stage_llm")
    stage_llm.run_llm_stage = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_llm"] = stage_llm

if "mailbot_v26.pipeline.stage_telegram" not in sys.modules:
    stage_telegram = types.ModuleType("mailbot_v26.pipeline.stage_telegram")
    stage_telegram.enqueue_tg = lambda **kwargs: None
    stage_telegram.send_preview_to_telegram = lambda **kwargs: None
    stage_telegram.send_system_notice = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_telegram"] = stage_telegram

from mailbot_v26.pipeline import processor


class StubRuntimeFlagStore:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def get_flags(self, *, force: bool = False):
        return (
            RuntimeFlags(enable_gigachat=False, enable_auto_priority=self.enabled),
            False,
        )

    def set_enable_auto_priority(self, enabled: bool) -> None:
        self.enabled = enabled


def _llm_result(
    priority: str = "🔴", action_line: str = "Позвонить клиенту"
) -> SimpleNamespace:
    return SimpleNamespace(
        priority=priority,
        action_line=action_line,
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


def _enable_auto_priority_gate(monkeypatch) -> None:
    monkeypatch.setattr(
        processor,
        "auto_priority_gate_config",
        AutoPriorityGateConfig(
            enabled=True,
            window_days=30,
            min_samples=30,
            max_correction_rate=0.15,
            cooldown_hours=24,
        ),
    )
    monkeypatch.setattr(
        processor.auto_priority_quality_gate,
        "evaluate",
        lambda **kwargs: GateResult(
            passed=True,
            reason="ok",
            window_days=30,
            samples=30,
            corrections=1,
            correction_rate=0.03,
            engine=kwargs.get("engine", "priority_v2_auto"),
        ),
    )


def test_confidence_below_threshold_returns_none():
    engine = AutoActionEngine(confidence_threshold=0.75)
    result = engine.propose(
        llm_action_line="Ответить на письмо",
        shadow_action="Связаться с клиентом",
        priority="🔴",
        confidence=0.7,
    )

    assert result is None


def test_priority_not_red_returns_none():
    engine = AutoActionEngine(confidence_threshold=0.75)
    result = engine.propose(
        llm_action_line="Ответить на письмо",
        shadow_action="Связаться с клиентом",
        priority="🟡",
        confidence=0.9,
    )

    assert result is None


def test_shadow_action_matches_llm_returns_none():
    engine = AutoActionEngine(confidence_threshold=0.75)
    result = engine.propose(
        llm_action_line="Связаться с клиентом",
        shadow_action="Связаться с клиентом",
        priority="🔴",
        confidence=0.9,
    )

    assert result is None


def test_valid_proposed_action():
    engine = AutoActionEngine(confidence_threshold=0.75)
    result = engine.propose(
        llm_action_line="Ответить на письмо",
        shadow_action="Оплатить счёт до 25 сентября",
        priority="🔴",
        confidence=0.81,
    )

    assert result == {
        "type": "PAYMENT",
        "text": "Оплатить счёт до 25 сентября",
        "source": "shadow",
        "confidence": 0.81,
    }


def test_flag_off_skips_auto_actions(monkeypatch):
    llm_result = _llm_result()
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🔴", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [("Позвонить клиенту", "reason")],
    )

    monkeypatch.setattr(
        processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None)
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
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(False))

    called = False

    def _fail_if_called(**kwargs):  # pragma: no cover - defensive
        nonlocal called
        called = True

    monkeypatch.setattr(processor.auto_action_engine, "propose", _fail_if_called)

    payload_store: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        payload_store["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

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

    assert called is False
    payload = payload_store["payload"]
    assert payload.metadata["chat_id"] == "chat"
    assert payload.metadata["account_email"] == "account@example.com"
    assert payload.metadata["action_line"] == "Позвонить клиенту"
    assert payload.metadata["body_summary"] == "Body summary"
    assert payload.metadata["attachment_summaries"] == [
        {"filename": "file.txt", "summary": "summary"}
    ]


def test_valid_auto_action_persisted(monkeypatch, tmp_path):
    db_path = tmp_path / "auto_action.sqlite"
    llm_result = _llm_result(priority="🟡", action_line="Ответить на письмо")

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.priority_confidence_engine,
        "score",
        lambda **kwargs: 0.9,
    )
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🔴", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [
            ("Оплатить счёт до 25 сентября", "analytics")
        ],
    )

    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=True,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=True,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=True,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_QUALITY_METRICS=True,
        ),
    )
    runtime_store = StubRuntimeFlagStore(True)
    monkeypatch.setattr(processor, "runtime_flag_store", runtime_store)
    _enable_auto_priority_gate(monkeypatch)
    _reset_auto_priority_engine(monkeypatch, runtime_store)
    monkeypatch.setattr(
        processor.auto_priority_gates,
        "evaluate",
        lambda **kwargs: GateDecision(open=True, reasons=()),
    )
    monkeypatch.setattr(
        processor,
        "auto_action_engine",
        AutoActionEngine(confidence_threshold=0.75),
    )
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=11,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 2, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("""
            SELECT
                priority,
                original_priority,
                proposed_action_type,
                proposed_action_text,
                proposed_action_confidence
            FROM emails ORDER BY id DESC LIMIT 1
            """).fetchone()

    assert row == (
        "🔴",
        "🟡",
        "PAYMENT",
        "Оплатить счёт до 25 сентября",
        0.9,
    )


def test_telegram_payload_not_changed(monkeypatch):
    llm_result = _llm_result()
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🔴", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [("Проверить оплату", "reason")],
    )

    baseline_payload: dict[str, object] = {}
    monkeypatch.setattr(
        processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None)
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

    def _enqueue_baseline(*, email_id: int, payload) -> None:
        baseline_payload["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_baseline)

    processor.process_message(
        account_email="account@example.com",
        message_id=12,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 3, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    with_auto_actions: dict[str, object] = {}
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=True,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(processor, "runtime_flag_store", StubRuntimeFlagStore(False))
    monkeypatch.setattr(
        processor.auto_action_engine,
        "propose",
        lambda **kwargs: {
            "type": "REVIEW",
            "text": "Проверить оплату",
            "source": "shadow",
            "confidence": 0.8,
        },
    )

    def _enqueue_with_auto(*, email_id: int, payload) -> None:
        with_auto_actions["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_with_auto)

    processor.process_message(
        account_email="account@example.com",
        message_id=13,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 4, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    baseline = baseline_payload["payload"]
    with_auto = with_auto_actions["payload"]
    assert (baseline.html_text, baseline.priority, baseline.metadata) == (
        with_auto.html_text,
        with_auto.priority,
        with_auto.metadata,
    )
