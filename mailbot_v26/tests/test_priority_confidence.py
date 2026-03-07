from __future__ import annotations

import sys
import types
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.priority.auto_engine import AutoPriorityEngine
from mailbot_v26.priority.auto_gates import GateDecision
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.observability.notification_sla import NotificationSLAResult
from mailbot_v26.priority.confidence_engine import PriorityConfidenceEngine
from mailbot_v26.worker.telegram_sender import DeliveryResult


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


def _set_healthy_policy_inputs(monkeypatch) -> None:
    processor.system_health.reset()
    monkeypatch.setattr(
        processor,
        "compute_notification_sla",
        lambda **_kwargs: NotificationSLAResult(
            delivery_rate_24h=1.0,
            delivery_rate_7d=1.0,
            salvage_rate_24h=0.0,
            p50_latency_24h=10.0,
            p90_latency_24h=20.0,
            p99_latency_24h=30.0,
            p50_latency_7d=10.0,
            p90_latency_7d=20.0,
            p99_latency_7d=30.0,
            top_error_reasons_24h=[],
            error_rate_24h=0.0,
            undelivered_24h=0,
            delivered_24h=1,
            total_24h=1,
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
    _set_healthy_policy_inputs(monkeypatch)
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

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
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
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda *_args, **_kwargs: SimpleNamespace(
            entropy=1.0,
            printable_ratio=1.0,
            quality_score=1.0,
            is_usable=True,
            reason="ok",
        ),
    )

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

    assert sent["payload"].priority == llm_result.priority


def test_telegram_payload_unchanged(monkeypatch):
    _set_healthy_policy_inputs(monkeypatch)
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

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
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
            ENABLE_QUALITY_METRICS=True,
        ),
    )
    runtime_store = StubRuntimeFlagStore(True)
    monkeypatch.setattr(processor, "runtime_flag_store", runtime_store)
    _enable_auto_priority_gate(monkeypatch)
    _reset_auto_priority_engine(monkeypatch, runtime_store)
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda *_args, **_kwargs: SimpleNamespace(
            entropy=1.0,
            printable_ratio=1.0,
            quality_score=1.0,
            is_usable=True,
            reason="ok",
        ),
    )
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

    base_text = processor._build_telegram_text(
        priority="🟡",
        from_email="sender@example.com",
        subject="Subject",
        action_line=llm_result.action_line,
        mail_type="",
        body_summary=llm_result.body_summary,
        body_text="Body",
        attachments=[],
    )
    telegram_text = base_text

    payload = sent["payload"]
    assert payload.priority == "🟡"
    assert payload.html_text.startswith(telegram_text)
    assert "Insights" in payload.html_text
    assert payload.metadata["chat_id"] == "chat"
    assert payload.metadata["account_email"] == "account@example.com"
    assert payload.metadata["action_line"] == llm_result.action_line
    assert payload.metadata["body_summary"] == llm_result.body_summary
    assert payload.metadata["attachment_summaries"] == llm_result.attachment_summaries
