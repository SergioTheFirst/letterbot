from __future__ import annotations

import logging
from dataclasses import dataclass

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.system.orchestrator import SystemOrchestrator, SystemPolicyDecision
from mailbot_v26.system_health import OperationalMode


@dataclass
class DummyFlags:
    ENABLE_AUTO_PRIORITY: bool = True
    ENABLE_PREVIEW_ACTIONS: bool = True
    ENABLE_DAILY_DIGEST: bool = True
    ENABLE_WEEKLY_DIGEST: bool = True
    ENABLE_ANOMALY_ALERTS: bool = True
    ENABLE_QUALITY_METRICS: bool = True


def test_orchestrator_full_mode_allows_priority_and_preview() -> None:
    orchestrator = SystemOrchestrator()
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=5,
        correction_rate=0.05,
        engine="priority_v2_auto",
    )
    decision = orchestrator.evaluate(
        system_mode=OperationalMode.FULL,
        metrics=None,
        gates=None,
        runtime_flags=RuntimeFlags(enable_gigachat=True, enable_auto_priority=True),
        feature_flags=DummyFlags(),
        telegram_ok=True,
        has_daily_digest_content=True,
        has_weekly_digest_content=True,
        auto_priority_gate_result=gate_result,
        auto_priority_gate_enabled=True,
        enable_quality_metrics=True,
    )

    assert decision.allow_auto_priority is True
    assert decision.allow_auto_priority_v2 is True
    assert decision.allow_preview is True


def test_orchestrator_degraded_no_llm_disables_llm_and_auto_priority() -> None:
    orchestrator = SystemOrchestrator()
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=5,
        correction_rate=0.05,
        engine="priority_v2_auto",
    )
    decision = orchestrator.evaluate(
        system_mode=OperationalMode.DEGRADED_NO_LLM,
        metrics=None,
        gates=None,
        runtime_flags=RuntimeFlags(enable_gigachat=True, enable_auto_priority=True),
        feature_flags=DummyFlags(),
        telegram_ok=True,
        has_daily_digest_content=True,
        has_weekly_digest_content=True,
        auto_priority_gate_result=gate_result,
        auto_priority_gate_enabled=True,
        enable_quality_metrics=True,
    )

    assert decision.allow_llm is False
    assert decision.allow_auto_priority is False
    assert decision.allow_preview is False


def test_orchestrator_emergency_read_only_blocks_preview() -> None:
    orchestrator = SystemOrchestrator()
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=5,
        correction_rate=0.05,
        engine="priority_v2_auto",
    )
    decision = orchestrator.evaluate(
        system_mode=OperationalMode.EMERGENCY_READ_ONLY,
        metrics=None,
        gates=None,
        runtime_flags=RuntimeFlags(enable_gigachat=True, enable_auto_priority=True),
        feature_flags=DummyFlags(),
        telegram_ok=True,
        has_daily_digest_content=True,
        has_weekly_digest_content=True,
        auto_priority_gate_result=gate_result,
        auto_priority_gate_enabled=True,
        enable_quality_metrics=True,
    )

    assert decision.allow_preview is False
    assert decision.allow_auto_priority is False


def test_orchestrator_fallback_path_logs(caplog) -> None:
    orchestrator = SystemOrchestrator()
    fallback = SystemPolicyDecision(
        mode=OperationalMode.FULL,
        allow_llm=True,
        allow_preview=True,
        allow_auto_priority=True,
        allow_auto_priority_v2=True,
        allow_daily_digest=True,
        allow_weekly_digest=True,
        allow_anomaly_alerts=True,
        auto_priority_gate_result=None,
        reasons=["legacy_fallback"],
    )
    exploding_flags = type(
        "ExplodingFlags",
        (),
        {
            "__getattr__": lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("boom")
            )
        },
    )()

    with caplog.at_level(logging.ERROR):
        decision = orchestrator.evaluate(
            system_mode=OperationalMode.FULL,
            metrics=None,
            gates=None,
            runtime_flags=RuntimeFlags(),
            feature_flags=exploding_flags,  # type: ignore[arg-type]
            telegram_ok=True,
            has_daily_digest_content=True,
            has_weekly_digest_content=True,
            fallback_decision=fallback,
        )

    assert decision == fallback
    assert any(
        '"event":"system_policy_fallback_used"' in record.message
        for record in caplog.records
    )
