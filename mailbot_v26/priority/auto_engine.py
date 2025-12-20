from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability import get_logger
from mailbot_v26.priority.auto_gates import (
    AutoPriorityCircuitBreaker,
    AutoPriorityGates,
    GateDecision,
    PRIORITY_ORDER,
)
from mailbot_v26.system_health import OperationalMode, SystemHealth

logger = get_logger("mailbot")


@dataclass(frozen=True)
class AutoPriorityOutcome:
    final_priority: str
    original_priority: str | None
    priority_reason: str | None
    confidence_score: float | None
    confidence_decision: str | None
    gate_decision: GateDecision | None
    applied: bool
    skipped_reason: str | None


class AutoPriorityEngine:
    def __init__(
        self,
        gates: AutoPriorityGates,
        breaker: AutoPriorityCircuitBreaker,
        runtime_flag_store: RuntimeFlagStore,
        system_health: SystemHealth,
        *,
        enabled_flag: Callable[[], bool],
    ) -> None:
        self._gates = gates
        self._breaker = breaker
        self._runtime_flag_store = runtime_flag_store
        self._system_health = system_health
        self._enabled_flag = enabled_flag

    def evaluate(
        self,
        *,
        llm_priority: str,
        shadow_priority: str,
        shadow_reason: str | None,
        confidence_score: float | None,
    ) -> AutoPriorityOutcome:
        original_priority: str | None = None
        priority_reason: str | None = None
        confidence_decision: str | None = None
        gate_decision: GateDecision | None = None
        skipped_reason: str | None = None

        runtime_flags, _ = self._runtime_flag_store.get_flags()
        auto_priority_enabled = bool(self._enabled_flag()) and runtime_flags.enable_auto_priority

        if not auto_priority_enabled:
            skipped_reason = "auto_priority_disabled"
            logger.info("auto_priority_skipped", reason=skipped_reason)
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=None,
                gate_decision=None,
                applied=False,
                skipped_reason=skipped_reason,
            )

        if self._system_health.mode != OperationalMode.FULL:
            skipped_reason = f"system_mode_{self._system_health.mode.value}"
            logger.info("auto_priority_skipped", reason=skipped_reason)
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=None,
                gate_decision=None,
                applied=False,
                skipped_reason=skipped_reason,
            )

        breaker_status = self._breaker.check()
        if breaker_status.tripped:
            self._runtime_flag_store.set_enable_auto_priority(False)
            logger.warning(
                "auto_priority_disabled",
                marker="AUTO-PRIORITY-DISABLED",
                reason=breaker_status.reason or "reject_rate",
                reject_rate=breaker_status.reject_rate,
            )
            skipped_reason = "circuit_breaker"
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=None,
                gate_decision=None,
                applied=False,
                skipped_reason=skipped_reason,
            )

        delta = PRIORITY_ORDER.get(shadow_priority, 0) - PRIORITY_ORDER.get(llm_priority, 0)
        if delta <= 0:
            skipped_reason = "shadow_not_higher"
            logger.info("auto_priority_skipped", reason=skipped_reason)
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=None,
                gate_decision=None,
                applied=False,
                skipped_reason=skipped_reason,
            )

        gate_decision = self._gates.evaluate(
            llm_priority=llm_priority,
            shadow_priority=shadow_priority,
            confidence_score=confidence_score,
        )
        if not gate_decision.open:
            skipped_reason = "gate_failed"
            logger.info(
                "auto_priority_gate_failed",
                reasons=gate_decision.reasons,
                llm_priority=llm_priority,
                shadow_priority=shadow_priority,
            )
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=None,
                gate_decision=gate_decision,
                applied=False,
                skipped_reason=skipped_reason,
            )

        if confidence_score is None or confidence_score < AutoPriorityGates.MIN_CONFIDENCE:
            skipped_reason = "min_confidence"
            confidence_decision = "SKIPPED"
            logger.info(
                "auto_priority_skipped",
                reason=skipped_reason,
                confidence=confidence_score or 0.0,
            )
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=confidence_decision,
                gate_decision=gate_decision,
                applied=False,
                skipped_reason=skipped_reason,
            )

        if delta > AutoPriorityGates.MAX_PRIORITY_DELTA:
            skipped_reason = "priority_delta"
            logger.info("auto_priority_skipped", reason=skipped_reason, delta=delta)
            return AutoPriorityOutcome(
                final_priority=llm_priority,
                original_priority=None,
                priority_reason=None,
                confidence_score=confidence_score,
                confidence_decision=None,
                gate_decision=gate_decision,
                applied=False,
                skipped_reason=skipped_reason,
            )

        original_priority = llm_priority
        priority_reason = shadow_reason or "Auto-priority escalation"
        confidence_decision = "APPLIED"
        logger.info(
            "auto_priority_applied",
            llm_priority=original_priority,
            shadow_priority=shadow_priority,
            reason=priority_reason,
            confidence=confidence_score or 0.0,
        )

        return AutoPriorityOutcome(
            final_priority=shadow_priority,
            original_priority=original_priority,
            priority_reason=priority_reason,
            confidence_score=confidence_score,
            confidence_decision=confidence_decision,
            gate_decision=gate_decision,
            applied=True,
            skipped_reason=None,
        )


__all__ = ["AutoPriorityEngine", "AutoPriorityOutcome"]
