from __future__ import annotations

from dataclasses import dataclass
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics


PRIORITY_ORDER = {"🔵": 0, "🟡": 1, "🔴": 2}
logger = get_logger("mailbot")


@dataclass(frozen=True)
class GateDecision:
    open: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CircuitBreakerStatus:
    tripped: bool
    reason: str | None
    reject_rate: float | None
    confidence_p50: float | None


class AutoPriorityGates:
    MIN_SHADOW_ACCURACY_30D = 0.85
    MAX_REJECT_RATE_30D = 0.15
    MIN_CONFIDENCE = 0.85
    MIN_SAMPLE_SIZE = 100
    MAX_PRIORITY_DELTA = 1

    def __init__(self, analytics: KnowledgeAnalytics) -> None:
        self._analytics = analytics

    def evaluate(
        self,
        *,
        llm_priority: str,
        shadow_priority: str,
        confidence_score: float | None,
    ) -> GateDecision:
        reasons: list[str] = []

        score = confidence_score or 0.0
        if score < self.MIN_CONFIDENCE:
            reasons.append("min_confidence")

        delta = PRIORITY_ORDER.get(shadow_priority, 0) - PRIORITY_ORDER.get(
            llm_priority, 0
        )
        if delta <= 0:
            reasons.append("shadow_not_higher")
        if delta > self.MAX_PRIORITY_DELTA:
            reasons.append("priority_delta")

        try:
            accuracy_stats = self._analytics.shadow_accuracy(days=30)
        except Exception:
            accuracy_stats = {"total": 0, "accuracy": 0.0}
            reasons.append("shadow_accuracy_error")

        total_samples = int(accuracy_stats.get("total", 0) or 0)
        accuracy = float(accuracy_stats.get("accuracy", 0.0) or 0.0)
        if total_samples < self.MIN_SAMPLE_SIZE:
            reasons.append("sample_size")
        if accuracy < self.MIN_SHADOW_ACCURACY_30D:
            reasons.append("shadow_accuracy")

        try:
            reject_stats = self._analytics.auto_priority_reject_rate(days=30)
        except Exception:
            reject_stats = {"total": 0, "reject_rate": 1.0}
            reasons.append("reject_rate_error")

        reject_total = int(reject_stats.get("total", 0) or 0)
        reject_rate = float(reject_stats.get("reject_rate", 1.0) or 1.0)
        if reject_total < self.MIN_SAMPLE_SIZE:
            reasons.append("reject_sample_size")
        if reject_rate > self.MAX_REJECT_RATE_30D:
            reasons.append("reject_rate")

        decision = GateDecision(open=not reasons, reasons=tuple(reasons))
        logger.info(
            "AUTO-PRIORITY-GATE",
            open=decision.open,
            reasons=decision.reasons,
            llm_priority=llm_priority,
            shadow_priority=shadow_priority,
            confidence=score,
            delta=delta,
            accuracy_30d=accuracy,
            accuracy_total=total_samples,
            reject_rate_30d=reject_rate,
            reject_total=reject_total,
        )
        return decision


class AutoPriorityCircuitBreaker:
    MAX_REJECT_RATE_24H = 0.25

    def __init__(self, analytics: KnowledgeAnalytics) -> None:
        self._analytics = analytics

    def check(self) -> CircuitBreakerStatus:
        reasons: list[str] = []
        reject_rate: float | None = None

        try:
            reject_stats = self._analytics.auto_priority_reject_rate(hours=24)
            reject_rate = float(reject_stats.get("reject_rate", 0.0) or 0.0)
            if int(reject_stats.get("total", 0) or 0) > 0 and reject_rate > self.MAX_REJECT_RATE_24H:
                reasons.append("reject_rate_24h")
        except Exception:
            pass

        return CircuitBreakerStatus(
            tripped=bool(reasons),
            reason=",".join(reasons) if reasons else None,
            reject_rate=reject_rate,
            confidence_p50=None,
        )


__all__ = [
    "AutoPriorityCircuitBreaker",
    "AutoPriorityGates",
    "CircuitBreakerStatus",
    "GateDecision",
]
