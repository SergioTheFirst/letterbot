from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from mailbot_v26.config.delivery_policy import DeliveryPolicyConfig


class DeliveryMode(str, Enum):
    IMMEDIATE = "IMMEDIATE"
    BATCH_TODAY = "BATCH_TODAY"
    DEFER_TO_MORNING = "DEFER_TO_MORNING"
    SILENT_LOG = "SILENT_LOG"


@dataclass(frozen=True, slots=True)
class DeliveryDecision:
    mode: DeliveryMode
    reason_codes: list[str]
    thresholds_used: dict[str, int]
    attention_debt: int


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    now_local: datetime
    immediate_sent_last_hour: int
    max_immediate_per_hour: int


@dataclass(frozen=True, slots=True)
class DeliveryScores:
    value: int
    risk: int
    confidence: int


_PRIORITY_VALUE = {
    "🔴": 90,
    "🟠": 70,
    "🟡": 50,
    "🔵": 30,
}

_SEVERITY_RISK = {"LOW": 10, "MEDIUM": 30, "HIGH": 60}


def compute_attention_debt(
    *, immediate_sent_last_hour: int, max_immediate_per_hour: int
) -> int:
    if max_immediate_per_hour <= 0:
        return 0
    ratio = immediate_sent_last_hour / max_immediate_per_hour
    return min(100, max(0, int(ratio * 100)))


def score_email(
    *,
    priority: str,
    commitments_count: int,
    deadlines_count: int,
    insight_severity: str | None,
    relationship_health_delta: float | None,
) -> DeliveryScores:
    value = _PRIORITY_VALUE.get(priority, 30)
    risk = 0
    confidence = 60

    if commitments_count > 0:
        risk += 30
        value += 10
    if deadlines_count > 0:
        risk += 40
        value += 10

    severity = (insight_severity or "").upper()
    risk += _SEVERITY_RISK.get(severity, 0)
    if severity:
        confidence += 10

    if relationship_health_delta is not None and relationship_health_delta < 0:
        risk += 10

    return DeliveryScores(
        value=min(100, max(0, value)),
        risk=min(100, max(0, risk)),
        confidence=min(100, max(0, confidence)),
    )


def decide_delivery(
    *,
    scores: DeliveryScores,
    context: DeliveryContext,
    policy: DeliveryPolicyConfig,
    attention_gate_deferred: bool = False,
) -> DeliveryDecision:
    reason_codes: list[str] = []
    thresholds_used = {
        "immediate_value_threshold": policy.immediate_value_threshold,
        "batch_value_threshold": policy.batch_value_threshold,
        "critical_risk_threshold": policy.critical_risk_threshold,
        "max_immediate_per_hour": policy.max_immediate_per_hour,
    }
    attention_debt = compute_attention_debt(
        immediate_sent_last_hour=context.immediate_sent_last_hour,
        max_immediate_per_hour=context.max_immediate_per_hour,
    )

    if attention_gate_deferred:
        reason_codes.append("attention_gate")
        return DeliveryDecision(
            mode=DeliveryMode.BATCH_TODAY,
            reason_codes=reason_codes,
            thresholds_used=thresholds_used,
            attention_debt=attention_debt,
        )

    if scores.risk >= policy.critical_risk_threshold:
        reason_codes.append("critical_risk")
        return DeliveryDecision(
            mode=DeliveryMode.IMMEDIATE,
            reason_codes=reason_codes,
            thresholds_used=thresholds_used,
            attention_debt=attention_debt,
        )

    if context.max_immediate_per_hour > 0 and (
        context.immediate_sent_last_hour >= context.max_immediate_per_hour
    ):
        reason_codes.append("attention_debt")
        return DeliveryDecision(
            mode=DeliveryMode.BATCH_TODAY,
            reason_codes=reason_codes,
            thresholds_used=thresholds_used,
            attention_debt=attention_debt,
        )

    if scores.value >= policy.immediate_value_threshold:
        reason_codes.append("high_value")
        return DeliveryDecision(
            mode=DeliveryMode.IMMEDIATE,
            reason_codes=reason_codes,
            thresholds_used=thresholds_used,
            attention_debt=attention_debt,
        )

    if scores.value >= policy.batch_value_threshold:
        reason_codes.append("default_batch")
        return DeliveryDecision(
            mode=DeliveryMode.BATCH_TODAY,
            reason_codes=reason_codes,
            thresholds_used=thresholds_used,
            attention_debt=attention_debt,
        )

    reason_codes.append("low_signal")
    return DeliveryDecision(
        mode=DeliveryMode.SILENT_LOG,
        reason_codes=reason_codes,
        thresholds_used=thresholds_used,
        attention_debt=attention_debt,
    )


def packaging_directives(*, confidence: int, evidence_flags: dict[str, bool]) -> dict[str, bool]:
    show_uncertainty = confidence < 50
    show_consequences = bool(evidence_flags.get("has_evidence"))
    return {
        "show_uncertainty": show_uncertainty,
        "show_consequences": show_consequences,
    }


__all__ = [
    "DeliveryMode",
    "DeliveryDecision",
    "DeliveryContext",
    "DeliveryScores",
    "compute_attention_debt",
    "score_email",
    "decide_delivery",
    "packaging_directives",
]
