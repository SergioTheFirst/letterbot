from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from mailbot_v26.insights.aggregator import Insight
from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.observability import get_logger
from mailbot_v26.priority.auto_gates import PRIORITY_ORDER

logger = get_logger("mailbot")

_HIGH_PRIORITY_THRESHOLD = PRIORITY_ORDER.get("🔴", 2)
_RELATIONSHIP_DROP_THRESHOLD = -0.1
_SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


@dataclass(frozen=True, slots=True)
class AttentionGateInput:
    priority: str
    commitments: Sequence[Commitment]
    deadlines_count: int
    insight_severity: str | None
    attachments_only: bool
    relationship_health_delta: float | None
    email_id: int | None = None


@dataclass(frozen=True, slots=True)
class AttentionGateResult:
    deferred: bool
    reason: str


def max_insight_severity(insights: Sequence[Insight]) -> str | None:
    if not insights:
        return None
    best_rank = 0
    best_label: str | None = None
    for insight in insights:
        label = (insight.severity or "").upper()
        rank = _SEVERITY_RANK.get(label, 0)
        if rank > best_rank:
            best_rank = rank
            best_label = label
    return best_label


def apply_attention_gate(payload: AttentionGateInput) -> AttentionGateResult:
    priority_rank = PRIORITY_ORDER.get(payload.priority, 0)
    commitments_count = len(payload.commitments)
    deadlines_count = payload.deadlines_count
    severity_rank = _SEVERITY_RANK.get((payload.insight_severity or "").upper(), 0)

    if priority_rank >= _HIGH_PRIORITY_THRESHOLD:
        reason = "priority_high"
        logger.info(
            "[ATTENTION-GATE] bypassed",
            email_id=payload.email_id,
            reason=reason,
        )
        return AttentionGateResult(deferred=False, reason=reason)

    if commitments_count > 0 or deadlines_count > 0:
        reason = "commitments_present"
        logger.info(
            "[ATTENTION-GATE] bypassed",
            email_id=payload.email_id,
            reason=reason,
            commitments_count=commitments_count,
            deadlines_count=deadlines_count,
        )
        return AttentionGateResult(deferred=False, reason=reason)

    if severity_rank >= _SEVERITY_RANK["HIGH"]:
        reason = "insight_high"
        logger.info(
            "[ATTENTION-GATE] bypassed",
            email_id=payload.email_id,
            reason=reason,
        )
        return AttentionGateResult(deferred=False, reason=reason)

    if (
        payload.relationship_health_delta is not None
        and payload.relationship_health_delta <= _RELATIONSHIP_DROP_THRESHOLD
    ):
        reason = "relationship_declining"
        logger.info(
            "[ATTENTION-GATE] bypassed",
            email_id=payload.email_id,
            reason=reason,
            relationship_health_delta=payload.relationship_health_delta,
        )
        return AttentionGateResult(deferred=False, reason=reason)

    if (
        payload.attachments_only
        and priority_rank <= PRIORITY_ORDER.get("🔵", 0)
        and severity_rank <= _SEVERITY_RANK.get("LOW", 1)
    ):
        reason = "attachments_only_low_signal"
        logger.info(
            "[ATTENTION-GATE] deferred",
            email_id=payload.email_id,
            reason=reason,
        )
        return AttentionGateResult(deferred=True, reason=reason)

    reason = "default_send"
    logger.info(
        "[ATTENTION-GATE] allowed",
        email_id=payload.email_id,
        reason=reason,
    )
    return AttentionGateResult(deferred=False, reason=reason)


__all__ = [
    "AttentionGateInput",
    "AttentionGateResult",
    "apply_attention_gate",
    "max_insight_severity",
]
