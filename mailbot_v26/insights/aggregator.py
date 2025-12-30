from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

from mailbot_v26.insights.relationship_health import HealthSnapshot
from mailbot_v26.insights.temporal_reasoning import TemporalState


@dataclass(frozen=True)
class Insight:
    type: str
    severity: str
    explanation: str
    recommendation: str
    metadata: dict[str, str] = field(default_factory=dict)


RELIABILITY_OVERDUE_THRESHOLD = 2
RELIABILITY_WINDOW_DAYS = 30
HIGH_RISK_HEALTH_THRESHOLD = 60.0


def aggregate_insights(
    temporal_insights: Sequence[TemporalState],
    trust_score: float | None,
    relationship_health: HealthSnapshot | None,
) -> list[Insight]:
    reference_time = _latest_detected_at(temporal_insights)
    overdue_count = _count_overdue_commitments(temporal_insights, reference_time)
    trust_trend = _trust_trend(relationship_health)
    health_score = _health_score(relationship_health)

    insights: list[Insight] = []

    if overdue_count >= RELIABILITY_OVERDUE_THRESHOLD:
        severity = "MEDIUM"
        explanation = "Контрагент начал хуже выполнять обещания."
        if trust_trend == "down":
            severity = "HIGH"
            explanation = "Контрагент начал хуже выполнять обещания, доверие снижается."
        elif trust_score is None:
            severity = "LOW"
            explanation = "Фиксируются просрочки, но данных о доверии пока недостаточно."
        insights.append(
            Insight(
                type="Reliability Degradation",
                severity=severity,
                explanation=explanation,
                recommendation="Сверьте текущие договорённости и подготовьте мягкий follow-up.",
            )
        )

    if _has_state(temporal_insights, {"silence_break", "silence_anomaly"}) and _has_state(
        temporal_insights,
        {"escalation_window_open", "escalation_window"},
    ):
        insights.append(
            Insight(
                type="Follow-up Recommended",
                severity="MEDIUM",
                explanation="Долгая пауза совпала с окном эскалации.",
                recommendation="Сейчас уместно напомнить и уточнить статус.",
            )
        )

    if _has_state(temporal_insights, {"commitment_overdue"}) and _has_state(
        temporal_insights,
        {"response_overdue", "delayed_response"},
    ):
        if health_score is None:
            severity = "MEDIUM"
            explanation = "Есть просрочки и задержки, но данных о здоровье отношений мало."
        elif health_score < HIGH_RISK_HEALTH_THRESHOLD:
            severity = "HIGH"
            explanation = "Просрочки и задержки ответов усиливают риск срыва договорённости."
        else:
            severity = None
            explanation = ""
        if severity:
            insights.append(
                Insight(
                    type="High-Risk Window",
                    severity=severity,
                    explanation=explanation,
                    recommendation="Держите ситуацию под контролем и согласуйте следующий шаг.",
                )
            )

    return insights


def append_narrative_insight(
    insights: list[Insight],
    *,
    fact: str,
    pattern: str | None,
    action: str | None,
) -> None:
    metadata: dict[str, str] = {"fact": fact}
    if pattern:
        metadata["pattern"] = pattern
    if action:
        metadata["action"] = action
    insights.append(
        Insight(
            type="Narrative",
            severity="INFO",
            explanation=fact,
            recommendation=action or "",
            metadata=metadata,
        )
    )


def _latest_detected_at(temporal_insights: Sequence[TemporalState]) -> datetime | None:
    timestamps = [
        timestamp
        for insight in temporal_insights
        if (timestamp := _parse_iso(insight.detected_at)) is not None
    ]
    return max(timestamps) if timestamps else None


def _count_overdue_commitments(
    temporal_insights: Sequence[TemporalState],
    reference_time: datetime | None,
) -> int:
    if reference_time is None:
        return sum(
            1
            for insight in temporal_insights
            if insight.state_type == "commitment_overdue"
        )
    window_start = reference_time - timedelta(days=RELIABILITY_WINDOW_DAYS)
    return sum(
        1
        for insight in temporal_insights
        if insight.state_type == "commitment_overdue"
        and (timestamp := _parse_iso(insight.detected_at)) is not None
        and window_start <= timestamp <= reference_time
    )


def _trust_trend(relationship_health: HealthSnapshot | None) -> str | None:
    if relationship_health is None:
        return None
    trend_delta = relationship_health.components_breakdown.get("trend_delta")
    if trend_delta is None:
        return None
    if trend_delta < 0:
        return "down"
    if trend_delta > 0:
        return "up"
    return "flat"


def _health_score(relationship_health: HealthSnapshot | None) -> float | None:
    if relationship_health is None:
        return None
    return relationship_health.health_score


def _has_state(temporal_insights: Iterable[TemporalState], types: set[str]) -> bool:
    return any(insight.state_type in types for insight in temporal_insights)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


__all__ = ["Insight", "aggregate_insights", "append_narrative_insight"]
