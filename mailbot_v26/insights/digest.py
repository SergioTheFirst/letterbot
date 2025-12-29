from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from mailbot_v26.insights.aggregator import Insight
from mailbot_v26.insights.relationship_health import HealthSnapshot

STATUS_STABLE = "Stable"
STATUS_ATTENTION = "Attention Needed"
STATUS_RISK = "Risk Zone"

SLIGHT_TREND_DROP_THRESHOLD = -0.05
SIGNIFICANT_TREND_DROP_THRESHOLD = -0.15
HEALTH_DROP_THRESHOLD = 60.0
TRUST_STABLE_SCORE = 0.6
TRUST_RISK_SCORE = 0.4


@dataclass(frozen=True)
class InsightDigest:
    headline: str
    status_label: str
    short_explanation: str


def build_insight_digest(
    insights: Sequence[Insight],
    trust_score: float | None,
    relationship_health: HealthSnapshot | None,
) -> InsightDigest:
    serious_count = sum(1 for insight in insights if _is_serious(insight))
    weak_count = sum(1 for insight in insights if _is_weak(insight))
    has_insights = bool(insights)

    trend_delta = _trend_delta(relationship_health)
    health_score = relationship_health.health_score if relationship_health else None

    trust_drop, trust_slight_drop, trust_stable = _trust_status(
        trust_score, trend_delta
    )
    health_drop = health_score is not None and health_score < HEALTH_DROP_THRESHOLD

    if not has_insights and trust_score is None and health_score is None:
        return _fallback_digest()

    if serious_count >= 2 and (trust_drop or health_drop):
        status_label = STATUS_RISK
        headline = "Контакт в зоне риска."
    elif 1 <= weak_count <= 2 and trust_slight_drop:
        status_label = STATUS_ATTENTION
        headline = "Нужна лёгкая проверка."
    elif not has_insights and trust_stable and not health_drop:
        status_label = STATUS_STABLE
        headline = "Ситуация стабильна."
    else:
        status_label = STATUS_ATTENTION
        headline = "Ситуация требует внимания."

    short_explanation = _build_explanation(
        serious_count=serious_count,
        weak_count=weak_count,
        has_insights=has_insights,
        trust_score=trust_score,
        trend_delta=trend_delta,
        health_score=health_score,
    )

    return InsightDigest(
        headline=headline,
        status_label=status_label,
        short_explanation=short_explanation,
    )


def _fallback_digest() -> InsightDigest:
    return InsightDigest(
        headline="Недостаточно данных для оценки.",
        status_label=STATUS_ATTENTION,
        short_explanation="Пока нет достаточного объёма сигналов, чтобы оценить состояние.",
    )


def _is_serious(insight: Insight) -> bool:
    return insight.severity.upper() == "HIGH"


def _is_weak(insight: Insight) -> bool:
    return insight.severity.upper() in {"LOW", "MEDIUM"}


def _trend_delta(relationship_health: HealthSnapshot | None) -> float | None:
    if relationship_health is None:
        return None
    value = relationship_health.components_breakdown.get("trend_delta")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _trust_status(
    trust_score: float | None,
    trend_delta: float | None,
) -> tuple[bool, bool, bool]:
    if trend_delta is not None:
        trust_drop = trend_delta <= SIGNIFICANT_TREND_DROP_THRESHOLD
        trust_slight_drop = SIGNIFICANT_TREND_DROP_THRESHOLD < trend_delta < 0
        trust_stable = trend_delta >= 0
        return trust_drop, trust_slight_drop, trust_stable

    if trust_score is None:
        return False, False, False

    trust_drop = trust_score <= TRUST_RISK_SCORE
    trust_stable = trust_score >= TRUST_STABLE_SCORE
    trust_slight_drop = not trust_drop and not trust_stable
    return trust_drop, trust_slight_drop, trust_stable


def _build_explanation(
    *,
    serious_count: int,
    weak_count: int,
    has_insights: bool,
    trust_score: float | None,
    trend_delta: float | None,
    health_score: float | None,
) -> str:
    lines: list[str] = []
    if has_insights:
        lines.append(f"Сильные сигналы: {serious_count}, слабые: {weak_count}.")
    else:
        lines.append("Активных сигналов не найдено.")

    trust_line = _trust_line(trust_score, trend_delta)
    if trust_line:
        lines.append(trust_line)

    if health_score is not None:
        lines.append(f"Здоровье отношений: {health_score:.0f}/100.")

    return "\n".join(lines)


def _trust_line(trust_score: float | None, trend_delta: float | None) -> str | None:
    if trend_delta is not None:
        if trend_delta <= SIGNIFICANT_TREND_DROP_THRESHOLD:
            return "Доверие заметно снижается."
        if trend_delta < 0:
            return "Доверие слегка снижается."
        if trend_delta > 0:
            return "Доверие растёт."
        return "Доверие стабильно."

    if trust_score is None:
        return "Данных о доверии пока недостаточно."
    if trust_score <= TRUST_RISK_SCORE:
        return "Доверие заметно снижено."
    if trust_score >= TRUST_STABLE_SCORE:
        return "Доверие стабильно."
    return "Доверие слегка снижается."


__all__ = ["InsightDigest", "build_insight_digest"]
