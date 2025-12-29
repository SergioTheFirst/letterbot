from __future__ import annotations

from mailbot_v26.insights.aggregator import Insight
from mailbot_v26.insights.digest import (
    STATUS_ATTENTION,
    STATUS_RISK,
    STATUS_STABLE,
    build_insight_digest,
)
from mailbot_v26.insights.relationship_health import HealthSnapshot


def _health_snapshot(health_score: float, trend_delta: float) -> HealthSnapshot:
    return HealthSnapshot(
        entity_id="entity-1",
        health_score=health_score,
        components_breakdown={"trend_delta": trend_delta},
        data_window_days=90,
        reason=None,
    )


def test_digest_stable_when_no_insights_and_trust_up() -> None:
    digest = build_insight_digest(
        insights=[],
        trust_score=0.7,
        relationship_health=_health_snapshot(80.0, 0.1),
    )

    assert digest.status_label == STATUS_STABLE
    assert "стабильна" in digest.headline.lower()


def test_digest_attention_for_weak_insights_and_slight_drop() -> None:
    digest = build_insight_digest(
        insights=[
            Insight(
                type="Reliability Degradation",
                severity="MEDIUM",
                explanation="",
                recommendation="",
            )
        ],
        trust_score=0.55,
        relationship_health=_health_snapshot(72.0, -0.03),
    )

    assert digest.status_label == STATUS_ATTENTION
    assert digest.headline


def test_digest_risk_zone_for_serious_signals_and_health_drop() -> None:
    digest = build_insight_digest(
        insights=[
            Insight(
                type="High-Risk Window",
                severity="HIGH",
                explanation="",
                recommendation="",
            ),
            Insight(
                type="Reliability Degradation",
                severity="HIGH",
                explanation="",
                recommendation="",
            ),
        ],
        trust_score=0.35,
        relationship_health=_health_snapshot(50.0, -0.2),
    )

    assert digest.status_label == STATUS_RISK
    assert "зоне риска" in digest.headline.lower()


def test_digest_fallback_for_missing_data_is_deterministic() -> None:
    first = build_insight_digest(
        insights=[],
        trust_score=None,
        relationship_health=None,
    )
    second = build_insight_digest(
        insights=[],
        trust_score=None,
        relationship_health=None,
    )

    assert first.status_label == STATUS_ATTENTION
    assert first == second
