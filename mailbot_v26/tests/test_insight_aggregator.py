from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mailbot_v26.insights.aggregator import aggregate_insights
from mailbot_v26.insights.relationship_health import HealthSnapshot
from mailbot_v26.insights.temporal_reasoning import TemporalState


def _temporal_state(state_type: str, detected_at: datetime) -> TemporalState:
    return TemporalState(
        entity_id="entity-1",
        state_type=state_type,
        severity="HIGH",
        detected_at=detected_at.isoformat(),
        due_at=None,
        evidence={},
    )


def test_aggregate_insights_from_multiple_temporal_signals() -> None:
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)
    temporal_insights = [
        _temporal_state("commitment_overdue", now - timedelta(days=2)),
        _temporal_state("commitment_overdue", now - timedelta(days=10)),
        _temporal_state("response_overdue", now - timedelta(days=1)),
        _temporal_state("silence_break", now - timedelta(days=15)),
        _temporal_state("escalation_window_open", now - timedelta(days=1)),
    ]
    health_snapshot = HealthSnapshot(
        entity_id="entity-1",
        health_score=45.0,
        components_breakdown={"trend_delta": -0.2},
        data_window_days=90,
        reason=None,
    )

    insights = aggregate_insights(temporal_insights, trust_score=0.4, relationship_health=health_snapshot)

    insight_types = {insight.type for insight in insights}
    assert "⚠️ Reliability Degradation" in insight_types
    assert "⏳ Follow-up Recommended" in insight_types
    assert "🚨 High-Risk Window" in insight_types


def test_aggregate_insights_lowers_severity_with_missing_trust_data() -> None:
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)
    temporal_insights = [
        _temporal_state("commitment_overdue", now - timedelta(days=2)),
        _temporal_state("commitment_overdue", now - timedelta(days=10)),
    ]

    insights = aggregate_insights(temporal_insights, trust_score=None, relationship_health=None)

    assert insights
    reliability = next(insight for insight in insights if insight.type == "⚠️ Reliability Degradation")
    assert reliability.severity == "LOW"


def test_aggregate_insights_handles_missing_health_snapshot() -> None:
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)
    temporal_insights = [
        _temporal_state("commitment_overdue", now - timedelta(days=2)),
        _temporal_state("response_overdue", now - timedelta(days=1)),
    ]

    insights = aggregate_insights(temporal_insights, trust_score=0.5, relationship_health=None)

    high_risk = next(insight for insight in insights if insight.type == "🚨 High-Risk Window")
    assert high_risk.severity == "MEDIUM"
