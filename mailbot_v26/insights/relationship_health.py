from __future__ import annotations

from dataclasses import dataclass

from mailbot_v26.insights.trust_score import TrustScoreCalculator, TrustScoreResult
from mailbot_v26.storage.analytics import KnowledgeAnalytics


@dataclass(frozen=True)
class HealthSnapshot:
    entity_id: str
    health_score: float | None
    components_breakdown: dict[str, float | int | None]
    data_window_days: int
    reason: str | None


class RelationshipHealthCalculator:
    RESPONSE_TIME_EVENT_TYPE = "response_time"
    RESPONSE_TIME_METADATA_KEY = "response_time_hours"
    RESPONSE_TIME_SHORT_WINDOW_DAYS = 30
    RESPONSE_TIME_LONG_WINDOW_DAYS = 90
    COMMITMENT_WINDOW_DAYS = 30
    TREND_SHORT_WINDOW_DAYS = 30
    TREND_LONG_WINDOW_DAYS = 60
    DATA_WINDOW_DAYS = 90

    def __init__(
        self,
        analytics: KnowledgeAnalytics,
        trust_score_calculator: TrustScoreCalculator,
    ) -> None:
        self.analytics = analytics
        self.trust_score_calculator = trust_score_calculator

    def compute(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        trust_score_result: TrustScoreResult | None = None,
    ) -> HealthSnapshot:
        trust_result = trust_score_result or self.trust_score_calculator.compute(
            entity_id=entity_id,
            from_email=from_email,
        )
        trust_score = trust_result.snapshot.score

        trust_score_30d = self.trust_score_calculator.compute(
            entity_id=entity_id,
            from_email=from_email,
            response_window_days=self.TREND_SHORT_WINDOW_DAYS,
            trend_window_days=self.TREND_SHORT_WINDOW_DAYS,
        ).snapshot.score
        trust_score_60d = self.trust_score_calculator.compute(
            entity_id=entity_id,
            from_email=from_email,
            response_window_days=self.TREND_LONG_WINDOW_DAYS,
            trend_window_days=self.TREND_LONG_WINDOW_DAYS,
        ).snapshot.score

        commitment_stats = self.analytics.commitment_stats_by_sender(
            from_email=from_email or "",
            days=self.COMMITMENT_WINDOW_DAYS,
        )
        commitments_expired_30d = int(commitment_stats.get("expired_count", 0) or 0)

        response_30d = self._average_response_time(
            entity_id,
            self.RESPONSE_TIME_SHORT_WINDOW_DAYS,
        )
        response_90d = self._average_response_time(
            entity_id,
            self.RESPONSE_TIME_LONG_WINDOW_DAYS,
        )

        response_time_delta = (
            response_30d - response_90d
            if response_30d is not None and response_90d is not None
            else None
        )
        trend_delta = (
            trust_score_30d - trust_score_60d
            if trust_score_30d is not None and trust_score_60d is not None
            else None
        )

        components: dict[str, float | int | None] = {
            "trust_score_component": None,
            "commitment_health": None,
            "responsiveness_anomaly": None,
            "trend_direction": None,
            "trust_score": trust_score,
            "commitments_expired_30d": commitments_expired_30d,
            "response_time_avg_30d": response_30d,
            "response_time_avg_90d": response_90d,
            "response_time_delta": response_time_delta,
            "trust_score_30d": trust_score_30d,
            "trust_score_60d": trust_score_60d,
            "trend_delta": trend_delta,
        }

        if (
            trust_score is None
            or trust_score_30d is None
            or trust_score_60d is None
            or response_30d is None
            or response_90d is None
        ):
            return HealthSnapshot(
                entity_id=entity_id,
                health_score=None,
                components_breakdown=components,
                data_window_days=self.DATA_WINDOW_DAYS,
                reason="insufficient_history",
            )

        trust_score_component = trust_score * 100.0
        commitment_health = 0.0
        if commitments_expired_30d == 0:
            commitment_health = 20.0
        elif commitments_expired_30d >= 2:
            commitment_health = -30.0

        responsiveness_anomaly = 0.0
        if response_90d > 0 and response_30d > response_90d * 1.5:
            responsiveness_anomaly = -20.0

        trend_direction = 0.0
        if trend_delta is not None:
            if trend_delta > 0:
                trend_direction = 10.0
            elif trend_delta < 0:
                trend_direction = -10.0

        total = (
            trust_score_component
            + commitment_health
            + responsiveness_anomaly
            + trend_direction
        )
        health_score = max(0.0, min(100.0, total))

        components.update(
            {
                "trust_score_component": trust_score_component,
                "commitment_health": commitment_health,
                "responsiveness_anomaly": responsiveness_anomaly,
                "trend_direction": trend_direction,
            }
        )
        return HealthSnapshot(
            entity_id=entity_id,
            health_score=round(health_score, 2),
            components_breakdown=components,
            data_window_days=self.DATA_WINDOW_DAYS,
            reason=None,
        )

    def _average_response_time(self, entity_id: str, window_days: int) -> float | None:
        values = self.analytics.interaction_event_response_times(
            entity_id=entity_id,
            event_type=self.RESPONSE_TIME_EVENT_TYPE,
            days=window_days,
            metadata_key=self.RESPONSE_TIME_METADATA_KEY,
        )
        if not values:
            return None
        return sum(values) / len(values)
