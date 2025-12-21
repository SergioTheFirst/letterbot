from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from mailbot_v26.insights.relationship_health import HealthSnapshot
from mailbot_v26.insights.trust_score import TrustScoreCalculator, TrustScoreResult
from mailbot_v26.storage.analytics import KnowledgeAnalytics


@dataclass(frozen=True)
class RelationshipAnomaly:
    entity_id: str
    anomaly_type: str
    severity: str
    detected_at: str
    evidence: dict[str, float | int | str | None]


class RelationshipAnomalyDetector:
    RESPONSE_SPIKE_RATIO = 1.8
    COMMITMENT_WINDOW_DAYS = 30
    SILENCE_WINDOW_DAYS = 14
    SILENCE_BASELINE_WEEKLY = 1.0

    def __init__(
        self,
        analytics: KnowledgeAnalytics,
        trust_score_calculator: TrustScoreCalculator,
    ) -> None:
        self.analytics = analytics
        self.trust_score_calculator = trust_score_calculator

    def detect(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        trust_score_result: TrustScoreResult | None = None,
        health_snapshot: HealthSnapshot | None = None,
    ) -> list[RelationshipAnomaly]:
        anomalies: list[RelationshipAnomaly] = []
        detected_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        response_30d = self._average_response_time(entity_id, 30)
        response_90d = self._average_response_time(entity_id, 90)
        response_ratio = None
        if response_30d is not None and response_90d is not None and response_90d > 0:
            response_ratio = response_30d / response_90d
            if response_ratio > self.RESPONSE_SPIKE_RATIO:
                anomalies.append(
                    RelationshipAnomaly(
                        entity_id=entity_id,
                        anomaly_type="RESPONSE_TIME_SPIKE",
                        severity=self._severity_for_response_spike(response_ratio),
                        detected_at=detected_at,
                        evidence={
                            "response_time_avg_30d": response_30d,
                            "response_time_avg_90d": response_90d,
                            "ratio": round(response_ratio, 2),
                        },
                    )
                )

        commitments_expired_30d = self._commitments_expired(from_email, self.COMMITMENT_WINDOW_DAYS)
        rhs_30d = self._compute_rhs_score(entity_id, from_email, 30, 60)
        rhs_60d = self._compute_rhs_score(entity_id, from_email, 60, 120)
        rhs_delta = None
        if rhs_30d is not None and rhs_60d is not None:
            rhs_delta = rhs_30d - rhs_60d
        if commitments_expired_30d >= 2 or (
            commitments_expired_30d >= 1 and rhs_delta is not None and rhs_delta < 0
        ):
            anomalies.append(
                RelationshipAnomaly(
                    entity_id=entity_id,
                    anomaly_type="COMMITMENT_BREAK_PATTERN",
                    severity=self._severity_for_commitment_break(commitments_expired_30d),
                    detected_at=detected_at,
                    evidence={
                        "commitments_expired_30d": commitments_expired_30d,
                        "rhs_delta": rhs_delta,
                    },
                )
            )

        if rhs_30d is not None and rhs_60d is not None:
            rhs_window_delta = rhs_30d - rhs_60d
            if rhs_window_delta <= -20:
                anomalies.append(
                    RelationshipAnomaly(
                        entity_id=entity_id,
                        anomaly_type="RELATIONSHIP_HEALTH_DROP",
                        severity=self._severity_for_health_drop(rhs_window_delta),
                        detected_at=detected_at,
                        evidence={
                            "rhs_30d": rhs_30d,
                            "rhs_60d": rhs_60d,
                            "rhs_delta": rhs_window_delta,
                        },
                    )
                )

        baseline = self._email_frequency_baseline(entity_id)
        recent_events = self.analytics.interaction_event_count(
            entity_id=entity_id,
            event_type="email_received",
            days=self.SILENCE_WINDOW_DAYS,
        )
        baseline_weekly = baseline * 7.0 if baseline is not None else None
        if (
            baseline_weekly is not None
            and baseline_weekly >= self.SILENCE_BASELINE_WEEKLY
            and recent_events == 0
        ):
            anomalies.append(
                RelationshipAnomaly(
                    entity_id=entity_id,
                    anomaly_type="SILENCE_ANOMALY",
                    severity=self._severity_for_silence(baseline_weekly),
                    detected_at=detected_at,
                    evidence={
                        "baseline_weekly": round(baseline_weekly, 2),
                        "recent_event_count": recent_events,
                    },
                )
            )

        return anomalies

    def _average_response_time(self, entity_id: str, window_days: int) -> float | None:
        values = self.analytics.interaction_event_response_times(
            entity_id=entity_id,
            event_type="response_time",
            days=window_days,
        )
        if not values:
            return None
        return sum(values) / len(values)

    def _commitments_expired(self, from_email: str | None, window_days: int) -> int:
        stats = self.analytics.commitment_stats_by_sender(
            from_email=from_email or "",
            days=window_days,
        )
        return int(stats.get("expired_count", 0) or 0)

    def _compute_rhs_score(
        self,
        entity_id: str,
        from_email: str | None,
        response_window_days: int,
        long_window_days: int,
    ) -> float | None:
        trust_short = self.trust_score_calculator.compute(
            entity_id=entity_id,
            from_email=from_email,
            response_window_days=response_window_days,
            trend_window_days=response_window_days,
        ).snapshot.score
        trust_long = self.trust_score_calculator.compute(
            entity_id=entity_id,
            from_email=from_email,
            response_window_days=long_window_days,
            trend_window_days=long_window_days,
        ).snapshot.score

        response_short = self._average_response_time(entity_id, response_window_days)
        response_long = self._average_response_time(entity_id, long_window_days)
        if (
            trust_short is None
            or trust_long is None
            or response_short is None
            or response_long is None
        ):
            return None

        commitments_expired = self._commitments_expired(from_email, response_window_days)
        commitment_health = 0.0
        if commitments_expired == 0:
            commitment_health = 20.0
        elif commitments_expired >= 2:
            commitment_health = -30.0

        responsiveness_anomaly = 0.0
        if response_long > 0 and response_short > response_long * 1.5:
            responsiveness_anomaly = -20.0

        trend_delta = trust_short - trust_long
        trend_direction = 0.0
        if trend_delta > 0:
            trend_direction = 10.0
        elif trend_delta < 0:
            trend_direction = -10.0

        total = trust_short * 100.0 + commitment_health + responsiveness_anomaly + trend_direction
        return round(max(0.0, min(100.0, total)), 2)

    def _email_frequency_baseline(self, entity_id: str) -> float | None:
        baseline = self.analytics.entity_baseline(entity_id=entity_id, metric="email_frequency")
        value = baseline.get("baseline_value")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _severity_for_response_spike(self, ratio: float) -> str:
        if ratio >= 2.5:
            return "HIGH"
        if ratio >= 2.0:
            return "MEDIUM"
        return "LOW"

    def _severity_for_commitment_break(self, expired_count: int) -> str:
        if expired_count >= 3:
            return "HIGH"
        if expired_count >= 2:
            return "MEDIUM"
        return "LOW"

    def _severity_for_health_drop(self, delta: float) -> str:
        if delta <= -40:
            return "HIGH"
        if delta <= -30:
            return "MEDIUM"
        return "LOW"

    def _severity_for_silence(self, baseline_weekly: float) -> str:
        if baseline_weekly >= 3:
            return "HIGH"
        if baseline_weekly >= 2:
            return "MEDIUM"
        return "LOW"
