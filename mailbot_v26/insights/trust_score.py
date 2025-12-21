from __future__ import annotations

from dataclasses import dataclass
from statistics import pstdev

from mailbot_v26.storage.analytics import KnowledgeAnalytics


@dataclass(frozen=True)
class TrustScoreComponents:
    commitment_reliability: float | None
    response_consistency: float | None
    trend: float | None


@dataclass(frozen=True)
class TrustSnapshot:
    entity_id: str
    score: float | None
    reason: str | None
    sample_size: int


@dataclass(frozen=True)
class TrustScoreResult:
    snapshot: TrustSnapshot
    components: TrustScoreComponents
    data_window_days: int


class TrustScoreCalculator:
    RESPONSE_WINDOW_DAYS = 60
    TREND_WINDOW_DAYS = 30
    MAX_RESPONSE_STDDEV_HOURS = 72.0
    MIN_RESPONSE_SAMPLES = 2
    MIN_TREND_SAMPLES = 2

    def __init__(self, analytics: KnowledgeAnalytics) -> None:
        self.analytics = analytics

    def compute(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        response_window_days: int | None = None,
        trend_window_days: int | None = None,
    ) -> TrustScoreResult:
        response_window = response_window_days or self.RESPONSE_WINDOW_DAYS
        trend_window = trend_window_days or self.TREND_WINDOW_DAYS
        commitment_score, commitment_samples = self._commitment_reliability(
            from_email,
            days=trend_window,
        )
        response_score, response_samples = self._response_consistency(
            entity_id,
            days=response_window,
        )
        trend_score, trend_samples = self._trend_direction(
            entity_id,
            days=trend_window,
        )

        components = TrustScoreComponents(
            commitment_reliability=commitment_score,
            response_consistency=response_score,
            trend=trend_score,
        )
        sample_size = commitment_samples + response_samples + trend_samples

        if (
            commitment_score is None
            or response_score is None
            or trend_score is None
        ):
            snapshot = TrustSnapshot(
                entity_id=entity_id,
                score=None,
                reason="insufficient_data",
                sample_size=sample_size,
            )
            return TrustScoreResult(
                snapshot=snapshot,
                components=components,
                data_window_days=response_window,
            )

        trust_score = (
            0.5 * commitment_score
            + 0.3 * response_score
            + 0.2 * trend_score
        )
        trust_score = max(0.0, min(1.0, trust_score))

        snapshot = TrustSnapshot(
            entity_id=entity_id,
            score=round(trust_score, 4),
            reason=None,
            sample_size=sample_size,
        )
        return TrustScoreResult(
            snapshot=snapshot,
            components=components,
            data_window_days=response_window,
        )

    def _commitment_reliability(
        self,
        from_email: str | None,
        *,
        days: int,
    ) -> tuple[float | None, int]:
        if not from_email:
            return None, 0
        stats = self.analytics.commitment_stats_by_sender(
            from_email=from_email,
            days=days,
        )
        fulfilled = int(stats.get("fulfilled_count", 0) or 0)
        expired = int(stats.get("expired_count", 0) or 0)
        denominator = fulfilled + expired
        if denominator <= 0:
            return None, 0
        return fulfilled / denominator, denominator

    def _response_consistency(self, entity_id: str, *, days: int) -> tuple[float | None, int]:
        if not entity_id:
            return None, 0
        timestamps = self.analytics.interaction_event_times(
            entity_id=entity_id,
            event_type="email_received",
            days=days,
        )
        if len(timestamps) <= self.MIN_RESPONSE_SAMPLES:
            return None, len(timestamps)
        deltas: list[float] = []
        for prev, current in zip(timestamps, timestamps[1:]):
            delta_hours = (current - prev).total_seconds() / 3600.0
            if delta_hours >= 0:
                deltas.append(delta_hours)
        if len(deltas) < self.MIN_RESPONSE_SAMPLES:
            return None, len(deltas)
        deviation = pstdev(deltas)
        normalized = 1.0 - min(
            max(deviation / self.MAX_RESPONSE_STDDEV_HOURS, 0.0),
            1.0,
        )
        return normalized, len(deltas)

    def _trend_direction(self, entity_id: str, *, days: int) -> tuple[float | None, int]:
        if not entity_id:
            return None, 0
        counts = self.analytics.interaction_event_counts(
            entity_id=entity_id,
            event_type="email_received",
            recent_days=days,
            previous_days=days,
        )
        recent = int(counts.get("recent", 0) or 0)
        previous = int(counts.get("previous", 0) or 0)
        total = recent + previous
        if total < self.MIN_TREND_SAMPLES:
            return None, total
        if previous == 0:
            return (1.0 if recent > 0 else None), total
        if recent > previous:
            return 1.0, total
        if recent < previous:
            return 0.0, total
        return 0.5, total
