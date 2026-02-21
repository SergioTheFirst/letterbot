from __future__ import annotations

import configparser
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.ini"

_QUALITY_OK = "OK"
_QUALITY_LOW = "LOW_DATA"

_LOGGER = logging.getLogger(__name__)


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
    data_quality: str = _QUALITY_LOW
    model_version: str = "v1"
    computed_at: datetime | None = None


@dataclass(frozen=True)
class TrustScoreResult:
    snapshot: TrustSnapshot
    components: TrustScoreComponents
    data_window_days: int


class TrustScoreCalculator:
    RESPONSE_WINDOW_DAYS = 60
    TREND_WINDOW_DAYS = 30
    DEFAULT_MAX_RESPONSE_STDDEV_HOURS = 72.0
    DEFAULT_MIN_RESPONSE_SAMPLES = 2
    DEFAULT_MIN_TREND_SAMPLES = 2
    DEFAULT_HALF_LIFE_DAYS = 90.0
    DEFAULT_WEIGHT_COMMITMENT = 0.5
    DEFAULT_WEIGHT_RESPONSE = 0.3
    DEFAULT_WEIGHT_TREND = 0.2

    def __init__(self, analytics: KnowledgeAnalytics) -> None:
        self.analytics = analytics
        self._config = self._load_config()

    @dataclass(frozen=True)
    class _TrustConfig:
        half_life_days: float
        weight_commitment: float
        weight_response: float
        weight_trend: float
        max_response_stddev_hours: float
        min_response_samples: int
        min_trend_samples: int

    def _load_config(self) -> _TrustConfig:
        half_life = self.DEFAULT_HALF_LIFE_DAYS
        weight_commitment = self.DEFAULT_WEIGHT_COMMITMENT
        weight_response = self.DEFAULT_WEIGHT_RESPONSE
        weight_trend = self.DEFAULT_WEIGHT_TREND
        max_response_stddev = self.DEFAULT_MAX_RESPONSE_STDDEV_HOURS
        min_response_samples = self.DEFAULT_MIN_RESPONSE_SAMPLES
        min_trend_samples = self.DEFAULT_MIN_TREND_SAMPLES

        parser = read_user_ini_with_defaults(
            _CONFIG_PATH,
            logger=_LOGGER,
            scope_label="trust score settings",
        )
        section = parser["trust"] if "trust" in parser else None
        if section is not None:
            try:
                half_life = max(1.0, float(section.get("half_life_days", fallback=half_life)))
            except ValueError:
                half_life = self.DEFAULT_HALF_LIFE_DAYS
            try:
                weight_commitment = float(section.get("weight_commitment", fallback=weight_commitment))
            except ValueError:
                weight_commitment = self.DEFAULT_WEIGHT_COMMITMENT
            try:
                weight_response = float(section.get("weight_response", fallback=weight_response))
            except ValueError:
                weight_response = self.DEFAULT_WEIGHT_RESPONSE
            try:
                weight_trend = float(section.get("weight_trend", fallback=weight_trend))
            except ValueError:
                weight_trend = self.DEFAULT_WEIGHT_TREND
            try:
                max_response_stddev = max(
                    1.0,
                    float(section.get("max_response_stddev_hours", fallback=max_response_stddev)),
                )
            except ValueError:
                max_response_stddev = self.DEFAULT_MAX_RESPONSE_STDDEV_HOURS
            try:
                min_response_samples = max(
                    1,
                    int(section.get("min_response_samples", fallback=min_response_samples)),
                )
            except ValueError:
                min_response_samples = self.DEFAULT_MIN_RESPONSE_SAMPLES
            try:
                min_trend_samples = max(
                    1,
                    int(section.get("min_trend_samples", fallback=min_trend_samples)),
                )
            except ValueError:
                min_trend_samples = self.DEFAULT_MIN_TREND_SAMPLES

        return self._TrustConfig(
            half_life_days=half_life,
            weight_commitment=weight_commitment,
            weight_response=weight_response,
            weight_trend=weight_trend,
            max_response_stddev_hours=max_response_stddev,
            min_response_samples=min_response_samples,
            min_trend_samples=min_trend_samples,
        )

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
        now = datetime.now(timezone.utc)
        commitment_score, commitment_samples = self._commitment_reliability(
            entity_id,
            from_email=from_email,
            now=now,
            days=trend_window,
        )
        response_score, response_samples = self._response_consistency(
            entity_id,
            now=now,
            days=response_window,
        )
        trend_score, trend_samples = self._trend_direction(
            entity_id,
            now=now,
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
                data_quality=_QUALITY_LOW,
                model_version="v2",
                computed_at=now,
            )
            return TrustScoreResult(
                snapshot=snapshot,
                components=components,
                data_window_days=max(response_window, trend_window),
            )

        weight_sum = (
            self._config.weight_commitment
            + self._config.weight_response
            + self._config.weight_trend
        )
        if weight_sum <= 0:
            weight_sum = (
                self.DEFAULT_WEIGHT_COMMITMENT
                + self.DEFAULT_WEIGHT_RESPONSE
                + self.DEFAULT_WEIGHT_TREND
            )
            weight_commitment = self.DEFAULT_WEIGHT_COMMITMENT
            weight_response = self.DEFAULT_WEIGHT_RESPONSE
            weight_trend = self.DEFAULT_WEIGHT_TREND
        else:
            weight_commitment = self._config.weight_commitment / weight_sum
            weight_response = self._config.weight_response / weight_sum
            weight_trend = self._config.weight_trend / weight_sum

        trust_score = (
            weight_commitment * commitment_score
            + weight_response * response_score
            + weight_trend * trend_score
        )
        trust_score = max(0.0, min(1.0, trust_score))

        snapshot = TrustSnapshot(
            entity_id=entity_id,
            score=round(trust_score, 4),
            reason=None,
            sample_size=sample_size,
            data_quality=_QUALITY_OK,
            model_version="v2",
            computed_at=now,
        )
        return TrustScoreResult(
            snapshot=snapshot,
            components=components,
            data_window_days=max(response_window, trend_window),
        )

    def _commitment_reliability(
        self,
        entity_id: str,
        *,
        from_email: str | None,
        now: datetime,
        days: int,
    ) -> tuple[float | None, int]:
        if not entity_id:
            return None, 0
        since_ts = now.timestamp() - (days * 24 * 60 * 60)
        rows = self.analytics.event_rows_for_entity(
            entity_id=entity_id,
            event_type="commitment_status_changed",
            since_ts=since_ts,
        )
        fulfilled_weight = 0.0
        expired_weight = 0.0
        sample_size = 0
        for row in rows:
            payload = self.analytics.event_payload(row)
            status = str(payload.get("new_status") or payload.get("status") or "").lower()
            if status not in {"fulfilled", "expired"}:
                continue
            weight = self._decay_weight(
                now_ts=now.timestamp(),
                event_ts=float(row.get("ts_utc") or 0.0),
            )
            if weight <= 0:
                continue
            sample_size += 1
            if status == "fulfilled":
                fulfilled_weight += weight
            elif status == "expired":
                expired_weight += weight
        denominator = fulfilled_weight + expired_weight
        if denominator <= 0:
            return None, 0
        score = fulfilled_weight / denominator
        return score, sample_size

    def _response_consistency(
        self,
        entity_id: str,
        *,
        now: datetime,
        days: int,
    ) -> tuple[float | None, int]:
        if not entity_id:
            return None, 0
        since_ts = now.timestamp() - (days * 24 * 60 * 60)
        rows = self.analytics.event_rows_for_entity(
            entity_id=entity_id,
            event_type="response_time",
            since_ts=since_ts,
        )
        values: list[tuple[float, float]] = []
        for row in rows:
            payload = self.analytics.event_payload(row)
            raw = payload.get("response_time_hours") or payload.get("response_time")
            try:
                hours = float(raw)
            except (TypeError, ValueError):
                continue
            if hours < 0:
                continue
            weight = self._decay_weight(
                now_ts=now.timestamp(),
                event_ts=float(row.get("ts_utc") or 0.0),
            )
            if weight <= 0:
                continue
            values.append((hours, weight))
        if len(values) >= self._config.min_response_samples:
            return self._normalized_consistency(values)

        fallback_rows = self.analytics.event_rows_for_entity(
            entity_id=entity_id,
            event_type="email_received",
            since_ts=since_ts,
        )
        timestamps = sorted(
            float(row.get("ts_utc") or 0.0)
            for row in fallback_rows
            if float(row.get("ts_utc") or 0.0) > 0
        )
        deltas: list[tuple[float, float]] = []
        for prev, current in zip(timestamps, timestamps[1:]):
            delta_hours = (current - prev) / 3600.0
            if delta_hours < 0:
                continue
            weight = self._decay_weight(now_ts=now.timestamp(), event_ts=current)
            if weight <= 0:
                continue
            deltas.append((delta_hours, weight))
        if len(deltas) < self._config.min_response_samples:
            return None, len(deltas)
        return self._normalized_consistency(deltas)

    def _trend_direction(
        self,
        entity_id: str,
        *,
        now: datetime,
        days: int,
    ) -> tuple[float | None, int]:
        if not entity_id:
            return None, 0
        since_ts = now.timestamp() - (2 * days * 24 * 60 * 60)
        rows = self.analytics.event_rows_for_entity(
            entity_id=entity_id,
            event_type="email_received",
            since_ts=since_ts,
        )
        recent = 0
        previous = 0
        split_ts = now.timestamp() - (days * 24 * 60 * 60)
        for row in rows:
            event_ts = float(row.get("ts_utc") or 0.0)
            if event_ts <= 0:
                continue
            if event_ts >= split_ts:
                recent += 1
            else:
                previous += 1
        total = recent + previous
        if total < self._config.min_trend_samples:
            return None, total
        if previous == 0:
            return (1.0 if recent > 0 else None), total
        if recent > previous:
            return 1.0, total
        if recent < previous:
            return 0.0, total
        return 0.5, total

    def _decay_weight(self, *, now_ts: float, event_ts: float) -> float:
        if event_ts <= 0:
            return 0.0
        age_days = max(0.0, (now_ts - event_ts) / 86400.0)
        return math.exp(-age_days / self._config.half_life_days)

    def _normalized_consistency(
        self,
        values: list[tuple[float, float]],
    ) -> tuple[float | None, int]:
        weight_sum = sum(weight for _, weight in values)
        if weight_sum <= 0:
            return None, len(values)
        weighted_mean = sum(value * weight for value, weight in values) / weight_sum
        variance = sum(
            weight * (value - weighted_mean) ** 2 for value, weight in values
        ) / weight_sum
        stddev = math.sqrt(variance)
        normalized = 1.0 - min(
            max(stddev / self._config.max_response_stddev_hours, 0.0),
            1.0,
        )
        return normalized, len(values)
