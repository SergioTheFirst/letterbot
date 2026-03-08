from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class Anomaly:
    type: str
    severity: str
    title: str
    details: str


def compute_anomalies(
    *,
    entity_id: str,
    analytics: KnowledgeAnalytics,
    now_dt: datetime | None = None,
) -> list[Anomaly]:
    if not entity_id:
        return []

    now = now_dt or datetime.now(timezone.utc)
    anomalies: list[Anomaly] = []

    response_anomaly = _response_time_anomaly(entity_id, analytics, now)
    if response_anomaly is not None:
        anomalies.append(response_anomaly)

    frequency_anomaly = _frequency_drop_anomaly(entity_id, analytics, now)
    if frequency_anomaly is not None:
        anomalies.append(frequency_anomaly)

    commitment_anomaly = _commitment_proximity_anomaly(entity_id, analytics, now)
    if commitment_anomaly is not None:
        anomalies.append(commitment_anomaly)

    return anomalies


def _response_time_anomaly(
    entity_id: str, analytics: KnowledgeAnalytics, now: datetime
) -> Anomaly | None:
    latest = analytics.get_latest_response_time(entity_id=entity_id, now_dt=now)
    if latest is None:
        return None

    latest_value = latest.get("response_time_hours")
    latest_time = latest.get("event_time")
    if latest_value is None or latest_time is None:
        return None

    baseline = analytics.get_avg_response_time(
        entity_id=entity_id,
        window=30,
        end_dt=latest_time,
    )
    sample_size = int(baseline.get("sample_size") or 0)
    baseline_avg = baseline.get("avg_hours")
    if baseline_avg is None or sample_size < 3:
        return None

    current = float(latest_value)
    baseline_avg = float(baseline_avg)
    ratio = current / baseline_avg if baseline_avg > 0 else None
    delta = current - baseline_avg
    if ratio is None:
        return None
    if ratio < 3.0 and delta < 24.0:
        return None

    severity = "WARN"
    if ratio >= 3.0 or delta >= 48.0:
        severity = "ALERT"

    details = (
        f"Текущее: {current:.1f} ч, базовое: {baseline_avg:.1f} ч, " f"Δ {delta:.1f} ч"
    )
    return Anomaly(
        type="RESPONSE_TIME_DELAY",
        severity=severity,
        title="Ответ задерживается",
        details=details,
    )


def _frequency_drop_anomaly(
    entity_id: str, analytics: KnowledgeAnalytics, now: datetime
) -> Anomaly | None:
    window_short = 7
    window_long = 30
    stats = analytics.get_rolling_frequency(
        entity_id=entity_id,
        window_short=window_short,
        window_long=window_long,
        now_dt=now,
    )
    count_short = int(stats.get("count_short") or 0)
    count_long = int(stats.get("count_long") or 0)
    history_days = int(stats.get("history_days") or 0)
    if history_days < 14 or count_long == 0:
        return None

    weekly_avg = count_long / (window_long / 7.0)
    if weekly_avg <= 0:
        return None
    ratio = count_short / weekly_avg
    if ratio >= 0.5:
        return None

    severity = "WARN" if ratio <= 0.2 else "INFO"
    details = f"За 7 дней: {count_short}, " f"норма в неделю: {weekly_avg:.1f}"
    return Anomaly(
        type="FREQUENCY_DROP",
        severity=severity,
        title="Падение частоты общения",
        details=details,
    )


def _commitment_proximity_anomaly(
    entity_id: str, analytics: KnowledgeAnalytics, now: datetime
) -> Anomaly | None:
    commitments = analytics.get_upcoming_commitments(
        entity_id=entity_id, hours=48, now_dt=now
    )
    if not commitments:
        return None

    deadlines: list[datetime] = []
    for item in commitments:
        deadline_iso = item.get("deadline_iso")
        if not deadline_iso:
            continue
        parsed = parse_sqlite_datetime(str(deadline_iso))
        if parsed is None:
            continue
        deadlines.append(parsed)

    if not deadlines:
        return None

    soonest = min(deadlines)
    hours_left = (soonest - now).total_seconds() / 3600.0
    if hours_left < 0:
        return None

    severity = "INFO"
    if hours_left <= 12:
        severity = "ALERT"
    elif hours_left <= 24:
        severity = "WARN"

    details = (
        f"Дедлайн через {hours_left:.1f} ч, всего обязательств: {len(commitments)}"
    )
    return Anomaly(
        type="COMMITMENT_DUE",
        severity=severity,
        title="Скорый дедлайн обязательства",
        details=details,
    )


def max_anomaly_severity(anomalies: list[Anomaly]) -> str | None:
    if not anomalies:
        return None
    order = {"INFO": 1, "WARN": 2, "ALERT": 3}
    return max(anomalies, key=lambda item: order.get(item.severity, 0)).severity


__all__ = ["Anomaly", "compute_anomalies", "max_anomaly_severity"]
