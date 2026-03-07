from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics


logger = get_logger("mailbot")

_DELIVERY_CUTOFF = timedelta(hours=2)


@dataclass(frozen=True, slots=True)
class ErrorBreakdown:
    reason: str
    count: int
    share: float


@dataclass(frozen=True, slots=True)
class NotificationSLAResult:
    delivery_rate_24h: float
    delivery_rate_7d: float
    salvage_rate_24h: float
    p50_latency_24h: float | None
    p90_latency_24h: float | None
    p99_latency_24h: float | None
    p50_latency_7d: float | None
    p90_latency_7d: float | None
    p99_latency_7d: float | None
    top_error_reasons_24h: list[ErrorBreakdown]
    error_rate_24h: float
    undelivered_24h: int
    delivered_24h: int
    total_24h: int

    def degraded_reasons(self) -> list[str]:
        reasons: list[str] = []
        if self.delivery_rate_24h < 0.95:
            reasons.append("delivery_rate_below_slo")
        if (self.p90_latency_24h or 0) > 120:
            reasons.append("latency_p90_exceeds_slo")
        if self.error_rate_24h > 0.05:
            reasons.append("error_spike")
        return reasons


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * percentile
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


def _window_start(now: datetime, days: int) -> float:
    return (now - timedelta(days=days)).timestamp()


def _safe_payload(analytics: KnowledgeAnalytics, row: dict[str, object]) -> dict[str, object]:
    try:
        return analytics.event_payload(row)
    except Exception:  # pragma: no cover - defensive
        return {}


def _error_breakdown(values: dict[str, int], total: int) -> list[ErrorBreakdown]:
    if total <= 0:
        return []
    ranked = sorted(values.items(), key=lambda item: (-int(item[1]), item[0].lower()))
    return [
        ErrorBreakdown(reason=reason or "unknown", count=count, share=count / total)
        for reason, count in ranked
    ]


def _latencies(deliveries: Iterable[tuple[float, float]]) -> tuple[float | None, float | None, float | None]:
    latencies = [max(0.0, delivered - detected) for detected, delivered in deliveries]
    return (
        _percentile(latencies, 0.5),
        _percentile(latencies, 0.9),
        _percentile(latencies, 0.99),
    )


def compute_notification_sla(
    *,
    analytics: KnowledgeAnalytics,
    now: datetime | None = None,
    delivery_cutoff: timedelta = _DELIVERY_CUTOFF,
) -> NotificationSLAResult:
    anchor = now or datetime.now(timezone.utc)
    start_24h = _window_start(anchor, 1)
    start_7d = _window_start(anchor, 7)

    received_rows_7d = analytics._event_rows(  # noqa: SLF001
        account_id=None,
        event_type=EventType.EMAIL_RECEIVED.value,
        since_ts=start_7d,
    )
    deliveries_rows_7d = analytics._event_rows(  # noqa: SLF001
        account_id=None,
        event_type=EventType.TELEGRAM_DELIVERED.value,
        since_ts=start_7d,
    )
    failed_rows_7d = analytics._event_rows(  # noqa: SLF001
        account_id=None,
        event_type=EventType.TELEGRAM_FAILED.value,
        since_ts=start_7d,
    )

    detection_ts: dict[int, float] = {}
    for row in received_rows_7d:
        email_id = row.get("email_id")
        ts_utc = row.get("ts_utc")
        if email_id is None or ts_utc is None:
            continue
        try:
            detection_ts[int(email_id)] = float(ts_utc)
        except (TypeError, ValueError):
            continue

    delivery_events: dict[int, tuple[bool, float, str, int, str | None]] = {}
    for row in deliveries_rows_7d + failed_rows_7d:
        email_id = row.get("email_id")
        ts_utc = row.get("ts_utc")
        if email_id is None or ts_utc is None:
            continue
        payload = _safe_payload(analytics, row)
        delivered = bool(payload.get("delivered", row.get("event_type") == EventType.TELEGRAM_DELIVERED.value))
        occurred = float(payload.get("occurred_at_utc", ts_utc))
        mode = str(payload.get("mode") or "html").strip() or "html"
        retry_count = int(payload.get("retry_count") or 0)
        error = payload.get("error")
        delivery_events[int(email_id)] = (delivered, occurred, mode, retry_count, str(error) if error else None)

    cutoff_ts = anchor.timestamp() - delivery_cutoff.total_seconds()

    delivered_latencies_24h: list[tuple[float, float]] = []
    delivered_latencies_7d: list[tuple[float, float]] = []
    delivered_24h = 0
    salvage_24h = 0
    undelivered_24h = 0
    error_reasons: dict[str, int] = {}

    for email_id, detected_ts in detection_ts.items():
        delivery = delivery_events.get(email_id)
        if delivery:
            delivered, occurred, mode, _, error_reason = delivery
            if delivered:
                delivered_latencies_7d.append((detected_ts, occurred))
                if detected_ts >= start_24h:
                    delivered_24h += 1
                    delivered_latencies_24h.append((detected_ts, occurred))
                    if mode == "plain_salvage":
                        salvage_24h += 1
            elif detected_ts >= start_24h and (detected_ts <= cutoff_ts):
                undelivered_24h += 1
                if error_reason:
                    error_reasons[error_reason] = error_reasons.get(error_reason, 0) + 1
            if detected_ts >= start_24h and error_reason:
                error_reasons[error_reason] = error_reasons.get(error_reason, 0) + 1
        else:
            if detected_ts >= start_24h and detected_ts <= cutoff_ts:
                undelivered_24h += 1
                error_reasons["missing_delivery"] = error_reasons.get("missing_delivery", 0) + 1

    total_24h = sum(1 for ts in detection_ts.values() if ts >= start_24h)
    delivered_7d = len(delivered_latencies_7d)
    total_7d = len(detection_ts)

    delivery_rate_24h = delivered_24h / total_24h if total_24h else 1.0
    delivery_rate_7d = delivered_7d / total_7d if total_7d else 1.0
    salvage_rate_24h = salvage_24h / delivered_24h if delivered_24h else 0.0

    p50_24h, p90_24h, p99_24h = _latencies(delivered_latencies_24h)
    p50_7d, p90_7d, p99_7d = _latencies(delivered_latencies_7d)

    error_rate_24h = (undelivered_24h + sum(error_reasons.values())) / total_24h if total_24h else 0.0

    result = NotificationSLAResult(
        delivery_rate_24h=delivery_rate_24h,
        delivery_rate_7d=delivery_rate_7d,
        salvage_rate_24h=salvage_rate_24h,
        p50_latency_24h=p50_24h,
        p90_latency_24h=p90_24h,
        p99_latency_24h=p99_24h,
        p50_latency_7d=p50_7d,
        p90_latency_7d=p90_7d,
        p99_latency_7d=p99_7d,
        top_error_reasons_24h=_error_breakdown(error_reasons, total_24h),
        error_rate_24h=error_rate_24h,
        undelivered_24h=undelivered_24h,
        delivered_24h=delivered_24h,
        total_24h=total_24h,
    )

    logger.info(
        "notification_sla_computed",
        delivery_rate_24h=delivery_rate_24h,
        delivery_rate_7d=delivery_rate_7d,
        p90_latency_24h=p90_24h,
        error_rate_24h=error_rate_24h,
        salvage_rate_24h=salvage_rate_24h,
    )

    return result


__all__ = [
    "NotificationSLAResult",
    "ErrorBreakdown",
    "compute_notification_sla",
    "NotificationAlertStore",
]


class NotificationAlertStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_sla_alerts (
                    key TEXT PRIMARY KEY,
                    last_alert_fingerprint TEXT,
                    last_alert_at_utc REAL,
                    consecutive_failures INTEGER DEFAULT 0
                );
                """
            )
            conn.commit()

    def record_failure(self, now: datetime | None = None) -> int:
        try:
            with sqlite3.connect(self.path) as conn:
                row = conn.execute(
                    "SELECT consecutive_failures, last_alert_fingerprint, last_alert_at_utc FROM notification_sla_alerts WHERE key = 'singleton'",
                ).fetchone()
                current = int(row[0]) if row and row[0] is not None else 0
                last_fp = row[1] if row else None
                last_ts = row[2] if row else None
                next_value = current + 1
                conn.execute(
                    """
                    INSERT INTO notification_sla_alerts(key, consecutive_failures, last_alert_at_utc, last_alert_fingerprint)
                    VALUES('singleton', ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET consecutive_failures = excluded.consecutive_failures,
                        last_alert_at_utc = COALESCE(notification_sla_alerts.last_alert_at_utc, excluded.last_alert_at_utc),
                        last_alert_fingerprint = COALESCE(notification_sla_alerts.last_alert_fingerprint, excluded.last_alert_fingerprint)
                    """,
                    (next_value, last_ts, last_fp),
                )
                conn.commit()
            logger.info("notification_sla_failure_recorded", consecutive_failures=next_value)
            return next_value
        except Exception:  # pragma: no cover - defensive fallback
            logger.error("notification_alert_store_failure")
            return 0

    def reset_failures(self) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO notification_sla_alerts(key, consecutive_failures)
                    VALUES('singleton', 0)
                    ON CONFLICT(key) DO UPDATE SET consecutive_failures = 0
                    """,
                )
                conn.commit()
        except Exception:  # pragma: no cover - defensive fallback
            logger.error("notification_alert_store_failure")

    def should_alert(
        self,
        *,
        fingerprint: str,
        now: datetime | None = None,
        cooldown_hours: int = 6,
    ) -> bool:
        anchor = now or datetime.now(timezone.utc)
        try:
            with sqlite3.connect(self.path) as conn:
                row = conn.execute(
                    "SELECT last_alert_fingerprint, last_alert_at_utc FROM notification_sla_alerts WHERE key = 'singleton'",
                ).fetchone()
        except Exception:  # pragma: no cover - defensive fallback
            logger.error("notification_alert_store_failure")
            row = None
        if row:
            last_fp, last_ts = row
            if last_fp == fingerprint:
                if last_ts and (anchor.timestamp() - float(last_ts)) < cooldown_hours * 3600:
                    return False
        return True

    def save_alert(self, fingerprint: str, now: datetime | None = None) -> None:
        anchor = now or datetime.now(timezone.utc)
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO notification_sla_alerts(key, last_alert_fingerprint, last_alert_at_utc)
                    VALUES('singleton', ?, ?)
                    ON CONFLICT(key) DO UPDATE SET last_alert_fingerprint = excluded.last_alert_fingerprint,
                        last_alert_at_utc = excluded.last_alert_at_utc
                    """,
                    (fingerprint, anchor.timestamp()),
                )
                conn.commit()
        except Exception:  # pragma: no cover - defensive fallback
            logger.error("notification_alert_store_failure")
