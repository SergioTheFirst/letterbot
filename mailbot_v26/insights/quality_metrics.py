from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class CountBreakdown:
    key: str
    count: int


@dataclass(frozen=True, slots=True)
class QualityMetricsSnapshot:
    window_days: int
    corrections_total: int
    by_new_priority: list[CountBreakdown]
    by_engine: list[CountBreakdown]
    correction_rate: float | None
    emails_received: int


def _window_start(now: datetime | None, window_days: int) -> float:
    anchor = now or datetime.now(timezone.utc)
    return (anchor - timedelta(days=window_days)).timestamp()


def _sorted_breakdown(rows: dict[str, int]) -> list[CountBreakdown]:
    return [
        CountBreakdown(key=key, count=count)
        for key, count in sorted(
            rows.items(), key=lambda item: (-int(item[1]), str(item[0]).lower())
        )
    ]


def compute_quality_metrics(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str | None,
    window_days: int,
    now: datetime | None = None,
) -> QualityMetricsSnapshot:
    since_ts = _window_start(now, window_days)

    correction_rows = analytics._event_rows(  # noqa: SLF001
        account_id=account_email,
        event_type=EventType.PRIORITY_CORRECTION_RECORDED.value,
        since_ts=since_ts,
    )

    corrections_total = 0
    by_new_priority: dict[str, int] = {}
    by_engine: dict[str, int] = {}

    for row in correction_rows:
        payload = analytics.event_payload(row)
        new_priority = str(payload.get("new_priority") or "unknown").strip() or "unknown"
        engine = str(payload.get("engine") or "unknown").strip() or "unknown"
        corrections_total += 1
        by_new_priority[new_priority] = by_new_priority.get(new_priority, 0) + 1
        by_engine[engine] = by_engine.get(engine, 0) + 1

    emails_received_rows = analytics._event_rows(  # noqa: SLF001
        account_id=account_email,
        event_type=EventType.EMAIL_RECEIVED.value,
        since_ts=since_ts,
    )
    emails_received = len(emails_received_rows)
    correction_rate: float | None = None
    if emails_received > 0:
        correction_rate = corrections_total / emails_received

    snapshot = QualityMetricsSnapshot(
        window_days=window_days,
        corrections_total=corrections_total,
        by_new_priority=_sorted_breakdown(by_new_priority),
        by_engine=_sorted_breakdown(by_engine),
        correction_rate=correction_rate,
        emails_received=emails_received,
    )

    logger.info(
        "priority_quality_metrics_computed",
        corrections_total=corrections_total,
        emails_received=emails_received,
        window_days=window_days,
    )

    return snapshot


__all__ = ["QualityMetricsSnapshot", "CountBreakdown", "compute_quality_metrics"]
