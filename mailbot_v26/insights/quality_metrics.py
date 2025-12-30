from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class BreakdownRow:
    key: str
    evaluated: int
    corrections: int


@dataclass(frozen=True, slots=True)
class QualityMetricsSnapshot:
    window_days: int
    evaluated_total: int
    corrections_total: int
    accuracy: float | None
    by_mail_type: list[BreakdownRow]
    by_sender: list[BreakdownRow]
    by_priority: list[BreakdownRow]
    top_errors: list[dict[str, object]]


def _window_start(now: datetime | None, window_days: int) -> float:
    anchor = now or datetime.now(timezone.utc)
    return (anchor - timedelta(days=window_days)).timestamp()


def _sort_rows(rows: Iterable[BreakdownRow]) -> list[BreakdownRow]:
    return sorted(
        rows,
        key=lambda row: (-row.corrections, -row.evaluated, row.key.lower()),
    )[:5]


def compute_quality_metrics(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str | None,
    window_days: int,
    now: datetime | None = None,
) -> QualityMetricsSnapshot:
    """Compute deterministic priority quality metrics from events.

    evaluated_total is defined as the count of ``telegram_delivered`` events
    within the window that carry a priority shown to the user. All ratios are
    derived from this denominator to keep the metric explainable and auditable.
    """

    since_ts = _window_start(now, window_days)
    delivered_rows = analytics._event_rows(  # noqa: SLF001
        account_id=account_email,
        event_type=EventType.TELEGRAM_DELIVERED.value,
        since_ts=since_ts,
    )
    delivered_by_email: dict[str, dict[str, str]] = {}
    mail_type_evaluated: dict[str, int] = {}
    sender_evaluated: dict[str, int] = {}
    priority_evaluated: dict[str, int] = {}
    for row in delivered_rows:
        email_id = row.get("email_id")
        if email_id is None:
            continue
        email_key = str(email_id)
        payload = analytics.event_payload(row)
        priority = str(payload.get("priority") or "").strip()
        mail_type = str(payload.get("mail_type") or "").strip()
        from_email = str(payload.get("from_email") or "").strip()
        delivered_by_email[email_key] = {
            "priority": priority,
            "mail_type": mail_type,
            "from_email": from_email,
        }
        mail_type_evaluated[mail_type] = mail_type_evaluated.get(mail_type, 0) + 1
        sender_evaluated[from_email] = sender_evaluated.get(from_email, 0) + 1
        priority_evaluated[priority] = priority_evaluated.get(priority, 0) + 1

    correction_rows = analytics._event_rows(  # noqa: SLF001
        account_id=account_email,
        event_type=EventType.PRIORITY_CORRECTION_RECORDED.value,
        since_ts=since_ts,
    )
    corrections_total = 0
    mail_type_corrections: dict[str, int] = {}
    sender_corrections: dict[str, int] = {}
    priority_corrections: dict[str, int] = {}
    error_patterns: dict[tuple[str, str, str], int] = {}

    for row in correction_rows:
        email_id = row.get("email_id")
        if email_id is None:
            continue
        email_key = str(email_id)
        delivered_payload = delivered_by_email.get(email_key)
        if delivered_payload is None:
            continue
        payload = analytics.event_payload(row)
        old_priority = str(payload.get("old_priority") or delivered_payload.get("priority") or "").strip()
        new_priority = str(payload.get("new_priority") or "").strip()
        mail_type = delivered_payload.get("mail_type") or ""
        from_email = delivered_payload.get("from_email") or ""

        corrections_total += 1
        mail_type_corrections[mail_type] = mail_type_corrections.get(mail_type, 0) + 1
        sender_corrections[from_email] = sender_corrections.get(from_email, 0) + 1
        priority_corrections[old_priority] = priority_corrections.get(old_priority, 0) + 1
        key = (mail_type, old_priority, new_priority)
        error_patterns[key] = error_patterns.get(key, 0) + 1

    evaluated_total = len(delivered_by_email)
    accuracy: float | None = None
    if evaluated_total > 0:
        accuracy = max(0.0, 1.0 - (corrections_total / evaluated_total))

    logger.info(
        "priority_quality_metrics_computed",
        evaluated_total=evaluated_total,
        corrections_total=corrections_total,
        window_days=window_days,
    )

    return QualityMetricsSnapshot(
        window_days=window_days,
        evaluated_total=evaluated_total,
        corrections_total=corrections_total,
        accuracy=accuracy,
        by_mail_type=_sort_rows(
            BreakdownRow(
                key=mail_type or "",
                evaluated=mail_type_evaluated.get(mail_type, 0),
                corrections=mail_type_corrections.get(mail_type, 0),
            )
            for mail_type in mail_type_evaluated
        ),
        by_sender=_sort_rows(
            BreakdownRow(
                key=sender or "",
                evaluated=sender_evaluated.get(sender, 0),
                corrections=sender_corrections.get(sender, 0),
            )
            for sender in sender_evaluated
        ),
        by_priority=_sort_rows(
            BreakdownRow(
                key=priority or "",
                evaluated=priority_evaluated.get(priority, 0),
                corrections=priority_corrections.get(priority, 0),
            )
            for priority in priority_evaluated
        ),
        top_errors=sorted(
            (
                {
                    "mail_type": key[0],
                    "old_priority": key[1],
                    "new_priority": key[2],
                    "count": count,
                }
                for key, count in error_patterns.items()
            ),
            key=lambda item: (
                -int(item["count"]),
                str(item["mail_type"]).lower(),
                str(item["old_priority"]),
                str(item["new_priority"]),
            ),
        )[:5],
    )


__all__ = ["QualityMetricsSnapshot", "BreakdownRow", "compute_quality_metrics"]
