from __future__ import annotations

import contextlib
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional


@dataclass(frozen=True, slots=True)
class ImportanceScore:
    """EN: Importance score container. RU: Контейнер оценки важности."""

    score: int
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PercentileGateResult:
    """EN: Percentile gate result. RU: Результат пороговой проверки."""

    is_top: bool
    anchored: bool
    anchor_ts_utc: float


def heuristic_importance(
    *,
    subject: str,
    body_text: str,
    from_email: str,
    attachments: Iterable[dict],
) -> ImportanceScore:
    """EN: Deterministic importance score. RU: Детерминированная оценка важности."""

    score = 0
    reasons: list[str] = []
    subject_lower = (subject or "").lower()
    body_lower = (body_text or "").lower()

    if any(keyword in subject_lower for keyword in ("срочно", "urgent", "asap", "немедленно")):
        score += 40
        reasons.append("subject_urgent")
    if any(keyword in subject_lower for keyword in ("срок", "deadline", "due", "оплата")):
        score += 25
        reasons.append("subject_deadline")
    if any(keyword in body_lower for keyword in ("срочно", "urgent", "asap", "немедленно")):
        score += 20
        reasons.append("body_urgent")
    if any(keyword in body_lower for keyword in ("срок", "deadline", "due", "оплата")):
        score += 15
        reasons.append("body_deadline")
    if attachments:
        score += 10
        reasons.append("attachments")
    if from_email and "@" in from_email:
        score += 5
        reasons.append("sender_present")
    if len(body_text or "") > 2000:
        score += 5
        reasons.append("long_body")

    score = max(0, min(100, score))
    return ImportanceScore(score=score, reasons=tuple(reasons))


def record_importance_score(
    *,
    db_path: Path,
    account_email: str,
    email_id: int,
    score: int,
    occurred_at: datetime,
    connection_factory: Optional[Callable[[], sqlite3.Connection]] = None,
) -> None:
    """EN: Persist importance score. RU: Сохранить оценку важности."""

    with _connect(db_path, connection_factory) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO email_importance_scores (
                account_email, email_id, score, ts_utc
            ) VALUES (?, ?, ?, ?)
            """,
            (account_email, email_id, int(score), occurred_at.timestamp()),
        )
        conn.commit()


def is_top_percentile(
    *,
    db_path: Path,
    account_email: str,
    current_score: int,
    percentile_threshold: int,
    window_days: int,
    anchor_ts_utc: float | None = None,
    received_at: datetime | None = None,
    now: datetime | None = None,
    connection_factory: Optional[Callable[[], sqlite3.Connection]] = None,
) -> PercentileGateResult:
    """EN: Determine if score is in top percentile. RU: Проверить попадание в топ."""
    # Event-time anchoring is required for deterministic reprocessing.
    anchor_dt, anchored = _resolve_anchor(
        anchor_ts_utc=anchor_ts_utc,
        received_at=received_at,
        now=now,
    )

    scores = _load_recent_scores(
        db_path=db_path,
        account_email=account_email,
        window_days=window_days,
        anchor_dt=anchor_dt,
        connection_factory=connection_factory,
    )
    if not scores:
        return PercentileGateResult(
            is_top=False,
            anchored=anchored,
            anchor_ts_utc=anchor_dt.timestamp(),
        )
    threshold_value = _percentile(scores, percentile_threshold / 100.0)
    return PercentileGateResult(
        is_top=current_score >= threshold_value,
        anchored=anchored,
        anchor_ts_utc=anchor_dt.timestamp(),
    )


def _load_recent_scores(
    *,
    db_path: Path,
    account_email: str,
    window_days: int,
    anchor_dt: datetime,
    connection_factory: Optional[Callable[[], sqlite3.Connection]] = None,
) -> list[int]:
    anchor_dt = anchor_dt.astimezone(timezone.utc)
    since_ts = (anchor_dt - timedelta(days=window_days)).timestamp()
    with _connect(db_path, connection_factory) as conn:
        rows = conn.execute(
            """
            SELECT score
            FROM email_importance_scores
            WHERE account_email = ? AND ts_utc >= ?
            ORDER BY ts_utc ASC, email_id ASC
            """,
            (account_email, since_ts),
        ).fetchall()
    return [int(row[0]) for row in rows]


def _resolve_anchor(
    *,
    anchor_ts_utc: float | None,
    received_at: datetime | None,
    now: datetime | None,
) -> tuple[datetime, bool]:
    if anchor_ts_utc is not None:
        return datetime.fromtimestamp(anchor_ts_utc, tz=timezone.utc), True
    if received_at is not None:
        return _coerce_utc(received_at), True
    if now is not None:
        return _coerce_utc(now), True
    return datetime.now(timezone.utc), False


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _percentile(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    sorted_values = sorted(values)
    index = int(math.ceil(percentile * len(sorted_values))) - 1
    index = max(0, min(index, len(sorted_values) - 1))
    return int(sorted_values[index])


def _connect(
    db_path: Path,
    connection_factory: Optional[Callable[[], sqlite3.Connection]],
) -> contextlib.AbstractContextManager[sqlite3.Connection]:
    if connection_factory is not None:
        return contextlib.nullcontext(connection_factory())
    return sqlite3.connect(db_path)


__all__ = [
    "ImportanceScore",
    "PercentileGateResult",
    "heuristic_importance",
    "record_importance_score",
    "is_top_percentile",
]
