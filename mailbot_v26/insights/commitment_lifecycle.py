from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable


CONFIRMATION_PATTERN = re.compile(
    r"\b(отправил|высылаю|прикладываю|сделал|готово)\b",
    re.IGNORECASE,
)


@dataclass(slots=True)
class CommitmentRecord:
    commitment_id: int
    commitment_text: str
    deadline_iso: str | None
    status: str
    created_at: datetime


@dataclass(slots=True)
class CommitmentStatusUpdate:
    commitment_id: int
    commitment_text: str
    deadline_iso: str | None
    old_status: str
    new_status: str
    reason: str


def parse_sqlite_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _parse_deadline_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed_date = date.fromisoformat(value)
    except ValueError:
        return None
    return datetime.combine(parsed_date, time.min, tzinfo=timezone.utc)


def _has_confirmation(text: str) -> bool:
    if not text:
        return False
    return bool(CONFIRMATION_PATTERN.search(text))


def evaluate_commitment_updates(
    pending_commitments: Iterable[CommitmentRecord],
    *,
    message_body: str,
    message_received_at: datetime,
    now: datetime | None = None,
) -> list[CommitmentStatusUpdate]:
    updates: list[CommitmentStatusUpdate] = []
    if now is None:
        now = datetime.now(timezone.utc)
    now = _coerce_datetime(now)
    received_at = _coerce_datetime(message_received_at)
    has_confirmation = _has_confirmation(message_body)

    for commitment in pending_commitments:
        created_at = _coerce_datetime(commitment.created_at)
        if has_confirmation and received_at > created_at:
            updates.append(
                CommitmentStatusUpdate(
                    commitment_id=commitment.commitment_id,
                    commitment_text=commitment.commitment_text,
                    deadline_iso=commitment.deadline_iso,
                    old_status=commitment.status,
                    new_status="fulfilled",
                    reason="confirmation_text",
                )
            )
            continue

        deadline_at = _parse_deadline_iso(commitment.deadline_iso)
        if deadline_at is not None:
            if now > deadline_at + timedelta(hours=24):
                updates.append(
                    CommitmentStatusUpdate(
                        commitment_id=commitment.commitment_id,
                        commitment_text=commitment.commitment_text,
                        deadline_iso=commitment.deadline_iso,
                        old_status=commitment.status,
                        new_status="expired",
                        reason="deadline_passed",
                    )
                )
            continue

        if now > created_at + timedelta(days=7):
            updates.append(
                CommitmentStatusUpdate(
                    commitment_id=commitment.commitment_id,
                    commitment_text=commitment.commitment_text,
                    deadline_iso=commitment.deadline_iso,
                    old_status=commitment.status,
                    new_status="unknown",
                    reason="stale_without_deadline",
                )
            )

    return updates


__all__ = [
    "CommitmentRecord",
    "CommitmentStatusUpdate",
    "evaluate_commitment_updates",
    "parse_sqlite_datetime",
]
