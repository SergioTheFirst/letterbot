from datetime import datetime, timezone

from mailbot_v26.insights.commitment_lifecycle import (
    CommitmentRecord,
    evaluate_commitment_updates,
)


def test_commitment_pending_to_fulfilled() -> None:
    created_at = datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)
    received_at = datetime(2024, 1, 2, 9, 0, tzinfo=timezone.utc)
    pending = [
        CommitmentRecord(
            commitment_id=1,
            commitment_text="Вышлю документы",
            deadline_iso=None,
            status="pending",
            created_at=created_at,
        )
    ]

    updates = evaluate_commitment_updates(
        pending,
        message_body="Отправил документы, как обещал.",
        message_received_at=received_at,
        now=received_at,
    )

    assert len(updates) == 1
    assert updates[0].new_status == "fulfilled"
    assert updates[0].reason == "confirmation_text"


def test_commitment_pending_to_expired() -> None:
    created_at = datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)
    now = datetime(2024, 1, 6, 1, 0, tzinfo=timezone.utc)
    pending = [
        CommitmentRecord(
            commitment_id=2,
            commitment_text="Вышлю документы до 2024-01-05",
            deadline_iso="2024-01-05",
            status="pending",
            created_at=created_at,
        )
    ]

    updates = evaluate_commitment_updates(
        pending,
        message_body="",
        message_received_at=now,
        now=now,
    )

    assert len(updates) == 1
    assert updates[0].new_status == "expired"
    assert updates[0].reason == "deadline_passed"


def test_commitment_pending_to_unknown() -> None:
    created_at = datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)
    now = datetime(2024, 1, 9, 9, 1, tzinfo=timezone.utc)
    pending = [
        CommitmentRecord(
            commitment_id=3,
            commitment_text="Созвонимся позже",
            deadline_iso=None,
            status="pending",
            created_at=created_at,
        )
    ]

    updates = evaluate_commitment_updates(
        pending,
        message_body="",
        message_received_at=now,
        now=now,
    )

    assert len(updates) == 1
    assert updates[0].new_status == "unknown"
    assert updates[0].reason == "stale_without_deadline"
