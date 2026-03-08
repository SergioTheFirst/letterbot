from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

from mailbot_v26.behavior.deadlock_detector import maybe_emit_deadlock
from mailbot_v26.config.deadlock_policy import DeadlockPolicyConfig
from mailbot_v26.events.contract import EventType
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_emails(
    db_path, *, account_email: str, thread_key: str, count: int, received_at: datetime
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO emails (account_email, thread_key, received_at)
            VALUES (?, ?, ?)
            """,
            [
                (account_email, thread_key, received_at.isoformat())
                for _ in range(count)
            ],
        )
        conn.commit()


def test_deadlock_emits_when_threshold_reached(tmp_path) -> None:
    db_path = tmp_path / "deadlock.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = DeadlockPolicyConfig(window_days=5, min_messages=3, cooldown_hours=24)
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        thread_key="thread-1",
        count=3,
        received_at=now - timedelta(days=1),
    )

    emitted = maybe_emit_deadlock(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        thread_key="thread-1",
        policy=policy,
        now_ts=now.timestamp(),
    )

    assert emitted is True
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT event_type, payload
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.DEADLOCK_DETECTED.value,),
        ).fetchone()
    assert row is not None
    payload = json.loads(row[1])
    assert payload["thread_key"] == "thread-1"
    assert payload["count_window"] == 3


def test_deadlock_dedupes_within_cooldown(tmp_path) -> None:
    db_path = tmp_path / "deadlock.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = DeadlockPolicyConfig(window_days=5, min_messages=2, cooldown_hours=24)
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        thread_key="thread-2",
        count=2,
        received_at=now - timedelta(days=1),
    )

    first = maybe_emit_deadlock(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        thread_key="thread-2",
        policy=policy,
        now_ts=now.timestamp(),
    )
    second = maybe_emit_deadlock(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        thread_key="thread-2",
        policy=policy,
        now_ts=(now + timedelta(hours=1)).timestamp(),
    )

    assert first is True
    assert second is False
    with sqlite3.connect(db_path) as conn:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.DEADLOCK_DETECTED.value,),
        ).fetchone()[0]
    assert count == 1


def test_deadlock_does_not_emit_below_threshold(tmp_path) -> None:
    db_path = tmp_path / "deadlock.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = DeadlockPolicyConfig(window_days=5, min_messages=4, cooldown_hours=24)
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        thread_key="thread-3",
        count=3,
        received_at=now - timedelta(days=1),
    )

    emitted = maybe_emit_deadlock(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        thread_key="thread-3",
        policy=policy,
        now_ts=now.timestamp(),
    )

    assert emitted is False
    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]
    assert count == 0


def test_deadlock_can_reemit_after_seven_day_cooldown(tmp_path) -> None:
    db_path = tmp_path / "deadlock.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = DeadlockPolicyConfig(window_days=5, min_messages=2, cooldown_hours=168)
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        thread_key="thread-4",
        count=2,
        received_at=now - timedelta(days=1),
    )

    first = maybe_emit_deadlock(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        thread_key="thread-4",
        policy=policy,
        now_ts=now.timestamp(),
    )
    _seed_emails(
        db_path,
        account_email="account@example.com",
        thread_key="thread-4",
        count=2,
        received_at=now + timedelta(days=7),
    )
    second = maybe_emit_deadlock(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        thread_key="thread-4",
        policy=policy,
        now_ts=(now + timedelta(days=8)).timestamp(),
    )

    assert first is True
    assert second is True
