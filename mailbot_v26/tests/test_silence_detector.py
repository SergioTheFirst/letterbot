from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

from mailbot_v26.behavior.silence_detector import run_silence_scan
from mailbot_v26.config.silence_policy import SilencePolicyConfig
from mailbot_v26.events.contract import EventType
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_emails(
    db_path,
    *,
    account_email: str,
    from_email: str,
    received_at_list: list[datetime],
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO emails (account_email, from_email, received_at)
            VALUES (?, ?, ?)
            """,
            [
                (account_email, from_email, received_at.isoformat())
                for received_at in received_at_list
            ],
        )
        conn.commit()


def test_silence_emits_when_contact_silent(tmp_path) -> None:
    db_path = tmp_path / "silence.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = SilencePolicyConfig(
        lookback_days=60,
        min_messages=3,
        silence_factor=2.0,
        min_silence_days=7,
        cooldown_hours=72,
        max_per_run=20,
    )
    now = datetime(2024, 1, 20, tzinfo=timezone.utc)
    received_at = now - timedelta(days=20)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        from_email="contact@example.com",
        received_at_list=[received_at, received_at, received_at],
    )

    emitted = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        now_ts=now.timestamp(),
        policy=policy,
    )

    assert emitted == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT payload
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.SILENCE_SIGNAL_DETECTED.value,),
        ).fetchone()
    assert row is not None
    payload = json.loads(row[0])
    assert payload["contact"] == "contact@example.com"
    assert payload["count_window"] == 3


def test_silence_does_not_emit_when_recent(tmp_path) -> None:
    db_path = tmp_path / "silence.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = SilencePolicyConfig(
        lookback_days=60,
        min_messages=3,
        silence_factor=2.0,
        min_silence_days=7,
        cooldown_hours=72,
        max_per_run=20,
    )
    now = datetime(2024, 1, 20, tzinfo=timezone.utc)
    received_at = now - timedelta(days=2)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        from_email="fresh@example.com",
        received_at_list=[received_at, received_at, received_at],
    )

    emitted = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        now_ts=now.timestamp(),
        policy=policy,
    )

    assert emitted == 0
    with sqlite3.connect(db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM events_v1"
        ).fetchone()[0]
    assert count == 0


def test_silence_dedupes_within_cooldown(tmp_path) -> None:
    db_path = tmp_path / "silence.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = SilencePolicyConfig(
        lookback_days=60,
        min_messages=3,
        silence_factor=2.0,
        min_silence_days=7,
        cooldown_hours=72,
        max_per_run=20,
    )
    now = datetime(2024, 1, 20, tzinfo=timezone.utc)
    received_at = now - timedelta(days=20)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        from_email="contact@example.com",
        received_at_list=[received_at, received_at, received_at],
    )

    first = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        now_ts=now.timestamp(),
        policy=policy,
    )
    second = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        now_ts=(now + timedelta(hours=1)).timestamp(),
        policy=policy,
    )

    assert first == 1
    assert second == 0
    with sqlite3.connect(db_path) as conn:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.SILENCE_SIGNAL_DETECTED.value,),
        ).fetchone()[0]
    assert count == 1


def test_silence_respects_max_per_run_ordering(tmp_path) -> None:
    db_path = tmp_path / "silence.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = SilencePolicyConfig(
        lookback_days=60,
        min_messages=3,
        silence_factor=2.0,
        min_silence_days=7,
        cooldown_hours=72,
        max_per_run=1,
    )
    now = datetime(2024, 1, 20, tzinfo=timezone.utc)
    older = now - timedelta(days=20)
    newer = now - timedelta(days=10)
    _seed_emails(
        db_path,
        account_email="account@example.com",
        from_email="older@example.com",
        received_at_list=[older, older, older],
    )
    _seed_emails(
        db_path,
        account_email="account@example.com",
        from_email="newer@example.com",
        received_at_list=[newer, newer, newer],
    )

    emitted = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account@example.com",
        now_ts=now.timestamp(),
        policy=policy,
    )

    assert emitted == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT payload
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.SILENCE_SIGNAL_DETECTED.value,),
        ).fetchone()
    assert row is not None
    payload = json.loads(row[0])
    assert payload["contact"] == "older@example.com"


def test_silence_scope_aggregates_and_dedupes(tmp_path) -> None:
    db_path = tmp_path / "silence.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    policy = SilencePolicyConfig(
        lookback_days=60,
        min_messages=3,
        silence_factor=2.0,
        min_silence_days=7,
        cooldown_hours=72,
        max_per_run=20,
    )
    now = datetime(2024, 1, 20, tzinfo=timezone.utc)
    received_at = now - timedelta(days=20)
    _seed_emails(
        db_path,
        account_email="account-a@example.com",
        from_email="contact@example.com",
        received_at_list=[received_at, received_at],
    )
    _seed_emails(
        db_path,
        account_email="account-b@example.com",
        from_email="contact@example.com",
        received_at_list=[received_at, received_at],
    )

    first = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account-a@example.com",
        account_emails=[
            "account-b@example.com",
            " account-a@example.com ",
            "account-a@example.com",
        ],
        now_ts=now.timestamp(),
        policy=policy,
    )
    second = run_silence_scan(
        knowledge_db=knowledge_db,
        event_emitter=emitter,
        account_email="account-b@example.com",
        account_emails=["account-b@example.com", "account-a@example.com"],
        now_ts=(now + timedelta(hours=1)).timestamp(),
        policy=policy,
    )

    assert first == 1
    assert second == 0
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT payload
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.SILENCE_SIGNAL_DETECTED.value,),
        ).fetchone()
    assert row is not None
    payload = json.loads(row[0])
    assert payload["count_window"] == 4
