from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from mailbot_v26.insights.trust_score import TrustScoreCalculator
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_entity(db_path, from_email: str) -> str:
    store = ContextStore(db_path)
    resolution = store.resolve_sender_entity(
        from_email=from_email,
        from_name="Sender",
    )
    assert resolution is not None
    return resolution.entity_id


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str,
    entity_id: str | None,
    email_id: int | None,
    payload: dict[str, object],
) -> None:
    payload_json = json.dumps(payload, ensure_ascii=False)
    stable = json.dumps(
        {
            "event_type": event_type,
            "ts_utc": ts_utc,
            "account_id": account_id,
            "entity_id": entity_id,
            "email_id": email_id,
            "payload": payload,
            "schema_version": 1,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    fingerprint = hashlib.sha256(stable.encode("utf-8")).hexdigest()
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type,
            ts_utc,
            ts,
            account_id,
            entity_id,
            email_id,
            payload,
            payload_json,
            schema_version,
            fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_type,
            ts_utc,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            account_id,
            entity_id,
            email_id,
            payload_json,
            payload_json,
            1,
            fingerprint,
        ),
    )


def _seed_email_received(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    now: datetime,
) -> None:
    for days in (5, 10, 40, 45):
        ts = (now - timedelta(days=days)).timestamp()
        _insert_event(
            conn,
            event_type="email_received",
            ts_utc=ts,
            account_id="account@example.com",
            entity_id=entity_id,
            email_id=None,
            payload={"from_email": "sender@example.com"},
        )


def _seed_response_times(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    now: datetime,
) -> None:
    for days, hours in ((3, 2.0), (6, 4.0), (12, 3.0)):
        ts = (now - timedelta(days=days)).timestamp()
        _insert_event(
            conn,
            event_type="response_time",
            ts_utc=ts,
            account_id="account@example.com",
            entity_id=entity_id,
            email_id=None,
            payload={"response_time_hours": hours},
        )


def _seed_commitment_status(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    now: datetime,
    days_offset: int,
    status: str,
) -> None:
    ts = (now - timedelta(days=days_offset)).timestamp()
    _insert_event(
        conn,
        event_type="commitment_status_changed",
        ts_utc=ts,
        account_id="account@example.com",
        entity_id=entity_id,
        email_id=None,
        payload={
            "new_status": status,
            "from_email": "sender@example.com",
        },
    )


def test_trust_v2_decay_prefers_recent(tmp_path) -> None:
    now = datetime.now(timezone.utc)

    def _score(recent_status: str, old_status: str) -> float:
        db_path = tmp_path / f"trust_decay_{recent_status}_{old_status}.sqlite"
        KnowledgeDB(db_path)
        entity_id = _seed_entity(db_path, "sender@example.com")
        analytics = KnowledgeAnalytics(db_path)
        calculator = TrustScoreCalculator(analytics)
        with sqlite3.connect(db_path) as conn:
            _seed_email_received(conn, entity_id=entity_id, now=now)
            _seed_response_times(conn, entity_id=entity_id, now=now)
            _seed_commitment_status(
                conn,
                entity_id=entity_id,
                now=now,
                days_offset=5,
                status=recent_status,
            )
            _seed_commitment_status(
                conn,
                entity_id=entity_id,
                now=now,
                days_offset=120,
                status=old_status,
            )
            conn.commit()
        result = calculator.compute(
            entity_id=entity_id,
            from_email="sender@example.com",
        )
        assert result.snapshot.score is not None
        return float(result.snapshot.score)

    recent_good = _score("fulfilled", "expired")
    recent_bad = _score("expired", "fulfilled")

    assert recent_good > recent_bad


def test_trust_v2_redemption_arc(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    db_path = tmp_path / "trust_redemption.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    analytics = KnowledgeAnalytics(db_path)
    calculator = TrustScoreCalculator(analytics)
    with sqlite3.connect(db_path) as conn:
        _seed_email_received(conn, entity_id=entity_id, now=now)
        _seed_response_times(conn, entity_id=entity_id, now=now)
        _seed_commitment_status(
            conn,
            entity_id=entity_id,
            now=now,
            days_offset=150,
            status="expired",
        )
        for offset in (2, 4, 6, 8, 12):
            _seed_commitment_status(
                conn,
                entity_id=entity_id,
                now=now,
                days_offset=offset,
                status="fulfilled",
            )
        conn.commit()

    result = calculator.compute(
        entity_id=entity_id,
        from_email="sender@example.com",
    )

    assert result.snapshot.score is not None
    assert result.snapshot.score > 0.6


def test_trust_v2_insufficient_data(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    db_path = tmp_path / "trust_insufficient.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    analytics = KnowledgeAnalytics(db_path)
    calculator = TrustScoreCalculator(analytics)
    with sqlite3.connect(db_path) as conn:
        _seed_commitment_status(
            conn,
            entity_id=entity_id,
            now=now,
            days_offset=2,
            status="fulfilled",
        )
        conn.commit()

    result = calculator.compute(
        entity_id=entity_id,
        from_email="sender@example.com",
    )

    assert result.snapshot.score is None
    assert result.snapshot.reason == "insufficient_data"
    assert result.snapshot.data_quality == "LOW_DATA"
