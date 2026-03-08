from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from mailbot_v26.behavior.trust_bootstrap import (
    compute_trust_bootstrap_snapshot,
    is_bootstrap_active,
    is_ready_for_action_templates,
)
from mailbot_v26.config.trust_bootstrap import TrustBootstrapConfig
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str,
) -> None:
    payload: dict[str, object] = {"account_email": account_id}
    payload_json = json.dumps(payload, ensure_ascii=False)
    stable = json.dumps(
        {
            "event_type": event_type,
            "ts_utc": ts_utc,
            "account_id": account_id,
            "entity_id": None,
            "email_id": None,
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
            None,
            None,
            payload_json,
            payload_json,
            1,
            fingerprint,
        ),
    )


def _seed_email_received(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    start_ts: float,
    count: int,
) -> None:
    step = 60
    for idx in range(count):
        _insert_event(
            conn,
            event_type="email_received",
            ts_utc=start_ts + (idx * step),
            account_id=account_id,
        )


def _seed_corrections(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    start_ts: float,
    corrections: int,
    surprises: int,
) -> None:
    for idx in range(corrections):
        _insert_event(
            conn,
            event_type="priority_correction_recorded",
            ts_utc=start_ts + (idx * 120),
            account_id=account_id,
        )
    for idx in range(surprises):
        _insert_event(
            conn,
            event_type="surprise_detected",
            ts_utc=start_ts + (idx * 120),
            account_id=account_id,
        )


def _setup_db(tmp_path) -> tuple[KnowledgeAnalytics, sqlite3.Connection]:
    db_path = tmp_path / "bootstrap.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    conn = sqlite3.connect(db_path)
    return analytics, conn


def test_trust_bootstrap_active_when_samples_low(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=30)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=10)
    conn.commit()

    config = TrustBootstrapConfig(learning_days=14, min_samples=50)
    assert is_bootstrap_active(
        account_id,
        now.timestamp(),
        analytics=analytics,
        config=config,
    )


def test_trust_bootstrap_active_when_days_low(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=3)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=60)
    conn.commit()

    config = TrustBootstrapConfig(learning_days=14, min_samples=50)
    assert is_bootstrap_active(
        account_id,
        now.timestamp(),
        analytics=analytics,
        config=config,
    )


def test_trust_bootstrap_active_when_surprise_rate_high(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=30)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=60)
    correction_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id=account_id,
        start_ts=correction_start,
        corrections=10,
        surprises=5,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=14,
        min_samples=50,
        max_allowed_surprise_rate=0.30,
    )
    snapshot = compute_trust_bootstrap_snapshot(
        analytics=analytics,
        account_email=account_id,
        now_ts=now.timestamp(),
        config=config,
    )
    assert snapshot.active


def test_trust_bootstrap_inactive_when_ready(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=30)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=60)
    correction_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id=account_id,
        start_ts=correction_start,
        corrections=10,
        surprises=2,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=14,
        min_samples=50,
        max_allowed_surprise_rate=0.30,
    )
    snapshot = compute_trust_bootstrap_snapshot(
        analytics=analytics,
        account_email=account_id,
        now_ts=now.timestamp(),
        config=config,
    )
    assert snapshot.active is False


def test_trust_bootstrap_aggregates_account_scope(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=30)).timestamp()
    _seed_email_received(
        conn, account_id="account@example.com", start_ts=start_ts, count=3
    )
    _seed_email_received(conn, account_id="alt@example.com", start_ts=start_ts, count=3)
    corrections_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id="account@example.com",
        start_ts=corrections_start,
        corrections=2,
        surprises=0,
    )
    _seed_corrections(
        conn,
        account_id="alt@example.com",
        start_ts=corrections_start,
        corrections=2,
        surprises=0,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=14,
        min_samples=6,
        templates_window_days=7,
        templates_min_corrections=4,
        templates_max_surprise_rate=0.25,
    )
    snapshot = compute_trust_bootstrap_snapshot(
        analytics=analytics,
        account_email="account@example.com",
        account_emails=["account@example.com", "alt@example.com"],
        now_ts=now.timestamp(),
        config=config,
    )
    assert snapshot.active is False
    assert snapshot.samples_count == 6
    assert is_ready_for_action_templates(
        "account@example.com",
        now.timestamp(),
        analytics=analytics,
        config=config,
        account_emails=["account@example.com", "alt@example.com"],
    )
