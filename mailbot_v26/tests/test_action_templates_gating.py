from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from mailbot_v26.config.trust_bootstrap import TrustBootstrapConfig
from mailbot_v26.events.contract import EventType
from mailbot_v26.pipeline import daily_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
) -> None:
    payload: dict[str, object] = {"account_email": account_id}
    payload_json = json.dumps(payload, ensure_ascii=False)
    stable = json.dumps(
        {
            "event_type": event_type.value,
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
            event_type.value,
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
            event_type=EventType.EMAIL_RECEIVED,
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
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=start_ts + (idx * 120),
            account_id=account_id,
        )
    for idx in range(surprises):
        _insert_event(
            conn,
            event_type=EventType.SURPRISE_DETECTED,
            ts_utc=start_ts + (idx * 120),
            account_id=account_id,
        )


def _setup_db(tmp_path) -> tuple[KnowledgeAnalytics, sqlite3.Connection]:
    db_path = tmp_path / "gating.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    conn = sqlite3.connect(db_path)
    return analytics, conn


def _render_digest(data: daily_digest.DigestData) -> str:
    enriched = replace(
        data,
        deadlock_insights=[{"from_email": "boss@example.com", "subject": "Счёт"}],
        silence_insights=[{"contact": "client@example.com", "days_silent": 5}],
        digest_insights_enabled=True,
        digest_insights_max_items=3,
    )
    return daily_digest._build_digest_text(enriched)


def _collect_data(
    *,
    analytics: KnowledgeAnalytics,
    account_id: str,
    now: datetime,
    config: TrustBootstrapConfig,
    include_trust_bootstrap: bool,
) -> daily_digest.DigestData:
    return daily_digest._collect_digest_data(
        analytics=analytics,
        account_email=account_id,
        include_digest_insights=True,
        include_digest_action_templates=True,
        include_trust_bootstrap=include_trust_bootstrap,
        trust_bootstrap_config=config,
        now=now,
    )


def test_action_templates_gated_when_corrections_low(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=10)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=2)
    correction_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id=account_id,
        start_ts=correction_start,
        corrections=10,
        surprises=0,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=1,
        min_samples=1,
        templates_window_days=7,
        templates_min_corrections=20,
        templates_max_surprise_rate=0.15,
    )
    data = _collect_data(
        analytics=analytics,
        account_id=account_id,
        now=now,
        config=config,
        include_trust_bootstrap=True,
    )
    text = _render_digest(data)
    assert "Текст:" not in text


def test_action_templates_gated_when_surprise_rate_high(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=10)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=2)
    correction_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id=account_id,
        start_ts=correction_start,
        corrections=20,
        surprises=5,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=1,
        min_samples=1,
        templates_window_days=7,
        templates_min_corrections=20,
        templates_max_surprise_rate=0.15,
    )
    data = _collect_data(
        analytics=analytics,
        account_id=account_id,
        now=now,
        config=config,
        include_trust_bootstrap=True,
    )
    text = _render_digest(data)
    assert "Текст:" not in text


def test_action_templates_shown_when_ready(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=10)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=2)
    correction_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id=account_id,
        start_ts=correction_start,
        corrections=20,
        surprises=2,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=1,
        min_samples=1,
        templates_window_days=7,
        templates_min_corrections=20,
        templates_max_surprise_rate=0.15,
    )
    data = _collect_data(
        analytics=analytics,
        account_id=account_id,
        now=now,
        config=config,
        include_trust_bootstrap=True,
    )
    text = _render_digest(data)
    assert "Текст:" in text


def test_action_templates_not_gated_without_trust_bootstrap(tmp_path) -> None:
    analytics, conn = _setup_db(tmp_path)
    account_id = "account@example.com"
    now = datetime.now(timezone.utc)
    start_ts = (now - timedelta(days=10)).timestamp()
    _seed_email_received(conn, account_id=account_id, start_ts=start_ts, count=2)
    correction_start = (now - timedelta(days=2)).timestamp()
    _seed_corrections(
        conn,
        account_id=account_id,
        start_ts=correction_start,
        corrections=5,
        surprises=5,
    )
    conn.commit()

    config = TrustBootstrapConfig(
        learning_days=1,
        min_samples=1,
        templates_window_days=7,
        templates_min_corrections=20,
        templates_max_surprise_rate=0.15,
    )
    data = _collect_data(
        analytics=analytics,
        account_id=account_id,
        now=now,
        config=config,
        include_trust_bootstrap=False,
    )
    text = _render_digest(data)
    assert "Текст:" in text
