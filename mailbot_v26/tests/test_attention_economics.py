from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.insights.attention_economics import compute_attention_economics
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _insert_email(
    conn: sqlite3.Connection,
    *,
    account_email: str,
    from_email: str,
    body_words: int,
    created_at: str,
    attachments: int = 0,
) -> None:
    body_summary = " ".join(["w"] * body_words)
    cur = conn.execute(
        """
        INSERT INTO emails (
            account_email,
            from_email,
            subject,
            deferred_for_digest,
            body_summary,
            created_at
        )
        VALUES (?, ?, 'subject', 0, ?, ?)
        """,
        (account_email, from_email, body_summary, created_at),
    )
    email_id = cur.lastrowid
    for idx in range(attachments):
        conn.execute(
            "INSERT INTO attachments (email_id, filename, summary) VALUES (?, ?, ?)",
            (email_id, f"file{idx}.txt", "summary"),
        )


def _emit_email_event(
    emitter: ContractEventEmitter,
    *,
    ts_utc: float,
    account_id: str,
    from_email: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=None,
            email_id=None,
            payload={
                "from_email": from_email,
                "subject": "subject",
                "body_summary": "summary text",
                "attachments_count": 0,
            },
        )
    )


def _emit_score_event(
    emitter: ContractEventEmitter,
    *,
    ts_utc: float,
    account_id: str,
    entity_id: str,
    event_type: EventType,
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=entity_id,
            email_id=None,
            payload=payload,
        )
    )


def test_at_risk_only_uses_health_or_anomalies(tmp_path: Path) -> None:
    db_path = tmp_path / "attention.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)

    now = datetime.utcnow()
    earlier = (now - timedelta(days=3)).isoformat()
    now_iso = now.isoformat()

    with sqlite3.connect(db_path) as conn:
        for _ in range(3):
            _insert_email(
                conn,
                account_email="acc@example.com",
                from_email="risk@example.com",
                body_words=400,
                attachments=1,
                created_at=now_iso,
            )
        for _ in range(2):
            _insert_email(
                conn,
                account_email="acc@example.com",
                from_email="trust-only@example.com",
                body_words=120,
                created_at=now_iso,
            )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, account_email, from_email, subject, body_summary, created_at FROM emails"
        ).fetchall()
        for row in rows:
            created_at = datetime.fromisoformat(str(row["created_at"]))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            emitter.emit(
                EventV1(
                    event_type=EventType.EMAIL_RECEIVED,
                    ts_utc=created_at.timestamp(),
                    account_id=str(row["account_email"]),
                    entity_id=None,
                    email_id=int(row["id"]),
                    payload={
                        "from_email": row["from_email"],
                        "subject": row["subject"],
                        "body_summary": row["body_summary"],
                        "attachments_count": 1 if row["from_email"] == "risk@example.com" else 0,
                    },
                )
            )

    emitter.emit(
        EventV1(
            event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
            ts_utc=datetime.fromisoformat(earlier).replace(tzinfo=timezone.utc).timestamp(),
            account_id="acc@example.com",
            entity_id="risk@example.com",
            email_id=None,
            payload={"health_score": 80.0},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
            ts_utc=datetime.fromisoformat(now_iso).replace(tzinfo=timezone.utc).timestamp(),
            account_id="acc@example.com",
            entity_id="risk@example.com",
            email_id=None,
            payload={"health_score": 60.0},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.TRUST_SCORE_UPDATED,
            ts_utc=datetime.fromisoformat(earlier).replace(tzinfo=timezone.utc).timestamp(),
            account_id="acc@example.com",
            entity_id="trust-only@example.com",
            email_id=None,
            payload={"trust_score": 0.8},
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.TRUST_SCORE_UPDATED,
            ts_utc=datetime.fromisoformat(now_iso).replace(tzinfo=timezone.utc).timestamp(),
            account_id="acc@example.com",
            entity_id="trust-only@example.com",
            email_id=None,
            payload={"trust_score": 0.6},
        )
    )

    result = compute_attention_economics(
        analytics=analytics,
        account_email="acc@example.com",
        window_days=7,
        now=now,
    )

    assert result is not None
    assert result.sample_size == 5

    at_risk_ids = {entity.entity_id for entity in result.at_risk}
    assert at_risk_ids == {"risk@example.com"}


def test_attention_economics_scopes_account_emails(tmp_path: Path) -> None:
    db_path = tmp_path / "attention_scope.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    _emit_email_event(
        emitter,
        ts_utc=now.timestamp(),
        account_id="acc-a@example.com",
        from_email="alpha@example.com",
    )
    _emit_email_event(
        emitter,
        ts_utc=now.timestamp(),
        account_id="acc-b@example.com",
        from_email="beta@example.com",
    )
    _emit_score_event(
        emitter,
        ts_utc=now.timestamp(),
        account_id="acc-a@example.com",
        entity_id="alpha@example.com",
        event_type=EventType.TRUST_SCORE_UPDATED,
        payload={"trust_score": 0.7},
    )
    _emit_score_event(
        emitter,
        ts_utc=now.timestamp(),
        account_id="acc-b@example.com",
        entity_id="beta@example.com",
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        payload={"health_score": 80.0},
    )

    result = compute_attention_economics(
        analytics=analytics,
        account_email="acc-a@example.com",
        account_emails=["acc-a@example.com", "acc-b@example.com"],
        window_days=7,
        now=now,
        sample_threshold=1,
    )

    assert result is not None
    assert result.sample_size == 2
    assert {entity.entity_id for entity in result.entities} == {
        "alpha@example.com",
        "beta@example.com",
    }


def test_attention_economics_empty_scope_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "attention_empty.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    _emit_email_event(
        emitter,
        ts_utc=now.timestamp(),
        account_id="acc-a@example.com",
        from_email="alpha@example.com",
    )
    _emit_email_event(
        emitter,
        ts_utc=now.timestamp(),
        account_id="acc-b@example.com",
        from_email="beta@example.com",
    )

    result = compute_attention_economics(
        analytics=analytics,
        account_email="acc-a@example.com",
        account_emails=[],
        window_days=7,
        now=now,
        sample_threshold=1,
    )

    assert result is not None
    assert result.sample_size == 1
    assert [entity.entity_id for entity in result.entities] == ["alpha@example.com"]
