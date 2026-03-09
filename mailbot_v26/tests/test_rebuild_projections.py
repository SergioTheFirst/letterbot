from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _insert_interpretation(
    conn: sqlite3.Connection,
    *,
    ts_utc: float,
    account_id: str,
    sender_email: str,
    doc_kind: str,
    amount: float | None = None,
    due_date: str | None = None,
    issuer_key: str | None = None,
    issuer_label: str | None = None,
    issuer_domain: str | None = None,
) -> None:
    payload = {
        "sender_email": sender_email,
        "doc_kind": doc_kind,
        "amount": amount,
        "due_date": due_date,
        "priority": "рџџЎ",
        "action": "Проверить",
        "confidence": 0.9,
        "context": "NEW_MESSAGE",
        "document_id": f"doc-{ts_utc}",
        "issuer_key": issuer_key,
        "issuer_label": issuer_label,
        "issuer_domain": issuer_domain,
    }
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "message_interpretation",
            ts_utc,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            account_id,
            None,
            int(ts_utc),
            json.dumps(payload, ensure_ascii=False),
            json.dumps(payload, ensure_ascii=False),
            1,
            f"interp-{account_id}-{ts_utc}",
        ),
    )


def test_projection_does_not_own_semantic_truth_business_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-business.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, priority, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "account@example.com",
                "noise@example.com",
                "Invoice from raw email should not count",
                "raw summary only",
                "рџ”µ",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            sender_email="billing@vendor.example",
            doc_kind="invoice",
            amount=87500.0,
            due_date="15.04.2026",
            issuer_key="domain:vendor.example",
            issuer_label="vendor.example",
            issuer_domain="vendor.example",
        )
        conn.commit()

    summary = analytics.business_summary(
        account_email="account@example.com",
        window_days=7,
        now=now,
        top_issuer_limit=3,
    )

    assert summary["payable_amount_total"] == 87500
    assert summary["payable_invoice_count"] == 1


def test_projection_does_not_own_semantic_truth_top_issuer_profiles(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-issuers.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            sender_email="billing@vendor.example",
            doc_kind="invoice",
            amount=5000.0,
            due_date="15.04.2026",
            issuer_key="domain:vendor.example",
            issuer_label="vendor.example",
            issuer_domain="vendor.example",
        )
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp() + 1,
            account_id="account@example.com",
            sender_email="legal@vendor.example",
            doc_kind="contract",
            issuer_key="domain:vendor.example",
            issuer_label="vendor.example",
            issuer_domain="vendor.example",
        )
        conn.commit()

    profiles = analytics.top_issuer_profiles(
        account_email="account@example.com",
        days=7,
        limit=5,
        now=now,
    )

    assert len(profiles) == 1
    assert profiles[0]["issuer_key"] == "domain:vendor.example"
    assert profiles[0]["total_documents"] == 2


def test_projection_does_not_own_semantic_truth_no_projection_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-nowrite.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            sender_email="billing@vendor.example",
            doc_kind="invoice",
            amount=5000.0,
            due_date="15.04.2026",
            issuer_key="domain:vendor.example",
            issuer_label="vendor.example",
            issuer_domain="vendor.example",
        )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        before_events = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]
        before_emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]

    analytics.business_summary(
        account_email="account@example.com",
        window_days=7,
        now=now,
        top_issuer_limit=3,
    )
    analytics.top_issuer_profiles(
        account_email="account@example.com",
        days=7,
        limit=5,
        now=now,
    )

    with sqlite3.connect(db_path) as conn:
        after_events = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]
        after_emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]

    assert after_events == before_events
    assert after_emails == before_emails


def test_projection_does_not_own_semantic_truth_report_is_replay_stable(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-stable.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            sender_email="billing@vendor.example",
            doc_kind="invoice",
            amount=12000.0,
            due_date="15.04.2026",
            issuer_key="domain:vendor.example",
            issuer_label="vendor.example",
            issuer_domain="vendor.example",
        )
        conn.commit()

    first = analytics.business_summary(
        account_email="account@example.com",
        window_days=7,
        now=now,
        top_issuer_limit=3,
    )
    second = analytics.business_summary(
        account_email="account@example.com",
        window_days=7,
        now=now,
        top_issuer_limit=3,
    )

    assert first == second
