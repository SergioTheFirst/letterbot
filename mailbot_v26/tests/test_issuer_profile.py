from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.domain.issuer_profile import (
    build_issuer_profile,
    issuer_profile_from_interpretation_payload,
)
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
    issuer_tax_id: str | None = None,
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
        "issuer_tax_id": issuer_tax_id,
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


def test_same_sender_domain_with_stable_issuer_markers_produces_same_issuer_profile() -> None:
    left = build_issuer_profile(
        sender_email="ops@vendor.example",
        subject="Invoice",
        body_text="ИНН 7701234567",
    )
    right = build_issuer_profile(
        sender_email="billing@vendor.example",
        subject="Contract",
        body_text="ИНН 7701234567",
    )

    assert left is not None
    assert right is not None
    assert left.issuer_key == right.issuer_key == "tax:7701234567"


def test_weak_sender_match_without_issuer_markers_does_not_overfit() -> None:
    first = build_issuer_profile(sender_email="first@generic-mail.test")
    second = build_issuer_profile(sender_email="second@generic-mail.test")

    assert first is not None
    assert second is not None
    assert first.issuer_key == "email:first@generic-mail.test"
    assert second.issuer_key == "email:second@generic-mail.test"
    assert first.issuer_key != second.issuer_key


def test_issuer_profile_from_interpretation_payload_prefers_canonical_fields() -> None:
    profile = issuer_profile_from_interpretation_payload(
        {
            "sender_email": "billing@vendor.example",
            "issuer_key": "domain:vendor.example",
            "issuer_label": "vendor.example",
            "issuer_domain": "vendor.example",
            "issuer_tax_id": "7701234567",
        }
    )

    assert profile is not None
    assert profile.issuer_key == "domain:vendor.example"
    assert profile.issuer_tax_id == "7701234567"


def test_top_issuer_profiles_use_canonical_interpretation_events(tmp_path: Path) -> None:
    db_path = tmp_path / "issuer-profiles.sqlite"
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
            amount=87500.0,
            due_date="15.04.2026",
        )
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp() + 1,
            account_id="account@example.com",
            sender_email="ops@vendor.example",
            doc_kind="contract",
            amount=None,
            due_date=None,
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
    assert profiles[0]["payable_amount_total"] == 87500
    assert profiles[0]["contract_review_count"] == 1
    assert profiles[0]["total_documents"] == 2


def test_business_summary_is_rebuildable_from_interpretation_events(tmp_path: Path) -> None:
    db_path = tmp_path / "issuer-business.sqlite"
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
            amount=87500.0,
            due_date="15.04.2026",
        )
        _insert_interpretation(
            conn,
            ts_utc=now.timestamp() + 1,
            account_id="account@example.com",
            sender_email="legal@vendor.example",
            doc_kind="contract",
            amount=None,
            due_date=None,
        )
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, priority, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "account@example.com",
                "noise@example.com",
                "Invoice that should not count",
                "raw email only",
                "рџ”µ",
                now.isoformat(),
                now.isoformat(),
            ),
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
    assert summary["contract_review_count"] == 1
    assert summary["documents_waiting_attention_count"] == 2
    assert summary["top_issuers"][0]["issuer_key"] == "domain:vendor.example"
