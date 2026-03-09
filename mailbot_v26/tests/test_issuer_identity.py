from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.domain.document_templates import select_document_template
from mailbot_v26.domain.issuer_identity import (
    build_issuer_fingerprint,
    normalize_sender_identity,
    resolve_sender_profile_key,
)
from mailbot_v26.domain.template_promotion import (
    analyze_template_promotion_candidates,
    clear_runtime_template_promotion_cache,
    find_runtime_template_promotion,
)
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_interpretation(
    emitter: EventEmitter,
    *,
    email_id: int,
    sender_email: str,
    doc_kind: str,
    account_id: str = "account@example.com",
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.MESSAGE_INTERPRETATION,
            ts_utc=float(email_id),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "sender_email": sender_email,
                "doc_kind": doc_kind,
                "amount": 1000.0,
                "due_date": None,
                "priority": "рџџЎ",
                "action": "РџСЂРѕРІРµСЂРёС‚СЊ",
                "confidence": 0.9,
                "context": "NEW_MESSAGE",
                "document_id": f"doc-{email_id}",
            },
        )
    )


def _emit_priority_correction(
    emitter: EventEmitter,
    *,
    email_id: int,
    new_priority: str,
    account_id: str = "account@example.com",
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=float(email_id) + 0.5,
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "new_priority": new_priority,
                "old_priority": "рџџЎ",
                "engine": "priority_v2_auto",
                "source": "telegram_inbound",
            },
        )
    )


def test_same_domain_same_display_is_same_identity() -> None:
    left = normalize_sender_identity(
        "docs@vendor.example",
        display_name="Vendor Group",
    )
    right = normalize_sender_identity(
        "ops@vendor.example",
        display_name="Vendor Group",
    )

    assert left["confidence"] == "strong"
    assert right["confidence"] == "strong"
    assert left["key"] == right["key"]


def test_same_domain_different_display_is_different_identity() -> None:
    left = normalize_sender_identity(
        "docs@vendor.example",
        display_name="Vendor Finance",
    )
    right = normalize_sender_identity(
        "ops@vendor.example",
        display_name="Vendor Legal",
    )

    assert left["key"] != right["key"]


def test_email_only_falls_back_to_weak_confidence() -> None:
    identity = normalize_sender_identity("noreply@vendor.example")

    assert identity["confidence"] == "weak"
    assert identity["key"] == "noreply@vendor.example"


def test_fingerprint_is_stable_across_calls() -> None:
    identity = normalize_sender_identity(
        "ops@vendor.example",
        display_name="Vendor Ops",
    )

    assert build_issuer_fingerprint(identity) == build_issuer_fingerprint(identity)
    assert resolve_sender_profile_key(
        "ops@vendor.example",
        display_name="Vendor Ops",
    ) == resolve_sender_profile_key(
        "ops@vendor.example",
        display_name="Vendor Ops",
    )


def test_issuer_identity_does_not_override_content_semantics() -> None:
    strong_identity = normalize_sender_identity(
        "billing@vendor.example",
        display_name="Vendor Billing",
    )
    weak_identity = normalize_sender_identity("noreply@vendor.example")

    strong_match = select_document_template(
        sender_email="billing@vendor.example",
        subject="Invoice #42",
        body_text="Счет на оплату. Итого 10 000 руб. Оплатить до 15.04.2026.",
    )
    weak_match = select_document_template(
        sender_email="billing@vendor.example",
        subject="Invoice #42",
        body_text="Счет на оплату. Итого 10 000 руб. Оплатить до 15.04.2026.",
    )

    assert strong_identity["confidence"] == "strong"
    assert weak_identity["confidence"] == "weak"
    assert strong_match is not None
    assert weak_match is not None
    assert strong_match.template.id == weak_match.template.id


def test_weak_identity_does_not_escalate_action(tmp_path: Path) -> None:
    db_path = tmp_path / "weak-identity-promotion.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (11, 12, 13, 14, 15):
        _emit_interpretation(
            emitter,
            email_id=email_id,
            sender_email="noreply@vendor.example",
            doc_kind="invoice",
        )
        _emit_priority_correction(
            emitter,
            email_id=email_id,
            new_priority="рџ”ґ",
        )

    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="noreply@vendor.example",
        doc_kind="invoice",
    )

    assert promotion is None
    assert reason == "weak_identity"


def test_relationship_profile_grouping_uses_new_key(tmp_path: Path) -> None:
    db_path = tmp_path / "relationship-grouping.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "user@example.com",
                "billing@vendor.example",
                "Invoice 1",
                "summary",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "user@example.com",
                "billing+alias@vendor.example",
                "Invoice 2",
                "summary",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.commit()

    profiles = analytics.top_sender_relationship_profiles(
        account_email="user@example.com",
        days=7,
        limit=5,
        now=now,
    )

    assert len(profiles) == 1
    assert profiles[0]["sender_emails"] == [
        "billing+alias@vendor.example",
        "billing@vendor.example",
    ]
    assert profiles[0]["sender_identity_confidence"] == "medium"
    assert profiles[0]["sender_profile_key"] == resolve_sender_profile_key(
        "billing@vendor.example"
    )


def test_template_promotion_scoped_by_issuer_fingerprint(tmp_path: Path) -> None:
    db_path = tmp_path / "issuer-fingerprint-promotion.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (21, 22, 23, 24, 25):
        _emit_interpretation(
            emitter,
            email_id=email_id,
            sender_email="billing@vendor.example",
            doc_kind="invoice",
        )
        _emit_priority_correction(
            emitter,
            email_id=email_id,
            new_priority="рџ”ґ",
        )

    signals = analyze_template_promotion_candidates(db_path, min_corrections=3)
    fingerprint_signal = next(
        signal for signal in signals if signal.scope_kind == "issuer_fingerprint"
    )
    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="billing@vendor.example",
        doc_kind="invoice",
    )

    assert fingerprint_signal.identity_confidence == "medium"
    assert promotion is not None
    assert reason is None
    assert promotion.signal.scope_kind == "issuer_fingerprint"
    assert promotion.signal.scope_value == fingerprint_signal.scope_value
