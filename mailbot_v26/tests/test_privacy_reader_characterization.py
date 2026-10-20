"""Characterization of the WP3a privacy readers on raw (unmigrated) event rows.

WP3a removes subjects and body summaries from event payloads. These tests pin
what the four readers that consume those fields return for rows written by
older releases, so every later WP3a step (derived payload fields, emit-side
changes, the scrub migration) must keep their outputs identical:

- R1 PriorityEngineV2 reminder chains (legacy ``events`` email_received rows),
- R2 KnowledgeAnalytics.uncertainty_queue_items (PRIORITY_DECISION_RECORDED),
- R3 KnowledgeAnalytics.weekly_attention_entities (EMAIL_RECEIVED word counts),
- R4 sender relationship trust score (substring test over EMAIL_RECEIVED text).

Rows are inserted with plain SQL, exactly as older releases left them.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.priority.priority_engine_v2 import (
    PriorityEngineV2,
    PriorityV2Config,
    VipSenderMatcher,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB

ACCOUNT = "owner@example.com"
OTHER_ACCOUNT = "other@example.com"
# Inert before WP3a step 3; afterwards the engine may load it read-only. Raw
# rows must give the same chains with and without a key.
_SUBJECT_KEY_HEX = "5a" * 32


def _new_db(tmp_path: Path, *, with_subject_key: bool = False) -> Path:
    db_path = tmp_path / "characterization.sqlite"
    KnowledgeDB(db_path)
    if with_subject_key:
        db_path.with_name(f"{db_path.name}.subject.key").write_text(
            _SUBJECT_KEY_HEX + "\n", encoding="ascii"
        )
    return db_path


def _payload_text(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _insert_legacy_email_received(
    conn: sqlite3.Connection, *, ts: datetime, payload: dict[str, object]
) -> None:
    conn.execute(
        "INSERT INTO events (id, type, timestamp, entity_id, email_id, payload) "
        "VALUES (?, 'email_received', ?, NULL, NULL, ?)",
        (uuid.uuid4().hex, ts.isoformat(), _payload_text(payload)),
    )


def _insert_v1_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    payload: dict[str, object],
    account_id: str = ACCOUNT,
    email_id: int | None = None,
) -> None:
    text = _payload_text(payload)
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type, ts_utc, ts, account_id, entity_id, email_id,
            payload, payload_json, schema_version, fingerprint
        ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, 1, ?)
        """,
        (
            event_type,
            ts_utc,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            account_id,
            email_id,
            text,
            text,
            uuid.uuid4().hex,
        ),
    )


def _insert_email(
    conn: sqlite3.Connection,
    *,
    from_email: str,
    subject: str,
    received_at: str,
    account_email: str = ACCOUNT,
    body_summary: str = "",
) -> int:
    cursor = conn.execute(
        "INSERT INTO emails (account_email, from_email, subject, received_at, body_summary) "
        "VALUES (?, ?, ?, ?, ?)",
        (account_email, from_email, subject, received_at, body_summary),
    )
    return int(cursor.lastrowid)


# --- R1: reminder chains -----------------------------------------------------

_NOW = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
_SENDER = "billing@vendor.example"
_OTHER_SENDER = "collections@agency.example"
_SUBJECT = "Reminder: Invoice 42"


def _reminder_chain_detail(db_path: Path, *, subject: str, from_email: str) -> str | None:
    engine = PriorityEngineV2(
        KnowledgeAnalytics(db_path),
        config=PriorityV2Config(),
        vip_senders=VipSenderMatcher(()),
    )
    result = engine.compute(
        subject=subject,
        body_text="",
        from_email=from_email,
        mail_type="UNKNOWN",
        received_at=_NOW,
        commitments=[],
    )
    details = [item.detail for item in result.breakdown if item.signal == "reminder_chain"]
    assert len(details) <= 1
    return details[0] if details else None


def _insert_baseline_chain(conn: sqlite3.Connection) -> None:
    """Two same-sender reminders: every extra matching event raises the chain to 3."""
    for days_ago in (1, 2):
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=days_ago),
            payload={
                "account_email": ACCOUNT,
                "from_email": _SENDER,
                "subject": f"Unrelated {days_ago}",
                "mail_type": "PAYMENT_REMINDER",
                "mail_type_reasons": ["mt.base=PAYMENT_REMINDER"],
            },
        )


@pytest.mark.parametrize("with_subject_key", [False, True])
@pytest.mark.parametrize(
    ("candidate", "expected_chain"),
    [
        # Other sender, same subject after one reply prefix, case and spaces.
        (
            {"from_email": _OTHER_SENDER, "subject": "RE:   reminder:   INVOICE 42 ", "mail_type": "PAYMENT_REMINDER"},
            "3",
        ),
        (
            {"from_email": _OTHER_SENDER, "subject": "Fwd: Reminder: Invoice 42", "mail_type": "reminder_escalation"},
            "3",
        ),
        (
            {"from_email": _OTHER_SENDER, "subject": "fw:Reminder:  Invoice 42", "mail_type": "REMINDER_FIRST"},
            "3",
        ),
        # Only one reply prefix is stripped.
        (
            {"from_email": _OTHER_SENDER, "subject": "Fwd: Re: Reminder: Invoice 42", "mail_type": "PAYMENT_REMINDER"},
            "2",
        ),
        (
            {"from_email": _OTHER_SENDER, "subject": "Another subject", "mail_type": "PAYMENT_REMINDER"},
            "2",
        ),
        ({"from_email": _OTHER_SENDER, "subject": "", "mail_type": "PAYMENT_REMINDER"}, "2"),
        ({"from_email": _OTHER_SENDER, "subject": None, "mail_type": "PAYMENT_REMINDER"}, "2"),
        ({"from_email": _OTHER_SENDER, "mail_type": "PAYMENT_REMINDER"}, "2"),
        # Same subject but not a reminder type.
        ({"from_email": _OTHER_SENDER, "subject": _SUBJECT, "mail_type": "INVOICE"}, "2"),
        # Same sender (case-insensitive) counts whatever the subject.
        (
            {"from_email": "BILLING@Vendor.Example", "subject": "Unrelated", "mail_type": "DEADLINE_REMINDER"},
            "3",
        ),
    ],
)
def test_r1_reminder_chain_matches_other_senders_by_normalized_subject(
    tmp_path: Path,
    with_subject_key: bool,
    candidate: dict[str, object],
    expected_chain: str,
) -> None:
    db_path = _new_db(tmp_path, with_subject_key=with_subject_key)
    with sqlite3.connect(db_path) as conn:
        _insert_baseline_chain(conn)
        _insert_legacy_email_received(
            conn, ts=_NOW - timedelta(days=3), payload={"account_email": ACCOUNT, **candidate}
        )
        conn.commit()

    assert _reminder_chain_detail(db_path, subject=_SUBJECT, from_email=_SENDER) == expected_chain


@pytest.mark.parametrize("with_subject_key", [False, True])
def test_r1_reminder_chain_window_is_fourteen_days(
    tmp_path: Path, with_subject_key: bool
) -> None:
    db_path = _new_db(tmp_path, with_subject_key=with_subject_key)
    with sqlite3.connect(db_path) as conn:
        _insert_baseline_chain(conn)
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=20),
            payload={"from_email": _OTHER_SENDER, "subject": _SUBJECT, "mail_type": "PAYMENT_REMINDER"},
        )
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=13),
            payload={"from_email": _OTHER_SENDER, "subject": "Re: " + _SUBJECT, "mail_type": "PAYMENT_REMINDER"},
        )
        conn.commit()

    assert _reminder_chain_detail(db_path, subject=_SUBJECT, from_email=_SENDER) == "3"


@pytest.mark.parametrize("with_subject_key", [False, True])
def test_r1_reminder_chain_cyrillic_subjects_compare_case_insensitively(
    tmp_path: Path, with_subject_key: bool
) -> None:
    db_path = _new_db(tmp_path, with_subject_key=with_subject_key)
    with sqlite3.connect(db_path) as conn:
        for days_ago, subject in ((1, "Re: НАПОМИНАНИЕ  об оплате №7"), (2, "напоминание об оплате №7")):
            _insert_legacy_email_received(
                conn,
                ts=_NOW - timedelta(days=days_ago),
                payload={"from_email": _OTHER_SENDER, "subject": subject, "mail_type": "PAYMENT_REMINDER"},
            )
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=3),
            payload={"from_email": _OTHER_SENDER, "subject": "Напоминание об оплате №8", "mail_type": "PAYMENT_REMINDER"},
        )
        conn.commit()

    assert (
        _reminder_chain_detail(db_path, subject="Напоминание об оплате №7", from_email=_SENDER)
        == "2"
    )


@pytest.mark.parametrize("with_subject_key", [False, True])
def test_r1_blank_current_subject_counts_every_reminder(
    tmp_path: Path, with_subject_key: bool
) -> None:
    db_path = _new_db(tmp_path, with_subject_key=with_subject_key)
    with sqlite3.connect(db_path) as conn:
        _insert_baseline_chain(conn)
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=3),
            payload={"from_email": _OTHER_SENDER, "subject": "Another subject", "mail_type": "PAYMENT_REMINDER"},
        )
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=4),
            payload={"from_email": _OTHER_SENDER, "subject": "Another subject", "mail_type": "INVOICE"},
        )
        conn.commit()

    assert _reminder_chain_detail(db_path, subject="   ", from_email=_SENDER) == "3"


@pytest.mark.parametrize("with_subject_key", [False, True])
def test_r1_blank_current_sender_counts_every_reminder(
    tmp_path: Path, with_subject_key: bool
) -> None:
    db_path = _new_db(tmp_path, with_subject_key=with_subject_key)
    with sqlite3.connect(db_path) as conn:
        _insert_baseline_chain(conn)
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=3),
            payload={"from_email": _OTHER_SENDER, "subject": "Another subject", "mail_type": "PAYMENT_REMINDER"},
        )
        _insert_legacy_email_received(
            conn,
            ts=_NOW - timedelta(days=4),
            payload={"from_email": "third@sender.example", "subject": "", "mail_type": "REMINDER_FIRST"},
        )
        conn.commit()

    assert _reminder_chain_detail(db_path, subject=_SUBJECT, from_email="") == "4"


# --- R2: uncertainty queue ---------------------------------------------------


def test_r2_uncertainty_queue_items_on_raw_priority_decisions(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path)
    now_ts = _NOW.timestamp()
    rows = [
        # (account, sender, subject, confidence, hours_ago)
        (ACCOUNT, "first@vendor.example", "Contract renewal", 50, 1),
        (ACCOUNT, "second@vendor.example", "Счёт №9 к оплате", 65, 2),
        (ACCOUNT, "confident@vendor.example", "Weekly report", 80, 3),
        (ACCOUNT, "negative@vendor.example", "Negative confidence", -5, 4),
        (ACCOUNT, "clamped@vendor.example", "Clamped confidence", 150, 5),
        (ACCOUNT, "broken@vendor.example", "Broken confidence", "n/a", 6),
        (ACCOUNT, "old@vendor.example", "Too old", 10, 72),
        (OTHER_ACCOUNT, "scoped@vendor.example", "Other account", 10, 0.5),
    ]
    with sqlite3.connect(db_path) as conn:
        for account, sender, subject, confidence, hours_ago in rows:
            email_id = _insert_email(
                conn,
                account_email=account,
                from_email=sender,
                subject=subject,
                received_at=(_NOW - timedelta(hours=hours_ago)).isoformat(),
            )
            _insert_v1_event(
                conn,
                event_type="priority_decision_recorded",
                ts_utc=now_ts - hours_ago * 3600,
                account_id=account,
                email_id=email_id,
                payload={
                    "priority": "🟡",
                    "confidence": confidence,
                    "sender": sender,
                    "subject": subject,
                    "engine": "priority_v2",
                },
            )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    since_ts = now_ts - 24 * 3600

    assert analytics.uncertainty_queue_items(
        ACCOUNT, since_ts=since_ts, min_confidence=70, limit=10
    ) == [
        {"sender": "first@vendor.example", "subject": "Contract renewal", "confidence": 50},
        {"sender": "second@vendor.example", "subject": "Счёт №9 к оплате", "confidence": 65},
        {"sender": "negative@vendor.example", "subject": "Negative confidence", "confidence": 0},
    ]
    assert analytics.uncertainty_queue_items(
        ACCOUNT, since_ts=since_ts, min_confidence=101, limit=2
    ) == [
        {"sender": "first@vendor.example", "subject": "Contract renewal", "confidence": 50},
        {"sender": "second@vendor.example", "subject": "Счёт №9 к оплате", "confidence": 65},
    ]
    assert analytics.uncertainty_queue_items(
        ACCOUNT,
        account_emails=[ACCOUNT, OTHER_ACCOUNT],
        since_ts=since_ts,
        min_confidence=101,
        limit=10,
    ) == [
        {"sender": "scoped@vendor.example", "subject": "Other account", "confidence": 10},
        {"sender": "first@vendor.example", "subject": "Contract renewal", "confidence": 50},
        {"sender": "second@vendor.example", "subject": "Счёт №9 к оплате", "confidence": 65},
        {"sender": "confident@vendor.example", "subject": "Weekly report", "confidence": 80},
        {"sender": "negative@vendor.example", "subject": "Negative confidence", "confidence": 0},
        {"sender": "clamped@vendor.example", "subject": "Clamped confidence", "confidence": 100},
    ]


# --- R3: weekly attention words ----------------------------------------------


def test_r3_weekly_attention_entities_on_raw_email_received(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path)
    now_ts = datetime.now(timezone.utc).timestamp()

    def live_payload(sender: str, subject: object) -> dict[str, object]:
        return {
            "from_email": sender,
            "subject": subject,
            "attachments_count": 0,
            "body_chars": 120,
            "word_count": 20,
            "occurred_at_utc": now_ts,
            "thread_key": "thread-1",
            "engine": "priority_v2",
        }

    def backfill_payload(sender: str, subject: object, body_summary: object) -> dict[str, object]:
        return {
            "from_email": sender,
            "subject": subject,
            "body_summary": body_summary,
            "attachments_count": 1,
        }

    events = [
        # summary wins over the subject
        (ACCOUNT, backfill_payload("summary@vendor.example", "Ignored subject words here", "Hello big world"), 1),
        # blank summary falls back to the subject
        (ACCOUNT, backfill_payload("summary@vendor.example", "Two words", "   "), 2),
        # a non-blank summary without words counts as 0 and hides the subject
        (ACCOUNT, backfill_payload("punctuation@vendor.example", "Lots of subject words", "!!!"), 3),
        # nothing to count: the sender is skipped entirely
        (ACCOUNT, backfill_payload("blank@vendor.example", "", ""), 4),
        (ACCOUNT, backfill_payload("blank@vendor.example", None, None), 5),
        # live payloads carry no body_summary
        (ACCOUNT, live_payload("live@vendor.example", "Счёт №123 оплачен_до пятницы"), 6),
        (ACCOUNT, live_payload("Upper@Vendor.example", "One two"), 7),
        (ACCOUNT, live_payload("lower@vendor.example", "Three four"), 8),
        # no sender: skipped
        (ACCOUNT, live_payload("", "Orphan subject"), 9),
        # outside the window or the account scope
        (ACCOUNT, live_payload("stale@vendor.example", "Old words"), 24 * 10),
        (OTHER_ACCOUNT, live_payload("scoped@vendor.example", "Scoped words here"), 10),
    ]
    with sqlite3.connect(db_path) as conn:
        for account, payload, hours_ago in events:
            _insert_v1_event(
                conn,
                event_type="email_received",
                ts_utc=now_ts - hours_ago * 3600,
                account_id=account,
                payload=payload,
            )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    assert analytics.weekly_attention_entities(account_email=ACCOUNT, days=7) == [
        {"entity": "summary@vendor.example", "words": 5},
        {"entity": "live@vendor.example", "words": 4},
        {"entity": "lower@vendor.example", "words": 2},
        {"entity": "Upper@Vendor.example", "words": 2},
        {"entity": "punctuation@vendor.example", "words": 0},
    ]
    scoped = analytics.weekly_attention_entities(
        account_email=ACCOUNT, account_emails=[ACCOUNT, OTHER_ACCOUNT], days=7
    )
    assert {"entity": "scoped@vendor.example", "words": 3} in scoped


# --- R4: sender trust score --------------------------------------------------


def _relationship_db(tmp_path: Path) -> Path:
    db_path = _new_db(tmp_path)
    base_ts = datetime(2026, 10, 10, 10, 0, tzinfo=timezone.utc).timestamp()

    def live(sender: str, subject: str) -> dict[str, object]:
        return {
            "from_email": sender,
            "subject": subject,
            "attachments_count": 0,
            "body_chars": 40,
            "word_count": 7,
            "occurred_at_utc": base_ts,
            "thread_key": "thread-x",
            "engine": "priority_v2",
        }

    trusted = "accounts@vendor.example"
    marker_address = "paid-invoices@billing.example"
    late = "late@vendor.example"
    with sqlite3.connect(db_path) as conn:
        for sender in (trusted, trusted, marker_address, late):
            _insert_email(
                conn,
                from_email=sender,
                subject="stored subject",
                received_at="2026-10-10T10:00:00+00:00",
            )
        _insert_email(
            conn,
            from_email=trusted,
            subject="older",
            received_at="2026-10-01T10:00:00+00:00",
        )
        v1_rows = [
            ("email_received", live(trusted, "Invoice paid, thank you")),  # +1
            ("email_received", live(trusted, "Thanks for the FAST REPLY")),  # +1
            ("email_received", live(trusted, "Dispute: wrong amount")),  # -1
            ("email_received", live(trusted, "Hello")),  # 0
            (
                "email_received",
                {  # backfill row: marker split between subject and summary, +1
                    "from_email": trusted,
                    "subject": "Invoice 7",
                    "body_summary": "It was paid yesterday",
                    "attachments_count": 1,
                },
            ),
            (
                "message_interpretation",
                {
                    "sender_email": trusted,
                    "doc_kind": "invoice",
                    "amount": 1200.0,
                    "due_date": "2026-10-20",
                    "priority": "🟡",
                    "action": "Pay invoice",
                    "confidence": 0.9,
                    "context": "",
                    "document_id": "INV-7",
                },
            ),
            # markers inside the address itself: every row of this sender counts
            ("email_received", live(marker_address, "Hello")),
            ("email_received", live(marker_address, "Statement")),
            ("email_received", live(late, "Invoice paid")),  # +1
            (
                "commitment_expired",
                {"commitment_id": 1, "from_email": late, "deadline_iso": "2026-10-01"},
            ),  # overdue, -2
        ]
        for offset, (event_type, payload) in enumerate(v1_rows):
            _insert_v1_event(
                conn, event_type=event_type, ts_utc=base_ts + offset, payload=payload
            )
        conn.commit()
    return db_path


@pytest.mark.parametrize(
    ("sender", "expected"),
    [
        (
            "accounts@vendor.example",
            {
                "emails_count": 3,
                "invoice_count": 1,
                "contract_count": 0,
                "overdue_count": 0,
                "last_contact_days": 5,
                "trust_score": 2,
            },
        ),
        (
            "paid-invoices@billing.example",
            {
                "emails_count": 1,
                "invoice_count": 0,
                "contract_count": 0,
                "overdue_count": 0,
                "last_contact_days": 5,
                "trust_score": 2,
            },
        ),
        (
            "late@vendor.example",
            {
                "emails_count": 1,
                "invoice_count": 0,
                "contract_count": 0,
                "overdue_count": 1,
                "last_contact_days": 5,
                "trust_score": 0,
            },
        ),
    ],
)
def test_r4_sender_relationship_profile_on_raw_events(
    tmp_path: Path, sender: str, expected: dict[str, int]
) -> None:
    db_path = _relationship_db(tmp_path)
    profile = KnowledgeAnalytics(db_path).sender_relationship_profile(
        account_email=ACCOUNT,
        sender_email=sender,
        now=datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc),
    )

    assert profile is not None
    assert profile["sender_email"] == sender
    assert {key: profile[key] for key in expected} == expected


def test_r4_top_sender_profiles_keep_order_and_scores(tmp_path: Path) -> None:
    db_path = _relationship_db(tmp_path)
    profiles = KnowledgeAnalytics(db_path).top_sender_relationship_profiles(
        account_email=ACCOUNT,
        days=30,
        limit=5,
        now=datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc),
        include_raw_senders=True,
    )

    assert [
        (profile["sender_email"], profile["emails_count"], profile["trust_score"])
        for profile in profiles
    ] == [
        ("accounts@vendor.example", 3, 2),
        ("late@vendor.example", 1, 0),
        ("paid-invoices@billing.example", 1, 2),
    ]
