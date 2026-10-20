"""WP3a step 3 (R2-R4): event readers give the same answers on scrubbed payloads.

WP3a replaces raw subjects and summaries in events_v1 payloads with derived
facts (mailbot_v26.events.privacy). Each reader must return for a scrubbed row
exactly what it returned for the raw row, and keep reading the raw rows older
releases left behind (pinned by test_privacy_reader_characterization.py):

- R2 uncertainty queue: the subject comes from the email's own row,
- R3 weekly attention: ``summary_words`` / ``subject_words``,
- R4 sender trust score: ``text_markers`` (words of events.privacy.TEXT_MARKERS).

Each database test reads a raw database, scrubs it in place the way WP3a
writers store payloads, and reads it again.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.events.privacy import scrub_email_received, scrub_payload
from mailbot_v26.storage import analytics as analytics_module
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.tests.test_privacy_reader_characterization import (
    _NOW,
    ACCOUNT,
    OTHER_ACCOUNT,
    _insert_email,
    _insert_v1_event,
    _new_db,
    _relationship_db,
)


def _scrub_in_place(db_path: Path, *, every_other: bool = False) -> None:
    """Rewrite events_v1 payloads the way WP3a writers store them.

    Rows go through scrub_payload and are serialized like EventEmitter.emit
    (json.dumps(..., ensure_ascii=False)). ``every_other`` scrubs only even
    row ids (a database the migration has not finished yet).
    """
    row_filter = " WHERE id % 2 = 0" if every_other else ""
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute(
            "SELECT id, event_type, payload, payload_json FROM events_v1" + row_filter
        ).fetchall()
        for row_id, event_type, payload, payload_json in rows:
            scrubbed = scrub_payload(event_type, json.loads(payload_json or payload))
            text = json.dumps(scrubbed, ensure_ascii=False)
            conn.execute(
                "UPDATE events_v1 SET payload = ?, payload_json = ? WHERE id = ?",
                (text, text, row_id),
            )
        conn.commit()
        stored = [
            str(text)
            for (text,) in conn.execute("SELECT COALESCE(payload_json, payload) FROM events_v1")
        ]
    assert stored
    raw_rows = [text for text in stored if '"subject"' in text or '"body_summary"' in text]
    if every_other:
        assert 0 < len(raw_rows) < len(stored)
    else:
        assert raw_rows == []


# --- R2: uncertainty queue ---------------------------------------------------

_R2_SINCE = _NOW.timestamp() - 24 * 3600


def _priority_payload(sender: str, subject: str, confidence: object) -> dict[str, object]:
    return {
        "priority": "🟡",
        "confidence": confidence,
        "sender": sender,
        "subject": subject,
        "engine": "priority_v2",
    }


def _uncertainty_db(tmp_path: Path) -> Path:
    db_path = _new_db(tmp_path)
    rows = [
        # (account, sender, subject, confidence, hours_ago)
        (ACCOUNT, "first@vendor.example", "Contract renewal", 50, 1),
        (ACCOUNT, "second@vendor.example", "Счёт №9 к оплате", 65, 2),
        (ACCOUNT, "confident@vendor.example", "Weekly report", 80, 3),
        (ACCOUNT, "negative@vendor.example", "Negative confidence", -5, 4),
        (ACCOUNT, "broken@vendor.example", "Broken confidence", "n/a", 6),
        (ACCOUNT, "old@vendor.example", "Too old", 10, 72),
        (OTHER_ACCOUNT, "scoped@vendor.example", "Other account", 10, 0.5),
    ]
    with closing(sqlite3.connect(db_path)) as conn:
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
                ts_utc=_NOW.timestamp() - hours_ago * 3600,
                account_id=account,
                email_id=email_id,
                payload=_priority_payload(sender, subject, confidence),
            )
        conn.commit()
    return db_path


def _uncertainty_views(db_path: Path) -> list[list[dict[str, object]]]:
    analytics = KnowledgeAnalytics(db_path)
    return [
        analytics.uncertainty_queue_items(
            ACCOUNT, since_ts=_R2_SINCE, min_confidence=70, limit=10
        ),
        analytics.uncertainty_queue_items(
            ACCOUNT, since_ts=_R2_SINCE, min_confidence=101, limit=2
        ),
        analytics.uncertainty_queue_items(
            ACCOUNT,
            account_emails=[ACCOUNT, OTHER_ACCOUNT],
            since_ts=_R2_SINCE,
            min_confidence=101,
            limit=10,
        ),
    ]


@pytest.mark.parametrize("every_other", [False, True])
def test_r2_scrubbed_events_list_the_same_items(tmp_path: Path, every_other: bool) -> None:
    db_path = _uncertainty_db(tmp_path)
    before = _uncertainty_views(db_path)

    _scrub_in_place(db_path, every_other=every_other)

    assert _uncertainty_views(db_path) == before
    assert before[0] == [
        {"sender": "first@vendor.example", "subject": "Contract renewal", "confidence": 50},
        {"sender": "second@vendor.example", "subject": "Счёт №9 к оплате", "confidence": 65},
        {"sender": "negative@vendor.example", "subject": "Negative confidence", "confidence": 0},
    ]


def test_r2_subject_is_the_stored_email_subject(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path)
    with closing(sqlite3.connect(db_path)) as conn:
        email_id = _insert_email(
            conn,
            from_email="a@vendor.example",
            subject="Stored subject",
            received_at=_NOW.isoformat(),
        )
        _insert_v1_event(
            conn,
            event_type="priority_decision_recorded",
            ts_utc=_NOW.timestamp(),
            email_id=email_id,
            payload=_priority_payload("a@vendor.example", "Event subject", 40),
        )
        conn.commit()

    items = KnowledgeAnalytics(db_path).uncertainty_queue_items(
        ACCOUNT, since_ts=_R2_SINCE, min_confidence=70, limit=5
    )

    assert items == [{"sender": "a@vendor.example", "subject": "Stored subject", "confidence": 40}]


def test_r2_events_without_an_email_row(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path)
    with closing(sqlite3.connect(db_path)) as conn:
        for hours_ago, email_id, payload in (
            (1, 999, _priority_payload("raw@vendor.example", "Raw only", 10)),
            (2, None, _priority_payload("noid@vendor.example", "No email id", 20)),
            (
                3,
                998,
                scrub_payload(
                    EventType.PRIORITY_DECISION_RECORDED,
                    _priority_payload("scrubbed@vendor.example", "Gone", 30),
                ),
            ),
        ):
            _insert_v1_event(
                conn,
                event_type="priority_decision_recorded",
                ts_utc=_NOW.timestamp() - hours_ago * 3600,
                email_id=email_id,
                payload=payload,
            )
        conn.commit()

    items = KnowledgeAnalytics(db_path).uncertainty_queue_items(
        ACCOUNT, since_ts=_R2_SINCE, min_confidence=70, limit=5
    )

    # Unmigrated rows keep their own subject; a scrubbed row of a deleted
    # email is still listed, without a subject.
    assert items == [
        {"sender": "raw@vendor.example", "subject": "Raw only", "confidence": 10},
        {"sender": "noid@vendor.example", "subject": "No email id", "confidence": 20},
        {"sender": "scrubbed@vendor.example", "subject": "", "confidence": 30},
    ]


def test_r2_database_without_emails_table_keeps_payload_subjects(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "events_only.sqlite"
    emitter = ContractEventEmitter(db_path)
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_DECISION_RECORDED,
            ts_utc=_NOW.timestamp(),
            account_id=ACCOUNT,
            entity_id=None,
            email_id=7,
            payload=_priority_payload("a@vendor.example", "Payload subject", 30),
        )
    )

    with caplog.at_level(logging.WARNING):
        items = KnowledgeAnalytics(db_path).uncertainty_queue_items(
            ACCOUNT, since_ts=_R2_SINCE, min_confidence=70, limit=5
        )

    assert items == [{"sender": "a@vendor.example", "subject": "Payload subject", "confidence": 30}]
    assert any(
        "email_subject_lookup_failed" in record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING
    )


def test_r2_resolves_every_subject_of_a_long_queue(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path)
    count = 1_100  # more ids than one SQL parameter batch
    with closing(sqlite3.connect(db_path)) as conn:
        for index in range(count):
            email_id = _insert_email(
                conn,
                from_email=f"s{index}@vendor.example",
                subject=f"Stored {index}",
                received_at=_NOW.isoformat(),
            )
            _insert_v1_event(
                conn,
                event_type="priority_decision_recorded",
                ts_utc=_NOW.timestamp() - index,
                email_id=email_id,
                payload=scrub_payload(
                    EventType.PRIORITY_DECISION_RECORDED,
                    _priority_payload(f"s{index}@vendor.example", f"Event {index}", 10),
                ),
            )
        conn.commit()

    items = KnowledgeAnalytics(db_path).uncertainty_queue_items(
        ACCOUNT, since_ts=0, min_confidence=101, limit=count + 10
    )

    assert [item["subject"] for item in items] == [f"Stored {index}" for index in range(count)]


# --- R3: weekly attention words ----------------------------------------------


def _attention_db(tmp_path: Path) -> Path:
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
        (ACCOUNT, backfill_payload("summary@vendor.example", "Ignored subject words here", "Hello big world"), 1),
        (ACCOUNT, backfill_payload("summary@vendor.example", "Two words", "   "), 2),
        (ACCOUNT, backfill_payload("punctuation@vendor.example", "Lots of subject words", "!!!"), 3),
        (ACCOUNT, backfill_payload("blank@vendor.example", "", ""), 4),
        (ACCOUNT, backfill_payload("blank@vendor.example", None, None), 5),
        (ACCOUNT, live_payload("live@vendor.example", "Счёт №123 оплачен_до пятницы"), 6),
        (ACCOUNT, live_payload("live@vendor.example", "\tTabbed\nline "), 7),
        (ACCOUNT, live_payload("Upper@Vendor.example", "One two"), 8),
        (ACCOUNT, live_payload("lower@vendor.example", "Three four"), 9),
        (ACCOUNT, live_payload("nosubject@vendor.example", None), 10),
        (ACCOUNT, live_payload("", "Orphan subject"), 11),
        (ACCOUNT, live_payload("stale@vendor.example", "Old words"), 24 * 10),
        (OTHER_ACCOUNT, live_payload("scoped@vendor.example", "Scoped words here"), 12),
    ]
    with closing(sqlite3.connect(db_path)) as conn:
        for account, payload, hours_ago in events:
            _insert_v1_event(
                conn,
                event_type="email_received",
                ts_utc=now_ts - hours_ago * 3600,
                account_id=account,
                payload=payload,
            )
        conn.commit()
    return db_path


def _attention_views(db_path: Path) -> list[list[dict[str, object]]]:
    analytics = KnowledgeAnalytics(db_path)
    return [
        analytics.weekly_attention_entities(account_email=ACCOUNT, days=7),
        analytics.weekly_attention_entities(
            account_email=ACCOUNT, account_emails=[ACCOUNT, OTHER_ACCOUNT], days=7
        ),
    ]


@pytest.mark.parametrize("every_other", [False, True])
def test_r3_scrubbed_payloads_give_the_same_word_counts(
    tmp_path: Path, every_other: bool
) -> None:
    db_path = _attention_db(tmp_path)
    before = _attention_views(db_path)

    _scrub_in_place(db_path, every_other=every_other)

    assert _attention_views(db_path) == before
    assert before[0] == [
        {"entity": "live@vendor.example", "words": 6},
        {"entity": "summary@vendor.example", "words": 5},
        {"entity": "lower@vendor.example", "words": 2},
        {"entity": "Upper@Vendor.example", "words": 2},
        {"entity": "punctuation@vendor.example", "words": 0},
    ]


def test_r3_malformed_derived_counts_fall_through(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path)
    now_ts = datetime.now(timezone.utc).timestamp()
    # Writers store non-negative ints; anything else is ignored, never raised.
    payloads = [
        {"from_email": "a@vendor.example", "summary_words": "seven", "subject_words": 3},
        {"from_email": "b@vendor.example", "summary_words": True, "subject_words": -1},
        {"from_email": "c@vendor.example", "summary_words": None, "subject": "Raw words here"},
        {"from_email": "d@vendor.example", "subject_words": 0, "text_markers": []},
    ]
    with closing(sqlite3.connect(db_path)) as conn:
        for index, payload in enumerate(payloads, start=1):
            _insert_v1_event(
                conn, event_type="email_received", ts_utc=now_ts - index, payload=payload
            )
        conn.commit()

    assert KnowledgeAnalytics(db_path).weekly_attention_entities(
        account_email=ACCOUNT, days=7
    ) == [
        {"entity": "a@vendor.example", "words": 3},
        {"entity": "c@vendor.example", "words": 3},
        {"entity": "d@vendor.example", "words": 0},
    ]


# --- R4: sender trust score --------------------------------------------------

_R4_NOW = datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)
_R4_SENDERS = (
    "accounts@vendor.example",
    "paid-invoices@billing.example",
    "late@vendor.example",
)


def _relationship_views(db_path: Path) -> tuple[object, ...]:
    analytics = KnowledgeAnalytics(db_path)
    profiles = [
        analytics.sender_relationship_profile(
            account_email=ACCOUNT, sender_email=sender, now=_R4_NOW
        )
        for sender in _R4_SENDERS
    ]
    top = analytics.top_sender_relationship_profiles(
        account_email=ACCOUNT, days=30, limit=5, now=_R4_NOW, include_raw_senders=True
    )
    return (profiles, top)


def _trust_scores(db_path: Path) -> dict[str, int]:
    analytics = KnowledgeAnalytics(db_path)
    scores: dict[str, int] = {}
    for sender in _R4_SENDERS:
        profile = analytics.sender_relationship_profile(
            account_email=ACCOUNT, sender_email=sender, now=_R4_NOW
        )
        assert profile is not None
        scores[sender] = int(profile["trust_score"])
    return scores


@pytest.mark.parametrize("every_other", [False, True])
def test_r4_scrubbed_events_keep_sender_profiles(tmp_path: Path, every_other: bool) -> None:
    db_path = _relationship_db(tmp_path)
    before = _relationship_views(db_path)

    _scrub_in_place(db_path, every_other=every_other)

    assert _relationship_views(db_path) == before
    assert _trust_scores(db_path) == {
        "accounts@vendor.example": 2,
        "paid-invoices@billing.example": 2,
        "late@vendor.example": 0,
    }


def test_r4_emitted_scrubbed_payloads_score_like_raw_ones(tmp_path: Path) -> None:
    # Written through the real emitter: its serialization must keep the
    # '"from_email": "<sender>"' text the sender LIKE filter looks for.
    db_path = _new_db(tmp_path)
    raw_sender = "raw.sender@vendor.example"
    scrubbed_sender = "scrubbed.sender@vendor.example"
    subjects = ["Invoice paid", "Thanks for the FAST reply", "Dispute", "\x0cast reply", "Hello"]
    with closing(sqlite3.connect(db_path)) as conn:
        for sender in (raw_sender, scrubbed_sender):
            _insert_email(
                conn, from_email=sender, subject="stored", received_at=_R4_NOW.isoformat()
            )
        conn.commit()
    emitter = ContractEventEmitter(db_path)
    for offset, subject in enumerate(subjects):
        for sender, transform in ((raw_sender, dict), (scrubbed_sender, scrub_email_received)):
            emitter.emit(
                EventV1(
                    event_type=EventType.EMAIL_RECEIVED,
                    ts_utc=_R4_NOW.timestamp() + offset,
                    account_id=ACCOUNT,
                    entity_id=None,
                    email_id=None,
                    payload=transform(
                        {"from_email": sender, "subject": subject, "attachments_count": 0}
                    ),
                )
            )
    analytics = KnowledgeAnalytics(db_path)

    def comparable(sender: str) -> dict[str, object]:
        profile = analytics.sender_relationship_profile(
            account_email=ACCOUNT, sender_email=sender, now=_R4_NOW
        )
        assert profile is not None
        return {
            key: value
            for key, value in profile.items()
            if not key.startswith("sender_")
        }

    assert comparable(scrubbed_sender) == comparable(raw_sender)
    assert comparable(raw_sender)["trust_score"] == 2


@pytest.mark.parametrize("scrubbed", [False, True])
def test_r4_trust_score_reads_only_text_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scrubbed: bool
) -> None:
    # The trust score may only test words that scrubbed payloads keep
    # (events.privacy.TEXT_MARKERS); without "paid" no row is a paid invoice.
    db_path = _relationship_db(tmp_path)
    if scrubbed:
        _scrub_in_place(db_path)
    monkeypatch.setattr(
        analytics_module, "TEXT_MARKERS", ("dispute", "fast reply", "invoice")
    )

    assert _trust_scores(db_path) == {
        "accounts@vendor.example": 0,
        "paid-invoices@billing.example": 0,
        "late@vendor.example": 0,
    }
