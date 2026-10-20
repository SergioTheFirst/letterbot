"""WP3a step 2: the events backfill stores scrubbed EMAIL_RECEIVED payloads and
dedupes them by (event_type, email_id), so re-running it on an older or an
already scrubbed database never adds duplicates."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import (
    EventType,
    EventV1,
    fingerprint,
    fingerprint_fields,
)
from mailbot_v26.events.privacy import scrub_payload
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tools.backfill_events import run_backfill

ACCOUNT = "owner@example.com"
SUBJECT_CANARY = "SUBJECT-CANARY-31337"
BODY_CANARY = "BODY-CANARY-27182"
_RECEIVED = [
    datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc),
    datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc),
    datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc),
]


def _db_with_emails(tmp_path: Path) -> tuple[Path, list[dict[str, object]]]:
    db_path = tmp_path / "backfill.sqlite"
    db = KnowledgeDB(db_path)
    emails: list[dict[str, object]] = []
    for index, received in enumerate(_RECEIVED):
        row = {
            "from_email": f"sender{index}@vendor.example",
            "subject": f"Invoice {index} paid {SUBJECT_CANARY}",
            "body_summary": f"Thanks for the fast reply {BODY_CANARY}",
            "received_at": received,
            "deferred": index == 0,
        }
        email_id = db.save_email(
            account_email=ACCOUNT,
            from_email=str(row["from_email"]),
            subject=str(row["subject"]),
            received_at=received.isoformat(),
            priority="🔵",
            action_line="Check",
            body_summary=str(row["body_summary"]),
            raw_body="",
            attachment_summaries=[],
            deferred_for_digest=bool(row["deferred"]),
        )
        assert email_id is not None
        row["id"] = email_id
        emails.append(row)
    return db_path, emails


def _insert_v1(
    conn: sqlite3.Connection, event: EventV1, *, fp: str | None = None
) -> None:
    text = json.dumps(event.payload, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type, ts_utc, ts, account_id, entity_id, email_id,
            payload, payload_json, schema_version, fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_type.value,
            event.ts_utc,
            datetime.fromtimestamp(event.ts_utc, tz=timezone.utc).isoformat(),
            event.account_id,
            event.entity_id,
            event.email_id,
            text,
            text,
            event.schema_version,
            fp or fingerprint(event),
        ),
    )


def _old_backfill_event(email: dict[str, object]) -> EventV1:
    """EMAIL_RECEIVED exactly as releases before WP3a backfilled it."""
    return EventV1(
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=email["received_at"].timestamp(),
        account_id=ACCOUNT,
        entity_id=None,
        email_id=int(email["id"]),
        payload={
            "from_email": email["from_email"],
            "subject": email["subject"],
            "body_summary": email["body_summary"],
            "attachments_count": 0,
        },
    )


def _live_event(email: dict[str, object]) -> EventV1:
    """EMAIL_RECEIVED as the processor emitted it before WP3a."""
    received_ts = email["received_at"].timestamp()
    return EventV1(
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=received_ts,
        account_id=ACCOUNT,
        entity_id="entity-1",
        email_id=int(email["id"]),
        payload={
            "from_email": email["from_email"],
            "subject": email["subject"],
            "attachments_count": 0,
            "body_chars": 120,
            "word_count": 20,
            "occurred_at_utc": received_ts,
            "thread_key": f"thread-{email['id']}",
            "engine": "priority_v2",
        },
    )


def _old_deferred_event(email: dict[str, object]) -> EventV1:
    return EventV1(
        event_type=EventType.ATTENTION_DEFERRED_FOR_DIGEST,
        ts_utc=email["received_at"].timestamp(),
        account_id=ACCOUNT,
        entity_id=None,
        email_id=int(email["id"]),
        payload={"reason": "backfill", "attachments_only": False, "attachments_count": 0},
    )


def _scrubbed_in_place(event: EventV1) -> tuple[EventV1, str]:
    """The row after the WP3a scrub migration: derived payload, new fingerprint."""
    payload = scrub_payload(event.event_type, event.payload)
    scrubbed = EventV1(
        event_type=event.event_type,
        ts_utc=event.ts_utc,
        account_id=event.account_id,
        entity_id=event.entity_id,
        email_id=event.email_id,
        payload=payload,
    )
    fp = fingerprint_fields(
        event_type=event.event_type.value,
        ts_utc=event.ts_utc,
        account_id=event.account_id,
        entity_id=event.entity_id,
        email_id=event.email_id,
        payload=payload,
        schema_version=event.schema_version,
    )
    return scrubbed, fp


def _email_received_rows(db_path: Path) -> list[tuple[int, str, str]]:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT email_id, payload, payload_json FROM events_v1 "
            "WHERE event_type = 'email_received' ORDER BY email_id, id"
        ).fetchall()


def _events_v1_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0])


def _all_event_text(db_path: Path) -> str:
    with sqlite3.connect(db_path) as conn:
        v1 = conn.execute("SELECT payload, payload_json FROM events_v1").fetchall()
        legacy = conn.execute("SELECT payload FROM events").fetchall()
    return "\n".join(str(value) for row in (*v1, *legacy) for value in row)


def test_backfill_stores_scrubbed_email_received_payloads(tmp_path: Path) -> None:
    db_path, emails = _db_with_emails(tmp_path)

    stats = run_backfill(db_path)

    rows = _email_received_rows(db_path)
    assert [row[0] for row in rows] == [email["id"] for email in emails]
    for (_email_id, payload, payload_json), email in zip(rows, emails, strict=True):
        assert payload == payload_json
        assert json.loads(payload) == {
            "from_email": email["from_email"],
            "attachments_count": 0,
            "subject_words": 6,  # "Invoice 0 paid SUBJECT-CANARY-31337"
            "summary_words": 8,  # "Thanks for the fast reply BODY-CANARY-27182"
            "text_markers": ["fast reply", "invoice", "paid"],
        }
    text = _all_event_text(db_path)
    assert SUBJECT_CANARY not in text and BODY_CANARY not in text
    # three EMAIL_RECEIVED plus one ATTENTION_DEFERRED_FOR_DIGEST
    assert (stats.inserted, stats.attempted) == (4, 4)


def test_backfill_rerun_on_its_own_output_inserts_nothing(tmp_path: Path) -> None:
    db_path, _ = _db_with_emails(tmp_path)
    run_backfill(db_path)
    before = _events_v1_count(db_path)

    stats = run_backfill(db_path, force=True)

    assert stats.inserted == 0
    assert _events_v1_count(db_path) == before


def test_backfill_on_db_backfilled_by_an_older_release_inserts_nothing(
    tmp_path: Path,
) -> None:
    db_path, emails = _db_with_emails(tmp_path)
    with sqlite3.connect(db_path) as conn:
        for email in emails:
            _insert_v1(conn, _old_backfill_event(email))
        _insert_v1(conn, _old_deferred_event(emails[0]))
        conn.commit()
    before = _events_v1_count(db_path)

    stats = run_backfill(db_path)  # no events_backfill_state row, like the live DB

    assert stats.inserted == 0
    assert _events_v1_count(db_path) == before


def test_backfill_skips_emails_that_already_have_live_events(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path, emails = _db_with_emails(tmp_path)
    with sqlite3.connect(db_path) as conn:
        _insert_v1(conn, _live_event(emails[0]))
        _insert_v1(conn, _old_deferred_event(emails[0]))
        _insert_v1(conn, _live_event(emails[1]))
        # an unrelated event type does not count as the email's EMAIL_RECEIVED
        _insert_v1(
            conn,
            EventV1(
                event_type=EventType.PRIORITY_DECISION_RECORDED,
                ts_utc=emails[2]["received_at"].timestamp(),
                account_id=ACCOUNT,
                entity_id=None,
                email_id=int(emails[2]["id"]),
                payload={"priority": "🔵", "confidence": 50, "sender": "x", "engine": "rules"},
            ),
        )
        conn.commit()
    caplog.set_level(logging.INFO, logger="mailbot")

    stats = run_backfill(db_path)

    assert stats.inserted == 1
    rows = _email_received_rows(db_path)
    assert [row[0] for row in rows] == [email["id"] for email in emails]
    assert "subject" not in json.loads(rows[2][1])
    assert "events_backfill_email_received_skipped" in caplog.text


def test_backfill_on_scrubbed_db_inserts_nothing(tmp_path: Path) -> None:
    db_path, emails = _db_with_emails(tmp_path)
    with sqlite3.connect(db_path) as conn:
        for event in (
            _live_event(emails[0]),
            _old_backfill_event(emails[1]),
            _live_event(emails[2]),
        ):
            scrubbed, fp = _scrubbed_in_place(event)
            _insert_v1(conn, scrubbed, fp=fp)
        _insert_v1(conn, _old_deferred_event(emails[0]))
        conn.commit()
    before = _events_v1_count(db_path)

    stats = run_backfill(db_path, force=True)

    assert stats.inserted == 0
    assert _events_v1_count(db_path) == before
    assert SUBJECT_CANARY not in _all_event_text(db_path)
