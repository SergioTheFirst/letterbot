"""R1: a queue stage transition (PARSE -> LLM -> TG) is one transaction.

The finished stage's row is replaced by the next stage's row atomically. A
crash or a failed write between the two used to leave an emails row with no
queue row: status NEW, never notified, never re-ingested (the next poll skips
it as a duplicate).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.tests import test_delivery_invariants as _invariants
from mailbot_v26.tests.test_delivery_invariants import (  # noqa: F401 - fixtures
    _email_row,
    _isolated_runtime,
    _make_ready,
    _queue_rows,
    _seed_email,
    _setup,
)

# Shared fixtures, re-exported by assignment so pytest discovers them here.
events = _invariants.events
telegram = _invariants.telegram


def _fail_inserts_of(storage: Storage, stage: str) -> None:
    """Every INSERT of a `stage` queue row fails, like a crash mid-transition."""
    with storage.conn:
        storage.conn.execute(
            f"""
            CREATE TRIGGER fail_{stage}_insert BEFORE INSERT ON queue
            WHEN NEW.stage = '{stage}'
            BEGIN SELECT RAISE(ABORT, 'injected {stage} insert failure'); END;
            """
        )


def _allow_inserts_of(storage: Storage, stage: str) -> None:
    with storage.conn:
        storage.conn.execute(f"DROP TRIGGER fail_{stage}_insert")


def _queue_detail(storage: Storage) -> list[tuple[str, int, str | None]]:
    return [
        (str(row[0]), int(row[1]), row[2])
        for row in storage.conn.execute(
            "SELECT stage, attempts, not_before FROM queue ORDER BY id"
        ).fetchall()
    ]


def _stored_email(storage: Storage) -> int:
    email_id, created = storage.persist_email_and_enqueue(
        account_email="account@example.com",
        uid=1,
        message_id="<m1@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at="2024-01-02T10:00:00+00:00",
        attachments_count=0,
    )
    assert created
    return email_id


def test_advance_stage_replaces_the_row_in_one_step(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id = _stored_email(storage)
    item = storage.claim_next(["PARSE"])
    assert item is not None

    storage.advance_stage(
        item["queue_id"], email_id, "LLM", not_before="2030-01-01T00:00:00"
    )

    # The next stage starts with a fresh attempt budget.
    assert _queue_detail(storage) == [("LLM", 0, "2030-01-01T00:00:00")]
    llm_row = storage.conn.execute("SELECT id FROM queue").fetchone()
    storage.advance_stage(int(llm_row[0]), email_id, "TG")
    assert _queue_detail(storage) == [("TG", 0, None)]


def test_failed_advance_keeps_the_finished_row(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id = _stored_email(storage)
    item = storage.claim_next(["PARSE"])
    assert item is not None
    _fail_inserts_of(storage, "LLM")

    with pytest.raises(sqlite3.DatabaseError, match="injected"):
        storage.advance_stage(item["queue_id"], email_id, "LLM")

    # Rolled back as a whole: the email is still queued, nothing half-done.
    assert _queue_detail(storage) == [("PARSE", 1, None)]
    assert not storage.conn.in_transaction
    _allow_inserts_of(storage, "LLM")
    storage.advance_stage(item["queue_id"], email_id, "LLM")
    assert _queue_detail(storage) == [("LLM", 0, None)]


@pytest.mark.parametrize(("stage", "next_stage"), [("PARSE", "LLM"), ("LLM", "TG")])
def test_failed_transition_keeps_the_email_queued_and_retries_it(
    tmp_path, monkeypatch, telegram, events, stage, next_stage
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage=stage)
    _fail_inserts_of(storage, next_stage)

    start_module._process_queue(storage, config, legacy_processor, flags)

    # Not lost: the finished stage stays queued and waits for its retry.
    rows = _queue_rows(storage)
    assert [(row_stage, attempts) for row_stage, attempts, _ in rows] == [(stage, 1)]
    assert "injected" in str(rows[0][2])
    assert _email_row(storage, email_id)[0] == "NEW"
    assert telegram.calls == 0

    _allow_inserts_of(storage, next_stage)
    _make_ready(storage)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(telegram.email_messages()) == 1
    assert _queue_rows(storage) == []
    assert _email_row(storage, email_id)[2]
