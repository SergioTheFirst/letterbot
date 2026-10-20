"""WP2a F5: the email row and its PARSE queue row are stored in one
transaction, so a crash between them cannot strand an email that the next
poll would skip as a duplicate forever."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.pipeline.processor import InboundMessage

LOGIN = "primary@example.com"
RECEIVED_AT = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc).isoformat()


def persist(storage: Storage, uid: int) -> tuple[int, bool]:
    return start_module._persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email=LOGIN,
        uid=uid,
        message_id=f"<{uid}@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=RECEIVED_AT,
        attachments_count=0,
        raw_email=b"raw",
        inbound=InboundMessage(subject="Subject", body="Body", sender="s@example.com"),
    )


def row_counts(db_path: Path) -> tuple[int, int]:
    """(emails, PARSE queue rows)."""
    with sqlite3.connect(db_path) as conn:
        emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        queued = conn.execute(
            "SELECT COUNT(*) FROM queue WHERE stage = 'PARSE'"
        ).fetchone()[0]
    return int(emails), int(queued)


def clear_pipeline_caches() -> None:
    for cache in (
        start_module.PIPELINE_CACHE,
        start_module.PIPELINE_INBOUND_CACHE,
        start_module.PIPELINE_RAW_CACHE,
    ):
        cache.clear()


@pytest.fixture(autouse=True)
def _clean_pipeline_cache():
    clear_pipeline_caches()
    yield
    clear_pipeline_caches()


def test_persist_email_and_enqueue_is_atomic(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    # A crash between the email insert and the queue insert left an email row
    # with no queue row; the next poll then skipped it as a duplicate forever.
    storage.conn.execute(
        "CREATE TRIGGER fail_enqueue BEFORE INSERT ON queue "
        "BEGIN SELECT RAISE(ABORT, 'simulated crash'); END;"
    )

    with pytest.raises(sqlite3.Error):
        persist(storage, 5)

    assert row_counts(storage.db_path) == (0, 0)
    assert start_module.PIPELINE_CACHE == {}

    storage.conn.execute("DROP TRIGGER fail_enqueue")
    email_id, created = persist(storage, 5)
    duplicate_id, created_again = persist(storage, 5)

    assert created is True
    assert (duplicate_id, created_again) == (email_id, False)
    assert row_counts(storage.db_path) == (1, 1)
    assert email_id in start_module.PIPELINE_CACHE


def test_duplicate_persist_leaves_stored_row_untouched(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id, _ = persist(storage, 7)
    storage.mark_done(
        storage.conn.execute("SELECT id FROM queue WHERE email_id = ?", (email_id,))
        .fetchone()[0]
    )
    storage.set_email_error(email_id, "dead letter")

    duplicate_id, created = persist(storage, 7)

    status = storage.conn.execute(
        "SELECT status FROM emails WHERE id = ?", (email_id,)
    ).fetchone()[0]
    assert (duplicate_id, created) == (email_id, False)
    assert status == "ERROR"
    assert row_counts(storage.db_path) == (1, 0)
