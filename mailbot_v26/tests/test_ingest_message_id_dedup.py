"""WP2c: Message-ID dedup after a UIDVALIDITY change.

A new UIDVALIDITY epoch renumbers the mailbox. Rows of the old epoch must not
swallow new mail that reuses their UIDs (F6), and mail already notified under
its old UID must not be sent again (A10). Message-IDs are stored normalized;
rows of older epochs are retired from their UIDs before the poll persists
anything; a settled Message-ID is skipped before the newest-N cut; an
undelivered orphan is re-attached to its new UID; a second copy in the same
batch is skipped; mail without a Message-ID is treated as new.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26 import imap_client
from mailbot_v26.bot_core.storage import Storage, normalize_message_id
from mailbot_v26.config_loader import IngestConfig
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.pipeline.processor import InboundMessage
from mailbot_v26.state_manager import StateManager
from mailbot_v26.tests.test_imap_client import (
    _FakeIMAPClient,
    _body_fetches,
    _install_fake,
)
from mailbot_v26.tests.test_imap_refetch import raw_email, server_item
from mailbot_v26.tests.test_ingest_cursor_commit import (
    LOGIN,
    START,
    _account,
    _imap_health_payloads,
    _install_main,
    _saved_state,
)
from mailbot_v26.tests.test_storage_atomic_persist import clear_pipeline_caches

OLD_EPOCH = 10
NEW_EPOCH = 20
_HEADER_FIELDS_KEY = b"BODY[HEADER.FIELDS (MESSAGE-ID)]"


@pytest.fixture(autouse=True)
def _clean_pipeline_cache():
    clear_pipeline_caches()
    yield
    clear_pipeline_caches()


def _store(
    storage: Storage,
    uid: int,
    message_id: str | None,
    *,
    uidvalidity: int | None = OLD_EPOCH,
    dedupe: bool = False,
    account_email: str = LOGIN,
) -> tuple[int, bool]:
    extra = {"dedupe_message_id": True} if dedupe else {}
    return storage.persist_email_and_enqueue(
        account_email=account_email,
        uid=uid,
        message_id=message_id,
        from_email=None,
        from_name=None,
        subject=None,
        received_at=None,
        attachments_count=0,
        uidvalidity=uidvalidity,
        **extra,
    )


def _drop_queue(storage: Storage, email_id: int) -> None:
    with storage.conn:
        storage.conn.execute("DELETE FROM queue WHERE email_id = ?", (email_id,))


def _notified(storage: Storage, email_id: int) -> None:
    storage.mark_telegram_delivered(email_id)
    _drop_queue(storage, email_id)


def _row(storage: Storage, email_id: int) -> tuple:
    return storage.conn.execute(
        "SELECT uid, uidvalidity, status, message_id FROM emails WHERE id = ?",
        (email_id,),
    ).fetchone()


def _queued(storage: Storage, email_id: int) -> list[str]:
    rows = storage.conn.execute(
        "SELECT stage FROM queue WHERE email_id = ? ORDER BY id", (email_id,)
    ).fetchall()
    return [str(row[0]) for row in rows]


def _email_count(storage: Storage) -> int:
    return int(storage.conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0])


def _mailbox(message_ids: dict[int, str | None]) -> dict[int, dict[bytes, object]]:
    """Server items inside the resync window, oldest UID first."""
    return {
        uid: server_item(
            raw_email(message_id, subject=f"Mail {uid}"),
            internaldate=START - timedelta(hours=1) + timedelta(minutes=uid),
        )
        for uid, message_id in message_ids.items()
    }


def _resync_client(
    monkeypatch, tmp_path: Path, mailbox, *, max_messages: int = 5
) -> tuple[_FakeIMAPClient, StateManager, ResilientIMAP]:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = NEW_EPOCH
    fake.set_messages(mailbox)
    _install_fake(monkeypatch, fake)
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(LOGIN, 10)
    state.update_uidvalidity(LOGIN, OLD_EPOCH, mailbox="INBOX")
    client = ResilientIMAP(
        _account(),
        state,
        start_time=START,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=max_messages,
    )
    return fake, state, client


def _old_and_new(new_uids: set[int], count: int = 12) -> dict[int, str | None]:
    return {
        uid: f"<new{uid}@example.com>" if uid in new_uids else f"<old{uid}@example.com>"
        for uid in range(1, count + 1)
    }


def _run_resync_poll(
    monkeypatch,
    tmp_path: Path,
    *,
    notified: dict[int, str],
    new_epoch: dict[int, str | None],
    orphans: dict[int, str] | None = None,
    max_messages: int = 5,
):
    """One worker cycle after the mailbox was rebuilt as NEW_EPOCH.

    `notified` mail was stored and delivered in OLD_EPOCH at those UIDs;
    `orphans` were stored then but never queued nor notified. The queue is
    not processed, so every PARSE row left is a pending send.
    """
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = NEW_EPOCH
    fake.set_messages(_mailbox(new_epoch))
    config, sent = _install_main(monkeypatch, tmp_path, fake)
    config.ingest = IngestConfig(
        first_run_bootstrap_hours=24, first_run_bootstrap_max_messages=max_messages
    )
    storage = Storage(config.storage.db_path)
    for uid, message_id in notified.items():
        email_id, _ = _store(storage, uid, message_id, uidvalidity=OLD_EPOCH)
        _notified(storage, email_id)
    for uid, message_id in (orphans or {}).items():
        email_id, _ = _store(storage, uid, message_id, uidvalidity=OLD_EPOCH)
        _drop_queue(storage, email_id)
    storage.close()
    seeded = StateManager(tmp_path / "state.json")
    seeded.update_last_uid(LOGIN, max(notified))
    seeded.update_uidvalidity(LOGIN, OLD_EPOCH, mailbox="INBOX")
    seeded.save(force=True)

    start_module.main(config_dir=tmp_path, max_cycles=1)
    return config, sent


def _pending_sends(db_path: Path) -> list[tuple[int, str]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT e.uid, e.message_id FROM queue q JOIN emails e ON e.id = q.email_id"
            " WHERE q.stage = 'PARSE' ORDER BY e.uid"
        ).fetchall()
    return [(int(uid), str(message_id)) for uid, message_id in rows]


def _uidvalidity_change_events(db_path: Path) -> list[dict]:
    return [
        payload
        for payload in _imap_health_payloads(db_path)
        if payload.get("subtype") == "uidvalidity_change"
    ]


# ---- storage: normalized Message-ID ----


def test_message_id_is_stored_normalized(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")

    folded, _ = _store(storage, 1, "\r\n\t<AbC.1@Example.COM>  ")
    blank, _ = _store(storage, 2, " \r\n ")
    absent, _ = _store(storage, 3, None)

    # folding whitespace goes; angle brackets and case stay
    assert _row(storage, folded)[3] == "<AbC.1@Example.COM>"
    assert _row(storage, blank)[3] is None
    assert _row(storage, absent)[3] is None
    assert normalize_message_id(" <a@x>\r\n") == "<a@x>"
    assert imap_client._normalize_message_id(" <a@x>\r\n") == "<a@x>"


def test_open_normalizes_raw_message_ids_and_indexes_them(tmp_path: Path) -> None:
    db_path = tmp_path / "old.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT NOT NULL,
                uid INTEGER NOT NULL,
                message_id TEXT,
                from_email TEXT,
                from_name TEXT,
                subject TEXT,
                received_at TEXT,
                attachments_count INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                error_last TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                telegram_delivered_at TEXT,
                UNIQUE(account_email, uid)
            )
            """
        )
        # rows stored before WP2c kept the raw (folded) header
        conn.executemany(
            "INSERT INTO emails (account_email, uid, message_id, status, created_at,"
            " updated_at) VALUES (?, ?, ?, 'NEW', '', '')",
            [
                (LOGIN, 1, "\r\n\t<folded@example.com>"),
                (LOGIN, 2, "<plain@example.com>"),
                (LOGIN, 3, None),
            ],
        )

    storage = Storage(db_path)

    rows = storage.conn.execute("SELECT message_id FROM emails ORDER BY id").fetchall()
    assert [row[0] for row in rows] == [
        "<folded@example.com>",
        "<plain@example.com>",
        None,
    ]
    plan = storage.conn.execute(
        "EXPLAIN QUERY PLAN SELECT id FROM emails"
        " WHERE account_email = ? AND message_id = ?",
        (LOGIN, "<folded@example.com>"),
    ).fetchall()
    assert any("idx_emails_account_message_id" in str(step) for step in plan)


# ---- storage: retire the old UID epoch (F6) ----


def test_retire_uid_epoch_detaches_rows_of_other_epochs(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    old, _ = _store(storage, 1, "<a@x>", uidvalidity=OLD_EPOCH)
    legacy, _ = _store(storage, 2, "<b@x>", uidvalidity=None)
    current, _ = _store(storage, 3, "<c@x>", uidvalidity=NEW_EPOCH)
    other, _ = _store(storage, 1, "<a@x>", account_email="other@example.com")

    assert storage.retire_uid_epoch(LOGIN, NEW_EPOCH) == 2

    assert _row(storage, old)[0] == -old
    assert _row(storage, legacy)[0] == -legacy
    assert _row(storage, current)[0] == 3
    assert _row(storage, other)[0] == 1
    # rows, Message-IDs and queue rows stay
    assert _row(storage, old)[3] == "<a@x>"
    assert _queued(storage, old) == ["PARSE"]
    assert storage.retire_uid_epoch(LOGIN, NEW_EPOCH) == 0
    # the new epoch's UID 1 is a new message, not the old row
    assert storage.find_email_id(LOGIN, 1) is None
    new_id, created = _store(storage, 1, "<new@x>", uidvalidity=NEW_EPOCH)
    assert created
    assert new_id != old


def test_uidvalidity_change_keeps_new_mail_on_a_reused_uid(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    # F6: the old row at UID 1 matched the new epoch's UID 1, so the new
    # message was skipped as a duplicate and never notified.
    with caplog.at_level(logging.INFO):
        config, sent = _run_resync_poll(
            monkeypatch,
            tmp_path,
            notified={1: "<old1@example.com>"},
            new_epoch={1: "<new1@example.com>"},
        )

    assert _pending_sends(config.storage.db_path) == [(1, "<new1@example.com>")]
    assert sent == []
    assert [event["retired_rows"] for event in _uidvalidity_change_events(
        config.storage.db_path
    )] == [1]
    retired = [r.getMessage() for r in caplog.records if "imap_uid_epoch_retired" in r.getMessage()]
    assert len(retired) == 1
    assert "rows=1" in retired[0]
