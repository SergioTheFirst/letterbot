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

import json
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
    _is_header_fetch,
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


def _mailbox(
    message_ids: dict[int, str | None], *, since_start: bool = False
) -> dict[int, dict[bytes, object]]:
    """Server items inside the resync window, oldest UID first. since_start:
    received after the process started, so polls after the resync see them."""
    first = START if since_start else START - timedelta(hours=1)
    return {
        uid: server_item(
            raw_email(message_id, subject=f"Mail {uid}"),
            internaldate=first + timedelta(minutes=uid),
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
    config, sent = _seed_resync(
        monkeypatch,
        tmp_path,
        fake,
        notified=notified,
        orphans=orphans,
        ingest=IngestConfig(
            first_run_bootstrap_hours=24, first_run_bootstrap_max_messages=max_messages
        ),
    )
    start_module.main(config_dir=tmp_path, max_cycles=1)
    return config, sent


def _seed_resync(
    monkeypatch,
    tmp_path: Path,
    fake: _FakeIMAPClient,
    *,
    notified: dict[int, str],
    ingest: IngestConfig,
    orphans: dict[int, str] | None = None,
):
    """Install the worker on `fake` with mail of OLD_EPOCH already stored
    (see _run_resync_poll) and the cursor at the last notified UID."""
    config, sent = _install_main(monkeypatch, tmp_path, fake)
    config.ingest = ingest
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
    return config, sent


def _cap_fetch(monkeypatch, limit: int) -> None:
    """The worker fetches at most `limit` messages per poll."""
    original = ResilientIMAP.fetch_new_messages

    def _capped(self, **kwargs):
        return original(self, **{**kwargs, "limit": limit})

    monkeypatch.setattr(ResilientIMAP, "fetch_new_messages", _capped)


class _HeaderGlitchIMAPClient(_FakeIMAPClient):
    """Answers header batches without the UIDs in `omit_headers`."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.omit_headers: set[int] = set()

    def fetch(self, uids, fields):  # type: ignore[override]
        data = super().fetch(uids, fields)
        if not _is_header_fetch(fields):
            return data
        return {uid: item for uid, item in data.items() if uid not in self.omit_headers}


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


# ---- settled Message-IDs: skipped in the resync poll ----


@pytest.mark.parametrize(
    "settle", ["delivered", "delivery_unknown", "delivery_failed", "queued", "reserved"]
)
def test_settled_message_id_is_known(tmp_path: Path, settle: str) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id, _ = _store(storage, 1, "<m@x>")
    if settle != "queued":
        _drop_queue(storage, email_id)
    if settle == "delivered":
        storage.mark_telegram_delivered(email_id)
    elif settle == "delivery_unknown":
        storage.set_email_delivery_unknown(email_id, "delivery_unknown")
    elif settle == "delivery_failed":
        storage.set_email_delivery_failed(email_id, "chat not found")
    elif settle == "reserved":
        # sent or maybe sent (at-most-once): never again
        storage.reserve_telegram_delivery(
            delivery_key=f"email:{email_id}",
            email_id=email_id,
            account_id=LOGIN,
            chat_id="chat",
        )
    storage.retire_uid_epoch(LOGIN, NEW_EPOCH)

    assert storage.is_known_message_id(LOGIN, " <m@x>\r\n")
    assert not storage.is_known_message_id("other@example.com", "<m@x>")


@pytest.mark.parametrize("status", ["NEW", "ERROR"])
def test_undelivered_orphan_message_id_is_not_known(tmp_path: Path, status: str) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id, _ = _store(storage, 1, "<m@x>")
    _drop_queue(storage, email_id)
    if status == "ERROR":
        storage.set_email_error(email_id, "context_unavailable:gone")

    assert not storage.is_known_message_id(LOGIN, "<m@x>")
    assert not storage.is_known_message_id(LOGIN, None)
    assert not storage.is_known_message_id(LOGIN, "<unknown@x>")


def test_header_probe_matches_the_message_id_stored_from_the_body(
    monkeypatch, tmp_path: Path
) -> None:
    # Exchange folds Message-ID onto its own line. The poll reads it from the
    # header batch; the worker stores what the full message says. Both forms
    # must meet, or the resync filter never recognises the mail.
    raw = (
        b"Subject: Folded\r\nFrom: a@example.com\r\n"
        b"Message-ID:\r\n <folded.1@example.com>\r\n\r\nBody\r\n"
    )
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.set_messages(
        {
            1: {
                b"RFC822": raw,
                b"RFC822.SIZE": len(raw),
                b"INTERNALDATE": START + timedelta(minutes=1),
                _HEADER_FIELDS_KEY: b"Message-ID:\r\n <folded.1@example.com>\r\n\r\n",
            }
        }
    )
    _install_fake(monkeypatch, fake)
    storage = Storage(tmp_path / "db.sqlite")
    client = ResilientIMAP(_account(), StateManager(tmp_path / "state.json"), START)

    [(uid, body)] = client.fetch_new_messages()
    stored_from_body = start_module.message_from_bytes(body).get("Message-ID")
    email_id, _ = _store(storage, uid, stored_from_body)

    assert stored_from_body != client.last_message_ids[uid]
    assert storage.is_known_message_id(LOGIN, client.last_message_ids[uid])
    assert _row(storage, email_id)[3] == client.last_message_ids[uid]


def test_resync_skips_settled_message_ids_before_the_newest_n_cut(
    monkeypatch, tmp_path: Path
) -> None:
    # 10 notified + 2 new, newest 5 allowed: the new mail is not among the
    # newest 5, so filtering after the cut would lose it and resend old mail.
    fake, state, client = _resync_client(
        monkeypatch, tmp_path, _mailbox(_old_and_new({3, 7}))
    )
    probed: list[str] = []

    def _known(message_id: str) -> bool:
        probed.append(message_id)
        return message_id.startswith("<old")

    messages = client.fetch_new_messages(is_known_message_id=_known)
    commit = client.commit_cursor([uid for uid, _ in messages])

    assert [uid for uid, _ in messages] == [3, 7]
    assert _body_fetches(fake) == [[3], [7]]
    assert sorted(probed) == sorted(_old_and_new({3, 7}).values())
    assert commit.cursor == 12
    assert state.get_uidvalidity(LOGIN) == NEW_EPOCH


def test_message_id_probe_is_not_consulted_without_uidvalidity_change(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = OLD_EPOCH
    fake.set_messages(
        {
            uid: server_item(
                raw_email(f"<m{uid}@x>"), internaldate=START + timedelta(minutes=uid)
            )
            for uid in (1, 2)
        }
    )
    _install_fake(monkeypatch, fake)
    state = StateManager(tmp_path / "state.json")
    state.update_uidvalidity(LOGIN, OLD_EPOCH, mailbox="INBOX")
    probed: list[str] = []

    client = ResilientIMAP(_account(), state, start_time=START)
    messages = client.fetch_new_messages(
        is_known_message_id=lambda mid: probed.append(mid) or True
    )

    assert [uid for uid, _ in messages] == [1, 2]
    assert probed == []


def test_resync_mail_without_message_id_is_fetched_as_new_and_logged(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    _fake, _state, client = _resync_client(
        monkeypatch, tmp_path, _mailbox({1: "<old1@example.com>", 2: None})
    )

    with caplog.at_level(logging.WARNING, logger=imap_client.__name__):
        messages = client.fetch_new_messages(is_known_message_id=lambda _mid: True)

    assert [uid for uid, _ in messages] == [2]
    missing = [r.getMessage() for r in caplog.records if "message_id_missing" in r.getMessage()]
    assert len(missing) == 1
    assert "count=1" in missing[0]


def test_message_id_probe_failure_fails_the_poll_and_keeps_the_epoch(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    # Failing open let settled mail take the newest-5 slots, so the new mail
    # at UIDs 3 and 7 fell outside them and was skipped for good.
    fake, state, client = _resync_client(
        monkeypatch, tmp_path, _mailbox(_old_and_new({3, 7}))
    )

    def _broken(_message_id: str) -> bool:
        raise sqlite3.OperationalError("database is locked")

    with caplog.at_level(logging.WARNING, logger=imap_client.__name__):
        with pytest.raises(sqlite3.OperationalError):
            client.fetch_new_messages(is_known_message_id=_broken)

    assert "imap_known_message_id_probe_failed" in caplog.text
    assert _body_fetches(fake) == []
    assert client.commit_cursor([]) is None
    assert (state.get_last_uid(LOGIN), state.get_uidvalidity(LOGIN)) == (10, OLD_EPOCH)

    retry = ResilientIMAP(
        _account(),
        state,
        start_time=START,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=5,
    )
    messages = retry.fetch_new_messages(
        is_known_message_id=lambda message_id: message_id.startswith("<old")
    )

    assert retry.last_uidvalidity_changed is True
    assert [uid for uid, _ in messages] == [3, 7]


def test_failed_resync_probe_never_lets_carried_old_mail_be_resent(
    monkeypatch, tmp_path: Path
) -> None:
    # Epoch 10 notified 4 mails since the start. The mailbox is rebuilt as
    # epoch 20: the same mails at UIDs 1-4, new mail at 5 and 6. One message
    # per poll, no bootstrap window, so the resync spans polls. Failing open,
    # the first poll fetched old UID 1, stored the new UIDVALIDITY and carried
    # 2-6 into polls that no longer checked Message-IDs: old mail sent again.
    old_ids = {uid: f"<old{uid}@example.com>" for uid in range(1, 5)}
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = NEW_EPOCH
    fake.set_messages(
        _mailbox(
            {**old_ids, 5: "<new5@example.com>", 6: "<new6@example.com>"},
            since_start=True,
        )
    )
    config, _sent = _seed_resync(
        monkeypatch,
        tmp_path,
        fake,
        notified=old_ids,
        ingest=IngestConfig(
            first_run_bootstrap_hours=0, first_run_bootstrap_max_messages=0
        ),
    )
    _cap_fetch(monkeypatch, limit=1)
    working_probe = Storage.is_known_message_id

    def _locked(_self, _account_email, _message_id):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(Storage, "is_known_message_id", _locked)
    start_module.main(config_dir=tmp_path, max_cycles=1)

    db_path = config.storage.db_path
    assert _pending_sends(db_path) == []
    saved = _saved_state(tmp_path)["accounts"][LOGIN]
    assert (saved["last_uid"], saved["uidvalidity"]) == (4, OLD_EPOCH)

    monkeypatch.setattr(Storage, "is_known_message_id", working_probe)
    for _poll in range(3):
        # the failed poll set a backoff; the next ones are due at once
        (tmp_path / "runtime_health.json").unlink(missing_ok=True)
        start_module.main(config_dir=tmp_path, max_cycles=1)

    assert _pending_sends(db_path) == [
        (5, "<new5@example.com>"),
        (6, "<new6@example.com>"),
    ]
    with sqlite3.connect(db_path) as conn:
        total, retired = conn.execute(
            "SELECT COUNT(*), SUM(uid < 0) FROM emails"
        ).fetchone()
    assert (total, retired) == (6, 4)
    saved = _saved_state(tmp_path)["accounts"][LOGIN]
    assert (saved["last_uid"], saved["uidvalidity"]) == (6, NEW_EPOCH)


def test_uidvalidity_change_sends_only_new_mail(monkeypatch, tmp_path: Path) -> None:
    # Epoch 10 notified 10 mails at UIDs 1-10. The mailbox is rebuilt as
    # epoch 20: the same 10 mails plus 2 new ones at UIDs 3 and 7 - UIDs the
    # old rows held. Newest 5 allowed.
    old_ids = [f"<old{n}@example.com>" for n in range(1, 11)]
    remaining = iter(old_ids)
    new_epoch = {
        uid: f"<new{uid}@example.com>" if uid in (3, 7) else next(remaining)
        for uid in range(1, 13)
    }

    config, sent = _run_resync_poll(
        monkeypatch,
        tmp_path,
        notified=dict(enumerate(old_ids, start=1)),
        new_epoch=new_epoch,
    )

    assert _pending_sends(config.storage.db_path) == [
        (3, "<new3@example.com>"),
        (7, "<new7@example.com>"),
    ]
    assert sent == []
    with sqlite3.connect(config.storage.db_path) as conn:
        total, retired = conn.execute(
            "SELECT COUNT(*), SUM(uid < 0) FROM emails"
        ).fetchone()
    assert (total, retired) == (12, 10)
    saved = _saved_state(tmp_path)["accounts"][LOGIN]
    assert (saved["last_uid"], saved["uidvalidity"]) == (12, NEW_EPOCH)
    assert [event["retired_rows"] for event in _uidvalidity_change_events(
        config.storage.db_path
    )] == [10]


# ---- persist during a resync: re-attach, same batch, no Message-ID ----


def test_resync_persist_skips_a_settled_message_id(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    old, _ = _store(storage, 1, "<m@x>")
    _notified(storage, old)
    storage.retire_uid_epoch(LOGIN, NEW_EPOCH)

    email_id, created = _store(storage, 5, "<m@x>", uidvalidity=NEW_EPOCH, dedupe=True)

    assert (email_id, created) == (old, False)
    assert _email_count(storage) == 1
    assert _queued(storage, old) == []
    assert _row(storage, old)[0] == -old


def test_resync_persist_reattaches_an_undelivered_orphan(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    old, _ = _store(storage, 1, "<m@x>")
    _drop_queue(storage, old)
    storage.set_email_error(old, "context_unavailable:gone")
    storage.retire_uid_epoch(LOGIN, NEW_EPOCH)

    email_id, created = _store(
        storage, 5, "\r\n <m@x>", uidvalidity=NEW_EPOCH, dedupe=True
    )

    assert (email_id, created) == (old, True)
    assert _row(storage, old) == (5, NEW_EPOCH, "NEW", "<m@x>")
    assert _queued(storage, old) == ["PARSE"]
    assert storage.find_email_id(LOGIN, 5) == old
    assert _email_count(storage) == 1


def test_resync_persist_skips_a_second_copy_in_the_same_batch(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")

    first, created_first = _store(
        storage, 3, "<dup@x>", uidvalidity=NEW_EPOCH, dedupe=True
    )
    second, created_second = _store(
        storage, 4, "<dup@x>", uidvalidity=NEW_EPOCH, dedupe=True
    )

    assert created_first
    assert (second, created_second) == (first, False)
    assert _email_count(storage) == 1


def test_resync_persist_stores_mail_without_message_id_as_new(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")

    first, created_first = _store(storage, 3, None, uidvalidity=NEW_EPOCH, dedupe=True)
    second, created_second = _store(
        storage, 4, None, uidvalidity=NEW_EPOCH, dedupe=True
    )

    assert created_first and created_second
    assert first != second


def test_normal_poll_persist_does_not_dedupe_by_message_id(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")

    first, _ = _store(storage, 3, "<dup@x>")
    second, created = _store(storage, 4, "<dup@x>")

    assert created
    assert second != first


def test_reattached_orphan_gets_its_pipeline_context(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    orphan, _ = _store(storage, 4, "<m@x>")
    _drop_queue(storage, orphan)
    storage.retire_uid_epoch(LOGIN, NEW_EPOCH)

    email_id, created = start_module._persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email=LOGIN,
        uid=9,
        message_id="<m@x>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=None,
        attachments_count=0,
        raw_email=b"raw",
        inbound=InboundMessage(subject="Subject", body="Body", sender="s@example.com"),
        uidvalidity=NEW_EPOCH,
        dedupe_message_id=True,
    )

    assert (email_id, created) == (orphan, True)
    assert start_module.PIPELINE_CACHE[orphan].uid == 9
    assert start_module.PIPELINE_RAW_CACHE[orphan] == b"raw"


def test_resync_poll_dedupes_copies_and_reattaches_orphans(
    monkeypatch, tmp_path: Path
) -> None:
    # Two copies of one new mail arrive in the resync batch, and a mail whose
    # old row was never notified (orphan) shows up under a new UID.
    config, sent = _run_resync_poll(
        monkeypatch,
        tmp_path,
        notified={1: "<old1@example.com>"},
        orphans={2: "<orphan@example.com>"},
        new_epoch={
            1: "<old1@example.com>",
            2: "<copy@example.com>",
            3: "<copy@example.com>",
            4: "<orphan@example.com>",
        },
    )

    assert _pending_sends(config.storage.db_path) == [
        (2, "<copy@example.com>"),
        (4, "<orphan@example.com>"),
    ]
    assert sent == []
    with sqlite3.connect(config.storage.db_path) as conn:
        rows = conn.execute(
            "SELECT message_id, COUNT(*) FROM emails GROUP BY message_id ORDER BY 1"
        ).fetchall()
    # the orphan row was re-attached, the second copy not stored
    assert rows == [
        ("<copy@example.com>", 1),
        ("<old1@example.com>", 1),
        ("<orphan@example.com>", 1),
    ]
    assert _saved_state(tmp_path)["accounts"][LOGIN]["last_uid"] == 4
    # re-attached mail is processed from this poll's content
    assert 4 in {ctx.uid for ctx in start_module.PIPELINE_CACHE.values()}


# ---- a resync spans polls until the cursor reaches the epoch's last UID ----


def test_resync_epoch_is_a_top_level_state_key_that_survives_a_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    state = StateManager(path)
    state.set_resync_epoch(LOGIN.upper(), NEW_EPOCH)
    state.save(force=True)

    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["resync_epoch"] == {LOGIN: NEW_EPOCH}
    assert StateManager(path).get_resync_epoch(LOGIN) == NEW_EPOCH

    state.set_resync_epoch(LOGIN, None)
    state.save(force=True)
    assert StateManager(path).get_resync_epoch(LOGIN) is None

    raw["resync_epoch"] = {"bad@example.com": "x", LOGIN: OLD_EPOCH}
    path.write_text(json.dumps(raw), encoding="utf-8")
    reloaded = StateManager(path)
    assert reloaded.get_resync_epoch("bad@example.com") is None
    assert reloaded.get_resync_epoch(LOGIN) == OLD_EPOCH


def test_resync_stays_on_until_the_cursor_reaches_the_highest_uid(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    # 3 notified + 3 new since the start, one message per poll: UIDs 5 and 6
    # are fetched after the new UIDVALIDITY is stored, still as a resync.
    _fake, state, client = _resync_client(
        monkeypatch,
        tmp_path,
        _mailbox(_old_and_new({4, 5, 6}, count=6), since_start=True),
        max_messages=0,
    )
    probed: list[str] = []

    def _known(message_id: str) -> bool:
        probed.append(message_id)
        return message_id.startswith("<old")

    def _poll(limit: int) -> tuple[ResilientIMAP, list[int]]:
        poll = ResilientIMAP(_account(), state, start_time=START)
        messages = poll.fetch_new_messages(limit=limit, is_known_message_id=_known)
        poll.commit_cursor([uid for uid, _ in messages])
        return poll, [uid for uid, _ in messages]

    first = client.fetch_new_messages(limit=1, is_known_message_id=_known)
    client.commit_cursor([uid for uid, _ in first])
    assert [uid for uid, _ in first] == [4]
    assert client.last_message_id_resync is True
    assert (state.get_last_uid(LOGIN), state.get_uidvalidity(LOGIN)) == (4, NEW_EPOCH)
    assert state.get_resync_epoch(LOGIN) == NEW_EPOCH

    probed.clear()
    second, uids = _poll(limit=1)
    assert uids == [5]
    assert second.last_uidvalidity_changed is False
    assert second.last_message_id_resync is True
    assert probed == ["<new5@example.com>", "<new6@example.com>"]
    assert state.get_resync_epoch(LOGIN) == NEW_EPOCH

    with caplog.at_level(logging.INFO, logger=imap_client.__name__):
        _third, uids = _poll(limit=1)
    assert uids == [6]
    assert state.get_resync_epoch(LOGIN) is None
    assert "imap_resync_complete" in caplog.text

    probed.clear()
    fourth, uids = _poll(limit=1)
    assert (uids, probed, fourth.last_message_id_resync) == ([], [], False)


def test_resync_marker_of_another_epoch_is_dropped(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = NEW_EPOCH
    fake.set_messages(_mailbox({1: "<m1@example.com>"}, since_start=True))
    _install_fake(monkeypatch, fake)
    state = StateManager(tmp_path / "state.json")
    state.update_uidvalidity(LOGIN, NEW_EPOCH, mailbox="INBOX")
    state.set_resync_epoch(LOGIN, OLD_EPOCH)
    probed: list[str] = []

    client = ResilientIMAP(_account(), state, start_time=START)
    with caplog.at_level(logging.WARNING, logger=imap_client.__name__):
        messages = client.fetch_new_messages(
            is_known_message_id=lambda mid: probed.append(mid) or True
        )
        client.commit_cursor([uid for uid, _ in messages])

    assert ([uid for uid, _ in messages], probed) == ([1], [])
    assert client.last_message_id_resync is False
    assert state.get_resync_epoch(LOGIN) is None
    assert "imap_resync_marker_dropped" in caplog.text


def test_resync_dedupes_old_mail_whose_headers_were_missing_in_the_change_poll(
    monkeypatch, tmp_path: Path
) -> None:
    # The server answers the change poll's header batch without UIDs 1-2, so
    # their Message-IDs cannot be checked then and the cursor stays before
    # them. The next poll, after the new UIDVALIDITY was stored, sees UIDs
    # 1-4 again; checking Message-IDs only in the change poll stored all
    # three notified mails (UID 3 was filtered once) as new.
    old_ids = {uid: f"<old{uid}@example.com>" for uid in range(1, 4)}
    fake = _HeaderGlitchIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = NEW_EPOCH
    fake.set_messages(
        _mailbox({**old_ids, 4: "<new4@example.com>"}, since_start=True)
    )
    fake.omit_headers = {1, 2}
    config, _sent = _seed_resync(
        monkeypatch,
        tmp_path,
        fake,
        notified=old_ids,
        ingest=IngestConfig(
            first_run_bootstrap_hours=24, first_run_bootstrap_max_messages=5
        ),
    )

    start_module.main(config_dir=tmp_path, max_cycles=1)

    first = _saved_state(tmp_path)
    assert first["accounts"][LOGIN]["last_uid"] == 0
    assert first["resync_epoch"] == {LOGIN: NEW_EPOCH}

    fake.omit_headers = set()
    fake.fetch_calls.clear()
    start_module.main(config_dir=tmp_path, max_cycles=1)

    assert _pending_sends(config.storage.db_path) == [(4, "<new4@example.com>")]
    assert _body_fetches(fake) == []
    saved = _saved_state(tmp_path)
    assert saved["accounts"][LOGIN]["last_uid"] == 4
    assert saved["resync_epoch"] == {}


def test_carried_orphan_is_reattached_in_a_later_resync_poll(
    monkeypatch, tmp_path: Path
) -> None:
    # The orphan (stored in epoch 10, never notified) is carried past the
    # change poll's cap; the next poll still re-attaches its old row.
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = NEW_EPOCH
    fake.set_messages(
        _mailbox(
            {1: "<old1@example.com>", 2: "<new2@example.com>", 3: "<orphan@example.com>"},
            since_start=True,
        )
    )
    config, _sent = _seed_resync(
        monkeypatch,
        tmp_path,
        fake,
        notified={1: "<old1@example.com>"},
        orphans={2: "<orphan@example.com>"},
        ingest=IngestConfig(
            first_run_bootstrap_hours=0, first_run_bootstrap_max_messages=0
        ),
    )
    _cap_fetch(monkeypatch, limit=1)

    for _poll in range(2):
        start_module.main(config_dir=tmp_path, max_cycles=1)

    assert _pending_sends(config.storage.db_path) == [
        (2, "<new2@example.com>"),
        (3, "<orphan@example.com>"),
    ]
    with sqlite3.connect(config.storage.db_path) as conn:
        rows = conn.execute(
            "SELECT message_id, COUNT(*) FROM emails GROUP BY message_id ORDER BY 1"
        ).fetchall()
    assert rows == [
        ("<new2@example.com>", 1),
        ("<old1@example.com>", 1),
        ("<orphan@example.com>", 1),
    ]
    assert _saved_state(tmp_path)["resync_epoch"] == {}
