"""WP2b: every stored email remembers the UIDVALIDITY epoch of its UID.

The worker stores the UIDVALIDITY seen by the poll next to the UID, so a
restart can tell whether the UID still names the same message.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.state_manager import StateManager
from mailbot_v26.tests.test_imap_client import _FakeIMAPClient, _install_fake
from mailbot_v26.tests.test_ingest_cursor_commit import (
    START,
    _account,
    _install_main,
    _messages as _ingest_messages,
)
from mailbot_v26.tests.test_storage_atomic_persist import clear_pipeline_caches


@pytest.fixture(autouse=True)
def _clean_pipeline_cache():
    clear_pipeline_caches()
    yield
    clear_pipeline_caches()


def test_current_uidvalidity_is_the_value_seen_by_the_last_poll(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = 4242
    fake.set_messages(_ingest_messages([1]))
    _install_fake(monkeypatch, fake)
    client = ResilientIMAP(_account(), StateManager(tmp_path / "state.json"), START)
    assert client.current_uidvalidity is None

    client.fetch_new_messages()

    assert client.current_uidvalidity == 4242


def test_worker_loop_stores_the_polled_uidvalidity(monkeypatch, tmp_path) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.uidvalidity = 4242
    fake.set_messages(_ingest_messages([1, 2]))
    config, _sent = _install_main(monkeypatch, tmp_path, fake)

    start_module.main(config_dir=tmp_path, max_cycles=1)

    with sqlite3.connect(config.storage.db_path) as conn:
        rows = conn.execute("SELECT uid, uidvalidity FROM emails ORDER BY uid").fetchall()
    assert rows == [(1, 4242), (2, 4242)]


def test_storage_adds_uidvalidity_column_to_existing_database(tmp_path) -> None:
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
        conn.execute(
            "INSERT INTO emails (account_email, uid, message_id, status, created_at,"
            " updated_at) VALUES ('a@example.com', 3, '<old@x>', 'NEW', '', '')"
        )

    storage = Storage(db_path)

    assert storage.get_email_locator(1) == {
        "account_email": "a@example.com",
        "uid": 3,
        "uidvalidity": None,
        "message_id": "<old@x>",
    }
    email_id, created = storage.persist_email_and_enqueue(
        account_email="a@example.com",
        uid=4,
        message_id="<new@x>",
        from_email=None,
        from_name=None,
        subject=None,
        received_at=None,
        attachments_count=0,
        uidvalidity=900,
    )
    assert created
    assert storage.get_email_locator(email_id)["uidvalidity"] == 900
    assert storage.get_email_locator(999) is None


def test_upsert_keeps_known_uidvalidity(tmp_path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    fields = dict(
        account_email="a@example.com",
        uid=1,
        message_id="<m@x>",
        from_email=None,
        from_name=None,
        subject=None,
        received_at=None,
        attachments_count=0,
    )
    email_id = storage.upsert_email(**fields, uidvalidity=55)
    assert storage.upsert_email(**fields) == email_id

    assert storage.get_email_locator(email_id)["uidvalidity"] == 55
