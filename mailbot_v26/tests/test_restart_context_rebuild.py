"""WP2b: a restart must not lose queued mail.

The raw email of a queued item lives only in the in-memory pipeline caches.
After a restart the queue rebuilds the context by refetching the message from
IMAP (by UID, or by Message-ID after a UIDVALIDITY change). A network error is
retried; a message that is gone or does not match is dead-lettered visibly
(IMAP_HEALTH dead_letter, stage CONTEXT, status ERROR) with no fail-open send.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
from mailbot_v26 import imap_client
from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    parse_raw_email,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.events.contract import EventType
from mailbot_v26.imap_client import CursorCommit, ResilientIMAP
from mailbot_v26.state_manager import StateManager
from mailbot_v26.tests import test_delivery_invariants as _invariants
from mailbot_v26.tests.test_delivery_invariants import (  # noqa: F401 - fixtures
    _ACCOUNT,
    _email_row,
    _fail_open_processor,
    _isolated_runtime,
    _patch_premium,
    _queue_rows,
    _setup,
)
from mailbot_v26.tests.test_imap_client import _install_fake
from mailbot_v26.tests.test_imap_refetch import (
    MESSAGE_ID,
    RefetchIMAPClient,
    header_searches,
    raw_email,
    server_item,
)

# Shared fixtures, re-exported by assignment so pytest discovers them here.
events = _invariants.events
telegram = _invariants.telegram

START = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
SUBJECT = "Invoice 42 from Supplier"


@pytest.fixture(autouse=True)
def _reset_context_dead_letters():
    start_module._CONTEXT_DEAD_LETTERS.clear()
    yield
    start_module._CONTEXT_DEAD_LETTERS.clear()


@pytest.fixture()
def server(monkeypatch) -> RefetchIMAPClient:
    fake = RefetchIMAPClient()
    fake.uidvalidity = 100
    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(imap_client.time, "sleep", lambda *_a, **_k: None)
    return fake


def _ingest(storage: Storage, config, server: RefetchIMAPClient, tmp_path: Path) -> int:
    """Poll the fake server and persist like the worker loop does."""
    imap = ResilientIMAP(_ACCOUNT, StateManager(tmp_path / "state.json"), START)
    messages = imap.fetch_new_messages()
    assert len(messages) == 1
    uid, raw = messages[0]
    email_id, created = start_module._persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email=_ACCOUNT.login,
        uid=uid,
        message_id=MESSAGE_ID,
        from_email="sender@example.com",
        from_name="Sender",
        subject=SUBJECT,
        received_at="Tue, 02 Jan 2024 12:05:00 +0000",
        attachments_count=0,
        raw_email=raw,
        inbound=parse_raw_email(raw, config),
        uidvalidity=imap.current_uidvalidity,
    )
    assert created
    return email_id


def _crash_at(storage: Storage, email_id: int, stage: str) -> Storage:
    """Leave only `stage` queued, drop every in-memory cache, reopen the DB."""
    if stage != "PARSE":
        with storage.conn:
            storage.conn.execute("DELETE FROM queue WHERE email_id = ?", (email_id,))
        storage.enqueue_stage(email_id, stage)
    storage.close()
    PIPELINE_CACHE.clear()
    PIPELINE_INBOUND_CACHE.clear()
    PIPELINE_RAW_CACHE.clear()
    return Storage(storage.db_path)


def _context_dead_letters(events) -> list:
    return [
        event
        for event in events.contract
        if event.event_type == EventType.IMAP_HEALTH
        and event.payload.get("subtype") == "dead_letter"
    ]


def _uidvalidity(storage: Storage, email_id: int) -> int | None:
    row = storage.conn.execute(
        "SELECT uidvalidity FROM emails WHERE id = ?", (email_id,)
    ).fetchone()
    return row[0]


@pytest.mark.parametrize("stage", ["PARSE", "LLM", "TG"])
def test_restart_refetches_message_and_notifies_once(
    tmp_path, monkeypatch, telegram, events, server, stage
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    llm_runs: list[int] = []

    def counting_llm(ctx) -> None:
        llm_runs.append(ctx.email_id)
        _invariants._fake_legacy_llm(ctx)

    monkeypatch.setattr(start_module, "stage_llm", counting_llm)
    server.set_messages({7: server_item(raw_email(subject=SUBJECT))})
    email_id = _ingest(storage, config, server, tmp_path)
    assert _uidvalidity(storage, email_id) == 100

    restarted = _crash_at(storage, email_id, stage)
    start_module._process_queue(restarted, config, legacy_processor, flags)

    messages = telegram.email_messages()
    assert len(messages) == 1
    # The caches were empty: the content comes from the refetched message.
    assert SUBJECT in messages[0]
    # A TG item runs the LLM stage inline, exactly once like the others.
    assert llm_runs == [email_id]
    assert telegram.notices() == []
    assert _queue_rows(restarted) == []
    status, _error, delivered_at = _email_row(restarted, email_id)
    assert delivered_at
    assert status != "ERROR"
    assert _context_dead_letters(events) == []
    assert server.logout_calls >= 2


def test_restart_runs_premium_processor_for_parse_item(
    tmp_path, monkeypatch, telegram, events, server
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=True
    )
    _patch_premium(monkeypatch, events)
    server.set_messages({7: server_item(raw_email(subject=SUBJECT))})
    email_id = _ingest(storage, config, server, tmp_path)

    restarted = _crash_at(storage, email_id, "PARSE")
    start_module._process_queue(restarted, config, legacy_processor, flags)

    assert len(telegram.email_messages()) == 1
    assert _queue_rows(restarted) == []
    assert _email_row(restarted, email_id)[2]


def test_restart_after_uidvalidity_change_finds_message_by_message_id(
    tmp_path, monkeypatch, telegram, events, server
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    raw = raw_email(subject=SUBJECT)
    server.set_messages({7: server_item(raw)})
    email_id = _ingest(storage, config, server, tmp_path)

    # Mailbox rebuilt while the worker was down: new epoch, new UID.
    server.uidvalidity = 200
    server.set_messages({2: server_item(raw)})
    restarted = _crash_at(storage, email_id, "TG")
    start_module._process_queue(restarted, config, legacy_processor, flags)

    assert len(telegram.email_messages()) == 1
    assert header_searches(server) == [["HEADER", "Message-ID", MESSAGE_ID]]
    assert _queue_rows(restarted) == []
    assert _context_dead_letters(events) == []


@pytest.mark.parametrize("stage", ["PARSE", "TG"])
def test_message_gone_after_restart_is_dead_lettered_visibly(
    tmp_path, monkeypatch, telegram, events, server, stage
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    server.set_messages({7: server_item(raw_email(subject=SUBJECT))})
    email_id = _ingest(storage, config, server, tmp_path)

    server.set_messages({})  # deleted from the mailbox while the worker was down
    restarted = _crash_at(storage, email_id, stage)
    start_module._process_queue(restarted, config, _fail_open_processor(), flags)

    # No content, so nothing to fail open with: no message, no failure notice.
    assert telegram.calls == 0
    assert _queue_rows(restarted) == []
    status, error_last, delivered_at = _email_row(restarted, email_id)
    assert status == "ERROR"
    assert error_last == "context_unavailable:gone"
    assert delivered_at is None
    dead = _context_dead_letters(events)
    assert len(dead) == 1
    assert dead[0].email_id == email_id
    assert dead[0].account_id == _ACCOUNT.login
    assert dead[0].payload["stage"] == "CONTEXT"
    assert dead[0].payload["queue_stage"] == stage
    assert dead[0].payload["reason"] == "gone"
    assert dead[0].payload["message_uid"] == 7
    assert SUBJECT not in repr(dead[0].payload)
    assert MESSAGE_ID not in repr(dead[0].payload)
    assert start_module._CONTEXT_DEAD_LETTERS == {_ACCOUNT.login: 1}


def test_message_id_mismatch_after_restart_is_dead_lettered(
    tmp_path, monkeypatch, telegram, events, server
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    server.set_messages({7: server_item(raw_email(subject=SUBJECT))})
    email_id = _ingest(storage, config, server, tmp_path)

    server.set_messages({7: server_item(raw_email("<someone-else@example.com>"))})
    restarted = _crash_at(storage, email_id, "LLM")
    start_module._process_queue(restarted, config, legacy_processor, flags)

    assert telegram.calls == 0
    status, error_last, _ = _email_row(restarted, email_id)
    assert (status, error_last) == ("ERROR", "context_unavailable:message_id_mismatch")
    assert [event.payload["reason"] for event in _context_dead_letters(events)] == [
        "message_id_mismatch"
    ]


@pytest.mark.parametrize("stage", ["PARSE", "TG"])
def test_network_error_during_refetch_is_retried(
    tmp_path, monkeypatch, telegram, events, server, stage
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    server.set_messages({7: server_item(raw_email(subject=SUBJECT))})
    email_id = _ingest(storage, config, server, tmp_path)

    server.login_error = OSError("connection refused")
    restarted = _crash_at(storage, email_id, stage)
    start_module._process_queue(restarted, config, legacy_processor, flags)

    assert telegram.calls == 0
    rows = _queue_rows(restarted)
    assert [(row_stage, attempts) for row_stage, attempts, _ in rows] == [(stage, 1)]
    assert "IMAP" in str(rows[0][2])
    assert _email_row(restarted, email_id)[0] == "NEW"
    assert _context_dead_letters(events) == []

    # IMAP is back: the retry rebuilds the context and notifies once.
    server.login_error = None
    with restarted.conn:
        restarted.conn.execute("UPDATE queue SET not_before = NULL")
    start_module._process_queue(restarted, config, legacy_processor, flags)

    assert len(telegram.email_messages()) == 1
    assert _queue_rows(restarted) == []


def test_unknown_email_keeps_the_no_context_path(tmp_path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    assert (
        start_module._rebuild_pipeline_context(
            storage=storage, config=None, email_id=12345, stage="TG"
        )
        is None
    )


@pytest.mark.parametrize("failing_step", ["parse_raw_email", "store_inbound"])
def test_failed_rebuild_leaves_no_cached_content(
    tmp_path, monkeypatch, telegram, events, server, failing_step
) -> None:
    config, storage, _legacy_processor, _flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    server.set_messages({7: server_item(raw_email(subject=SUBJECT))})
    email_id = _ingest(storage, config, server, tmp_path)
    restarted = _crash_at(storage, email_id, "PARSE")

    def broken_step(*_args, **_kwargs):
        raise RuntimeError(f"{failing_step} failed")

    monkeypatch.setattr(start_module, failing_step, broken_step)
    with pytest.raises(RuntimeError, match=f"{failing_step} failed"):
        start_module._rebuild_pipeline_context(
            storage=restarted, config=config, email_id=email_id, stage="PARSE"
        )

    # R8: a half-built context must not be reused by the retry.
    assert email_id not in PIPELINE_RAW_CACHE
    assert email_id not in PIPELINE_INBOUND_CACHE
    assert email_id not in PIPELINE_CACHE


def test_context_dead_letters_join_the_next_aggregated_alert(
    tmp_path, monkeypatch
) -> None:
    sent: list[str] = []

    def _send(payload, *_args, **_kwargs):
        sent.append(payload.html_text)
        return SimpleNamespace(delivered=True, message_id=1)

    monkeypatch.setattr(start_module, "send_telegram", _send)
    start_module._CONTEXT_DEAD_LETTERS[_ACCOUNT.login] = 2
    imap = SimpleNamespace(
        commit_cursor=lambda _uids: CursorCommit(
            cursor=5, dead_uids=(), failed_uids=(), carried=()
        )
    )

    start_module._commit_imap_cursor(
        imap=imap,
        persisted_uids=[],
        account=_ACCOUNT,
        login=_ACCOUNT.login,
        state=StateManager(tmp_path / "state.json"),
        bot_token="token",
    )

    assert len(sent) == 1
    assert "after a restart: 2" in sent[0] or "после перезапуска: 2" in sent[0]
    assert "attempts" not in sent[0] and "попыток" not in sent[0]
    assert start_module._CONTEXT_DEAD_LETTERS == {}
