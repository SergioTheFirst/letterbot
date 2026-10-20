"""WP2a: the IMAP cursor moves only past mail that is durably handled.

fetch_new_messages() records a plan and writes no state; commit_cursor()
advances the cursor to the end of the unbroken run of resolved UIDs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26 import imap_client
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.imap_client import IMAPError, ResilientIMAP
from mailbot_v26.state_manager import StateManager
from mailbot_v26.tests.test_imap_client import (
    _FakeIMAPClient,
    _body_fetches,
    _install_fake,
    _is_body_fetch,
    _is_header_fetch,
)

START = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
LOGIN = "user@example.com"


class _ScriptedIMAPClient(_FakeIMAPClient):
    """Fake server with per-UID body failures, headers missing from a batch
    response and RFC 3501 "UID n:*" semantics (n above the highest UID still
    matches the highest message)."""

    def __init__(self) -> None:
        super().__init__(host="imap.example.com", port=993, ssl=True)
        self.body_fail_uids: set[int] = set()
        self.header_omit_uids: set[int] = set()

    def search(self, criteria):  # type: ignore[override]
        matched = super().search(criteria)
        if not matched and self._uids and list(criteria[:1]) == ["UID"]:
            if len(criteria) == 2:
                return [max(self._uids)]
        return matched

    def fetch(self, uids, fields):  # type: ignore[override]
        if _is_body_fetch(fields) and set(uids) & self.body_fail_uids:
            self.fetch_calls.append((list(uids), list(fields)))
            raise OSError("connection reset by peer")
        result = super().fetch(uids, fields)
        if _is_header_fetch(fields):
            for uid in self.header_omit_uids:
                result.pop(uid, None)
        return result


def _messages(uids, *, start: datetime = START) -> dict[int, dict[bytes, object]]:
    return {
        uid: {
            b"RFC822": b"m%d" % uid,
            b"RFC822.SIZE": 10,
            b"INTERNALDATE": start + timedelta(minutes=uid),
        }
        for uid in uids
    }


@pytest.fixture()
def account() -> AccountConfig:
    return AccountConfig(
        account_id="test",
        login=LOGIN,
        password="secret",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )


@pytest.fixture()
def fake(monkeypatch) -> _ScriptedIMAPClient:
    monkeypatch.setattr(imap_client.time, "sleep", lambda _seconds: None)
    client = _ScriptedIMAPClient()
    _install_fake(monkeypatch, client)
    return client


def _state(tmp_path: Path, *, last_uid: int = 0) -> StateManager:
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(LOGIN, last_uid)
    state.update_check_time(LOGIN, datetime(2024, 1, 1, 0, 0, 0))
    return state


def _uids(messages) -> list[int]:
    return [uid for uid, _ in messages]


def test_fetch_leaves_state_untouched_until_commit(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.uidvalidity = 7
    fake.set_messages(_messages([1, 2, 3]))
    state = _state(tmp_path, last_uid=1)
    state.update_uidvalidity(LOGIN, 7, mailbox="INBOX")
    checked_before = state.get_last_check_time(LOGIN)

    client = ResilientIMAP(account, state, start_time=START)
    messages = client.fetch_new_messages()

    assert _uids(messages) == [2, 3]
    assert state.get_last_uid(LOGIN) == 1
    assert state.get_last_check_time(LOGIN) == checked_before

    commit = client.commit_cursor([2, 3])

    assert commit is not None
    assert commit.cursor == 3
    assert commit.dead_uids == ()
    assert commit.failed_uids == ()
    assert commit.carried == ()
    assert state.get_last_uid(LOGIN) == 3
    assert state.get_uidvalidity(LOGIN) == 7
    assert state.get_last_check_time(LOGIN) > checked_before


def test_commit_logs_one_fetch_plan_line_with_counts_only(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient, caplog
) -> None:
    fake.set_messages(_messages([1, 2, 3]))
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)

    with caplog.at_level(logging.INFO, logger=imap_client.__name__):
        client.fetch_new_messages()
        client.commit_cursor([1, 3])

    plan_lines = [
        rec.getMessage()
        for rec in caplog.records
        if rec.getMessage().startswith("imap_fetch_plan")
    ]
    assert len(plan_lines) == 1
    assert " cursor=1 " in plan_lines[0]
    assert "returned=3 persisted=2 failed=1 dead=0 carried=0" in plan_lines[0]


def test_overflow_beyond_cap_is_carried_to_next_poll(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    # A3: the cursor jumped to the highest UID, so mail beyond the cap was lost.
    fake.set_messages(_messages(range(1, 61)))
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)

    first = client.fetch_new_messages(limit=50)
    first_commit = client.commit_cursor(_uids(first))

    assert _uids(first) == list(range(1, 51))
    assert first_commit.cursor == 50
    assert first_commit.carried == tuple(range(51, 61))
    assert state.get_last_uid(LOGIN) == 50

    second = client.fetch_new_messages(limit=50)
    second_commit = client.commit_cursor(_uids(second))

    assert _uids(second) == list(range(51, 61))
    assert second_commit.cursor == 60
    assert second_commit.carried == ()
    assert state.get_last_uid(LOGIN) == 60


def test_unpersisted_uid_holds_cursor_and_known_uids_are_not_refetched(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.set_messages(_messages([1, 2, 3]))
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)

    first = client.fetch_new_messages()
    first_commit = client.commit_cursor([1, 3])

    assert _uids(first) == [1, 2, 3]
    assert first_commit.cursor == 1
    assert first_commit.failed_uids == (2,)
    assert state.get_last_uid(LOGIN) == 1

    fake.fetch_calls.clear()
    persisted = {1, 3}
    second = client.fetch_new_messages(is_known_uid=lambda uid: uid in persisted)
    second_commit = client.commit_cursor([2])

    assert _uids(second) == [2]
    assert _body_fetches(fake) == [[2]]
    assert second_commit.cursor == 3
    assert state.get_last_uid(LOGIN) == 3
    assert state.get_uid_attempts(LOGIN) == {}


def test_poison_uid_is_dead_lettered_after_three_attempts(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.set_messages(_messages([1, 2, 3]))
    fake.body_fail_uids = {2}
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)
    persisted: set[int] = set()

    commits = []
    for _poll in range(4):
        messages = client.fetch_new_messages(is_known_uid=lambda uid: uid in persisted)
        persisted.update(_uids(messages))
        commits.append(client.commit_cursor(_uids(messages)))

    assert [c.cursor for c in commits] == [1, 1, 3, 3]
    assert [c.failed_uids for c in commits] == [(2,), (2,), (2,), ()]
    assert [c.dead_uids for c in commits] == [(), (), (2,), ()]
    assert persisted == {1, 3}
    assert state.get_last_uid(LOGIN) == 3
    assert state.get_uid_attempts(LOGIN) == {}


def test_dead_uid_above_a_failing_uid_is_not_refetched_or_reported_twice(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.set_messages(_messages([1, 2, 3]))
    fake.body_fail_uids = {1, 2}
    state = _state(tmp_path)
    state.set_uid_attempts(LOGIN, {2: 2})
    client = ResilientIMAP(account, state, start_time=START)
    persisted: set[int] = set()

    commits = []
    body_fetches = []
    for _poll in range(3):
        fake.fetch_calls.clear()
        messages = client.fetch_new_messages(is_known_uid=lambda uid: uid in persisted)
        body_fetches.append({uid for chunk in _body_fetches(fake) for uid in chunk})
        persisted.update(_uids(messages))
        commits.append(client.commit_cursor(_uids(messages)))

    assert [c.dead_uids for c in commits] == [(2,), (), (1,)]
    assert [c.cursor for c in commits] == [0, 0, 3]
    # once dead, UID 2 is neither fetched again nor reported again
    assert body_fetches[1] == {1, 3}
    assert body_fetches[2] == {1}
    assert persisted == {3}
    assert state.get_uid_attempts(LOGIN) == {}


def test_header_missing_for_one_uid_is_retried_not_lost(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.set_messages(_messages([1, 2, 3]))
    fake.header_omit_uids = {2}
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)

    first = client.fetch_new_messages()
    first_commit = client.commit_cursor(_uids(first))

    assert _uids(first) == [1, 3]
    assert [2] not in _body_fetches(fake)
    assert first_commit.cursor == 1
    assert first_commit.failed_uids == (2,)

    fake.header_omit_uids = set()
    second = client.fetch_new_messages(is_known_uid=lambda uid: uid in {1, 3})
    second_commit = client.commit_cursor(_uids(second))

    assert _uids(second) == [2]
    assert second_commit.cursor == 3


def test_status_failure_keeps_stored_uidvalidity(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    # F7: a failed STATUS returned None and overwrote the stored UIDVALIDITY,
    # so a later UIDVALIDITY change went undetected.
    fake.uidvalidity = None
    fake.set_messages(_messages([1, 2]))
    state = _state(tmp_path, last_uid=1)
    state.update_uidvalidity(LOGIN, 10, mailbox="INBOX")
    client = ResilientIMAP(account, state, start_time=START)

    messages = client.fetch_new_messages()
    client.commit_cursor(_uids(messages))

    assert _uids(messages) == [2]
    assert state.get_uidvalidity(LOGIN) == 10
    assert state.get_last_uid(LOGIN) == 2


def test_search_returning_highest_uid_below_range_keeps_cursor(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    # "UID 6:*" on a mailbox whose highest UID is 5 returns 5 (RFC 3501).
    fake.set_messages(_messages([4, 5]))
    state = _state(tmp_path, last_uid=5)
    client = ResilientIMAP(account, state, start_time=START)

    messages = client.fetch_new_messages()
    commit = client.commit_cursor([])

    assert messages == []
    assert _body_fetches(fake) == []
    assert commit.cursor == 5
    assert state.get_last_uid(LOGIN) == 5


def test_uidvalidity_change_skips_known_probe_and_restarts_epoch(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.uidvalidity = 20
    fake.set_messages(_messages([1, 2], start=START - timedelta(hours=1)))
    state = _state(tmp_path, last_uid=777)
    state.update_uidvalidity(LOGIN, 10, mailbox="INBOX")
    state.set_uid_attempts(LOGIN, {800: 2})
    probed: list[int] = []

    def _probe(uid: int) -> bool:
        probed.append(uid)
        return True

    client = ResilientIMAP(
        account,
        state,
        start_time=START,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=5,
    )
    messages = client.fetch_new_messages(is_known_uid=_probe)

    assert _uids(messages) == [1, 2]
    assert probed == []
    assert client.last_uidvalidity_changed is True
    # nothing is written before the commit
    assert state.get_last_uid(LOGIN) == 777
    assert state.get_uidvalidity(LOGIN) == 10

    commit = client.commit_cursor([1, 2])

    assert commit.cursor == 2
    assert state.get_last_uid(LOGIN) == 2
    assert state.get_uidvalidity(LOGIN) == 20
    assert state.get_uid_attempts(LOGIN) == {}


def test_consecutive_body_failures_stop_fetching_and_carry_the_rest(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    # A flapping server must not burn an attempt on every UID in one poll.
    fake.set_messages(_messages([1, 2, 3, 4, 5]))
    fake.body_fail_uids = {1, 2, 3, 4, 5}
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)

    messages = client.fetch_new_messages()
    commit = client.commit_cursor([])

    assert messages == []
    assert {uid for chunk in _body_fetches(fake) for uid in chunk} == {1, 2}
    assert commit.failed_uids == (1, 2)
    assert commit.carried == (3, 4, 5)
    assert commit.cursor == 0
    assert state.get_uid_attempts(LOGIN) == {1: 1, 2: 1}


def test_commit_is_one_shot_per_successful_fetch(
    tmp_path: Path, account: AccountConfig, fake: _ScriptedIMAPClient
) -> None:
    fake.set_messages(_messages([1, 2]))
    state = _state(tmp_path)
    client = ResilientIMAP(account, state, start_time=START)

    assert client.commit_cursor([]) is None

    client.fetch_new_messages()
    first = client.commit_cursor([1])
    again = client.commit_cursor([1])

    assert first is not None and first.failed_uids == (2,)
    assert again is None
    assert state.get_uid_attempts(LOGIN) == {2: 1}

    fake.header_fetch_error = OSError("connection reset")
    with pytest.raises(IMAPError):
        client.fetch_new_messages()
    # a failed fetch leaves no plan behind: nothing to commit
    assert client.commit_cursor([]) is None
    assert state.get_last_uid(LOGIN) == 1

