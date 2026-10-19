import logging
import socket
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.config_loader import AccountConfig
from mailbot_v26 import imap_client
from mailbot_v26.imap_client import IMAPError, ResilientIMAP
from mailbot_v26.state_manager import StateManager

# imapclient with normalise_times=True converts INTERNALDATE to naive local
# system time. The fake pins that "local" zone to +03:00 so the A9 bug
# (naive local time read as UTC) reproduces in any CI timezone.
_FAKE_LOCAL_TZ = timezone(timedelta(hours=3))
_HEADER_FIELDS_KEY = b"BODY[HEADER.FIELDS (MESSAGE-ID)]"


def _is_header_fetch(fields) -> bool:
    return any("RFC822.SIZE" in str(field) for field in fields)


def _is_body_fetch(fields) -> bool:
    return any(field in (b"RFC822", "RFC822") for field in fields)


class _FakeIMAPClient:
    def __init__(self, host: str, port: int, ssl: bool) -> None:
        self.host = host
        self.port = port
        self.ssl = ssl
        self.logged_in = False
        self.folder = None
        self._uids: list[int] = []
        self._data: dict[int, dict[bytes, object]] = {}
        self.search_calls: list[list[object]] = []
        self.select_calls: list[tuple[str, bool]] = []
        self.fetch_calls: list[tuple[list[int], list[object]]] = []
        self.uidvalidity: int | None = None
        self.normalise_times = True
        self.logout_calls = 0
        self.logout_error: Exception | None = None
        self.header_fetch_error: Exception | None = None

    def set_messages(self, messages: dict[int, dict[bytes, object]]) -> None:
        self._uids = sorted(messages.keys())
        self._data = messages

    def login(self, login: str, password: str) -> None:
        self.logged_in = True

    def select_folder(self, folder: str, readonly: bool = False) -> None:
        self.folder = folder
        self.select_calls.append((folder, readonly))

    def search(self, criteria) -> list[int]:  # type: ignore[override]
        if not criteria:
            return []
        self.search_calls.append(list(criteria))
        matched = list(self._uids)
        if "UID" in criteria:
            range_spec = criteria[criteria.index("UID") + 1]
            if range_spec.endswith(":*"):
                start = int(range_spec.split(":")[0])
                matched = [uid for uid in matched if uid >= start]
            else:
                matched = []
        if "SINCE" in criteria:
            raw_since = criteria[criteria.index("SINCE") + 1]
            if isinstance(raw_since, datetime):
                since = raw_since.date()
            elif isinstance(raw_since, date):
                since = raw_since
            else:
                since = datetime.strptime(raw_since, "%d-%b-%Y").date()
            filtered = []
            for uid in matched:
                # RFC 3501 SINCE: internal date disregarding time and timezone,
                # i.e. the date in the message's own (server) timezone.
                internaldate = self._data[uid].get(b"INTERNALDATE")
                if isinstance(internaldate, datetime) and internaldate.date() >= since:
                    filtered.append(uid)
            matched = filtered
        return matched

    def fetch(self, uids, fields):  # type: ignore[override]
        self.fetch_calls.append((list(uids), list(fields)))
        if self.header_fetch_error is not None and _is_header_fetch(fields):
            raise self.header_fetch_error
        return {uid: self._present(self._data[uid]) for uid in uids}

    def _present(self, item: dict[bytes, object]) -> dict[bytes, object]:
        internaldate = item.get(b"INTERNALDATE")
        if (
            self.normalise_times
            and isinstance(internaldate, datetime)
            and internaldate.tzinfo is not None
        ):
            naive_local = internaldate.astimezone(_FAKE_LOCAL_TZ).replace(tzinfo=None)
            return {**item, b"INTERNALDATE": naive_local}
        return dict(item)

    def logout(self) -> None:
        self.logout_calls += 1
        if self.logout_error is not None:
            raise self.logout_error

    def folder_status(self, folder, items):  # type: ignore[override]
        _ = (folder, items)
        if self.uidvalidity is None:
            return {}
        return {"UIDVALIDITY": self.uidvalidity}


@pytest.fixture()
def account() -> AccountConfig:
    return AccountConfig(
        account_id="test",
        login="user@example.com",
        password="secret",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )


def test_fetch_skips_messages_before_start(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"old",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"newer",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [2, 3]
    assert state.get_last_uid(account.login) == 3
    assert state.get_last_check_time(account.login) is not None
    assert fake_client.search_calls[0] == ["UID", "1:*"]
    assert fake_client.search_calls[1][:2] == ["UID", "1:*"]
    assert "SINCE" in fake_client.search_calls[1]


def test_baseline_updates_when_only_old(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            10: {
                b"RFC822": b"first",
                b"INTERNALDATE": datetime(2023, 12, 31, 23, 59, 0, tzinfo=timezone.utc),
            },
            11: {
                b"RFC822": b"second",
                b"INTERNALDATE": datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert messages == []
    assert state.get_last_uid(account.login) == 11
    assert fake_client.search_calls[0] == ["UID", "1:*"]
    assert "SINCE" in fake_client.search_calls[1]


def test_steady_state_filters_prestart_emails(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"old",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"newer",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    client.fetch_new_messages()

    fake_client.search_calls.clear()
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"old",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"newer",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, 0, tzinfo=timezone.utc),
            },
            4: {
                b"RFC822": b"late",
                b"INTERNALDATE": datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    messages = client.fetch_new_messages()

    assert messages == []
    assert state.get_last_uid(account.login) == 4
    assert fake_client.search_calls[0] == ["UID", "4:*"]
    assert "SINCE" in fake_client.search_calls[1]


def test_cursor_advances_when_all_skipped(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(account.login, 1)

    fake_client.set_messages(
        {
            2: {
                b"RFC822": b"old",
                b"INTERNALDATE": datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"older",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 59, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert messages == []
    assert state.get_last_uid(account.login) == 3


def test_internaldate_timezone_normalization(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    local_tz = timezone(timedelta(hours=2))

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"on-time",
                b"INTERNALDATE": datetime(2024, 1, 2, 14, 0, 0, tzinfo=local_tz),
            },
            2: {
                b"RFC822": b"too-early",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [1]


def test_fetch_uses_normalized_state_login_key(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake_client.set_messages(
        {
            6: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid("user@example.com", 5)

    mixed_case = AccountConfig(
        account_id=account.account_id,
        login="User@Example.com",
        password=account.password,
        host=account.host,
        port=account.port,
        use_ssl=account.use_ssl,
        telegram_chat_id=account.telegram_chat_id,
    )
    client = ResilientIMAP(
        mixed_case,
        state,
        start_time=datetime(2024, 1, 3, 11, 0, 0, tzinfo=timezone.utc),
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [6]
    assert fake_client.search_calls[0] == ["UID", "6:*"]
    assert state.get_last_uid("user@example.com") == 6


def test_first_run_bootstrap_allows_recent_prestart_messages(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"recent-1",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"recent-2",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 30, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=True,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=20,
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [1, 2]
    assert client.last_fetch_included_prestart is True
    assert state.get_last_uid(account.login) == 2
    assert "SINCE" in fake_client.search_calls[1]


def test_non_first_run_still_blocks_prestart_messages_when_disabled(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            6: {
                b"RFC822": b"prestart",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(account.login, 5)

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=False,
    )
    messages = client.fetch_new_messages()

    assert messages == []
    assert state.get_last_uid(account.login) == 6


def test_non_first_run_blocks_prestart_messages_when_disabled(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    test_non_first_run_still_blocks_prestart_messages_when_disabled(
        monkeypatch, tmp_path, account
    )


def test_first_run_bootstrap_is_limited_by_hours_window(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"too-old",
                b"INTERNALDATE": datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"recent",
                b"INTERNALDATE": datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=True,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=20,
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [2]


def test_first_run_bootstrap_is_limited_by_message_count(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"m1",
                b"INTERNALDATE": datetime(2024, 1, 2, 8, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"m2",
                b"INTERNALDATE": datetime(2024, 1, 2, 8, 30, 0, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"m3",
                b"INTERNALDATE": datetime(2024, 1, 2, 9, 0, 0, tzinfo=timezone.utc),
            },
            4: {
                b"RFC822": b"m4",
                b"INTERNALDATE": datetime(2024, 1, 2, 9, 30, 0, tzinfo=timezone.utc),
            },
            5: {
                b"RFC822": b"m5",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=True,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=2,
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [4, 5]
    assert state.get_last_uid(account.login) == 5


def test_bootstrap_does_not_duplicate_already_seen_messages(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"first",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"second",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=True,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=20,
    )
    first_pass = client.fetch_new_messages()
    second_pass = client.fetch_new_messages()

    assert [uid for uid, _ in first_pass] == [1, 2]
    assert second_pass == []
    assert fake_client.search_calls[2] == ["UID", "3:*"]


def test_uidvalidity_change_resets_mailbox_state(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake_client.uidvalidity = 2
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"fresh-1",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"fresh-2",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(account.login, 5)
    state.update_uidvalidity(account.login, 1, mailbox="INBOX")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=False,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=20,
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [1, 2]
    assert fake_client.search_calls[0] == ["UID", "1:*"]
    assert state.get_last_uid(account.login) == 2
    assert state.get_uidvalidity(account.login) == 2
    assert client.last_uidvalidity_changed is True
    assert client.last_resync_reason == "uidvalidity_change"


def test_uidvalidity_change_triggers_bounded_resync_not_full_replay(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake_client.uidvalidity = 20
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"too-old",
                b"INTERNALDATE": datetime(2023, 12, 31, 11, 0, 0, tzinfo=timezone.utc),
            },
            2: {
                b"RFC822": b"m2",
                b"INTERNALDATE": datetime(2024, 1, 2, 8, 0, 0, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"m3",
                b"INTERNALDATE": datetime(2024, 1, 2, 9, 0, 0, tzinfo=timezone.utc),
            },
            4: {
                b"RFC822": b"m4",
                b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            },
            5: {
                b"RFC822": b"m5",
                b"INTERNALDATE": datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
            },
        }
    )

    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(account.login, 777)
    state.update_uidvalidity(account.login, 10, mailbox="INBOX")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        allow_prestart_emails=False,
        first_run_bootstrap=False,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=2,
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [4, 5]
    assert fake_client.search_calls[0] == ["UID", "1:*"]
    assert "SINCE" in fake_client.search_calls[1]
    assert client.last_uidvalidity_changed is True
    assert client.last_bootstrap_active is True
    assert client.last_resync_reason == "uidvalidity_change"


def test_mailbox_is_always_selected_read_only(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # A8: fetching RFC822 from a read-write selection sets \Seen in the
    # user's mailbox. Every selection, including reconnects, must be read-only.
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0, tzinfo=timezone.utc),
            },
        }
    )
    monkeypatch.setattr(
        imap_client, "_imap_client_cls", lambda: (lambda *args, **kwargs: fake_client)
    )
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()
    client.close()
    payload = client.fetch_full_rfc822(1)

    assert [uid for uid, _ in messages] == [1]
    assert payload == b"new"
    assert len(fake_client.select_calls) >= 2
    assert all(readonly is True for _, readonly in fake_client.select_calls)


# ---- WP2d: batched header FETCH, logout per cycle, per-client timeout, A9 ----


def _install_fake(
    monkeypatch, fake_client: _FakeIMAPClient, captured_kwargs: list | None = None
) -> None:
    def _factory(*args, **kwargs):
        if captured_kwargs is not None:
            captured_kwargs.append(dict(kwargs))
        return fake_client

    monkeypatch.setattr(imap_client, "_imap_client_cls", lambda: _factory)


def _header_fetches(fake_client: _FakeIMAPClient) -> list[list[int]]:
    return [uids for uids, fields in fake_client.fetch_calls if _is_header_fetch(fields)]


def _body_fetches(fake_client: _FakeIMAPClient) -> list[list[int]]:
    return [uids for uids, fields in fake_client.fetch_calls if _is_body_fetch(fields)]


def _message_id_header(uid: int) -> bytes:
    return f"Message-ID: <m{uid}@example.com>\r\n\r\n".encode("ascii")


def test_header_fetch_is_batched_in_chunks_of_200(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # A13: one header FETCH per UID (plus a second size probe per body) made a
    # 300-message poll cost 350 round trips.
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            uid: {
                b"RFC822": b"m%d" % uid,
                b"RFC822.SIZE": 10,
                b"INTERNALDATE": start_time + timedelta(seconds=uid),
                _HEADER_FIELDS_KEY: _message_id_header(uid),
            }
            for uid in range(1, 301)
        }
    )
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages(limit=50)

    header_fetches = _header_fetches(fake_client)
    assert [len(chunk) for chunk in header_fetches] == [200, 100]
    assert [uid for chunk in header_fetches for uid in chunk] == list(range(1, 301))
    header_fields = next(
        fields for _, fields in fake_client.fetch_calls if _is_header_fetch(fields)
    )
    assert "BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)]" in header_fields
    assert "INTERNALDATE" in header_fields
    assert "RFC822.HEADER" not in header_fields
    assert [uid for uid, _ in messages] == list(range(1, 51))
    # body fetch uses the size from the batch: no extra per-message size probe
    assert _body_fetches(fake_client) == [[uid] for uid in range(1, 51)]
    assert len(fake_client.fetch_calls) == 2 + 50


def test_message_ids_recorded_from_header_batch(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"one",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc),
                _HEADER_FIELDS_KEY: _message_id_header(1),
            },
            2: {
                b"RFC822": b"two",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 6, tzinfo=timezone.utc),
                _HEADER_FIELDS_KEY: b"\r\n",
            },
            3: {
                b"RFC822": b"three",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 7, tzinfo=timezone.utc),
                # some servers quote the field name in the response key
                b'BODY[HEADER.FIELDS ("MESSAGE-ID")]': (
                    b"message-id:\r\n  <m3@example.com>\r\n\r\n"
                ),
            },
        }
    )
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [1, 2, 3]
    assert len(_header_fetches(fake_client)) == 1
    assert client.last_message_ids == {
        1: "<m1@example.com>",
        2: None,
        3: "<m3@example.com>",
    }


def test_header_batch_failure_raises_and_keeps_cursor(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # A per-UID header failure used to be skipped while the cursor jumped past
    # it: the message was lost. A batch failing after retries must raise.
    monkeypatch.setattr(imap_client.time, "sleep", lambda _seconds: None)
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            2: {
                b"RFC822": b"two",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc),
            },
            3: {
                b"RFC822": b"three",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 6, tzinfo=timezone.utc),
            },
        }
    )
    fake_client.header_fetch_error = OSError("connection reset")
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(account.login, 1)

    client = ResilientIMAP(account, state, start_time=start_time)
    with pytest.raises(IMAPError):
        client.fetch_new_messages()

    assert state.get_last_uid(account.login) == 1
    assert _body_fetches(fake_client) == []
    assert fake_client.logout_calls >= 1


def test_fetch_logs_out_after_each_cycle(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # A22: a new ResilientIMAP per poll never logged out, leaking sessions.
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, tzinfo=timezone.utc),
            },
        }
    )
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    client.fetch_new_messages()
    assert fake_client.logout_calls == 1
    client.fetch_new_messages()
    assert fake_client.logout_calls == 2


def test_close_logs_failed_logout(
    monkeypatch, tmp_path: Path, account: AccountConfig, caplog
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, tzinfo=timezone.utc),
            },
        }
    )
    fake_client.logout_error = OSError("socket already closed")
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    with caplog.at_level(logging.WARNING, logger=imap_client.__name__):
        messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [1]
    assert any("imap_logout_failed" in rec.getMessage() for rec in caplog.records)


def test_connect_uses_per_client_socket_timeout(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # socket.setdefaulttimeout() changed the timeout of every socket in the
    # process; the timeout belongs to the IMAP client only.
    imapclient_module = pytest.importorskip("imapclient")
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"new",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, tzinfo=timezone.utc),
            },
        }
    )
    captured: list[dict] = []
    _install_fake(monkeypatch, fake_client, captured)
    state = StateManager(tmp_path / "state.json")
    previous_default = socket.getdefaulttimeout()
    socket.setdefaulttimeout(None)
    try:
        client = ResilientIMAP(
            account,
            state,
            start_time=start_time,
            connect_timeout=7.0,
            io_timeout=42.0,
        )
        client.fetch_new_messages()
        assert socket.getdefaulttimeout() is None
    finally:
        socket.setdefaulttimeout(previous_default)

    timeout = captured[0]["timeout"]
    assert isinstance(timeout, imapclient_module.SocketTimeout)
    assert (timeout.connect, timeout.read) == (7.0, 42.0)


def test_server_west_of_utc_mail_just_after_start_is_fetched(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # A9: SINCE compares the server-local date of INTERNALDATE. A -05:00
    # server files a 03:30 UTC message under the previous day, so SINCE on the
    # UTC start date skipped it.
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 3, 0, 0, tzinfo=timezone.utc)
    server_tz = timezone(timedelta(hours=-5))
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"prestart",
                b"INTERNALDATE": datetime(2024, 1, 1, 21, 30, tzinfo=server_tz),
            },
            2: {
                b"RFC822": b"after-start",
                b"INTERNALDATE": datetime(2024, 1, 1, 22, 30, tzinfo=server_tz),
            },
        }
    )
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [2]
    assert fake_client.search_calls[1] == ["UID", "1:*", "SINCE", date(2024, 1, 1)]


def test_naive_local_internaldate_not_mistaken_for_utc(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    # A9: with normalise_times=True imapclient returns naive local times, which
    # _safe_dt then read as UTC (a +03:00 PC saw a 09:30Z message as 12:30Z).
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"prestart",
                b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, tzinfo=_FAKE_LOCAL_TZ),
            },
            2: {
                b"RFC822": b"after-start",
                b"INTERNALDATE": datetime(2024, 1, 2, 14, 30, tzinfo=_FAKE_LOCAL_TZ),
            },
        }
    )
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert fake_client.normalise_times is False
    assert [uid for uid, _ in messages] == [2]


def test_uidvalidity_resync_since_is_date_with_one_day_margin(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake_client.uidvalidity = 20
    start_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    fake_client.set_messages(
        {
            1: {
                b"RFC822": b"m1",
                b"INTERNALDATE": datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc),
            },
        }
    )
    _install_fake(monkeypatch, fake_client)
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(account.login, 777)
    state.update_uidvalidity(account.login, 10, mailbox="INBOX")

    client = ResilientIMAP(
        account,
        state,
        start_time=start_time,
        first_run_bootstrap_hours=24,
        first_run_bootstrap_max_messages=5,
    )
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [1]
    # cutoff 2024-01-01 12:00Z minus one day of server-timezone margin
    assert fake_client.search_calls[1] == ["UID", "1:*", "SINCE", date(2023, 12, 31)]
