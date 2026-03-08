from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.config_loader import AccountConfig
from mailbot_v26 import imap_client
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.state_manager import StateManager


class _FakeIMAPClient:
    def __init__(self, host: str, port: int, ssl: bool) -> None:
        self.host = host
        self.port = port
        self.ssl = ssl
        self.logged_in = False
        self.folder = None
        self._uids: list[int] = []
        self._data: dict[int, dict[bytes, object]] = {}
        self.search_calls: list[list[str]] = []
        self.uidvalidity: int | None = None

    def set_messages(self, messages: dict[int, dict[bytes, object]]) -> None:
        self._uids = sorted(messages.keys())
        self._data = messages

    def login(self, login: str, password: str) -> None:
        self.logged_in = True

    def select_folder(self, folder: str) -> None:
        self.folder = folder

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
            since = datetime.strptime(
                criteria[criteria.index("SINCE") + 1],
                "%d-%b-%Y",
            ).date()
            filtered = []
            for uid in matched:
                internaldate = self._data[uid].get(b"INTERNALDATE")
                if isinstance(internaldate, datetime) and internaldate.date() >= since:
                    filtered.append(uid)
            matched = filtered
        return matched

    def fetch(self, uids, fields):  # type: ignore[override]
        return {uid: self._data[uid] for uid in uids}

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
