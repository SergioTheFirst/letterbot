from datetime import datetime
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
        command = criteria[0]
        if command == "UID" and len(criteria) > 1:
            range_spec = criteria[1]
            if range_spec.endswith(":*"):
                start = int(range_spec.split(":")[0])
                return [uid for uid in self._uids if uid >= start]
            return []
        if command == "SINCE" and len(criteria) > 1:
            since = datetime.strptime(criteria[1], "%d-%b-%Y").date()
            matched = []
            for uid in self._uids:
                internaldate = self._data[uid].get(b"INTERNALDATE")
                if isinstance(internaldate, datetime) and internaldate.date() >= since:
                    matched.append(uid)
            return matched
        return []

    def fetch(self, uids, fields):  # type: ignore[override]
        return {uid: self._data[uid] for uid in uids}


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


def test_fetch_skips_messages_before_start(monkeypatch, tmp_path: Path, account: AccountConfig) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0)

    fake_client.set_messages(
        {
            1: {b"RFC822": b"old", b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0)},
            2: {b"RFC822": b"new", b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0)},
            3: {b"RFC822": b"newer", b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, 0)},
        }
    )

    monkeypatch.setattr(imap_client, "IMAPClient", lambda *args, **kwargs: fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [2, 3]
    assert state.get_last_uid(account.login) == 3
    assert state.get_last_check_time(account.login) is not None
    assert fake_client.search_calls[0] == ["UID", "1:*"]
    assert fake_client.search_calls[1][0] == "SINCE"


def test_baseline_updates_when_only_old(monkeypatch, tmp_path: Path, account: AccountConfig) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0)

    fake_client.set_messages(
        {
            10: {b"RFC822": b"first", b"INTERNALDATE": datetime(2023, 12, 31, 23, 59, 0)},
            11: {b"RFC822": b"second", b"INTERNALDATE": datetime(2024, 1, 1, 8, 0, 0)},
        }
    )

    monkeypatch.setattr(imap_client, "IMAPClient", lambda *args, **kwargs: fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    messages = client.fetch_new_messages()

    assert messages == []
    assert state.get_last_uid(account.login) == 11
    assert fake_client.search_calls[0] == ["UID", "1:*"]
    assert fake_client.search_calls[1][0] == "SINCE"


def test_steady_state_uses_uid_cursor_only(
    monkeypatch, tmp_path: Path, account: AccountConfig
) -> None:
    fake_client = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    start_time = datetime(2024, 1, 2, 12, 0, 0)

    fake_client.set_messages(
        {
            1: {b"RFC822": b"old", b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0)},
            2: {b"RFC822": b"new", b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0)},
            3: {b"RFC822": b"newer", b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, 0)},
        }
    )

    monkeypatch.setattr(imap_client, "IMAPClient", lambda *args, **kwargs: fake_client)
    state = StateManager(tmp_path / "state.json")

    client = ResilientIMAP(account, state, start_time=start_time)
    client.fetch_new_messages()

    fake_client.search_calls.clear()
    fake_client.set_messages(
        {
            1: {b"RFC822": b"old", b"INTERNALDATE": datetime(2024, 1, 2, 10, 0, 0)},
            2: {b"RFC822": b"new", b"INTERNALDATE": datetime(2024, 1, 2, 12, 15, 0)},
            3: {b"RFC822": b"newer", b"INTERNALDATE": datetime(2024, 1, 2, 12, 30, 0)},
            4: {b"RFC822": b"late", b"INTERNALDATE": datetime(2024, 1, 1, 9, 0, 0)},
        }
    )

    messages = client.fetch_new_messages()

    assert [uid for uid, _ in messages] == [4]
    assert fake_client.search_calls == [["UID", "4:*"]]
