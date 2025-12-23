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

    def set_messages(self, messages: dict[int, dict[bytes, object]]) -> None:
        self._uids = sorted(messages.keys())
        self._data = messages

    def login(self, login: str, password: str) -> None:
        self.logged_in = True

    def select_folder(self, folder: str) -> None:
        self.folder = folder

    def search(self, criteria) -> list[int]:  # type: ignore[override]
        return list(self._uids)

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
