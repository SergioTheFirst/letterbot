from __future__ import annotations

from pathlib import Path

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.health import mail_accounts


class FakeIMAPClient:
    def __init__(self, host: str, port: int, ssl: bool) -> None:
        self.host = host
        self.port = port
        self.ssl = ssl

    def login(self, login: str, password: str) -> None:
        if self.host == "imap.bad":
            raise RuntimeError("auth failed")

    def select_folder(self, name: str) -> None:
        if self.host == "imap.broken":
            raise RuntimeError("select failed")

    def logout(self) -> None:
        return None


def _make_config(tmp_path: Path, accounts: list[AccountConfig]) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(check_interval=10, max_attachment_mb=10, admin_chat_id="admin-chat"),
        accounts=accounts,
        keys=KeysConfig(telegram_bot_token="token", cf_account_id="cf", cf_api_token="api"),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def test_all_accounts_ok_no_telegram_warning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mail_accounts, "IMAPClient", FakeIMAPClient)
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                name="A",
                login="ok@example.com",
                password="pw",
                host="imap.ok",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
        ],
    )
    messages: list[str] = []

    def _send_telegram(token: str, chat_id: str, text: str) -> bool:
        messages.append(text)
        return True

    accounts_to_poll = mail_accounts.run_startup_mail_account_healthcheck(config, _send_telegram)

    assert len(messages) == 0
    assert [account.login for account in accounts_to_poll] == ["ok@example.com"]


def test_failed_account_sends_warning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mail_accounts, "IMAPClient", FakeIMAPClient)
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                name="A",
                login="ok@example.com",
                password="pw",
                host="imap.ok",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
            AccountConfig(
                name="B",
                login="bad@example.com",
                password="pw",
                host="imap.bad",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
        ],
    )
    messages: list[str] = []

    def _send_telegram(token: str, chat_id: str, text: str) -> bool:
        messages.append(text)
        return True

    mail_accounts.run_startup_mail_account_healthcheck(config, _send_telegram)

    assert len(messages) == 1
    assert "bad@example.com" in messages[0]
    assert "auth failed" in messages[0]
    assert "НЕ будет обрабатывать" in messages[0]


def test_failed_account_excluded_from_polling(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mail_accounts, "IMAPClient", FakeIMAPClient)
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                name="A",
                login="ok@example.com",
                password="pw",
                host="imap.ok",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
            AccountConfig(
                name="C",
                login="broken@example.com",
                password="pw",
                host="imap.broken",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
        ],
    )

    accounts_to_poll = mail_accounts.run_startup_mail_account_healthcheck(
        config,
        lambda *_args, **_kwargs: True,
    )

    assert [account.login for account in accounts_to_poll] == ["ok@example.com"]
