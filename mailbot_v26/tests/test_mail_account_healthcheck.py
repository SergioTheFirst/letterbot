from __future__ import annotations

import imaplib
from pathlib import Path

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.health import mail_accounts
from mailbot_v26.worker.telegram_sender import DeliveryResult


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
        general=GeneralConfig(
            check_interval=10,
            max_email_mb=15,
            max_attachment_mb=10,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
            admin_chat_id="admin-chat",
        ),
        accounts=accounts,
        keys=KeysConfig(
            telegram_bot_token="token", cf_account_id="cf", cf_api_token="api"
        ),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def test_all_accounts_ok_no_telegram_warning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mail_accounts, "_imap_client_cls", lambda: FakeIMAPClient)
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                account_id="A",
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

    def _send_telegram(payload) -> DeliveryResult:
        messages.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False)

    accounts_to_poll = mail_accounts.run_startup_mail_account_healthcheck(
        config, _send_telegram
    )

    assert len(messages) == 0
    assert [account.login for account in accounts_to_poll] == ["ok@example.com"]


def test_failed_account_sends_warning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mail_accounts, "_imap_client_cls", lambda: FakeIMAPClient)
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                account_id="A",
                login="ok@example.com",
                password="pw",
                host="imap.ok",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
            AccountConfig(
                account_id="B",
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

    def _send_telegram(payload) -> DeliveryResult:
        messages.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False)

    mail_accounts.run_startup_mail_account_healthcheck(config, _send_telegram)

    assert len(messages) == 1
    assert "ACCOUNT LOGIN FAILED" in messages[0]
    assert "Account: B" in messages[0]
    assert "Host: imap.bad" in messages[0]
    assert "auth failed" in messages[0]


def test_failed_account_excluded_from_polling(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mail_accounts, "_imap_client_cls", lambda: FakeIMAPClient)
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                account_id="A",
                login="ok@example.com",
                password="pw",
                host="imap.ok",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
            AccountConfig(
                account_id="C",
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
        lambda *_args, **_kwargs: DeliveryResult(delivered=True, retryable=False),
    )

    assert [account.login for account in accounts_to_poll] == ["ok@example.com"]


def test_healthcheck_error_message_not_none(monkeypatch) -> None:
    class EmptyMessageIMAPClient(FakeIMAPClient):
        def login(self, login: str, password: str) -> None:
            raise Exception(None)

    monkeypatch.setattr(
        mail_accounts, "_imap_client_cls", lambda: EmptyMessageIMAPClient
    )

    results = mail_accounts.check_mail_accounts(
        [
            AccountConfig(
                account_id="E",
                login="empty@example.com",
                password="pw",
                host="imap.empty",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
        ]
    )

    assert results[0].status == "FAILED"
    assert results[0].error is not None
    assert results[0].error != "None"


def test_healthcheck_imap_auth_failure_details(monkeypatch) -> None:
    class AuthFailIMAPClient(FakeIMAPClient):
        def login(self, login: str, password: str) -> None:
            raise imaplib.IMAP4.error("AUTH failed")

    monkeypatch.setattr(mail_accounts, "_imap_client_cls", lambda: AuthFailIMAPClient)

    results = mail_accounts.check_mail_accounts(
        [
            AccountConfig(
                account_id="F",
                login="auth@example.com",
                password="pw",
                host="imap.auth",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
        ]
    )

    assert results[0].status == "FAILED"
    assert results[0].error is not None
    assert "AUTH failed" in results[0].error


def test_startup_healthcheck_returns_unavailable_outcome_on_check_error(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path,
        [
            AccountConfig(
                account_id="A",
                login="ok@example.com",
                password="pw",
                host="imap.ok",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            ),
        ],
    )

    def _boom(_accounts):
        raise TimeoutError("read operation timed out")

    monkeypatch.setattr(mail_accounts, "check_mail_accounts", _boom)

    outcome = mail_accounts.run_startup_mail_account_healthcheck(
        config,
        lambda *_args, **_kwargs: DeliveryResult(delivered=True, retryable=False),
        return_outcome=True,
    )

    assert outcome.unavailable_reason is not None
    assert "TimeoutError" in outcome.unavailable_reason
    assert [account.account_id for account in outcome.accounts_to_poll] == ["A"]
