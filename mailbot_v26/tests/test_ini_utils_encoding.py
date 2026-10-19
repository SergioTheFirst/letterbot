from __future__ import annotations

import logging
from pathlib import Path

import pytest

from mailbot_v26.config import ini_utils
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config_loader import load_accounts_config

_ACCOUNTS = (
    "[telegram]\n"
    "bot_token = 123:abc\n"
    "chat_id = 456\n"
    "[work]\n"
    "login = user@example.com\n"
    "password = pass\n"
    "host = imap.example.com\n"
    "name = Работа\n"
)


@pytest.fixture(autouse=True)
def _reset_warn_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ini_utils, "_WARNED_CONFIG_ONCE", set())


def test_utf8_bom_file_written_by_notepad_parses(tmp_path: Path) -> None:
    accounts = tmp_path / "accounts.ini"
    accounts.write_bytes(_ACCOUNTS.encode("utf-8-sig"))

    parser = read_user_ini_with_defaults(accounts)

    assert parser.sections() == ["telegram", "work"]
    assert parser.get("work", "name") == "Работа"
    loaded = load_accounts_config(tmp_path)
    assert [account.account_id for account in loaded] == ["work"]


def test_ansi_cp1251_file_raises_clear_error_naming_file(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    accounts = tmp_path / "accounts.ini"
    accounts.write_bytes(_ACCOUNTS.encode("cp1251"))

    with caplog.at_level(logging.ERROR, logger=ini_utils.__name__):
        with pytest.raises(ini_utils.IniEncodingError) as excinfo:
            read_user_ini_with_defaults(accounts)

    message = str(excinfo.value)
    assert str(accounts) in message
    assert "UTF-8" in message
    assert "Работа" not in message
    assert isinstance(excinfo.value, ValueError)
    assert any(str(accounts) in record.getMessage() for record in caplog.records)


def test_ansi_file_is_not_silently_replaced_by_defaults(tmp_path: Path) -> None:
    (tmp_path / "accounts.ini").write_bytes(_ACCOUNTS.encode("cp1251"))

    with pytest.raises(ini_utils.IniEncodingError):
        load_accounts_config(tmp_path)
