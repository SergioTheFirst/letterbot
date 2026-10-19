"""Notepad saves "UTF-8 with BOM"; every user INI reader must accept it."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.config.auto_priority_gate import load_auto_priority_gate_config
from mailbot_v26.tools.config_bootstrap import (
    migrate_two_file_config,
    run_config_ready,
)

_ACCOUNTS_INI = "\n".join(
    [
        "[work]",
        "login = user@example.com",
        "password = secret-pass",
        "host = imap.example.com",
        "port = 993",
        "use_ssl = true",
        "telegram_chat_id = 123",
        "",
        "[telegram]",
        "bot_token = tg-token",
        "chat_id = 123",
        "",
    ]
)


def _write(path: Path, text: str, *, bom: bool) -> None:
    path.write_text(text, encoding="utf-8-sig" if bom else "utf-8")
    assert path.read_bytes().startswith(b"\xef\xbb\xbf") is bom


def test_startup_preflight_accepts_bom_accounts_ini(tmp_path: Path) -> None:
    _write(tmp_path / "settings.ini", "[general]\ncheck_interval = 120\n", bom=True)
    _write(tmp_path / "accounts.ini", _ACCOUNTS_INI, bom=True)

    ready, critical, warnings = start_module._run_startup_preflight(tmp_path)  # noqa: SLF001

    assert ready is True
    assert critical == []
    assert warnings == []


def test_config_ready_accepts_bom_accounts_ini(tmp_path: Path) -> None:
    _write(tmp_path / "accounts.ini", _ACCOUNTS_INI, bom=True)

    assert run_config_ready(tmp_path) == 0


@pytest.mark.parametrize(
    ("accounts_bom", "keys_bom"),
    [(True, False), (False, True)],
    ids=["bom-accounts", "bom-keys"],
)
def test_migrate_config_reads_bom_accounts_and_keys(
    tmp_path: Path, accounts_bom: bool, keys_bom: bool
) -> None:
    _write(tmp_path / "settings.ini", "[general]\ncheck_interval = 120\n", bom=False)
    _write(
        tmp_path / "accounts.ini",
        "[work]\nlogin = user@example.com\nhost = imap.example.com\n",
        bom=accounts_bom,
    )
    _write(
        tmp_path / "keys.ini",
        "[telegram]\nbot_token = tg-token\n\n[cloudflare]\naccount_id = cf\n",
        bom=keys_bom,
    )

    migrate_two_file_config(tmp_path)

    parser = configparser.ConfigParser()
    parser.read(tmp_path / "accounts.ini", encoding="utf-8-sig")
    assert parser.sections() == ["work", "telegram", "cloudflare"]
    assert parser["work"]["login"] == "user@example.com"
    assert parser["telegram"]["bot_token"] == "tg-token"
    assert parser["cloudflare"]["account_id"] == "cf"


def test_auto_priority_gate_reads_bom_config_ini(tmp_path: Path) -> None:
    _write(
        tmp_path / "config.ini",
        "[auto_priority_gate]\nenabled = false\nwindow_days = 7\n",
        bom=True,
    )

    cfg = load_auto_priority_gate_config(tmp_path)

    assert cfg.enabled is False
    assert cfg.window_days == 7


def test_auto_priority_gate_reads_bom_legacy_no_section_config_ini(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path / "config.ini",
        "auto_priority_gate.enabled = false\nwindow_days = 14\n",
        bom=True,
    )

    cfg = load_auto_priority_gate_config(tmp_path)

    assert cfg.enabled is False
    assert cfg.window_days == 14
