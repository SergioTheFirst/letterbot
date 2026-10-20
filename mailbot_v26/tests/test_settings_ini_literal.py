"""S2: accounts.ini / settings.ini values are read as written (no %-interpolation).

No shipped example relies on %(name)s substitution, so every reader of the
user INI files takes "%" literally: a password like "50%off" works instead of
stopping Letterbot, and the Settings writer never escapes "%". The same files
also must not warn about a missing per-mailbox chat id when [telegram] chat_id
covers it.
"""

from __future__ import annotations

from pathlib import Path

from mailbot_v26.config.ini_utils import new_user_ini_parser, read_user_ini_with_defaults
from mailbot_v26.config_loader import load_accounts_config
from mailbot_v26.start import _run_startup_preflight
from mailbot_v26.tools.config_bootstrap import check_config_ready, validate_config

PERCENT_PASSWORD = "50%off%(x)s%%"


def _write_config(config_dir: Path, *, mailbox_chat: str = "272250747", default_chat: str = "") -> None:
    config_dir.mkdir(exist_ok=True)
    (config_dir / "settings.ini").write_text("[general]\ncheck_interval = 120\n", encoding="utf-8")
    lines = [
        "[work_mail]",
        "login = worker@gmail.com",
        f"password = {PERCENT_PASSWORD}",
        "host = imap.gmail.com",
        "port = 993",
        "use_ssl = true",
    ]
    if mailbox_chat:
        lines.append(f"telegram_chat_id = {mailbox_chat}")
    lines += ["", "[telegram]", "bot_token = 123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"]
    if default_chat:
        lines.append(f"chat_id = {default_chat}")
    (config_dir / "accounts.ini").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_user_ini_parser_reads_percent_literally() -> None:
    parser = new_user_ini_parser()
    parser.read_string(f"[a]\nvalue = {PERCENT_PASSWORD}\n")

    assert parser.get("a", "value") == PERCENT_PASSWORD
    assert parser["a"]["value"] == PERCENT_PASSWORD


def test_read_user_ini_keeps_percent(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"
    path.write_text(f"[work_mail]\npassword = {PERCENT_PASSWORD}\n", encoding="utf-8")

    parser = read_user_ini_with_defaults(path)

    assert parser.get("work_mail", "password") == PERCENT_PASSWORD


def test_worker_loads_a_percent_password_as_written(tmp_path: Path) -> None:
    _write_config(tmp_path)

    accounts = load_accounts_config(tmp_path)

    assert [account.password for account in accounts] == [PERCENT_PASSWORD]


def test_readiness_checks_accept_a_percent_password(tmp_path: Path) -> None:
    _write_config(tmp_path)

    ready, critical, _ = check_config_ready(tmp_path)
    preflight_ok, preflight_critical, _ = _run_startup_preflight(tmp_path)
    valid, issues = validate_config(tmp_path)

    assert (ready, critical) == (True, [])
    assert (preflight_ok, preflight_critical) == (True, [])
    assert (valid, issues) == (True, [])


def test_validate_config_accepts_the_default_chat_for_a_mailbox(tmp_path: Path) -> None:
    _write_config(tmp_path, mailbox_chat="", default_chat="-1001234567890")

    valid, issues = validate_config(tmp_path)

    assert (valid, issues) == (True, [])


def test_validate_config_still_recommends_a_chat_without_any(tmp_path: Path) -> None:
    _write_config(tmp_path, mailbox_chat="", default_chat="CHANGE_ME")

    valid, issues = validate_config(tmp_path)

    assert valid is False
    assert issues == ["[work_mail] telegram_chat_id is recommended for Telegram delivery"]
