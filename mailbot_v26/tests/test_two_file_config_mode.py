from __future__ import annotations

import logging
from pathlib import Path

from mailbot_v26.config_loader import load_general_config, load_keys_config
from mailbot_v26.doctor import DoctorEntry, DoctorReport, report_exit_code


def test_malformed_settings_ini_falls_back_to_defaults_with_warning(
    tmp_path: Path, caplog
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[general]\ncheck_interval = broken\n", encoding="utf-8"
    )

    caplog.set_level(logging.WARNING)
    general = load_general_config(tmp_path)

    assert general.check_interval == 120
    assert "Invalid [general] values" in caplog.text


def test_legacy_keys_ini_is_ignored_in_two_file_mode(tmp_path: Path) -> None:
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8"
    )
    (tmp_path / "keys.ini").write_text(
        "[telegram]\nbot_token=tg\n[cloudflare]\naccount_id=acc\napi_token=tok\n",
        encoding="utf-8",
    )

    keys = load_keys_config(tmp_path)

    assert keys.telegram_bot_token == ""
    assert keys.cf_account_id == ""


def test_doctor_default_mode_returns_zero_with_warnings() -> None:
    report = DoctorReport(
        entries=[
            DoctorEntry("IMAP", "WARN", "timeout"),
            DoctorEntry("Telegram", "WARN", "timeout"),
        ],
        telegram_sent=False,
        telegram_error="timeout",
    )
    assert report_exit_code(report, strict=False) == 0


def test_doctor_strict_mode_returns_nonzero_with_critical_fail() -> None:
    report = DoctorReport(
        entries=[DoctorEntry("SQLite", "FAIL", "db offline")],
        telegram_sent=False,
        telegram_error="x",
    )
    assert report_exit_code(report, strict=True) == 2
