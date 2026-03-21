from __future__ import annotations

from pathlib import Path

import pytest

from mailbot_v26 import doctor
from mailbot_v26.worker.telegram_sender import DeliveryResult


@pytest.fixture(autouse=True)
def _reset_doctor_locale() -> None:
    doctor._set_doctor_locale("en")
    yield
    doctor._set_doctor_locale("en")


def test_doctor_mode_missing_yaml_and_ini_warns_and_does_not_crash(
    monkeypatch,
    tmp_path,
    capsys,
    caplog,
) -> None:
    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        doctor,
        "_check_dependencies",
        lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"),
    )
    monkeypatch.setattr(
        doctor,
        "_check_llm",
        lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")],
    )
    monkeypatch.setattr(
        doctor,
        "_check_imap",
        lambda _accounts: [doctor.DoctorEntry("IMAP", "FAIL", "нет")],
    )
    monkeypatch.setattr(
        doctor, "ping_telegram", lambda _token: (False, "missing token")
    )
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(
            delivered=False, retryable=False, error="missing token"
        ),
    )

    report = doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert report is not None
    assert "Optional legacy config.yaml not found" in output
    assert any(
        entry.component == "config.yaml" and entry.status == "WARN"
        for entry in report.entries
    )
    assert any(
        entry.component == "settings.ini (general)" and entry.status in {"OK", "FAIL"}
        for entry in report.entries
    )


def test_doctor_mode_invalid_ini_files_warns_and_does_not_crash(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    (tmp_path / "settings.ini").write_text("broken=true\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text("broken=true\n", encoding="utf-8")
    (tmp_path / "keys.ini").write_text("broken=true\n", encoding="utf-8")

    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        doctor,
        "_check_dependencies",
        lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"),
    )
    monkeypatch.setattr(
        doctor,
        "_check_llm",
        lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")],
    )
    monkeypatch.setattr(
        doctor,
        "_check_imap",
        lambda _accounts: [doctor.DoctorEntry("IMAP", "OK", "stub")],
    )
    monkeypatch.setattr(
        doctor, "ping_telegram", lambda _token: (False, "missing token")
    )
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(
            delivered=False, retryable=False, error="missing token"
        ),
    )

    report = doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert report is not None
    assert "stacktrace" not in output.lower()
    assert any(
        entry.component.startswith("settings.ini")
        and entry.status in {"WARN", "FAIL", "OK"}
        for entry in report.entries
    )
    assert any(entry.component.startswith("accounts.ini") for entry in report.entries)


def test_doctor_yaml_windows_backslash_error_reports_hint(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text('username: "HQ\\MedvedevSS"\n', encoding="utf-8")

    _raw, _config, errors = doctor._load_doctor_bot_config(tmp_path)

    assert errors
    assert "Use single quotes for Windows usernames/paths" in errors[0]


def test_doctor_two_file_mode_skips_legacy_yaml_and_keys_warnings(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[general]\ncheck_interval=120\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=user@example.com\npassword=p\nhost=imap.example.com\ntelegram_chat_id=1\n\n"
        "[telegram]\nbot_token=t\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        doctor,
        "_check_dependencies",
        lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"),
    )
    monkeypatch.setattr(
        doctor,
        "_check_llm",
        lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")],
    )
    monkeypatch.setattr(
        doctor,
        "_check_imap",
        lambda _accounts: [doctor.DoctorEntry("IMAP", "OK", "stub")],
    )
    monkeypatch.setattr(doctor, "ping_telegram", lambda _token: (True, "ok"))
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(delivered=True, retryable=False, error=None),
    )

    report = doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert "Optional legacy config.yaml" not in output
    assert all(entry.component != "config.yaml" for entry in report.entries)
    assert all(entry.component != "keys.ini" for entry in report.entries)


def test_doctor_reports_web_settings_and_warns_when_port_busy(
    monkeypatch,
    tmp_path,
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[general]\ncheck_interval=120\n[web]\nhost=127.0.0.1\nport=8787\n",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=user@example.com\npassword=p\nhost=imap.example.com\ntelegram_chat_id=1\n\n"
        "[telegram]\nbot_token=t\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        doctor,
        "_check_dependencies",
        lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"),
    )
    monkeypatch.setattr(
        doctor,
        "_check_llm",
        lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")],
    )
    monkeypatch.setattr(
        doctor,
        "_check_imap",
        lambda _accounts: [doctor.DoctorEntry("IMAP", "OK", "stub")],
    )
    monkeypatch.setattr(doctor, "_is_port_busy", lambda _host, _port: True)
    monkeypatch.setattr(doctor, "ping_telegram", lambda _token: (True, "ok"))
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(delivered=True, retryable=False, error=None),
    )

    report = doctor.run_doctor(config_dir=tmp_path)

    assert any(
        entry.component == "web (settings.ini)"
        and "host=127.0.0.1; port=8787" in entry.details
        for entry in report.entries
    )
    assert any(
        entry.component == "web port availability" and entry.status == "WARN"
        for entry in report.entries
    )
    assert any(
        str(tmp_path / "settings.ini") in (entry.details or "")
        for entry in report.entries
        if entry.component == "web port availability"
    )


def test_doctor_missing_accounts_ini_uses_explicit_config_dir_in_hint(tmp_path: Path) -> None:
    entry, accounts = doctor._validate_accounts_ini(tmp_path)

    assert accounts == []
    assert entry.status == "FAIL"
    assert str(tmp_path / "accounts.ini") in entry.details
    assert f'"{tmp_path / "accounts.ini"}"' in entry.details


def test_doctor_llm_placeholder_credentials_report_not_configured(monkeypatch) -> None:
    class _Router:
        _providers = {"cloudflare": object(), "gigachat": object()}

    assert doctor._check_provider(_Router(), "cloudflare", True, ("CHANGE_ME", "CHANGE_ME")).details == "NOT CONFIGURED"
    assert doctor._check_provider(_Router(), "gigachat", True, "CHANGE_ME").details == "NOT CONFIGURED"


def test_doctor_report_header_and_status_labels_follow_locale() -> None:
    entries = [doctor.DoctorEntry("Python", "OK", "v3.10")]

    doctor._set_doctor_locale("en")
    report_en = doctor._format_report(entries, Path("."))

    assert doctor._status_label("OK") == "OK"
    assert doctor._status_label("WARN") == "WARNING"
    assert doctor._status_label("FAIL") == "ERROR"
    assert "LETTERBOT DOCTOR REPORT" in report_en
    assert "Version:" in report_en

    doctor._set_doctor_locale("ru")
    report_ru = doctor._format_report(entries, Path("."))

    assert doctor._status_label("OK") == "ОК"
    assert doctor._status_label("WARN") == "ПРЕДУПРЕЖДЕНИЕ"
    assert doctor._status_label("FAIL") == "ОШИБКА"
    assert "ОТЧЁТ ДОКТОРА" in report_ru
    assert "Версия:" in report_ru


def test_doctor_run_uses_locale_from_settings_ini(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[ui]\nlocale = ru\n[general]\ncheck_interval=120\n",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=user@example.com\npassword=p\nhost=imap.example.com\ntelegram_chat_id=1\n\n"
        "[telegram]\nbot_token=t\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        doctor,
        "_check_dependencies",
        lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"),
    )
    monkeypatch.setattr(
        doctor,
        "_check_llm",
        lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")],
    )
    monkeypatch.setattr(
        doctor,
        "_check_imap",
        lambda _accounts: [doctor.DoctorEntry("IMAP", "OK", "stub")],
    )
    monkeypatch.setattr(doctor, "ping_telegram", lambda _token: (True, "ok"))
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(delivered=True, retryable=False, error=None),
    )

    doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert "ОТЧЁТ ДОКТОРА LETTERBOT" in output
    assert "Версия:" in output
