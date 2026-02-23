from __future__ import annotations

from mailbot_v26 import doctor
from mailbot_v26.worker.telegram_sender import DeliveryResult


def test_doctor_mode_missing_yaml_and_ini_warns_and_does_not_crash(
    monkeypatch,
    tmp_path,
    capsys,
    caplog,
) -> None:
    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(doctor, "_check_dependencies", lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"))
    monkeypatch.setattr(doctor, "_check_llm", lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")])
    monkeypatch.setattr(doctor, "_check_imap", lambda _accounts: [doctor.DoctorEntry("IMAP", "FAIL", "нет")])
    monkeypatch.setattr(doctor, "ping_telegram", lambda _token: (False, "missing token"))
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(delivered=False, retryable=False, error="missing token"),
    )

    report = doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert report is not None
    assert "copy config.ini.example config.ini" in output
    assert any(entry.component == "config.yaml" and entry.status == "WARN" for entry in report.entries)
    assert any(entry.component == "config.ini" and entry.status == "WARN" for entry in report.entries)
    assert "config.ini missing" in caplog.text

def test_doctor_mode_invalid_ini_files_warns_and_does_not_crash(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    (tmp_path / "config.ini").write_text("broken=true\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text("broken=true\n", encoding="utf-8")
    (tmp_path / "keys.ini").write_text("broken=true\n", encoding="utf-8")

    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(doctor, "_check_dependencies", lambda: doctor.DoctorEntry("Dependencies", "OK", "stub"))
    monkeypatch.setattr(doctor, "_check_llm", lambda _base_dir: [doctor.DoctorEntry("LLM", "OK", "stub")])
    monkeypatch.setattr(doctor, "_check_imap", lambda _accounts: [doctor.DoctorEntry("IMAP", "OK", "stub")])
    monkeypatch.setattr(doctor, "ping_telegram", lambda _token: (False, "missing token"))
    monkeypatch.setattr(
        doctor,
        "send_telegram",
        lambda _payload: DeliveryResult(delivered=False, retryable=False, error="missing token"),
    )

    report = doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert report is not None
    assert "stacktrace" not in output.lower()
    assert any(entry.component.startswith("config.ini") and entry.status in {"WARN", "FAIL", "OK"} for entry in report.entries)
    assert any(entry.component.startswith("accounts.ini") for entry in report.entries)


def test_doctor_yaml_windows_backslash_error_reports_hint(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("username: \"HQ\\MedvedevSS\"\n", encoding="utf-8")

    _raw, _config, errors = doctor._load_doctor_bot_config(tmp_path)

    assert errors
    assert "Use single quotes for Windows usernames/paths" in errors[0]
