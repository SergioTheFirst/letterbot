from __future__ import annotations

from mailbot_v26.doctor import DoctorEntry, DoctorReport, report_exit_code


def test_update_guard_doctor_gate() -> None:
    report = DoctorReport(
        entries=[
            DoctorEntry("SQLite", "FAIL", "db error"),
            DoctorEntry("Telegram", "OK", "active"),
            DoctorEntry("IMAP", "OK", "1 account"),
        ],
        telegram_sent=False,
        telegram_error=None,
    )
    assert report_exit_code(report) != 0
