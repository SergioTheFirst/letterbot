import sqlite3
from pathlib import Path

from mailbot_v26 import doctor
from mailbot_v26.health.mail_accounts import MailAccountHealth
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _write_config(tmp_path: Path, db_path: Path) -> None:
    config = tmp_path / "config.ini"
    config.write_text(
        """
[general]
check_interval = 120
admin_chat_id = 12345

[storage]
db_path = {db_path}

[llm]
primary = cloudflare
fallback = cloudflare

[gigachat]
enabled = false

[cloudflare]
enabled = false
""".format(db_path=db_path),
        encoding="utf-8",
    )

    keys = tmp_path / "keys.ini"
    keys.write_text(
        """
[telegram]
bot_token = test-token

[cloudflare]
account_id =
api_token =
""",
        encoding="utf-8",
    )

    accounts = tmp_path / "accounts.ini"
    accounts.write_text(
        """
[primary]
login = user@example.com
password = secret
host = imap.example.com
port = 993
use_ssl = true
telegram_chat_id = 12345
""",
        encoding="utf-8",
    )


def _write_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE emails (id INTEGER PRIMARY KEY);")
        conn.execute("CREATE TABLE events_v1 (id INTEGER PRIMARY KEY);")
        conn.execute("CREATE TABLE attachments (id INTEGER PRIMARY KEY);")
        conn.execute("CREATE TABLE commitments (id INTEGER PRIMARY KEY);")


def test_doctor_mode_reports_and_sends(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "mailbot.sqlite"
    _write_db(db_path)
    _write_config(tmp_path, db_path)

    monkeypatch.setattr(doctor, "DEPENDENCY_IMPORTS", ["configparser", "sqlite3"])
    monkeypatch.setattr(doctor, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(doctor, "ping_telegram", lambda _token: (True, "reachable"))

    def fake_send(_payload):
        return DeliveryResult(delivered=True, retryable=False, error=None)

    monkeypatch.setattr(doctor, "send_telegram", fake_send)

    def fake_check_mail_accounts(_accounts, *, timeout_sec=None):
        return [
            MailAccountHealth(
                account_id="primary",
                host="imap.example.com",
                status="OK",
                error=None,
            )
        ]

    monkeypatch.setattr(doctor, "check_mail_accounts", fake_check_mail_accounts)

    report = doctor.run_doctor(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert "ОТЧЁТ ДОКТОРА LETTERBOT" in output
    assert report.telegram_sent is True
    assert any(entry.component == "SQLite" for entry in report.entries)


def test_doctor_yaml_path_does_not_use_repo_root_implicitly(tmp_path):
    config_dir = tmp_path / "mailbot_v26" / "config"
    config_dir.mkdir(parents=True)
    (tmp_path / "config.yaml").write_text("root: true", encoding="utf-8")

    resolved = doctor._resolve_yaml_config_path(config_dir)

    assert resolved is None
