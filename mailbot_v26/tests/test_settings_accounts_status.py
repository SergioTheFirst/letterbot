"""S1: read-only mailbox status builder (web_observability/accounts_status.py).

The builder reads accounts.ini, the IMAP_HEALTH success events and
runtime_health.json and turns them into one row per mailbox. It must never
expose secrets (passwords, bot token, free-text IMAP errors) and must mirror
what the worker would do with the same accounts.ini.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import accounts_status
from mailbot_v26.web_observability.accounts_status import (
    build_home_card,
    collect_accounts_status,
    default_runtime_health_path,
    detect_provider,
    mask_login,
)

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
PASSWORD_MARKER = "PW-MARKER-5Q"
TOKEN_MARKER = "123456789:TOKEN-MARKER-ABCDEFGHIJKLMNOP"
ERROR_MARKER = "LAST-ERROR-MARKER-8R"
CHAT_ID = "272250747"
# Two halves of a password that a malformed line splits (configparser
# lowercases key names, so these are looked for in any case).
SECRET_HEAD = "Sup3rHeadQ7"
SECRET_TAIL = "Sup3rTailZ9"


def _account(
    section: str = "work_mail",
    *,
    login: str = "worker@gmail.com",
    password: str = PASSWORD_MARKER,
    host: str = "imap.gmail.com",
    port: str = "993",
    use_ssl: str = "true",
    chat: str | None = CHAT_ID,
) -> str:
    lines = [
        f"[{section}]",
        f"login = {login}",
        f"password = {password}",
        f"host = {host}",
        f"port = {port}",
        f"use_ssl = {use_ssl}",
    ]
    if chat is not None:
        lines.append(f"telegram_chat_id = {chat}")
    return "\n".join(lines) + "\n"


def _telegram(*, token: str = TOKEN_MARKER, chat: str = "") -> str:
    return f"[telegram]\nbot_token = {token}\nchat_id = {chat}\n"


def _config_dir(tmp_path: Path, accounts_text: str | bytes | None) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "settings.ini").write_text("[ui]\nlocale = en\n", encoding="utf-8")
    if accounts_text is not None:
        target = config_dir / "accounts.ini"
        if isinstance(accounts_text, bytes):
            target.write_bytes(accounts_text)
        else:
            target.write_text(accounts_text, encoding="utf-8")
    return config_dir


def _db(tmp_path: Path) -> Path:
    db_path = tmp_path / "status.sqlite"
    KnowledgeDB(db_path)
    return db_path


def _imap_event(
    db_path: Path, *, account_id: str, at: datetime, subtype: str = "success"
) -> None:
    payload = {"subtype": subtype, "detail": "IMAP poll succeeded"}
    event = EventV1(
        event_type=EventType.IMAP_HEALTH,
        ts_utc=at.timestamp(),
        account_id=account_id,
        entity_id=f"imap_health:{subtype}",
        email_id=None,
        payload=payload,
    )
    payload_json = json.dumps(payload)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id,
                                   email_id, payload, payload_json, schema_version,
                                   fingerprint)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_type.value,
                event.ts_utc,
                at.isoformat(),
                account_id,
                event.entity_id,
                None,
                payload_json,
                payload_json,
                1,
                fingerprint(event),
            ),
        )


def _runtime(tmp_path: Path, states: object) -> Path:
    path = tmp_path / "runtime_health.json"
    path.write_text(json.dumps(states), encoding="utf-8")
    return path


def _status(
    tmp_path: Path,
    accounts_text: str | bytes | None,
    *,
    runtime: object | None = None,
    events: list[tuple[str, datetime]] | None = None,
    locale: str = "en",
    db_path: Path | None = None,
) -> dict:
    config_dir = _config_dir(tmp_path, accounts_text)
    db = db_path or _db(tmp_path)
    for account_id, at in events or []:
        _imap_event(db, account_id=account_id, at=at)
    runtime_path = (
        _runtime(tmp_path, runtime)
        if runtime is not None
        else tmp_path / "missing-runtime.json"
    )
    return collect_accounts_status(
        config_dir=config_dir,
        db_path=db,
        runtime_health_path=runtime_path,
        locale=locale,
        now=NOW,
        tz=timezone.utc,
    )


def _row(status: dict, section: str = "work_mail") -> dict:
    rows = {row["id"]: row for row in status["accounts"]}
    return rows[section]


def _card_ids(status: dict) -> list[str]:
    return [card["id"] for card in status["cards"]]


def _blocking_codes(status: dict) -> set[tuple[str, str, str]]:
    return {(item["code"], item["section"], item["field"]) for item in status["blocking"]}


def _assert_no_secrets(status: dict) -> None:
    dumped = json.dumps(status, ensure_ascii=False)
    for marker in (PASSWORD_MARKER, TOKEN_MARKER, ERROR_MARKER, "TOKEN-MARKER"):
        assert marker not in dumped, marker


def _assert_no_secret_halves(status: dict) -> None:
    dumped = json.dumps(status, ensure_ascii=False).lower()
    for part in (SECRET_HEAD, SECRET_TAIL):
        assert part.lower() not in dumped, part


def test_ok_gmail_mailbox_with_recent_success(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account() + _telegram(),
        events=[("worker@gmail.com", NOW - timedelta(minutes=3))],
    )

    assert status["file"]["state"] == "ok"
    row = _row(status)
    assert row["state"] == "ok"
    assert row["state_label"] == "OK"
    assert row["severity"] == "success"
    assert row["login"] == "w…@gmail.com"
    assert row["provider"] == "gmail"
    assert row["server"] == "imap.gmail.com:993 · SSL"
    assert row["password_set"] is True
    assert row["chat"] == {
        "status": "set",
        "source": "mailbox",
        "label": "Set (this mailbox)",
    }
    assert row["last_check"] == "3 min ago"
    assert row["issues"] == []
    assert status["startup_blocked"] is False
    assert status["blocking"] == []
    assert status["cards"] == []
    assert status["telegram"]["token_set"] is True
    assert status["summary"]["ok"] == 1
    assert status["summary"]["attention"] == 0
    assert status["summary"]["text"] == "1 OK"
    _assert_no_secrets(status)


def test_gmail_login_rejected_shows_app_password_card_without_error_text(
    tmp_path: Path,
) -> None:
    runtime = {
        "work_mail": {
            "consecutive_failures": 2,
            "last_error": ERROR_MARKER,
            "last_error_class": "LoginError",
            "failure_bucket": "imap_auth",
            "next_retry_at_utc": (NOW + timedelta(minutes=5)).isoformat(),
            "cooldown_until_utc": None,
            "last_alert_fingerprint": f"LoginError:{ERROR_MARKER}:imap.gmail.com:993",
        }
    }
    status = _status(
        tmp_path,
        _account() + _telegram(),
        runtime=runtime,
        events=[("worker@gmail.com", NOW - timedelta(minutes=2))],
    )

    row = _row(status)
    assert row["state"] == "login_rejected"
    assert row["state_label"] == "Login rejected"
    assert row["severity"] == "danger"
    assert "app password" in row["detail"]
    assert "12:05" in row["detail"]
    assert _card_ids(status) == ["gmail"]
    gmail_card = status["cards"][0]
    assert "myaccount.google.com/apppasswords" in " ".join(gmail_card["steps"])
    assert "host = imap.gmail.com" in gmail_card["snippet"]
    assert "password = CHANGE_ME" in gmail_card["snippet"]
    assert status["summary"]["attention"] == 1
    assert status["summary"]["text"] == "1 needs attention"
    _assert_no_secrets(status)


def test_missing_chat_turns_row_amber_and_blocks_the_next_start(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account(chat=None) + _telegram(chat=""),
        events=[("worker@gmail.com", NOW - timedelta(minutes=1))],
    )

    row = _row(status)
    assert row["state"] == "ok"
    assert row["severity"] == "warn"
    assert row["chat"]["status"] == "missing"
    assert row["chat"]["label"] == "Missing"
    assert status["startup_blocked"] is True
    assert ("chat_missing", "work_mail", "telegram_chat_id") in _blocking_codes(status)
    assert status["summary"]["attention"] == 1
    assert status["summary"]["ok"] == 0


def test_default_chat_is_used_when_the_mailbox_has_none(tmp_path: Path) -> None:
    status = _status(tmp_path, _account(chat=None) + _telegram(chat="-1001234567890"))

    row = _row(status)
    assert row["chat"]["status"] == "set"
    assert row["chat"]["source"] == "default"
    assert status["telegram"]["default_chat"]["status"] == "set"
    assert status["startup_blocked"] is False


def test_placeholder_default_chat_blocks_start_even_with_mailbox_chats(
    tmp_path: Path,
) -> None:
    status = _status(tmp_path, _account() + _telegram(chat="CHANGE_ME"))

    assert status["telegram"]["default_chat"]["status"] == "invalid"
    assert ("chat_invalid", "telegram", "chat_id") in _blocking_codes(status)
    assert status["startup_blocked"] is True
    assert _row(status)["chat"]["status"] == "set"


def test_mailbox_chat_with_equals_sign_is_invalid(tmp_path: Path) -> None:
    status = _status(tmp_path, _account(chat="=272250747") + _telegram())

    row = _row(status)
    assert row["chat"]["status"] == "invalid"
    assert ("chat_invalid", "work_mail", "telegram_chat_id") in _blocking_codes(status)


def test_non_numeric_chat_is_flagged_as_a_warning_only(tmp_path: Path) -> None:
    status = _status(tmp_path, _account(chat="my chat") + _telegram())

    row = _row(status)
    assert row["chat"]["status"] == "invalid"
    assert row["severity"] in {"warn", "danger"}
    assert status["startup_blocked"] is False
    assert [issue["code"] for issue in row["issues"]] == ["chat_format"]


def test_missing_bot_token_blocks_start(tmp_path: Path) -> None:
    status = _status(tmp_path, _account() + _telegram(token="CHANGE_ME"))

    assert status["telegram"]["token_set"] is False
    assert ("token_missing", "telegram", "bot_token") in _blocking_codes(status)
    assert status["startup_blocked"] is True


def test_outlook_mailbox_is_not_supported(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account(login="someone@outlook.com", host="outlook.office365.com")
        + _telegram(),
    )

    row = _row(status)
    assert row["provider"] == "outlook"
    assert row["state"] == "unsupported"
    assert row["severity"] == "danger"
    assert "outlook" in _card_ids(status)
    outlook_card = next(card for card in status["cards"] if card["id"] == "outlook")
    assert outlook_card["snippet"] == ""
    assert "forward" in " ".join(outlook_card["steps"]).lower()


def test_outlook_login_with_placeholder_host_is_not_supported(tmp_path: Path) -> None:
    status = _status(
        tmp_path, _account(login="someone@hotmail.com", host="CHANGE_ME") + _telegram()
    )

    row = _row(status)
    assert row["provider"] == "outlook"
    assert row["state"] == "unsupported"


def test_outlook_mailbox_that_works_is_not_called_unsupported(tmp_path: Path) -> None:
    # Evidence beats the guess from the host name (e.g. a bridge or a legacy tenant).
    status = _status(
        tmp_path,
        _account(login="someone@outlook.com", host="outlook.office365.com")
        + _telegram(),
        events=[("someone@outlook.com", NOW - timedelta(minutes=2))],
    )

    assert _row(status)["state"] == "ok"
    assert "outlook" not in _card_ids(status)


def test_outlook_login_behind_a_local_bridge_is_not_guessed(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account(login="someone@outlook.com", host="localhost", port="1143", use_ssl="false")
        + _telegram(),
    )

    row = _row(status)
    assert row["provider"] == "other"
    assert row["state"] == "not_checked"


@pytest.mark.parametrize(
    ("age", "state", "severity"),
    [
        (timedelta(minutes=15), "ok", "success"),
        (timedelta(minutes=20), "delayed", "warn"),
        (timedelta(minutes=60), "delayed", "warn"),
        (timedelta(hours=2), "not_checking", "danger"),
    ],
)
def test_recency_of_the_last_success_decides_the_state(
    tmp_path: Path, age: timedelta, state: str, severity: str
) -> None:
    status = _status(
        tmp_path,
        _account() + _telegram(),
        events=[("worker@gmail.com", NOW - age)],
    )

    row = _row(status)
    assert row["state"] == state
    assert row["severity"] == severity


def test_no_evidence_means_not_checked_yet(tmp_path: Path) -> None:
    status = _status(tmp_path, _account() + _telegram())

    row = _row(status)
    assert row["state"] == "not_checked"
    assert row["state_label"] == "Not checked yet"
    assert row["severity"] == "muted"
    assert row["last_check"] == "never"
    assert status["summary"]["waiting"] == 1
    assert status["summary"]["text"] == "1 not checked yet"


@pytest.mark.parametrize(
    "recorded_as", ["Worker@Gmail.com", "worker@gmail.com"], ids=["as_spelled", "normalized"]
)
def test_last_success_matches_the_configured_login_and_its_normalized_form(
    tmp_path: Path, recorded_as: str
) -> None:
    # The worker records IMAP_HEALTH with the login exactly as accounts.ini
    # spells it; the normalized (casefolded) spelling is accepted too.
    status = _status(
        tmp_path,
        _account(login="Worker@Gmail.com") + _telegram(),
        events=[(recorded_as, NOW - timedelta(minutes=4))],
    )

    assert _row(status)["state"] == "ok"


def test_database_without_events_table_means_not_checked_yet(tmp_path: Path) -> None:
    empty_db = tmp_path / "empty.sqlite"
    sqlite3.connect(empty_db).close()
    status = _status(tmp_path, _account() + _telegram(), db_path=empty_db)

    assert _row(status)["state"] == "not_checked"
    assert status["evidence"]["db"] == "missing"


def test_failure_events_do_not_count_as_a_successful_check(tmp_path: Path) -> None:
    config_dir = _config_dir(tmp_path, _account() + _telegram())
    db_path = _db(tmp_path)
    _imap_event(
        db_path,
        account_id="worker@gmail.com",
        at=NOW - timedelta(minutes=1),
        subtype="dead_letter",
    )
    status = collect_accounts_status(
        config_dir=config_dir,
        db_path=db_path,
        runtime_health_path=tmp_path / "none.json",
        now=NOW,
        tz=timezone.utc,
    )

    assert _row(status)["state"] == "not_checked"


def test_cooldown_shows_paused_until_local_time(tmp_path: Path) -> None:
    runtime = {
        "work_mail": {
            "consecutive_failures": 3,
            "failure_bucket": "imap_network_timeout",
            "next_retry_at_utc": (NOW + timedelta(hours=2, minutes=30)).isoformat(),
            "cooldown_until_utc": (NOW + timedelta(hours=2, minutes=30)).isoformat(),
            "cooldown_reason": "repeated_handshake_connect_failures",
        }
    }
    status = _status(tmp_path, _account() + _telegram(), runtime=runtime)

    row = _row(status)
    assert row["state"] == "paused"
    assert row["state_label"] == "Paused until 14:30"
    assert row["paused_until"] == (NOW + timedelta(hours=2, minutes=30)).isoformat()
    assert row["severity"] == "danger"


def test_cooldown_on_another_day_includes_the_date(tmp_path: Path) -> None:
    until = NOW + timedelta(hours=20)
    runtime = {
        "work_mail": {
            "failure_bucket": "imap_network_timeout",
            "consecutive_failures": 3,
            "cooldown_until_utc": until.isoformat(),
            "next_retry_at_utc": until.isoformat(),
        }
    }
    status = _status(tmp_path, _account() + _telegram(), runtime=runtime)

    assert _row(status)["state_label"] == "Paused until 2026-10-21 08:00"


@pytest.mark.parametrize(
    ("bucket", "state"),
    [
        ("imap_connectivity", "cant_reach"),
        ("imap_network_timeout", "cant_reach"),
        ("imap_server_unavailable", "cant_reach"),
        ("other", "connection_problem"),
    ],
)
def test_failure_buckets_map_to_states(tmp_path: Path, bucket: str, state: str) -> None:
    runtime = {
        "work_mail": {
            "failure_bucket": bucket,
            "consecutive_failures": 1,
            "last_error": ERROR_MARKER,
            "next_retry_at_utc": (NOW + timedelta(minutes=1)).isoformat(),
        }
    }
    status = _status(
        tmp_path,
        _account() + _telegram(),
        runtime=runtime,
        events=[("worker@gmail.com", NOW - timedelta(minutes=2))],
    )

    assert _row(status)["state"] == state
    _assert_no_secrets(status)


def test_expired_cooldown_without_failure_is_ignored(tmp_path: Path) -> None:
    runtime = {
        "work_mail": {
            "failure_bucket": None,
            "consecutive_failures": 0,
            "cooldown_until_utc": (NOW - timedelta(hours=1)).isoformat(),
        }
    }
    status = _status(
        tmp_path,
        _account() + _telegram(),
        runtime=runtime,
        events=[("worker@gmail.com", NOW - timedelta(minutes=2))],
    )

    assert _row(status)["state"] == "ok"


def test_runtime_state_of_other_sections_is_not_applied(tmp_path: Path) -> None:
    runtime = {"old_name": {"failure_bucket": "imap_auth", "consecutive_failures": 4}}
    status = _status(
        tmp_path,
        _account() + _telegram(),
        runtime=runtime,
        events=[("worker@gmail.com", NOW - timedelta(minutes=2))],
    )

    assert _row(status)["state"] == "ok"


@pytest.mark.parametrize(
    "raw",
    ["not json", "[1, 2, 3]", '{"work_mail": "oops"}', '{"work_mail": {"failure_bucket": 5}}'],
)
def test_malformed_runtime_health_degrades_without_crashing(
    tmp_path: Path, raw: str
) -> None:
    config_dir = _config_dir(tmp_path, _account() + _telegram())
    runtime_path = tmp_path / "runtime_health.json"
    runtime_path.write_text(raw, encoding="utf-8")
    status = collect_accounts_status(
        config_dir=config_dir,
        db_path=_db(tmp_path),
        runtime_health_path=runtime_path,
        now=NOW,
        tz=timezone.utc,
    )

    assert _row(status)["state"] == "not_checked"


def test_unreadable_database_gives_unknown_state(tmp_path: Path) -> None:
    broken_db = tmp_path / "broken.sqlite"
    broken_db.write_bytes(b"this is not a sqlite database at all" * 10)
    status = _status(tmp_path, _account() + _telegram(), db_path=broken_db)

    row = _row(status)
    assert row["state"] == "unknown"
    assert row["last_check"] == "unknown"
    assert status["evidence"]["db"] == "unavailable"


def test_missing_database_means_not_checked_yet(tmp_path: Path) -> None:
    status = _status(
        tmp_path, _account() + _telegram(), db_path=tmp_path / "absent.sqlite"
    )

    assert _row(status)["state"] == "not_checked"
    assert status["evidence"]["db"] == "missing"
    assert not (tmp_path / "absent.sqlite").exists()


def test_missing_accounts_file_shows_first_run_checklist(tmp_path: Path) -> None:
    status = _status(tmp_path, None)

    assert status["file"]["state"] == "missing"
    assert status["file"]["path"].endswith("accounts.ini")
    assert status["accounts"] == []
    assert _card_ids(status) == ["first_run"]
    assert status["summary"]["text"] == "accounts.ini not found"
    assert status["startup_blocked"] is True


def test_template_only_file_shows_first_run_checklist(tmp_path: Path) -> None:
    template = _account(
        "example_account",
        login="user@example.com",
        password="CHANGE_ME",
        host="imap.example.com",
        chat="CHANGE_ME",
    ) + _telegram(token="CHANGE_ME", chat="CHANGE_ME")
    status = _status(tmp_path, template)

    assert status["file"]["state"] == "no_mailboxes"
    assert status["accounts"] == []
    assert _card_ids(status) == ["first_run"]
    first_run = status["cards"][0]
    assert "[example_account]" in " ".join(first_run["steps"])
    assert status["startup_blocked"] is True


def test_example_section_next_to_a_real_mailbox_blocks_start(tmp_path: Path) -> None:
    text = (
        _account()
        + _account(
            "example_account",
            login="user@example.com",
            password="CHANGE_ME",
            host="imap.example.com",
        )
        + _telegram()
    )
    status = _status(tmp_path, text)

    example = _row(status, "example_account")
    assert example["state"] == "setup_incomplete"
    assert [issue["code"] for issue in example["issues"]] == ["example_section"]
    assert ("example_section", "example_account", "") in _blocking_codes(status)
    assert "generic" in _card_ids(status)


def test_placeholder_password_is_setup_incomplete(tmp_path: Path) -> None:
    status = _status(tmp_path, _account(password="CHANGE_ME") + _telegram())

    row = _row(status)
    assert row["state"] == "setup_incomplete"
    assert row["password_set"] is False
    assert ("missing_field", "work_mail", "password") in _blocking_codes(status)
    assert row["issues"][0]["text"] == "[work_mail] password is empty or still CHANGE_ME"
    assert "gmail" in _card_ids(status)


def test_percent_in_password_gives_a_hint_without_the_value(tmp_path: Path) -> None:
    status = _status(tmp_path, _account(password="pa%ss" + PASSWORD_MARKER) + _telegram())

    row = _row(status)
    assert row["state"] == "setup_incomplete"
    assert row["password_set"] is True
    assert ("percent_sign", "work_mail", "password") in _blocking_codes(status)
    assert "percent" in _card_ids(status)
    _assert_no_secrets(status)


def test_percent_in_bot_token_is_reported_on_the_telegram_section(
    tmp_path: Path,
) -> None:
    status = _status(tmp_path, _account() + _telegram(token="12%34" + TOKEN_MARKER))

    assert ("percent_sign", "telegram", "bot_token") in _blocking_codes(status)
    _assert_no_secrets(status)


def test_percent_in_a_key_letterbot_does_not_read_only_warns(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account() + "notes = 100% mine\n" + _telegram(),
        events=[("worker@gmail.com", NOW - timedelta(minutes=1))],
    )

    row = _row(status)
    assert row["state"] == "ok"
    assert row["severity"] == "warn"
    # Only keys Letterbot reads are named: another "key" can be part of a
    # value (see the next test).
    assert [(i["code"], i["field"], i["blocking"]) for i in row["issues"]] == [
        ("percent_sign", "", False)
    ]
    assert row["issues"][0]["text"] == (
        "[work_mail] a value contains %: write %% instead of each %"
    )
    assert status["startup_blocked"] is False
    assert "percent" in _card_ids(status)


@pytest.mark.parametrize(
    "line",
    [
        # The password on its own line: "abc=def%" reads as key "abc".
        f"password =\n{SECRET_HEAD}={SECRET_TAIL}%",
        # A line without "=" splits at ":".
        f"password {SECRET_HEAD}:{SECRET_TAIL}%",
        f"password{SECRET_HEAD}:{SECRET_TAIL}%",
    ],
    ids=["value_on_its_own_line", "missing_equals", "missing_equals_and_space"],
)
def test_percent_in_a_value_read_as_a_key_names_no_key(
    tmp_path: Path, line: str
) -> None:
    text = (
        f"[work_mail]\nlogin = worker@gmail.com\n{line}\nnotes = 100% mine\n"
        f"host = imap.gmail.com\ntelegram_chat_id = {CHAT_ID}\n" + _telegram()
    )
    status = _status(tmp_path, text)

    row = _row(status)
    percent = [issue for issue in row["issues"] if issue["code"] == "percent_sign"]
    # One hint for both unknown keys, naming neither.
    assert [(issue["field"], issue["blocking"]) for issue in percent] == [("", False)]
    assert ("missing_field", "work_mail", "password") in _blocking_codes(status)
    _assert_no_secret_halves(status)


def test_percent_in_default_chat_marks_it_invalid_and_blocks(tmp_path: Path) -> None:
    status = _status(tmp_path, _account(chat=None) + _telegram(chat="-100%1"))

    assert status["telegram"]["default_chat"]["status"] == "invalid"
    assert _row(status)["chat"]["status"] == "invalid"
    assert ("percent_sign", "telegram", "chat_id") in _blocking_codes(status)
    assert ("chat_missing", "work_mail", "telegram_chat_id") not in _blocking_codes(status)


def test_escaped_percent_is_readable(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account(password="pa%%ss") + _telegram(),
        events=[("worker@gmail.com", NOW - timedelta(minutes=1))],
    )

    assert _row(status)["state"] == "ok"
    assert status["blocking"] == []


def test_cp1251_file_gives_an_encoding_card(tmp_path: Path) -> None:
    text = _account(password="пароль" + PASSWORD_MARKER) + _telegram()
    status = _status(tmp_path, text.encode("cp1251"))

    assert status["file"]["state"] == "encoding"
    assert status["accounts"] == []
    assert _card_ids(status) == ["encoding"]
    assert status["summary"]["severity"] == "danger"
    _assert_no_secrets(status)


def test_duplicate_section_is_unreadable_with_its_line(tmp_path: Path) -> None:
    text = _account() + _account(password="second-" + PASSWORD_MARKER) + _telegram()
    status = _status(tmp_path, text)

    assert status["file"]["state"] == "unreadable"
    assert status["file"]["line"] == 8
    assert _card_ids(status) == ["unreadable"]
    assert "line 8" in status["cards"][0]["intro"]
    _assert_no_secrets(status)


def test_line_outside_any_section_is_unreadable(tmp_path: Path) -> None:
    status = _status(tmp_path, f"password = {PASSWORD_MARKER}\n" + _account())

    assert status["file"]["state"] == "unreadable"
    assert status["file"]["line"] == 1
    _assert_no_secrets(status)


def test_bad_port_and_bad_ssl_skip_the_mailbox_without_blocking(tmp_path: Path) -> None:
    text = (
        _account(port="99999")
        + _account("second_mail", login="second@yandex.ru", host="imap.yandex.ru", use_ssl="maybe")
        + _telegram()
    )
    status = _status(tmp_path, text)

    first = _row(status, "work_mail")
    second = _row(status, "second_mail")
    assert first["state"] == "setup_incomplete"
    assert [issue["code"] for issue in first["issues"]] == ["bad_port"]
    assert second["state"] == "setup_incomplete"
    assert [issue["code"] for issue in second["issues"]] == ["bad_ssl"]
    assert status["startup_blocked"] is False
    assert {"gmail", "yandex", "generic"} <= set(_card_ids(status))


def test_invalid_section_name_is_skipped_by_the_worker(tmp_path: Path) -> None:
    status = _status(tmp_path, _account("Work Mail") + _telegram())

    row = _row(status, "Work Mail")
    assert row["state"] == "setup_incomplete"
    assert [issue["code"] for issue in row["issues"]] == ["bad_section_name"]
    assert status["startup_blocked"] is False


def test_padded_telegram_header_is_a_mailbox_section_like_the_preflight_sees_it(
    tmp_path: Path,
) -> None:
    # "[ telegram ]" keeps its spaces: the startup preflight checks it as a
    # mailbox (missing login/password/host) and the bot token is not found.
    status = _status(tmp_path, _account() + f"[ telegram ]\nbot_token = {TOKEN_MARKER}\n")

    padded = _row(status, " telegram ")
    assert padded["state"] == "setup_incomplete"
    blocking = _blocking_codes(status)
    assert ("missing_field", " telegram ", "login") in blocking
    assert ("token_missing", "telegram", "bot_token") in blocking
    _assert_no_secrets(status)


def test_section_named_after_an_address_is_masked(tmp_path: Path) -> None:
    status = _status(tmp_path, _account("owner.full@gmail.com") + _telegram())

    row = _row(status, "o…@gmail.com")
    assert row["issues"][0]["section"] == "o…@gmail.com"
    assert row["issues"][0]["text"].startswith("[o…@gmail.com] is not a valid name")
    assert "owner.full@gmail.com" not in json.dumps(status, ensure_ascii=False)


@pytest.mark.parametrize(
    "header",
    [
        # configparser takes a header up to the line's last "]".
        f"[work_mail] password = {SECRET_HEAD}]{SECRET_TAIL}",
        # Line breaks lost: the next lines joined onto the header.
        f"[work_mail] login = worker@gmail.com password = {SECRET_HEAD}{SECRET_TAIL}"
        " host = imap.gmail.com [telegram]",
    ],
    ids=["header_runs_into_a_value", "lines_joined"],
)
def test_header_with_more_text_is_shown_only_up_to_it(
    tmp_path: Path, header: str
) -> None:
    text = f"{header}\nlogin = worker@gmail.com\npassword = x\nhost = imap.gmail.com\n"
    status = _status(tmp_path, text + _telegram())

    row = _row(status, "work_mail…")
    assert "bad_section_name" in [issue["code"] for issue in row["issues"]]
    assert {issue["section"] for issue in row["issues"]} == {"work_mail…"}
    assert all(issue["text"].startswith("[work_mail…] ") for issue in row["issues"])
    _assert_no_secret_halves(status)


@pytest.mark.parametrize(
    "login_line",
    [
        # An indented line continues the value above it.
        f"login = worker@gmail.com\n    password = {PASSWORD_MARKER}",
        # configparser has no inline comments: the rest of the line is the value.
        f"login = worker@gmail.com ; password = {PASSWORD_MARKER}",
    ],
    ids=["continued", "inline"],
)
def test_login_with_more_text_shows_only_its_first_letter(
    tmp_path: Path, login_line: str
) -> None:
    text = (
        f"[work_mail]\n{login_line}\npassword = x\nhost = imap.gmail.com\n"
        f"telegram_chat_id = {CHAT_ID}\n" + _telegram()
    )
    status = _status(tmp_path, text)

    row = _row(status)
    assert row["login"] == "w…"
    assert row["provider"] == "gmail"
    _assert_no_secrets(status)


@pytest.mark.parametrize(
    "host_line",
    [
        f"host = imap.gmail.com\n    password = {PASSWORD_MARKER}",
        f"host = imap.gmail.com ; password = {PASSWORD_MARKER}",
    ],
    ids=["continued", "inline"],
)
def test_host_with_more_text_is_not_shown(tmp_path: Path, host_line: str) -> None:
    text = (
        f"[work_mail]\nlogin = worker@gmail.com\npassword = x\n{host_line}\n"
        f"telegram_chat_id = {CHAT_ID}\n" + _telegram()
    )
    status = _status(tmp_path, text)

    row = _row(status)
    assert row["host"] == ""
    assert row["server"] == "—"
    assert [issue["code"] for issue in row["issues"]] == ["bad_host"]
    _assert_no_secrets(status)


def test_host_longer_than_a_server_name_is_not_shown(tmp_path: Path) -> None:
    host = ".".join(["a" * 60] * 5)
    text = (
        f"[work_mail]\nlogin = worker@gmail.com\npassword = x\nhost = {host}\n"
        f"telegram_chat_id = {CHAT_ID}\n" + _telegram()
    )
    status = _status(tmp_path, text)

    row = _row(status)
    assert row["host"] == ""
    assert [issue["code"] for issue in row["issues"]] == ["bad_host"]


def test_host_typo_shows_the_login_provider_card(tmp_path: Path) -> None:
    runtime = {"work_mail": {"failure_bucket": "imap_connectivity", "consecutive_failures": 1}}
    status = _status(
        tmp_path, _account(host="imap.gmial.com") + _telegram(), runtime=runtime
    )

    row = _row(status)
    assert row["provider"] == "other"
    assert row["state"] == "cant_reach"
    assert _card_ids(status) == ["gmail", "generic"]


def test_port_993_without_ssl_is_a_warning(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account(use_ssl="false") + _telegram(),
        events=[("worker@gmail.com", NOW - timedelta(minutes=1))],
    )

    row = _row(status)
    assert [issue["code"] for issue in row["issues"]] == ["ssl_mismatch"]
    assert row["server"] == "imap.gmail.com:993 · no SSL"
    assert status["startup_blocked"] is False
    assert "generic" in _card_ids(status)


def test_missing_port_and_ssl_use_the_worker_defaults(tmp_path: Path) -> None:
    text = (
        "[work_mail]\nlogin = worker@gmail.com\n"
        f"password = {PASSWORD_MARKER}\nhost = imap.gmail.com\n"
        f"telegram_chat_id = {CHAT_ID}\n" + _telegram()
    )
    status = _status(tmp_path, text)

    row = _row(status)
    assert row["port"] == 993
    assert row["use_ssl"] is True
    assert row["issues"] == []


def test_missing_settings_ini_blocks_start(tmp_path: Path) -> None:
    config_dir = _config_dir(tmp_path, _account() + _telegram())
    (config_dir / "settings.ini").unlink()
    status = collect_accounts_status(
        config_dir=config_dir,
        db_path=_db(tmp_path),
        runtime_health_path=tmp_path / "none.json",
        now=NOW,
        tz=timezone.utc,
    )

    assert ("settings_missing", "", "") in _blocking_codes(status)


def test_without_config_dir_nothing_is_read(tmp_path: Path, monkeypatch) -> None:
    def _no_reads(*_args, **_kwargs):
        raise AssertionError("no file may be read without a config dir")

    monkeypatch.setattr(accounts_status, "load_runtime_states", _no_reads)
    monkeypatch.setattr(accounts_status, "load_last_success", _no_reads)
    status = collect_accounts_status(
        config_dir=None,
        db_path=_db(tmp_path),
        runtime_health_path=tmp_path / "runtime_health.json",
        now=NOW,
        tz=timezone.utc,
    )

    assert status["file"]["state"] == "no_config_dir"
    assert status["accounts"] == []
    assert status["cards"] == []


def test_summary_counts_and_home_card(tmp_path: Path) -> None:
    text = (
        _account()
        + _account("second_mail", login="second@yandex.ru", host="imap.yandex.ru")
        + _account("third_mail", login="third@mail.ru", host="imap.mail.ru")
        + _telegram()
    )
    runtime = {"third_mail": {"failure_bucket": "imap_auth", "consecutive_failures": 1}}
    status = _status(
        tmp_path,
        text,
        runtime=runtime,
        events=[
            ("worker@gmail.com", NOW - timedelta(minutes=1)),
            ("second@yandex.ru", NOW - timedelta(minutes=2)),
        ],
    )

    assert status["summary"]["total"] == 3
    assert status["summary"]["ok"] == 2
    assert status["summary"]["attention"] == 1
    assert status["summary"]["text"] == "2 OK · 1 needs attention"
    card = build_home_card(status, "en")
    assert card["label"] == "Mailboxes"
    assert card["headline"] == "2 OK · 1 needs attention"
    assert card["severity"] == "danger"
    assert card["url"] == "/accounts"


def test_russian_labels(tmp_path: Path) -> None:
    status = _status(
        tmp_path,
        _account() + _telegram(),
        events=[("worker@gmail.com", NOW - timedelta(minutes=3))],
        locale="ru",
    )

    row = _row(status)
    assert row["state_label"] == "Работает"
    assert row["last_check"] == "3 мин назад"
    assert status["summary"]["text"] == "1 в порядке"
    assert build_home_card(status, "ru")["label"] == "Почтовые ящики"


@pytest.mark.parametrize(
    ("login", "expected"),
    [
        ("worker@gmail.com", "w…@gmail.com"),
        ("  Boss@Example.com ", "B…@Example.com"),
        ("HQ\\MedvedevSS", "H…"),
        ("", ""),
        ("@nodomain", ""),
        ("me@почта.рф", "m…@почта.рф"),
        # The domain is shown only when the whole value is one address.
        ("worker@gmail.com\npassword = Secret1", "w…"),
        ("worker@gmail.com ; password = Secret1", "w…"),
        ("HQ\\owner\npassword = P@ss.word", "H…"),
        ("me;password=P@ss.word", "m…"),
        ("worker@gmail.com:993", "w…"),
    ],
)
def test_mask_login(login: str, expected: str) -> None:
    assert mask_login(login) == expected


@pytest.mark.parametrize(
    ("host", "login", "expected"),
    [
        ("imap.gmail.com", "", "gmail"),
        ("IMAP.GMAIL.COM", "", "gmail"),
        ("imap.yandex.ru", "", "yandex"),
        ("imap.yandex.com", "", "yandex"),
        ("imap.mail.ru", "", "mailru"),
        ("outlook.office365.com", "", "outlook"),
        ("imap-mail.outlook.com", "", "outlook"),
        ("mail.company.example", "", "other"),
        ("imap.gmial.com", "someone@gmail.com", "other"),
        ("localhost", "someone@outlook.com", "other"),
        ("", "someone@hotmail.com", "outlook"),
        ("CHANGE_ME", "someone@gmail.com", "gmail"),
        ("", "someone@inbox.ru", "mailru"),
        ("", "", "other"),
    ],
)
def test_detect_provider(host: str, login: str, expected: str) -> None:
    assert detect_provider(host, login) == expected


def test_runtime_health_path_matches_the_worker() -> None:
    from mailbot_v26 import start

    assert default_runtime_health_path() == start.CURRENT_DIR / "data" / "runtime_health.json"
