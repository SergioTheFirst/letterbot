"""S1: GET /accounts, GET /api/accounts/status and the Home card partial.

Read-only pages over accounts.ini + IMAP evidence: loopback needs no login,
network clients need the dashboard login, nothing is ever written, and no
secret (password, bot token, free-text IMAP error) reaches HTML or JSON.
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import render_template_string

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import extract_csrf_token
from mailbot_v26.web_observability import settings_api, settings_text
from mailbot_v26.web_observability.accounts_status import collect_accounts_status
from mailbot_v26.web_observability.app import create_app

PASSWORD_MARKER = "PW-PAGE-MARKER-3J"
TOKEN_MARKER = "987654321:TOKEN-PAGE-MARKER-QWERTYUIOPASDF"
ERROR_MARKER = "ERROR-PAGE-MARKER-6T"
FULL_LOGIN = "page.owner@gmail.com"
SECOND_LOGIN = "second.owner@yandex.ru"
MARKERS = (PASSWORD_MARKER, TOKEN_MARKER, "TOKEN-PAGE-MARKER", ERROR_MARKER, FULL_LOGIN, SECOND_LOGIN)
WEB_PASSWORD = "lan password 42"
LAN = {"REMOTE_ADDR": "192.168.1.5"}
# Halves of a password that a malformed line moves into a shown value, key
# or header (configparser lowercases keys: looked for in any case).
SECRET_HEAD = "Sup3rHeadQ7"
SECRET_TAIL = "Sup3rTailZ9"
SECRET = SECRET_HEAD + SECRET_TAIL
MALFORMED_ACCOUNTS = {
    # An indented line continues the value above it.
    "indented_after_login": (
        f"[work_mail]\nlogin = {FULL_LOGIN}\n    password = {SECRET}\n"
        "host = imap.gmail.com\n"
    ),
    "indented_after_host": (
        f"[work_mail]\nlogin = {FULL_LOGIN}\nhost = imap.gmail.com\n"
        f"    password = {SECRET}\n"
    ),
    "indented_after_windows_login": (
        "[work_mail]\nlogin = HQ\\owner\n"
        f"    password = P@{SECRET_HEAD}.{SECRET_TAIL}\nhost = imap.gmail.com\n"
    ),
    # configparser has no inline comments: the rest of the line is the value.
    "inline_after_login": (
        f"[work_mail]\nlogin = {FULL_LOGIN} ; password = {SECRET}\n"
        "password = x\nhost = imap.gmail.com\n"
    ),
    "inline_without_spaces": (
        f"[work_mail]\nlogin = owner;password=P@{SECRET_HEAD}.{SECRET_TAIL}\n"
        "password = x\nhost = imap.gmail.com\n"
    ),
    "inline_after_host": (
        f"[work_mail]\nlogin = {FULL_LOGIN}\npassword = x\n"
        f"host = imap.gmail.com ; password = {SECRET}\n"
    ),
    # A line without "=" splits at ":", and its % is reported on that "key".
    "missing_equals": (
        f"[work_mail]\nlogin = {FULL_LOGIN}\n"
        f"password {SECRET_HEAD}:{SECRET_TAIL}%\nhost = imap.gmail.com\n"
    ),
    "missing_equals_and_space": (
        f"[work_mail]\nlogin = {FULL_LOGIN}\n"
        f"password{SECRET_HEAD}:{SECRET_TAIL}%\nhost = imap.gmail.com\n"
    ),
    "value_on_its_own_line": (
        f"[work_mail]\nlogin = {FULL_LOGIN}\npassword =\n"
        f"{SECRET_HEAD}={SECRET_TAIL}%\nhost = imap.gmail.com\n"
    ),
    # configparser takes a header up to the line's last "]".
    "header_runs_into_a_value": (
        f"[work_mail] password = {SECRET_HEAD}]{SECRET_TAIL}\n"
        f"login = {FULL_LOGIN}\npassword = x\nhost = imap.gmail.com\n"
    ),
    "lines_joined": (
        f"[work_mail] login = {FULL_LOGIN} password = {SECRET} "
        "host = imap.gmail.com [telegram]\n"
    ),
}


@pytest.fixture(autouse=True)
def _no_env_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WEB_OBSERVABILITY_TOKEN", raising=False)


def _accounts_ini(*, password: str = PASSWORD_MARKER) -> str:
    return (
        "; my mailboxes\n"
        "[work_mail]\n"
        f"login = {FULL_LOGIN}\n"
        f"password = {password}\n"
        "host = imap.gmail.com\n"
        "port = 993\n"
        "use_ssl = true\n"
        "telegram_chat_id = 272250747\n"
        "\n"
        "[second_mail]\n"
        f"login = {SECOND_LOGIN}\n"
        f"password = second-{PASSWORD_MARKER}\n"
        "host = imap.yandex.ru\n"
        "port = 993\n"
        "use_ssl = true\n"
        "\n"
        "[telegram]\n"
        f"bot_token = {TOKEN_MARKER}\n"
        "chat_id = -1001234567890\n"
    )


def _setup(
    tmp_path: Path,
    *,
    accounts: str | bytes | None = None,
    locale: str = "en",
    seed_events: bool = True,
) -> dict[str, Path]:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text(
        f"[ui]\nlocale = {locale}\n", encoding="utf-8"
    )
    content = _accounts_ini() if accounts is None else accounts
    if isinstance(content, bytes):
        (config_dir / "accounts.ini").write_bytes(content)
    else:
        (config_dir / "accounts.ini").write_text(content, encoding="utf-8")
    runtime_path = tmp_path / "data" / "runtime_health.json"
    runtime_path.parent.mkdir()
    runtime_path.write_text(
        json.dumps(
            {
                "second_mail": {
                    "consecutive_failures": 2,
                    "last_error": ERROR_MARKER,
                    "last_error_class": "LoginError",
                    "failure_bucket": "imap_auth",
                    "next_retry_at_utc": (
                        datetime.now(timezone.utc) + timedelta(minutes=5)
                    ).isoformat(),
                    "last_alert_fingerprint": f"LoginError:{ERROR_MARKER}:x:993",
                }
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    if seed_events:
        _imap_success(db_path, FULL_LOGIN, datetime.now(timezone.utc) - timedelta(minutes=2))
    return {"config_dir": config_dir, "runtime": runtime_path, "db": db_path}


def _imap_success(db_path: Path, account_id: str, at: datetime) -> None:
    payload = {"subtype": "success", "detail": "IMAP poll succeeded"}
    event = EventV1(
        event_type=EventType.IMAP_HEALTH,
        ts_utc=at.timestamp(),
        account_id=account_id,
        entity_id="imap_health:success",
        email_id=None,
        payload=payload,
    )
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
                json.dumps(payload),
                json.dumps(payload),
                1,
                fingerprint(event),
            ),
        )


def _app(paths: dict[str, Path], **kwargs):
    app = create_app(
        db_path=paths["db"],
        password=kwargs.pop("password", WEB_PASSWORD),
        secret_key="secret",
        config_dir=paths["config_dir"],
        allow_cidrs=kwargs.pop("allow_cidrs", ["192.168.0.0/16"]),
        **kwargs,
    )
    app.config["SETTINGS_RUNTIME_HEALTH_PATH"] = paths["runtime"]
    return app


def _render_card(app, **context) -> str:
    """Render the Home-card partial the way a dashboard page would include it."""
    with app.test_request_context("/"):
        return render_template_string(
            '{% include "settings_accounts_card.html" %}', **context
        )


def _assert_no_markers(body: str) -> None:
    for marker in MARKERS:
        assert marker not in body, marker
    assert "last_error" not in body
    assert "fingerprint" not in body


def _login(client, password: str = WEB_PASSWORD) -> None:
    page = client.get("/login", environ_base=LAN)
    token = extract_csrf_token(page.get_data(as_text=True))
    response = client.post(
        "/login", data={"password": password, "csrf_token": token}, environ_base=LAN
    )
    assert response.status_code == 302


_SQLITE_SIDECARS = ("-wal", "-shm")


def _fingerprint_tree(paths: dict[str, Path]) -> dict[str, str]:
    # SQLite creates -wal/-shm next to a WAL database even for a read-only
    # (mode=ro) reader, like every other dashboard route; the database file
    # itself must not change and the -wal must stay empty (checked below).
    result: dict[str, str] = {}
    for folder in {paths["config_dir"], paths["runtime"].parent, paths["db"].parent}:
        for item in sorted(folder.rglob("*")):
            if item.is_file() and not item.name.endswith(_SQLITE_SIDECARS):
                result[str(item)] = hashlib.sha256(item.read_bytes()).hexdigest()
    return result


def test_accounts_page_lists_mailboxes_without_secrets(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path))
    with app.test_client() as client:
        response = client.get("/accounts")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    body = response.get_data(as_text=True)
    assert "Mail accounts" in body
    assert "p…@gmail.com" in body
    assert "s…@yandex.ru" in body
    assert "[work_mail]" in body
    assert "imap.gmail.com:993 · SSL" in body
    assert "Login rejected" in body
    assert "Yandex needs an app password" in body
    assert "1 OK · 1 needs attention" in body
    assert 'data-copy-target="snippet-yandex"' in body
    assert "settings.js" in body
    assert "Unencrypted connection" not in body
    _assert_no_markers(body)


def test_accounts_api_returns_status_json_without_secrets(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path))
    with app.test_client() as client:
        response = client.get("/api/accounts/status")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.mimetype == "application/json"
    payload = response.get_json()
    rows = {row["id"]: row for row in payload["accounts"]}
    assert rows["work_mail"]["login"] == "p…@gmail.com"
    assert rows["work_mail"]["state"] == "ok"
    assert rows["work_mail"]["password_set"] is True
    assert rows["second_mail"]["state"] == "login_rejected"
    assert rows["second_mail"]["chat"]["source"] == "default"
    assert payload["telegram"]["token_set"] is True
    assert payload["summary"]["ok"] == 1
    assert "password" not in rows["work_mail"]
    _assert_no_markers(response.get_data(as_text=True))


def test_reading_the_status_changes_no_file(tmp_path: Path) -> None:
    paths = _setup(tmp_path)
    app = _app(paths)
    before = _fingerprint_tree(paths)
    with app.test_client() as client:
        assert client.get("/accounts").status_code == 200
        assert client.get("/api/accounts/status").status_code == 200
    _render_card(app)
    assert _fingerprint_tree(paths) == before
    wal = paths["db"].with_name(paths["db"].name + "-wal")
    assert not wal.exists() or wal.stat().st_size == 0


@pytest.mark.parametrize(
    ("accounts", "expected"),
    [
        (_accounts_ini(password="pa%ss" + PASSWORD_MARKER), "write %% instead"),
        (_accounts_ini(password="пароль").encode("cp1251"), "Save accounts.ini as UTF-8"),
        ("password = x\n[work_mail]\n", "line 1"),
    ],
    ids=["percent", "cp1251", "unreadable"],
)
def test_broken_accounts_file_gives_a_hint_not_a_500(
    tmp_path: Path, accounts: str | bytes, expected: str
) -> None:
    app = _app(_setup(tmp_path, accounts=accounts))
    with app.test_client() as client:
        page = client.get("/accounts")
        api = client.get("/api/accounts/status")

    assert page.status_code == 200
    assert api.status_code == 200
    body = page.get_data(as_text=True)
    assert expected in body
    _assert_no_markers(body)
    _assert_no_markers(api.get_data(as_text=True))


@pytest.mark.parametrize("case", sorted(MALFORMED_ACCOUNTS))
def test_malformed_accounts_file_never_echoes_a_secret(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, case: str
) -> None:
    accounts = (
        MALFORMED_ACCOUNTS[case]
        + f"[telegram]\nbot_token = {TOKEN_MARKER}\nchat_id = -1001234567890\n"
    )
    paths = _setup(tmp_path, accounts=accounts)
    caplog.set_level(logging.DEBUG)
    status = collect_accounts_status(
        config_dir=paths["config_dir"],
        db_path=paths["db"],
        runtime_health_path=paths["runtime"],
    )
    app = _app(paths)
    with app.test_client() as client:
        page = client.get("/accounts")
        api = client.get("/api/accounts/status")

    assert status["file"]["state"] == "ok"
    assert page.status_code == 200
    assert api.status_code == 200
    shown = {
        "builder": json.dumps(status, ensure_ascii=False),
        "page": page.get_data(as_text=True),
        "api": api.get_data(as_text=True),
    }
    for surface, body in shown.items():
        _assert_no_markers(body)
        assert SECRET_HEAD.lower() not in body.lower(), surface
        assert SECRET_TAIL.lower() not in body.lower(), surface
    assert SECRET_HEAD.lower() not in caplog.text.lower()
    assert SECRET_TAIL.lower() not in caplog.text.lower()


def test_missing_accounts_file_shows_the_first_run_checklist(tmp_path: Path) -> None:
    paths = _setup(tmp_path)
    (paths["config_dir"] / "accounts.ini").unlink()
    app = _app(paths)
    with app.test_client() as client:
        body = client.get("/accounts").get_data(as_text=True)

    assert "Add your first mailbox" in body
    assert "accounts.ini not found" in body
    assert 'data-copy-target="snippet-first_run"' in body


def test_lan_needs_login_and_sees_the_http_warning(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path))
    with app.test_client() as client:
        page = client.get("/accounts", environ_base=LAN)
        api = client.get("/api/accounts/status", environ_base=LAN)
        assert page.status_code == 302
        assert page.headers["Location"].startswith("/login?next=")
        assert api.status_code == 401

        _login(client)
        page = client.get("/accounts", environ_base=LAN)
        api = client.get("/api/accounts/status", environ_base=LAN)

    assert page.status_code == 200
    assert api.status_code == 200
    body = page.get_data(as_text=True)
    assert "Unencrypted connection: use only on a trusted network." in body
    _assert_no_markers(body)


def test_lan_with_spoofed_forwarded_for_is_refused(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path), allow_local_smoke_bypass=True)
    headers = {"X-Forwarded-For": "127.0.0.1"}
    with app.test_client() as client:
        page = client.get("/accounts", environ_base=LAN, headers=headers)
        api = client.get("/api/accounts/status", environ_base=LAN, headers=headers)

    assert page.status_code == 403
    assert api.status_code == 403
    _assert_no_markers(page.get_data(as_text=True))


def test_request_without_client_address_is_refused(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app(_setup(tmp_path))
    caplog.set_level(logging.WARNING, logger=settings_api.__name__)
    with app.test_client() as client:
        page = client.get("/accounts", environ_base={"REMOTE_ADDR": ""})
        api = client.get("/api/accounts/status", environ_base={"REMOTE_ADDR": ""})

    assert page.status_code == 403
    assert api.status_code == 403
    denials = [
        record.getMessage()
        for record in caplog.records
        if "settings_access_denied" in record.getMessage()
    ]
    assert len(denials) == 2
    assert all("reason=no_client_address" in message for message in denials)


def test_builder_failure_gives_a_500_without_details(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app(_setup(tmp_path))

    def _boom(**_kwargs):
        raise RuntimeError(f"boom {PASSWORD_MARKER}")

    monkeypatch.setattr(settings_api, "collect_accounts_status", _boom)
    caplog.set_level(logging.ERROR, logger=settings_api.__name__)
    with app.test_client() as client:
        page = client.get("/accounts")
        api = client.get("/api/accounts/status")

    assert page.status_code == 500
    assert "status can't be shown right now" in html.unescape(page.get_data(as_text=True))
    assert api.status_code == 500
    assert api.get_json() == {"error": "status_unavailable"}
    assert api.headers["Cache-Control"] == "no-store"
    messages = [record.getMessage() for record in caplog.records]
    assert any("accounts_status_failed error_class=RuntimeError" in m for m in messages)
    assert all(PASSWORD_MARKER not in message for message in messages)
    _assert_no_markers(page.get_data(as_text=True))


def test_page_keeps_the_csp_no_inline_script_or_handler(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path))
    with app.test_client() as client:
        response = client.get("/accounts")

    body = response.get_data(as_text=True)
    assert "script-src 'self'" in response.headers["Content-Security-Policy"]
    scripts = re.findall(r"<script\b[^>]*>", body)
    assert scripts and all(" src=" in tag for tag in scripts), scripts
    assert not re.search(r"\son[a-z]+\s*=", body)


def test_lan_without_configured_password_is_refused(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path), password="")
    with app.test_client() as client:
        response = client.get("/accounts", environ_base=LAN)

    assert response.status_code == 403
    _assert_no_markers(response.get_data(as_text=True))


def test_russian_page(tmp_path: Path) -> None:
    app = _app(_setup(tmp_path, locale="ru"))
    with app.test_client() as client:
        body = client.get("/accounts").get_data(as_text=True)

    assert "Почтовые ящики" in body
    assert "Вход отклонён" in body
    assert "Для Яндекса нужен пароль приложения" in body
    assert "1 в порядке · 1 требует внимания" in body


def test_page_without_config_dir_reads_nothing(tmp_path: Path) -> None:
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        page = client.get("/accounts")
        api = client.get("/api/accounts/status")

    assert page.status_code == 200
    assert "Settings folder unknown" in page.get_data(as_text=True)
    assert api.get_json()["file"]["state"] == "no_config_dir"


def test_home_card_partial_renders_the_summary(tmp_path: Path) -> None:
    rendered = _render_card(_app(_setup(tmp_path)))

    assert 'data-testid="accounts-card"' in rendered
    assert "Mailboxes" in rendered
    assert "1 OK · 1 needs attention" in rendered
    assert 'href="/accounts"' in rendered
    _assert_no_markers(rendered)


def test_home_card_partial_accepts_a_prepared_card(tmp_path: Path) -> None:
    card = {
        "label": "Mailboxes",
        "headline": "3 OK",
        "severity": "success",
        "note": "",
        "url": "/accounts",
        "link_text": "Open mail accounts",
    }
    rendered = _render_card(_app(_setup(tmp_path)), accounts_card=card)

    assert "3 OK" in rendered


def test_home_card_survives_a_builder_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app(_setup(tmp_path))

    def _boom(**_kwargs):
        raise RuntimeError(f"boom {PASSWORD_MARKER}")

    monkeypatch.setattr(settings_api, "collect_accounts_status", _boom)
    caplog.set_level(logging.WARNING, logger=settings_api.__name__)
    rendered = _render_card(app)

    assert "Status unavailable" in rendered
    assert any("accounts_home_card_failed" in record.getMessage() for record in caplog.records)
    assert all(PASSWORD_MARKER not in record.getMessage() for record in caplog.records)


def test_text_catalogs_have_the_same_keys_and_placeholders() -> None:
    catalogs = settings_text.catalogs()
    assert set(catalogs) == {"en", "ru"}
    en, ru = catalogs["en"], catalogs["ru"]
    assert set(en) == set(ru)
    placeholder = re.compile(r"\{([a-z_]+)\}")
    for key, value in en.items():
        assert value.strip(), key
        assert ru[key].strip(), key
        assert set(placeholder.findall(value)) == set(placeholder.findall(ru[key])), key


@pytest.mark.parametrize("card_id", settings_text.CARD_IDS)
def test_every_fix_card_builds_in_both_languages(card_id: str) -> None:
    en = settings_text.fix_card(card_id, "en", line=3)
    ru = settings_text.fix_card(card_id, "ru", line=3)
    assert en["id"] == ru["id"] == card_id
    assert en["title"] and ru["title"]
    assert len(en["steps"]) == len(ru["steps"]) >= 1
    assert en["snippet"] == ru["snippet"]
    for line in en["snippet"].splitlines():
        if "=" in line:
            key, value = (part.strip() for part in line.split("=", 1))
            if key in {"login", "password", "telegram_chat_id"}:
                assert value == "CHANGE_ME", line
