"""S2: the Settings editor JSON API (/api/settings/...) and its page wiring.

Writes only accounts.ini / settings.ini (atomic, backed up, etag-guarded,
validated like the worker's preflight); secrets are write-only; loopback needs
no login, the network needs a session, CSRF header and same origin; "Test"
calls verify a login only, return codes (never server text), run with a hard
deadline and a per-client rate limit, and reach only the expected servers.
"""

from __future__ import annotations

import html
import imaplib
import json
import logging
import re
import socket
import ssl
import threading
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

import pytest

from mailbot_v26.config.ini_editor import list_backups
from mailbot_v26.config_loader import load_accounts_config
from mailbot_v26.health import mail_accounts
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import extract_csrf_token
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability import settings_api
from mailbot_v26.web_observability.app import create_app

PASSWORD = "Stored-Mailbox-PW-8K"
NEW_PASSWORD = "Brand-New-PW-50%off-Q2"
TOKEN = "123456789:AAStoredTokenMarkerQWERTYUIOPASDFGH"
NEW_TOKEN = "987654321:AANewTokenMarkerZXCVBNMASDFGHJKLQW"
SERVER_TEXT = "SERVER-SECRET-TEXT-4R"
WEB_PASSWORD = "lan password 42"
NEW_WEB_PASSWORD = "Fresh-Dashboard-PW-77"
LAN = {"REMOTE_ADDR": "192.168.1.5"}
ORIGIN = "http://localhost"
NOW = datetime(2026, 10, 20, 15, 4, 5)
SECRETS = (PASSWORD, NEW_PASSWORD, TOKEN, NEW_TOKEN, SERVER_TEXT, NEW_WEB_PASSWORD)

ACCOUNTS = (
    "; my mailboxes\n"
    "[work_mail]\n"
    "login = worker@gmail.com\n"
    f"password = {PASSWORD}\n"
    "host = imap.gmail.com\n"
    "port = 993\n"
    "use_ssl = true\n"
    "telegram_chat_id = 272250747\n"
    "\n"
    "[telegram]\n"
    f"bot_token = {TOKEN}\n"
    "chat_id = -1001234567890\n"
    "\n"
    "[gigachat]\n"
    "api_key = keep-me\n"
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WEB_OBSERVABILITY_TOKEN", raising=False)
    monkeypatch.delenv("WEB_PASSWORD", raising=False)


def _config(tmp_path: Path, accounts: str | None = ACCOUNTS, settings: str | None = "[ui]\nlocale = en\n") -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    if settings is not None:
        (config_dir / "settings.ini").write_text(settings, encoding="utf-8")
    if accounts is not None:
        (config_dir / "accounts.ini").write_text(accounts, encoding="utf-8")
    return config_dir


def _app(tmp_path: Path, config_dir: Path, **kwargs):
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password=kwargs.pop("password", WEB_PASSWORD),
        secret_key="secret",
        config_dir=config_dir,
        allow_cidrs=["192.168.0.0/16"],
        **kwargs,
    )
    app.config["SETTINGS_RUNTIME_HEALTH_PATH"] = tmp_path / "runtime_health.json"
    app.config["SETTINGS_NOW"] = lambda: NOW
    app.config["SETTINGS_TEST_INTERVAL_SECONDS"] = 0
    return app


def _csrf(client, environ: dict | None = None) -> str:
    page = client.get("/accounts", environ_base=environ or {})
    assert page.status_code == 200
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page.get_data(as_text=True))
    assert match is not None
    return match.group(1)


def _post(client, path: str, body: object, *, token: str, environ: dict | None = None, origin: str | None = ORIGIN):
    headers = {"X-CSRF-Token": token}
    if origin is not None:
        headers["Origin"] = origin
    return client.post(
        path,
        data=json.dumps(body),
        content_type="application/json",
        headers=headers,
        environ_base=environ or {},
    )


def _state(client, environ: dict | None = None) -> dict:
    response = client.get("/api/settings/accounts", environ_base=environ or {})
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    return response.get_json()


def _assert_no_secrets(text: str) -> None:
    for marker in SECRETS:
        assert marker not in text, marker


def _login(client) -> None:
    page = client.get("/login", environ_base=LAN)
    token = extract_csrf_token(page.get_data(as_text=True))
    response = client.post("/login", data={"password": WEB_PASSWORD, "csrf_token": token}, environ_base=LAN)
    assert response.status_code == 302


# ------------------------------------------------------------------ state


def test_state_lists_mailboxes_and_secrets_only_as_flags(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        response = client.get("/api/settings/accounts")
        body = response.get_data(as_text=True)
        state = response.get_json()

    _assert_no_secrets(body)
    assert re.fullmatch(r"[0-9a-f]{16}", state["etag"])
    assert state["file"] == "ok"
    assert state["accounts"] == [
        {
            "id": "work_mail",
            "editable": True,
            "template": False,
            "login": "worker@gmail.com",
            "login_hidden": False,
            "host": "imap.gmail.com",
            "port": 993,
            "use_ssl": True,
            "chat_id": "272250747",
            "chat_source": "mailbox",
            "password_set": True,
            "provider": "gmail",
        }
    ]
    assert state["telegram"] == {"token_set": True, "chat_id": "-1001234567890", "chat_status": "set"}
    assert state["web"]["password_set"] is True
    assert state["web"]["local"] is True


def test_state_hides_a_login_that_is_not_one_plain_value(tmp_path: Path) -> None:
    accounts = ACCOUNTS.replace("login = worker@gmail.com", f"login = worker ; password = {SERVER_TEXT}")
    app = _app(tmp_path, _config(tmp_path, accounts))
    with app.test_client() as client:
        state = _state(client)

    row = state["accounts"][0]
    assert (row["login"], row["login_hidden"]) == ("", True)
    assert SERVER_TEXT not in json.dumps(state)


def test_state_without_accounts_file(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path, accounts=None))
    with app.test_client() as client:
        state = _state(client)

    assert (state["file"], state["etag"], state["accounts"]) == ("missing", "missing", [])


# ------------------------------------------------------------------ saves


def test_update_changes_only_the_edited_lines_and_keeps_a_backup(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        etag = _state(client)["etag"]
        response = _post(
            client,
            "/api/settings/accounts",
            {
                "etag": etag,
                "changes": [
                    {
                        "op": "update",
                        "id": "work_mail",
                        "fields": {"host": "imap.yandex.ru", "telegram_chat_id": ""},
                        "password": NEW_PASSWORD,
                    }
                ],
            },
            token=token,
        )
        body = response.get_data(as_text=True)

    assert response.status_code == 200, body
    data = response.get_json()
    assert data["backup"] == "accounts.ini.bak-20261020-150405"
    assert data["saved_at"] == "15:04"
    assert data["changed"] == [
        "[work_mail] host",
        "[work_mail] password",
        "[work_mail] telegram_chat_id",
    ]
    assert data["etag"] != etag
    text = (config_dir / "accounts.ini").read_text(encoding="utf-8")
    assert text == (
        ACCOUNTS.replace(f"password = {PASSWORD}", f"password = {NEW_PASSWORD}")
        .replace("host = imap.gmail.com", "host = imap.yandex.ru")
        .replace("telegram_chat_id = 272250747\n", "")
    )
    assert (config_dir / data["backup"]).read_text(encoding="utf-8") == ACCOUNTS
    # "%" is written and read back as typed.
    assert [a.password for a in load_accounts_config(config_dir)] == [NEW_PASSWORD]
    _assert_no_secrets(body)
    _assert_no_secrets(caplog.text)
    assert "settings_saved file=accounts.ini changed=[work_mail] host" in caplog.text


def test_add_replaces_the_untouched_template_section(tmp_path: Path) -> None:
    template = (
        "[example_account]\nlogin = user@example.com\npassword = CHANGE_ME\n"
        "host = imap.example.com\nport = 993\nuse_ssl = true\n\n"
        f"[telegram]\nbot_token = {TOKEN}\nchat_id = 272250747\n"
    )
    config_dir = _config(tmp_path, template)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client,
            "/api/settings/accounts",
            {
                "etag": _state(client)["etag"],
                "changes": [
                    {
                        "op": "add",
                        "id": "gmail_mail",
                        "fields": {"login": "me@gmail.com", "host": "imap.gmail.com", "port": 993, "use_ssl": True},
                        "password": NEW_PASSWORD,
                    }
                ],
            },
            token=token,
        )

    assert response.status_code == 200, response.get_data(as_text=True)
    accounts = load_accounts_config(config_dir)
    assert [(a.account_id, a.login, a.password) for a in accounts] == [
        ("gmail_mail", "me@gmail.com", NEW_PASSWORD)
    ]
    assert "example_account" not in (config_dir / "accounts.ini").read_text(encoding="utf-8")


def test_first_save_creates_both_files_from_the_templates(tmp_path: Path) -> None:
    config_dir = _config(tmp_path, accounts=None, settings=None)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client,
            "/api/settings/accounts",
            {
                "etag": "missing",
                "changes": [
                    {
                        "op": "add",
                        "id": "work_mail",
                        "fields": {
                            "login": "me@yandex.ru",
                            "host": "imap.yandex.ru",
                            "port": "993",
                            "use_ssl": True,
                            "telegram_chat_id": "272250747",
                        },
                        "password": NEW_PASSWORD,
                    }
                ],
                "telegram": {"bot_token": NEW_TOKEN},
            },
            token=token,
        )

    assert response.status_code == 200, response.get_data(as_text=True)
    data = response.get_json()
    assert (data["backup"], data["settings_created"]) == (None, True)
    text = (config_dir / "accounts.ini").read_text(encoding="utf-8")
    assert "[example_account]" not in text
    assert "[cloudflare]" in text and "[gigachat]" in text
    assert (config_dir / "settings.ini").exists()
    accounts = load_accounts_config(config_dir)
    assert [(a.account_id, a.telegram_chat_id) for a in accounts] == [("work_mail", "272250747")]


def test_invalid_add_is_422_with_field_issues_and_writes_nothing(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    before = (config_dir / "accounts.ini").read_bytes()
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client,
            "/api/settings/accounts",
            {
                "etag": _state(client)["etag"],
                "changes": [
                    {
                        "op": "add",
                        "id": "new_mail",
                        "fields": {"login": "a@b.ru", "host": "imap gmail com", "port": 70000},
                        "password": " spaced ",
                    }
                ],
            },
            token=token,
        )

    assert response.status_code == 422
    codes = {(i["code"], i["field"]) for i in response.get_json()["issues"]}
    assert {("bad_host", "host"), ("bad_port", "port"), ("edge_spaces", "password")} <= codes
    assert (config_dir / "accounts.ini").read_bytes() == before
    assert list_backups(config_dir / "accounts.ini") == []


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"op": "add", "id": "Work Mail", "fields": {}}, "bad_id"),
        ({"op": "add", "id": "telegram", "fields": {}}, "bad_id"),
        ({"op": "add", "id": "work_mail", "fields": {}}, "account_exists"),
        ({"op": "update", "id": "ghost_mail", "fields": {}}, "unknown_account"),
        ({"op": "update", "id": "work_mail", "fields": {"secret_key": "x"}}, "unknown_field"),
        ({"op": "update", "id": "work_mail", "fields": {"login": "a\nb"}}, "bad_characters"),
        ({"op": "update", "id": "work_mail", "fields": {"telegram_chat_id": "=123"}}, "chat_invalid"),
    ],
)
def test_refused_changes(tmp_path: Path, change: dict, code: str) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client, "/api/settings/accounts", {"etag": _state(client)["etag"], "changes": [change]}, token=token
        )

    assert response.status_code == 422
    assert code in {issue["code"] for issue in response.get_json()["issues"]}


def test_existing_problems_do_not_block_an_unrelated_change(tmp_path: Path) -> None:
    accounts = ACCOUNTS.replace(f"bot_token = {TOKEN}", "bot_token = CHANGE_ME")
    config_dir = _config(tmp_path, accounts)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client,
            "/api/settings/accounts",
            {"etag": _state(client)["etag"], "changes": [{"op": "update", "id": "work_mail", "fields": {"login": "new@gmail.com"}}]},
            token=token,
        )

    assert response.status_code == 200, response.get_data(as_text=True)
    assert "token_missing" in {issue["code"] for issue in response.get_json()["issues"]}


def test_new_blocking_problem_blocks_the_save(tmp_path: Path) -> None:
    accounts = ACCOUNTS.replace("chat_id = -1001234567890\n", "")
    config_dir = _config(tmp_path, accounts)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client,
            "/api/settings/accounts",
            {"etag": _state(client)["etag"], "changes": [{"op": "update", "id": "work_mail", "fields": {"telegram_chat_id": ""}}]},
            token=token,
        )

    assert response.status_code == 422
    assert "chat_missing" in {issue["code"] for issue in response.get_json()["issues"]}


def test_stale_etag_is_409_and_writes_nothing(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        etag = _state(client)["etag"]
        (config_dir / "accounts.ini").write_text(ACCOUNTS + "; edited in Notepad\n", encoding="utf-8")
        response = _post(
            client,
            "/api/settings/accounts",
            {"etag": etag, "changes": [{"op": "remove", "id": "work_mail"}]},
            token=token,
        )

    assert response.status_code == 409
    assert response.get_json()["code"] == "CONFLICT"
    assert "[work_mail]" in (config_dir / "accounts.ini").read_text(encoding="utf-8")


def test_remove_then_restore_from_history(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        removed = _post(
            client,
            "/api/settings/accounts",
            {"etag": _state(client)["etag"], "changes": [{"op": "remove", "id": "work_mail"}]},
            token=token,
        )
        assert removed.status_code == 200
        assert "[work_mail]" not in (config_dir / "accounts.ini").read_text(encoding="utf-8")
        state = _state(client)
        assert state["backups"] == ["accounts.ini.bak-20261020-150405"]
        app.config["SETTINGS_NOW"] = lambda: NOW.replace(minute=30)
        restored = _post(
            client,
            "/api/settings/accounts/restore",
            {"etag": state["etag"], "name": state["backups"][0]},
            token=token,
        )
        unknown = _post(
            client,
            "/api/settings/accounts/restore",
            {"etag": restored.get_json()["etag"], "name": "../keys.ini"},
            token=token,
        )

    assert restored.status_code == 200, restored.get_data(as_text=True)
    assert restored.get_json()["backup"] == "accounts.ini.bak-20261020-153005"
    assert (config_dir / "accounts.ini").read_text(encoding="utf-8") == ACCOUNTS
    assert unknown.status_code == 404


def test_validate_reports_issues_without_writing(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    before = (config_dir / "accounts.ini").read_bytes()
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client,
            "/api/settings/accounts/validate",
            {"changes": [{"op": "update", "id": "work_mail", "fields": {"port": "abc"}}]},
            token=token,
        )

    assert response.status_code == 200
    assert response.get_json()["ok"] is False
    assert (config_dir / "accounts.ini").read_bytes() == before


# ------------------------------------------------------------------ access


@pytest.mark.parametrize("headers", [{}, {"X-Forwarded-For": "127.0.0.1"}], ids=["plain", "spoofed_xff"])
def test_network_client_without_login_is_refused(tmp_path: Path, headers: dict) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        state = client.get("/api/settings/accounts", headers=headers, environ_base=LAN)
        save = client.post(
            "/api/settings/accounts",
            data=json.dumps({"etag": "x", "changes": []}),
            content_type="application/json",
            headers={**headers, "Origin": ORIGIN},
            environ_base=LAN,
        )

    assert state.status_code in (401, 403)
    assert save.status_code in (401, 403)
    assert "[work_mail]" in (config_dir / "accounts.ini").read_text(encoding="utf-8")


def test_network_client_after_login_can_save(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        _login(client)
        token = _csrf(client, LAN)
        state = _state(client, LAN)
        response = _post(
            client,
            "/api/settings/accounts",
            {"etag": state["etag"], "changes": [{"op": "update", "id": "work_mail", "fields": {"login": "new@gmail.com"}}]},
            token=token,
            environ=LAN,
        )

    assert state["web"]["local"] is False
    assert response.status_code == 200, response.get_data(as_text=True)
    assert response.get_json()["changed"] == ["[work_mail] login"]
    assert "login = new@gmail.com" in (config_dir / "accounts.ini").read_text(encoding="utf-8")


def test_saving_unchanged_values_writes_nothing(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        etag = _state(client)["etag"]
        response = _post(
            client,
            "/api/settings/accounts",
            {"etag": etag, "changes": [{"op": "update", "id": "work_mail", "fields": {"port": 993, "use_ssl": True}}]},
            token=token,
        )

    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "unchanged": True, "changed": [], "etag": etag}
    assert list_backups(config_dir / "accounts.ini") == []


def test_request_without_client_address_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app(tmp_path, _config(tmp_path))
    monkeypatch.setattr(web_app, "_request_remote_addr", lambda: None)
    with app.test_client() as client:
        assert client.get("/api/settings/accounts").status_code == 403


@pytest.mark.parametrize(
    ("token_mode", "origin", "content_type", "status"),
    [
        ("missing", ORIGIN, "application/json", 403),
        ("wrong", ORIGIN, "application/json", 403),
        ("good", "http://evil.example", "application/json", 403),
        ("good", None, "application/json", 403),
        ("good", ORIGIN, "text/plain", 415),
    ],
    ids=["no_csrf", "bad_csrf", "cross_origin", "no_origin", "not_json"],
)
def test_writes_need_csrf_same_origin_and_json(
    tmp_path: Path, token_mode: str, origin: str | None, content_type: str, status: int
) -> None:
    config_dir = _config(tmp_path)
    before = (config_dir / "accounts.ini").read_bytes()
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        headers = {}
        if token_mode != "missing":
            headers["X-CSRF-Token"] = token if token_mode == "good" else "nope"
        if origin is not None:
            headers["Origin"] = origin
        response = client.post(
            "/api/settings/accounts",
            data=json.dumps({"etag": _state(client)["etag"], "changes": [{"op": "remove", "id": "work_mail"}]}),
            content_type=content_type,
            headers=headers,
        )

    assert response.status_code == status
    assert (config_dir / "accounts.ini").read_bytes() == before


# ------------------------------------------------------------------ IMAP test


class _FakeIMAP:
    calls: list[tuple] = []
    fail: dict[str, BaseException] = {}
    release: threading.Event | None = None

    def __init__(self, host: str, port: int, ssl: bool, timeout: float) -> None:
        type(self).calls.append(("connect", host, port, ssl))
        if type(self).release is not None:
            type(self).release.wait(5)
        if "connect" in type(self).fail:
            raise type(self).fail["connect"]

    def login(self, user: str, password: str) -> None:
        type(self).calls.append(("login", user, password))
        if "login" in type(self).fail:
            raise type(self).fail["login"]

    def select_folder(self, name: str, readonly: bool = False) -> None:
        assert (name, readonly) == ("INBOX", True)
        type(self).calls.append(("select",))

    def logout(self) -> None:
        type(self).calls.append(("logout",))


@pytest.fixture()
def fake_imap(monkeypatch: pytest.MonkeyPatch):
    _FakeIMAP.calls = []
    _FakeIMAP.fail = {}
    _FakeIMAP.release = None
    monkeypatch.setattr(mail_accounts, "_imap_client_cls", lambda: _FakeIMAP)
    return _FakeIMAP


def _test_imap(client, token: str, **body):
    payload = {"host": "imap.gmail.com", "port": 993, "use_ssl": True, "login": "worker@gmail.com", **body}
    return _post(client, "/api/settings/accounts/test-imap", payload, token=token)


@pytest.mark.parametrize(
    ("stage", "error", "code"),
    [
        (None, None, "OK"),
        ("login", imaplib.IMAP4.error(SERVER_TEXT), "AUTH"),
        ("connect", socket.gaierror(SERVER_TEXT), "DNS"),
        ("connect", ConnectionRefusedError(SERVER_TEXT), "REFUSED"),
        ("connect", ssl.SSLError(SERVER_TEXT), "TLS"),
        ("connect", socket.timeout(SERVER_TEXT), "TIMEOUT"),
        ("login", ValueError(SERVER_TEXT), "OTHER"),
    ],
)
def test_imap_test_returns_codes_never_server_text(
    tmp_path: Path, fake_imap, caplog: pytest.LogCaptureFixture, stage, error, code
) -> None:
    caplog.set_level(logging.DEBUG)
    if stage:
        fake_imap.fail[stage] = error
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _test_imap(client, token, password=NEW_PASSWORD)

    assert response.status_code == 200
    assert response.get_json() == {"ok": code == "OK", "code": code}
    assert ("logout",) in fake_imap.calls or stage == "connect"
    _assert_no_secrets(response.get_data(as_text=True))
    _assert_no_secrets(caplog.text)


def test_stored_password_is_used_only_for_the_saved_server(tmp_path: Path, fake_imap) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        same = _test_imap(client, token, id="work_mail", login="")
        other = _test_imap(client, token, id="work_mail", host="imap.attacker.example")
        other_port = _test_imap(client, token, id="work_mail", port=143, use_ssl=False)

    assert same.get_json() == {"ok": True, "code": "OK"}
    assert ("login", "worker@gmail.com", PASSWORD) in fake_imap.calls
    assert other.status_code == 422 and other.get_json()["code"] == "PASSWORD_REQUIRED"
    assert other_port.status_code == 422 and other_port.get_json()["code"] == "PASSWORD_REQUIRED"
    assert [call for call in fake_imap.calls if call[0] == "connect"] == [("connect", "imap.gmail.com", 993, True)]


@pytest.mark.parametrize(
    ("host", "port", "code"),
    [
        ("imap.gmail.com", 25, "PORT_NOT_ALLOWED"),
        ("169.254.169.254", 993, "HOST_NOT_ALLOWED"),
        ("0.0.0.0", 993, "HOST_NOT_ALLOWED"),
        ("0251.0376.0251.0376", 993, "HOST_NOT_ALLOWED"),
        ("0xa9.0xfe.0xa9.0xfe", 993, "HOST_NOT_ALLOWED"),
        ("http://imap.gmail.com", 993, "INVALID"),
        ("imap.gmail.com:993", 993, "INVALID"),
    ],
)
def test_imap_test_reaches_only_imap_servers(tmp_path: Path, fake_imap, host: str, port: int, code: str) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _test_imap(client, token, host=host, port=port, password=NEW_PASSWORD)

    assert response.status_code == 422
    assert response.get_json()["code"] == code
    assert fake_imap.calls == []


def test_saved_non_standard_port_can_be_tested(tmp_path: Path, fake_imap) -> None:
    accounts = ACCOUNTS.replace("host = imap.gmail.com\nport = 993", "host = 127.0.0.1\nport = 1143")
    app = _app(tmp_path, _config(tmp_path, accounts))
    with app.test_client() as client:
        token = _csrf(client)
        response = _test_imap(client, token, host="127.0.0.1", port=1143, password=NEW_PASSWORD)

    assert response.get_json() == {"ok": True, "code": "OK"}


def test_imap_test_is_rate_limited_per_client(tmp_path: Path, fake_imap) -> None:
    app = _app(tmp_path, _config(tmp_path))
    app.config["SETTINGS_TEST_INTERVAL_SECONDS"] = 5
    with app.test_client() as client:
        token = _csrf(client)
        first = _test_imap(client, token, password=NEW_PASSWORD)
        second = _test_imap(client, token, password=NEW_PASSWORD)

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.get_json()["code"] == "RATE_LIMIT"
    assert len([c for c in fake_imap.calls if c[0] == "connect"]) == 1


def test_imap_test_has_a_hard_deadline(tmp_path: Path, fake_imap) -> None:
    fake_imap.release = threading.Event()
    app = _app(tmp_path, _config(tmp_path))
    app.config["SETTINGS_TEST_DEADLINE_SECONDS"] = 0.2
    try:
        with app.test_client() as client:
            token = _csrf(client)
            response = _test_imap(client, token, password=NEW_PASSWORD)
    finally:
        fake_imap.release.set()

    assert response.get_json() == {"ok": False, "code": "TIMEOUT"}


def test_probe_classifies_by_stage() -> None:
    assert mail_accounts.classify_imap_failure(imaplib.IMAP4.error("x"), "login") == "AUTH"
    assert mail_accounts.classify_imap_failure(imaplib.IMAP4.error("x"), "select") == "OTHER"
    assert mail_accounts.classify_imap_failure(OSError("unreachable"), "connect") == "REFUSED"


# ------------------------------------------------------------------ Telegram test


@pytest.fixture()
def fake_telegram(monkeypatch: pytest.MonkeyPatch):
    calls: list[tuple] = []
    replies: dict[str, object] = {}

    def fake_http(method: str, token: str, payload, timeout: float):
        calls.append((method, token, payload))
        reply = replies.get(method, (200, {"ok": True, "result": {"username": "letter_bot"}}))
        if isinstance(reply, BaseException):
            raise reply
        return reply

    monkeypatch.setattr(settings_api, "_telegram_http", fake_http)
    return calls, replies


def test_check_bot_with_the_stored_token(tmp_path: Path, fake_telegram) -> None:
    calls, _ = fake_telegram
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/telegram/test", {"action": "getme"}, token=token)

    assert response.get_json() == {"ok": True, "code": "OK", "bot_username": "letter_bot"}
    assert calls == [("getMe", TOKEN, None)]
    _assert_no_secrets(response.get_data(as_text=True))


@pytest.mark.parametrize(
    ("reply", "code"),
    [
        ((401, {"ok": False, "description": SERVER_TEXT}), "AUTH"),
        (TimeoutError(SERVER_TEXT), "TIMEOUT"),
        (ConnectionError(f"https://api.telegram.org/bot{NEW_TOKEN}/getMe"), "NETWORK"),
    ],
)
def test_check_bot_failures_are_codes(
    tmp_path: Path, fake_telegram, caplog: pytest.LogCaptureFixture, reply, code
) -> None:
    caplog.set_level(logging.DEBUG)
    _, replies = fake_telegram
    replies["getMe"] = reply
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(
            client, "/api/settings/telegram/test", {"action": "getme", "bot_token": NEW_TOKEN}, token=token
        )

    assert response.get_json() == {"ok": False, "code": code}
    _assert_no_secrets(response.get_data(as_text=True))
    _assert_no_secrets(caplog.text)


def test_send_test_message_is_plain_text_to_the_default_chat(tmp_path: Path, fake_telegram) -> None:
    calls, _ = fake_telegram
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/telegram/test", {"action": "send"}, token=token)

    assert response.get_json() == {"ok": True, "code": "OK"}
    method, used_token, payload = calls[0]
    assert (method, used_token) == ("sendMessage", TOKEN)
    assert payload["chat_id"] == "-1001234567890"
    assert "parse_mode" not in payload
    assert payload["text"] == "Letterbot test message: notifications will arrive in this chat."


def test_telegram_test_without_any_token(tmp_path: Path, fake_telegram) -> None:
    accounts = ACCOUNTS.replace(f"bot_token = {TOKEN}", "bot_token = CHANGE_ME")
    app = _app(tmp_path, _config(tmp_path, accounts))
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/telegram/test", {"action": "getme"}, token=token)

    assert response.status_code == 422
    assert response.get_json()["code"] == "TOKEN_MISSING"
    assert fake_telegram[0] == []


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ({"action": "send", "chat_id": "my chat"}, "CHAT_FORMAT"),
        ({"action": "getme", "bot_token": "123:../../evil"}, "TOKEN_FORMAT"),
        ({"action": "delete"}, "BAD_REQUEST"),
    ],
)
def test_telegram_test_refuses_odd_input_before_any_call(
    tmp_path: Path, fake_telegram, body: dict, code: str
) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/telegram/test", body, token=token)

    assert response.status_code in (400, 422)
    assert response.get_json()["code"] == code
    assert fake_telegram[0] == []


# ------------------------------------------------------------------ dashboard password


def test_loopback_sets_the_dashboard_password_and_it_works_at_once(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir, password="")
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/web-password", {"new_password": NEW_WEB_PASSWORD}, token=token)
        body = response.get_data(as_text=True)
    with app.test_client() as lan_client:
        page = lan_client.get("/login", environ_base=LAN)
        login = lan_client.post(
            "/login",
            data={"password": NEW_WEB_PASSWORD, "csrf_token": extract_csrf_token(page.get_data(as_text=True))},
            environ_base=LAN,
        )

    assert response.status_code == 200, body
    assert response.get_json()["backup"] == "settings.ini.bak-20261020-150405"
    _assert_no_secrets(body)
    text = (config_dir / "settings.ini").read_text(encoding="utf-8")
    assert text == "[ui]\nlocale = en\n\n[web_ui]\npassword = " + NEW_WEB_PASSWORD + "\n"
    assert login.status_code == 302


@pytest.mark.parametrize(
    ("password", "code"),
    [("short", "LENGTH"), ("CHANGE_ME", "INVALID"), (" padded-password ", "INVALID"), ("password", "LENGTH")],
)
def test_weak_dashboard_passwords_are_refused(tmp_path: Path, password: str, code: str) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/web-password", {"new_password": password}, token=token)

    assert response.status_code == 422
    assert response.get_json()["code"] == code
    assert "web_ui" not in (config_dir / "settings.ini").read_text(encoding="utf-8")


def test_network_password_change_needs_the_current_password(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        _login(client)
        token = _csrf(client, LAN)
        wrong = _post(
            client,
            "/api/settings/web-password",
            {"new_password": NEW_WEB_PASSWORD, "current_password": "guess"},
            token=token,
            environ=LAN,
        )
        right = _post(
            client,
            "/api/settings/web-password",
            {"new_password": NEW_WEB_PASSWORD, "current_password": WEB_PASSWORD},
            token=token,
            environ=LAN,
        )

    assert wrong.status_code == 403 and wrong.get_json()["code"] == "CURRENT_PASSWORD_WRONG"
    assert right.status_code == 200
    assert app.config["WEB_PASSWORD"] == NEW_WEB_PASSWORD


def test_env_password_is_reported_not_overwritten(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEB_PASSWORD", "from-the-environment-1")
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/web-password", {"new_password": NEW_WEB_PASSWORD}, token=token)

    assert response.status_code == 409 and response.get_json()["code"] == "ENV_OVERRIDE"


# ------------------------------------------------------------------ network access


def test_lan_switch_rules(tmp_path: Path) -> None:
    config_dir = _config(tmp_path, settings="[web]\nhost = 127.0.0.1\nport = 8787\n")
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        token = _csrf(client)
        unsupported = _post(client, "/api/settings/web-access", {"lan": True}, token=token)
        app.config["SETTINGS_LAN_TOGGLE"] = True
        enabled = _post(client, "/api/settings/web-access", {"lan": True}, token=token)
        state = _state(client)
        _login(client)
        lan_token = _csrf(client, LAN)
        from_network = _post(client, "/api/settings/web-access", {"lan": False}, token=lan_token, environ=LAN)

    assert unsupported.status_code == 409 and unsupported.get_json()["code"] == "LAUNCHER_UNSUPPORTED"
    assert enabled.status_code == 200
    assert enabled.get_json()["restart_required"] is True
    assert "host = 0.0.0.0" in (config_dir / "settings.ini").read_text(encoding="utf-8")
    assert (state["web"]["lan_saved"], state["web"]["restart_required"]) == (True, True)
    assert from_network.status_code == 403 and from_network.get_json()["code"] == "LOOPBACK_ONLY"


def test_lan_needs_a_dashboard_password(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir, password="CHANGE_ME")
    app.config["SETTINGS_LAN_TOGGLE"] = True
    with app.test_client() as client:
        token = _csrf(client)
        response = _post(client, "/api/settings/web-access", {"lan": True}, token=token)

    assert response.status_code == 409 and response.get_json()["code"] == "PASSWORD_REQUIRED"


def test_in_process_web_thread_admits_only_private_networks_off_loopback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import werkzeug.serving

    apps = []
    real_create_app = web_app.create_app

    def spy(**kwargs):
        app = real_create_app(**kwargs)
        apps.append((kwargs, app))
        return app

    class _Server:
        def serve_forever(self) -> None:
            return None

    monkeypatch.setattr(web_app, "create_app", spy)
    monkeypatch.setattr(werkzeug.serving, "make_server", lambda *a, **k: _Server())
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    for host in ("127.0.0.1", "0.0.0.0"):
        web_app.start_web_in_thread(config_dir=tmp_path, db_path=db_path, host=host, port=8787, ready_timeout=15)

    (loopback_kwargs, loopback_app), (lan_kwargs, lan_app) = apps
    assert loopback_kwargs["allow_cidrs"] is None
    assert lan_kwargs["allow_cidrs"] == list(web_app.LAN_PRIVATE_CIDRS)
    assert loopback_app.config["SETTINGS_LAN_TOGGLE"] is True and lan_app.config["SETTINGS_LAN_TOGGLE"] is True


# ------------------------------------------------------------------ page


def test_page_ships_the_editor_without_inline_script(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        page = client.get("/accounts").get_data(as_text=True)

    assert 'data-settings-editor' in page
    assert re.search(r"\son[a-z]+\s*=", page) is None
    assert all(" src=" in tag for tag in re.findall(r"<script\b[^>]*>", page))
    data = re.search(r'id="settings-editor-data" hidden data-json="([^"]*)"', page)
    assert data is not None
    assert json.loads(html.unescape(data.group(1)))["texts"]["editor.add"] == "Add mailbox"
    _assert_no_secrets(page)
    assert 'autocomplete="new-password"' in page


class _EditorAncestry(HTMLParser):
    """Records, for the editor and web-card sections, whether they sit inside
    div.settings-page (only div/section are tracked: always closed explicitly)."""

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[tuple[str, bool]] = []
        self.inside: dict[str, bool] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        names = dict(attrs)
        for marker in ("data-settings-editor", "data-web-card"):
            if marker in names:
                self.inside[marker] = any(flag for _, flag in self.stack)
        if tag in ("div", "section"):
            self.stack.append((tag, "settings-page" in (names.get("class") or "").split()))

    def handle_endtag(self, tag: str) -> None:
        if tag in ("div", "section"):
            for index in range(len(self.stack) - 1, -1, -1):
                if self.stack[index][0] == tag:
                    del self.stack[index:]
                    break


def test_hidden_editor_parts_stay_hidden_despite_display_rules(tmp_path: Path) -> None:
    # .settings-field / .settings-toolbar / .status-pill set display, and an
    # author rule beats the browser's own [hidden] rule: without this one a
    # "Password saved" pill shows next to an empty password box.
    css = (Path(settings_api.__file__).parent / "static" / "settings.css").read_text(encoding="utf-8")
    assert re.search(r"\.settings-page\s+\[hidden\]\s*\{\s*display:\s*none\s*!important;?\s*\}", css)
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        page = client.get("/accounts").get_data(as_text=True)
    parser = _EditorAncestry()
    parser.feed(page)
    assert parser.inside == {"data-settings-editor": True, "data-web-card": True}


def test_applied_reports_the_saved_hash(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        applied = client.get("/api/settings/applied").get_json()
        etag = _state(client)["etag"]

    assert applied == {"ok": True, "saved_sha12": etag[:12], "applied_sha12": None, "applied_at": None}
