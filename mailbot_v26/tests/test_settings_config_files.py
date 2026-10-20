"""P3c: the configuration-files buttons on the Settings page (/accounts).

init-config and migrate-config run from the Settings page under the
CONSTITUTION IV exception: CSRF header, same origin, loopback or signed in over
the network; init-config creates only the missing accounts.ini/settings.ini
from the CLI templates and never overwrites; migrate-config converts a legacy
config.ini/config.yaml only while accounts.ini does not exist and never
changes, renames, deletes or copies the legacy files; answers and log lines
name files only.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import extract_csrf_token
from mailbot_v26.tools.config_bootstrap import ACCOUNTS_TEMPLATE, SETTINGS_TEMPLATE
from mailbot_v26.web_observability.app import create_app

WEB_PASSWORD = "lan password 42"
LAN = {"REMOTE_ADDR": "192.168.1.5"}
ORIGIN = "http://localhost"
MAILBOX_PASSWORD = "Commands-Mailbox-PW-5H"
TOKEN = "123456789:AACommandsTokenMarkerQWERTYUIOPAS"
ACCOUNTS = (
    "[work_mail]\n"
    "login = worker@gmail.com\n"
    f"password = {MAILBOX_PASSWORD}\n"
    "host = imap.gmail.com\n"
    "port = 993\n"
    "use_ssl = true\n"
    "telegram_chat_id = 272250747\n"
    "\n"
    "[telegram]\n"
    f"bot_token = {TOKEN}\n"
)
SETUP_ROUTES = ("/api/settings/init-config", "/api/settings/migrate-config")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WEB_OBSERVABILITY_TOKEN", raising=False)
    monkeypatch.delenv("WEB_PASSWORD", raising=False)
    monkeypatch.delattr(sys, "frozen", raising=False)


def _config(tmp_path: Path, accounts: str | None = ACCOUNTS, settings: str | None = "[ui]\nlocale = en\n") -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    if settings is not None:
        (config_dir / "settings.ini").write_text(settings, encoding="utf-8")
    if accounts is not None:
        (config_dir / "accounts.ini").write_text(accounts, encoding="utf-8")
    return config_dir


def _app(tmp_path: Path, config_dir: Path | None):
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    db_path = data_dir / "web.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password=WEB_PASSWORD,
        secret_key="secret",
        config_dir=config_dir,
        allow_cidrs=["192.168.0.0/16"],
    )
    app.config["SETTINGS_RUNTIME_HEALTH_PATH"] = tmp_path / "runtime_health.json"
    return app


def _files(folder: Path) -> dict[str, tuple[bytes, int]]:
    return {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in sorted(folder.iterdir())
        if path.is_file()
    }


def _login(client) -> None:
    page = client.get("/login", environ_base=LAN)
    token = extract_csrf_token(page.get_data(as_text=True))
    response = client.post("/login", data={"password": WEB_PASSWORD, "csrf_token": token}, environ_base=LAN)
    assert response.status_code == 302


def _csrf(client, environ: dict | None = None) -> str:
    page = client.get("/accounts", environ_base=environ or {})
    assert page.status_code == 200
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page.get_data(as_text=True))
    assert match is not None
    return match.group(1)


def _post(client, path: str, body: object, *, token: str | None, environ: dict | None = None):
    headers = {"Origin": ORIGIN}
    if token is not None:
        headers["X-CSRF-Token"] = token
    return client.post(
        path,
        data=json.dumps(body),
        content_type="application/json",
        headers=headers,
        environ_base=environ or {},
    )


# ------------------------------------------------------------------ init-config


def test_init_config_creates_only_the_missing_files(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_dir = _config(tmp_path, accounts=None)
    settings_before = (config_dir / "settings.ini").read_bytes()
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        setup = client.get("/api/settings/accounts").get_json()["setup"]
        token = _csrf(client)
        with caplog.at_level(logging.INFO):
            first = _post(client, "/api/settings/init-config", {}, token=token)
            again = _post(client, "/api/settings/init-config", {}, token=token)
        after = client.get("/api/settings/accounts").get_json()["setup"]

    assert setup == {"missing": ["accounts.ini"], "legacy": [], "can_migrate": False}
    assert first.status_code == 200
    assert first.get_json() == {"ok": True, "created": ["accounts.ini"]}
    assert first.headers["Cache-Control"] == "no-store"
    assert again.get_json() == {"ok": True, "created": []}
    assert after["missing"] == []
    assert (config_dir / "accounts.ini").read_text(encoding="utf-8") == ACCOUNTS_TEMPLATE
    assert (config_dir / "settings.ini").read_bytes() == settings_before
    assert sorted(path.name for path in config_dir.iterdir()) == ["accounts.ini", "settings.ini"]
    saved = [record.getMessage() for record in caplog.records if "settings_saved" in record.getMessage()]
    assert saved == ["settings_saved file=accounts.ini changed=created backup=- client=loopback"]
    for record in caplog.records:
        assert str(config_dir) not in record.getMessage()
        assert "CHANGE_ME" not in record.getMessage()


def test_init_config_creates_both_files_in_an_empty_folder(tmp_path: Path) -> None:
    config_dir = _config(tmp_path, accounts=None, settings=None)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        response = _post(client, "/api/settings/init-config", {}, token=_csrf(client))

    assert response.get_json() == {"ok": True, "created": ["accounts.ini", "settings.ini"]}
    assert (config_dir / "settings.ini").read_text(encoding="utf-8") == SETTINGS_TEMPLATE
    assert (config_dir / "accounts.ini").read_text(encoding="utf-8") == ACCOUNTS_TEMPLATE


def test_init_config_never_overwrites_existing_files(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    before = _files(config_dir)
    with app.test_client() as client:
        response = _post(client, "/api/settings/init-config", {}, token=_csrf(client))

    assert response.get_json() == {"ok": True, "created": []}
    assert _files(config_dir) == before


def test_init_config_is_refused_while_a_legacy_config_waits_for_conversion(tmp_path: Path) -> None:
    config_dir = _config(tmp_path, accounts=None, settings=None)
    (config_dir / "config.ini").write_bytes(b"[general]\ncheck_interval = 120\n")
    app = _app(tmp_path, config_dir)
    before = _files(config_dir)
    with app.test_client() as client:
        response = _post(client, "/api/settings/init-config", {}, token=_csrf(client))

    assert response.status_code == 409
    assert response.get_json()["code"] == "LEGACY_CONFIG"
    assert _files(config_dir) == before


@pytest.mark.parametrize("path", SETUP_ROUTES)
def test_setup_buttons_need_csrf_and_a_network_login(tmp_path: Path, path: str) -> None:
    config_dir = _config(tmp_path, accounts=None, settings=None)
    (config_dir / "config.yaml").write_bytes(b"schema_version: 1\n")
    app = _app(tmp_path, config_dir)
    before = _files(config_dir)
    with app.test_client() as client:
        no_token = _post(client, path, {}, token=None)
        wrong_token = _post(client, path, {}, token="not-the-token")
        network = _post(client, path, {}, token=None, environ=LAN)

    assert no_token.status_code == 403
    assert wrong_token.status_code == 403
    assert network.status_code in (401, 403)
    assert _files(config_dir) == before


# ------------------------------------------------------------------ (f) migrate-config


LEGACY = {
    "config.ini": b"[general]\ncheck_interval = 120\n[web_ui]\npassword = Legacy-Web-PW-9Z\n",
    "config.yaml": b"schema_version: 1\naccounts: []\n",
    "keys.ini": b"[telegram]\nbot_token = 555:Legacy-Token-QWERTYUIOPASDFGHJKLZX\n",
}


def test_migrate_config_converts_and_leaves_legacy_files_untouched(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_dir = _config(tmp_path, accounts=None, settings=None)
    for name, data in LEGACY.items():
        (config_dir / name).write_bytes(data)
    stamps = {name: (config_dir / name).stat().st_mtime_ns for name in LEGACY}
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        setup = client.get("/api/settings/accounts").get_json()["setup"]
        token = _csrf(client)
        with caplog.at_level(logging.INFO):
            response = _post(client, "/api/settings/migrate-config", {}, token=token)
        after = client.get("/api/settings/accounts").get_json()["setup"]

    assert setup == {"missing": [], "legacy": ["config.ini", "config.yaml"], "can_migrate": True}
    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "created": ["settings.ini", "accounts.ini"]}
    assert after["can_migrate"] is False
    assert (config_dir / "settings.ini").read_bytes() == LEGACY["config.ini"]
    assert "[example_account]" in (config_dir / "accounts.ini").read_text(encoding="utf-8")
    for name, data in LEGACY.items():
        assert (config_dir / name).read_bytes() == data
        assert (config_dir / name).stat().st_mtime_ns == stamps[name]
    assert sorted(path.name for path in config_dir.iterdir()) == sorted(
        [*LEGACY, "accounts.ini", "settings.ini"]
    )
    text = response.get_data(as_text=True) + "\n".join(record.getMessage() for record in caplog.records)
    assert "Legacy-Web-PW-9Z" not in text and "Legacy-Token" not in text
    saved = [record.getMessage() for record in caplog.records if "settings_saved" in record.getMessage()]
    assert saved == [
        "settings_saved file=settings.ini changed=migrated backup=- client=loopback",
        "settings_saved file=accounts.ini changed=migrated backup=- client=loopback",
    ]


def test_migrate_config_is_refused_when_accounts_ini_exists(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    (config_dir / "config.ini").write_bytes(LEGACY["config.ini"])
    app = _app(tmp_path, config_dir)
    before = _files(config_dir)
    with app.test_client() as client:
        setup = client.get("/api/settings/accounts").get_json()["setup"]
        response = _post(client, "/api/settings/migrate-config", {}, token=_csrf(client))

    assert setup["can_migrate"] is False
    assert response.status_code == 409
    assert response.get_json()["code"] == "ACCOUNTS_EXIST"
    assert _files(config_dir) == before


def test_migrate_config_without_legacy_files_is_refused(tmp_path: Path) -> None:
    config_dir = _config(tmp_path, accounts=None, settings=None)
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        response = _post(client, "/api/settings/migrate-config", {}, token=_csrf(client))

    assert response.status_code == 409
    assert response.get_json()["code"] == "NO_LEGACY"
    assert list(config_dir.iterdir()) == []


def test_settings_page_has_the_configuration_files_card(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        body = client.get("/accounts").get_data(as_text=True)

    assert 'id="settings-setup"' in body
    assert 'data-action="init-config"' in body
    assert 'data-action="migrate-config"' in body
    assert "Create missing configuration files" in body
    assert "Convert legacy configuration" in body
