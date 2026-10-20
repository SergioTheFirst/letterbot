"""P3c: the Commands page (/commands).

Owner decision 2026-10-20: no command exists on the command line without an
entry in the dashboard. Every subcommand of ``python -m mailbot_v26`` and every
module with an ``if __name__ == "__main__"`` block has a commands_registry
entry; read-only commands run on /commands and write nothing; init-config and
migrate-config run from the Settings page under the CONSTITUTION IV exception
(CSRF, same origin, loopback or signed in, atomic, never overwrite, legacy
files untouched, file names only in answers and logs).
"""

from __future__ import annotations

import argparse
import ast
import html
import json
import os
import re
import sys
from pathlib import Path

import pytest

import mailbot_v26
from mailbot_v26.__main__ import _build_parser
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import extract_csrf_token
from mailbot_v26.version import get_version
from mailbot_v26.web_observability import commands_page, commands_registry
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


# ------------------------------------------------------------------ (a) parity


def _flags(parser: argparse.ArgumentParser) -> set[str]:
    return {
        option
        for action in parser._actions
        for option in action.option_strings
        if option not in ("-h", "--help")
    }


def _subcommand_flags() -> dict[str, set[str]]:
    parser = _build_parser()
    (subparsers,) = [action for action in parser._actions if isinstance(action, argparse._SubParsersAction)]
    return {name: _flags(sub) for name, sub in subparsers.choices.items()}


def _is_main_guard(node: ast.stmt) -> bool:
    if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
        return False
    operands = [node.test.left, *node.test.comparators]
    return any(isinstance(item, ast.Name) and item.id == "__name__" for item in operands) and any(
        isinstance(item, ast.Constant) and item.value == "__main__" for item in operands
    )


def _runnable_modules() -> set[str]:
    package = Path(mailbot_v26.__file__).resolve().parent
    modules: set[str] = set()
    for path in package.rglob("*.py"):
        parts = path.relative_to(package.parent).with_suffix("").parts
        if "tests" in parts:
            continue
        source = path.read_text(encoding="utf-8")
        if "__main__" not in source:
            continue
        if any(_is_main_guard(node) for node in ast.parse(source).body):
            name = ".".join(parts)
            modules.add(name[: -len(".__main__")] if name.endswith(".__main__") else name)
    return modules


def test_every_command_line_entry_point_has_a_dashboard_entry(tmp_path: Path) -> None:
    subcommands = _subcommand_flags()
    modules = _runnable_modules()
    assert {
        "doctor",
        "init-config",
        "migrate-config",
        "validate-config",
        "config-ready",
        "backup",
        "restore",
        "export",
        "version",
    } <= set(subcommands)
    assert len(modules) >= 22
    assert {"mailbot_v26", "mailbot_v26.start", "mailbot_v26.tools.cleanup"} <= modules

    entries = commands_registry.ENTRIES
    cli = [entry for entry in entries if entry.module == commands_registry.CLI_MODULE]
    for name, flags in subcommands.items():
        covered = [entry for entry in cli if entry.subcommand == name]
        assert covered, f"`{name}` has no dashboard entry"
        listed = set().union(*(entry.options for entry in covered))
        assert flags <= listed, f"`{name}` options without a dashboard entry: {sorted(flags - listed)}"
    assert any(entry.subcommand == "" for entry in cli), "running Letterbot has no entry"
    root_listed = set().union(*(entry.options for entry in cli))
    assert _flags(_build_parser()) <= root_listed
    missing = modules - {entry.module for entry in entries}
    assert not missing, f"runnable modules without a dashboard entry: {sorted(missing)}"

    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        page = client.get("/commands")
    body = page.get_data(as_text=True)
    assert page.status_code == 200
    for name in subcommands:
        assert f'data-subcommand="{name}"' in body, name
    for module in modules:
        assert f'data-module="{module}"' in body, module


def test_registry_entries_name_a_dashboard_place_or_a_reason(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    rules = {rule.rule for rule in app.url_map.iter_rules()}
    with app.test_client() as client:
        pages = {"/commands": client.get("/commands").get_data(as_text=True)}
        pages["/accounts"] = client.get("/accounts").get_data(as_text=True)

    keys = [entry.key for entry in commands_registry.ENTRIES]
    assert len(keys) == len(set(keys))
    assert {entry.group for entry in commands_registry.ENTRIES} == set(commands_registry.GROUPS)
    for entry in commands_registry.ENTRIES:
        assert bool(entry.href) != bool(entry.reason), entry.key
        if entry.href:
            path, _, anchor = entry.href.partition("#")
            assert path in rules, entry.key
            if anchor:
                assert f'id="{anchor}"' in pages[path], entry.key
        else:
            assert entry.reason in commands_registry.REASONS, entry.key


def test_commands_copy_exists_in_both_languages() -> None:
    catalogs = commands_registry.catalogs()
    assert set(catalogs["en"]) == set(catalogs["ru"])
    for key, value in catalogs["en"].items():
        assert set(re.findall(r"\{(\w+)\}", value)) == set(
            re.findall(r"\{(\w+)\}", catalogs["ru"][key])
        ), key
    for entry in commands_registry.ENTRIES:
        assert f"purpose.{entry.key}" in catalogs["en"], entry.key
    for reason in commands_registry.REASONS:
        assert f"reason.{reason}" in catalogs["en"], reason


def test_cli_only_entries_show_the_exact_command_for_this_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    folder = os.path.abspath(config_dir)
    db_path = os.path.abspath(app.config["DB_PATH"])
    with app.test_client() as client:
        source = html.unescape(client.get("/commands").get_data(as_text=True))
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        frozen = html.unescape(client.get("/commands").get_data(as_text=True))

    assert f'python -m mailbot_v26 --config-dir "{folder}"' in source
    assert "python -m mailbot_v26 backup" in source
    assert f'python -m mailbot_v26.tools.cleanup --run --db "{db_path}"' in source
    assert f'python -m mailbot_v26.tools.run_stack all --config-dir "{folder}"' in source
    assert f'Letterbot.exe --config-dir "{folder}"' in frozen
    assert "Letterbot.exe backup" in frozen
    assert "Letterbot.exe does not include this tool" in frozen
    assert "Letterbot.exe does not include this tool" not in source
    assert "Command line only" in source
    assert "never starts or stops Letterbot" in source
    assert "data-copy-target=" in source
    # Dashboard entries link to their place instead.
    assert 'href="/doctor"' in source and 'href="/accounts#settings-setup"' in source


def test_commands_page_has_no_inline_script_or_handler(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        body = client.get("/commands").get_data(as_text=True)
    scripts = re.findall(r"<script\b[^>]*>", body)
    assert scripts and all("src=" in tag and "nonce=" in tag for tag in scripts)
    assert "commands.js" in body
    assert re.search(r"\son[a-z]+\s*=", body) is None
    script = (Path(commands_page.__file__).parent / "static" / "commands.js").read_text(encoding="utf-8")
    assert "navigator.clipboard" in script and "execCommand" in script and "selectNodeContents" in script


# ------------------------------------------------------------------ (b) access


def test_commands_page_opens_on_loopback_and_after_network_login(tmp_path: Path) -> None:
    app = _app(tmp_path, _config(tmp_path))
    with app.test_client() as client:
        local = client.get("/commands")
        assert local.status_code == 200
        assert 'data-testid="commands-page"' in local.get_data(as_text=True)
        assert local.headers["Cache-Control"] == "no-store"

    with app.test_client() as client:
        page = client.get("/commands", environ_base=LAN)
        api = client.get("/api/commands/validate-config", environ_base=LAN)
        assert page.status_code == 302 and "/login" in page.headers["Location"]
        assert api.status_code in (401, 403)

        _login(client)
        signed_in = client.get("/commands", environ_base=LAN)
        assert signed_in.status_code == 200
        assert 'data-testid="commands-page"' in signed_in.get_data(as_text=True)
        assert client.get("/api/commands/validate-config", environ_base=LAN).status_code == 200


# ------------------------------------------------------------------ (c) read-only


def test_commands_page_and_apis_write_nothing(tmp_path: Path) -> None:
    config_dir = _config(tmp_path)
    app = _app(tmp_path, config_dir)
    data_dir = Path(app.config["DB_PATH"]).parent
    api_paths = sorted(
        rule.rule for rule in app.url_map.iter_rules() if rule.rule.startswith("/api/commands/")
    )
    assert len(api_paths) >= 5
    db_name = Path(app.config["DB_PATH"]).name
    shm = f"{db_name}-shm"
    config_before = _files(config_dir)
    data_before = {name: state for name, state in _files(data_dir).items() if name != shm}

    with app.test_client() as client:
        for path in ["/commands", *api_paths]:
            response = client.get(path)
            assert response.status_code == 200, path

    assert _files(config_dir) == config_before
    data_after = {name: state for name, state in _files(data_dir).items() if name != shm}
    # The database and any WAL content are byte-identical with the same mtime;
    # a read-only reader may only add SQLite's -shm index and an empty -wal.
    for name, state in data_before.items():
        assert data_after.get(name) == state, name
    for name in set(data_after) - set(data_before):
        assert name == f"{db_name}-wal" and data_after[name][0] == b"", name


def test_read_only_results_match_the_command_line_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    accounts = (
        "[Box owner@example.com]\n"
        f"password = {MAILBOX_PASSWORD}\n"
        "[work_mail]\n"
        "login = worker@gmail.com\n"
        f"password = {MAILBOX_PASSWORD}\n"
        "port = 993\n"
        "use_ssl = true\n"
        "[telegram]\n"
        f"bot_token = {TOKEN}\n"
    )
    config_dir = _config(tmp_path, accounts=accounts, settings="[web]\nhost = 0.0.0.0\nport = 8787\n")
    monkeypatch.setattr(commands_page, "get_primary_ipv4", lambda: "192.168.1.20")
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        validate = client.get("/api/commands/validate-config").get_json()
        ready = client.get("/api/commands/config-ready").get_json()
        cleanup = client.get("/api/commands/cleanup-status").get_json()
        version = client.get("/api/commands/version").get_json()
        lan = client.get("/api/commands/lan-url").get_json()
        page = html.unescape(client.get("/commands").get_data(as_text=True))

    texts = [finding["text"] for finding in validate["findings"]]
    assert validate["state"] == "warn"
    assert "Invalid account_id 'Box o…@example.com': use lowercase [a-z0-9_], no spaces" in texts
    assert "[work_mail] host is required" in texts
    assert validate["compat"]["state"] == "absent"
    assert ready["state"] == "not_ready"
    assert "No ready IMAP account found in accounts.ini" in ready["critical"]
    assert cleanup["state"] == "ok"
    assert cleanup["eligible_total"] == 0
    assert {"trust_snapshots", "relationship_health_snapshots"} <= set(cleanup["missing_tables"])
    assert version == {"command": "version", "version": get_version()}
    assert lan["state"] == "lan" and lan["url"] == "http://192.168.1.20:8787/"
    for shown in (texts[0], "No ready IMAP account found", "http://192.168.1.20:8787/", get_version()):
        assert shown in page
    for body in (json.dumps([validate, ready, cleanup, lan]), page):
        assert MAILBOX_PASSWORD not in body and TOKEN not in body
        assert "owner@example.com" not in body


def test_unreadable_accounts_ini_degrades_to_a_visible_result(tmp_path: Path) -> None:
    config_dir = _config(tmp_path, accounts=None)
    (config_dir / "accounts.ini").write_bytes(b"[work_mail]\npassword = \xff\xfe\n")
    app = _app(tmp_path, config_dir)
    with app.test_client() as client:
        validate = client.get("/api/commands/validate-config").get_json()
        ready = client.get("/api/commands/config-ready").get_json()
        page = client.get("/commands")

    assert validate["state"] == "unavailable" and validate["error"] == "IniEncodingError"
    assert ready["state"] == "unavailable"
    assert page.status_code == 200
    assert "IniEncodingError" in page.get_data(as_text=True)


def test_commands_page_without_config_folder_still_renders(tmp_path: Path) -> None:
    app = _app(tmp_path, None)
    with app.test_client() as client:
        page = client.get("/commands")
        validate = client.get("/api/commands/validate-config").get_json()
    assert page.status_code == 200
    assert validate["state"] == "no_config_dir"
    body = html.unescape(page.get_data(as_text=True))
    assert "python -m mailbot_v26 backup" in body
    assert '--config-dir "' not in body
