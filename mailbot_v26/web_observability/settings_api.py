"""Settings lane web routes.

S1 (read-only): GET /accounts renders the mailbox status page, GET
/api/accounts/status returns the same status as JSON, and the Jinja global
``settings_accounts_home_card`` feeds the Home-page card partial
(templates/settings_accounts_card.html) that the dashboard can include.

S2 (editor): a JSON API under /api/settings/ for the /accounts page scripts
(static/settings.js). It is the CONSTITUTION IV rev. 2026-10 "Settings"
exception: it may write accounts.ini and settings.ini, never the database.
- Access is the dashboard rule: no password on this computer (loopback), a
  signed-in session over the network; a request without a client address is
  refused, X-Forwarded-For is never trusted here. Writes also need the CSRF
  token (X-CSRF-Token header), a same-origin Origin/Referer and a JSON body.
- Secrets (mailbox passwords, the bot token, the dashboard password) are
  write-only: requests may carry them, no response, log line or error does.
  A login is returned only in its expected shape (a malformed line can carry
  a password into it, see accounts_status).
- Every save is validated (the same rules as the worker's preflight, via
  accounts_status.inspect_accounts, on the candidate file), written
  atomically after a timestamped backup (config/ini_editor), guarded by a
  lock and an etag (409 when the file changed meanwhile), and logged as
  settings_saved naming the changed fields only.
- "Test" calls only verify a login (IMAP: connect, login, SELECT INBOX
  read-only, logout; Telegram: getMe or one plain-text message on request).
  They run off the request thread with a hard deadline, one per client per
  5 s, return a code (never server text), may use a stored password only for
  the server it is saved for, and reach only IMAP ports 143/993 unless that
  exact host:port is already saved.
"""

from __future__ import annotations

import configparser
import ipaddress
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

from mailbot_v26.config.ini_editor import (
    AddSection,
    Edit,
    IniEditError,
    RemoveKey,
    RemoveSection,
    SetValue,
    apply_edits,
    atomic_write,
    file_etag,
    list_backups,
    restore_backup,
    write_with_backup,
)
from mailbot_v26.config_loader import (
    ACCOUNT_ID_PATTERN,
    load_web_config,
    parse_telegram_chat_id,
)
from mailbot_v26.config_yaml import _is_default_weak_web_password
from mailbot_v26.health import mail_accounts
from mailbot_v26.tools.config_bootstrap import ACCOUNTS_TEMPLATE, SETTINGS_TEMPLATE
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.accounts_status import (
    EXAMPLE_SECTION,
    PLACEHOLDER,
    SYSTEM_SECTIONS,
    ConfigIssue,
    _chat_id_status,
    _host_looks_valid,
    _issue_payload,
    _port,
    _shown_host,
    _use_ssl,
    all_issues,
    build_home_card,
    collect_accounts_status,
    default_runtime_health_path,
    detect_provider,
    inspect_accounts,
    parse_accounts_text,
    unavailable_home_card,
)
from mailbot_v26.web_observability.settings_text import (
    editor_texts,
    resolve_locale,
    text,
)

logger = logging.getLogger(__name__)

ACCOUNTS_TEMPLATE_NAME = "accounts.html"
HOME_CARD_GLOBAL = "settings_accounts_home_card"
_FORBIDDEN_TEXT = (
    "Forbidden: the Settings pages open only on this computer or after signing "
    "in at /login."
)

MAX_BODY_BYTES = 64 * 1024
MAX_CHANGES = 50
MAX_VALUE_LENGTH = 512
MAX_LOGIN_LENGTH = 254
MIN_WEB_PASSWORD_LENGTH = 10
MAX_WEB_PASSWORD_LENGTH = 256
TEST_INTERVAL_SECONDS = 5.0
TEST_DEADLINE_SECONDS = 10.0
TEST_MAX_CONCURRENT = 2
IMAP_TEST_PORTS = frozenset({143, 993})
LAN_BIND = "0.0.0.0"
LOOPBACK_BIND = "127.0.0.1"
TELEGRAM_API = "https://api.telegram.org"
EDITABLE_FIELDS = ("login", "host", "port", "use_ssl", "telegram_chat_id")
PROVIDER_PRESETS: dict[str, dict[str, object]] = {
    "gmail": {"host": "imap.gmail.com", "port": 993, "use_ssl": True},
    "yandex": {"host": "imap.yandex.ru", "port": 993, "use_ssl": True},
    "mailru": {"host": "imap.mail.ru", "port": 993, "use_ssl": True},
    "outlook": {"unsupported": True},
    "other": {},
}

_CONTROL_RE = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")
# A login shown back for editing: one address or DOMAIN\user, nothing that
# could join it to more text (space, ";", "=", ":", "#").
_LOGIN_SHAPE_RE = re.compile(r"[\w.+'@\\-]{1,254}")
_TOKEN_RE = re.compile(r"\d{3,20}:[A-Za-z0-9_-]{20,128}")
_NUMERIC_LABEL_RE = re.compile(r"(?:0x[0-9a-fA-F]*|\d+)")
_BOT_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{3,64}")


# ---------------------------------------------------------------- shared


def _locale(app: Any) -> str:
    return resolve_locale(app.config.get("UI_LOCALE"))


def _where(exc: BaseException) -> str:
    """'file.py:123' of the innermost frame: debuggable without the message,
    which can quote a line of accounts.ini."""
    tb = exc.__traceback__
    while tb is not None and tb.tb_next is not None:
        tb = tb.tb_next
    if tb is None:
        return "-"
    return f"{Path(tb.tb_frame.f_code.co_filename).name}:{tb.tb_lineno}"


def _access_denial(app: Any) -> Any | None:
    """None when this computer or a signed-in session asks; else a logged 403."""
    remote_addr = web_app._request_remote_addr()
    if remote_addr and (
        web_app._is_loopback(remote_addr) or web_app._ensure_authenticated(app)
    ):
        return None
    reason = "not_signed_in" if remote_addr else "no_client_address"
    logger.warning(
        "settings_access_denied reason=%s remote=%s path=%s",
        reason,
        remote_addr or "-",
        getattr(web_app.request, "path", "-"),
    )
    return web_app._text_response(_FORBIDDEN_TEXT, 403)


def _is_local_request() -> bool:
    remote_addr = web_app._request_remote_addr()
    return bool(remote_addr) and web_app._is_loopback(remote_addr)


def _client_key() -> str:
    return web_app._request_remote_addr() or "-"


def _http_warning() -> bool:
    """A network client talks plain HTTP: warn that the traffic is not encrypted."""
    remote_addr = web_app._request_remote_addr()
    scheme = str(getattr(web_app.request, "scheme", "http") or "http")
    return bool(remote_addr) and not web_app._is_loopback(remote_addr) and scheme != "https"


def _no_store(response: Any) -> Any:
    headers = getattr(response, "headers", None)
    if headers is not None:
        headers["Cache-Control"] = "no-store"
    return response


def _json(payload: dict[str, object], status: int = 200) -> Any:
    response = web_app.jsonify(payload)
    response.status_code = status
    return _no_store(response)


def _error(status: int, code: str, **extra: object) -> Any:
    return _json({"ok": False, "code": code, **extra}, status)


def _same_origin() -> bool:
    """The write comes from this site's own page (fetch sends Origin)."""
    request = web_app.request
    expected = f"{request.scheme}://{request.host}".lower()
    origin = str(request.headers.get("Origin") or "").strip().lower()
    if origin:
        return origin == expected
    referer = str(request.headers.get("Referer") or "").strip()
    if referer:
        parts = urlsplit(referer)
        return f"{parts.scheme}://{parts.netloc}".lower() == expected
    return False


def _write_denial(app: Any) -> Any | None:
    denial = _access_denial(app)
    if denial is not None:
        return denial
    request = web_app.request
    reason = ""
    if not web_app._validate_csrf_token():
        reason, status, code = "csrf", 403, "CSRF"
    elif not _same_origin():
        reason, status, code = "origin", 403, "ORIGIN"
    elif request.content_length is None or request.content_length > MAX_BODY_BYTES:
        reason, status, code = "size", 413, "TOO_LARGE"
    elif not request.is_json:
        reason, status, code = "content_type", 415, "JSON_REQUIRED"
    if not reason:
        return None
    logger.warning(
        "settings_write_refused reason=%s remote=%s path=%s",
        reason,
        _client_key(),
        request.path,
    )
    return _error(status, code)


def _body() -> dict[str, Any] | None:
    data = web_app.request.get_json(silent=True)
    return data if isinstance(data, dict) else None


def _config_dir(app: Any) -> Path | None:
    value = app.config.get("SETTINGS_CONFIG_DIR")
    return Path(value) if value is not None else None


def _now(app: Any) -> datetime:
    clock: Callable[[], datetime] = app.config.get("SETTINGS_NOW") or datetime.now
    return clock()


def _read_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


# ---------------------------------------------------------------- S1 pages


def _collect(app: Any, locale: str) -> dict[str, object]:
    return collect_accounts_status(
        config_dir=app.config.get("SETTINGS_CONFIG_DIR"),
        db_path=app.config.get("DB_PATH"),
        runtime_health_path=app.config.get("SETTINGS_RUNTIME_HEALTH_PATH"),
        locale=locale,
    )


def _status_or_none(app: Any, locale: str) -> dict[str, object] | None:
    try:
        return _collect(app, locale)
    except Exception as exc:
        logger.error(
            "accounts_status_failed error_class=%s at=%s",
            type(exc).__name__,
            _where(exc),
        )
        return None


def _home_card(app: Any) -> dict[str, object]:
    locale = _locale(app)
    try:
        return build_home_card(_collect(app, locale), locale)
    except Exception as exc:
        logger.warning(
            "accounts_home_card_failed error_class=%s at=%s",
            type(exc).__name__,
            _where(exc),
        )
        return unavailable_home_card(locale)


# ---------------------------------------------------------------- editor state


def _issue_dicts(issues: list[ConfigIssue], locale: str) -> list[dict[str, object]]:
    result = []
    for issue in issues:
        payload = _issue_payload(issue, locale)
        payload["severity"] = "error" if issue.blocking else "warning"
        result.append(payload)
    return result


def _editor_accounts(parser: configparser.ConfigParser) -> list[dict[str, object]]:
    default_raw = (
        parser.get("telegram", "chat_id", fallback="").strip()
        if parser.has_section("telegram")
        else ""
    )
    default_set = _chat_id_status(default_raw)[0] == "set"
    rows: list[dict[str, object]] = []
    for section in parser.sections():
        if section.lower() in SYSTEM_SECTIONS:
            continue
        if not ACCOUNT_ID_PATTERN.fullmatch(section):
            # Not editable here; the status table names it (masked).
            rows.append({"id": None, "editable": False})
            continue
        login = parser.get(section, "login", fallback="").strip()
        host = parser.get(section, "host", fallback="").strip()
        chat_raw = parser.get(section, "telegram_chat_id", fallback="").strip()
        chat_ok = _chat_id_status(chat_raw)[0] == "set"
        port, _ = _port(parser, section)
        use_ssl, _ = _use_ssl(parser, section)
        password = parser.get(section, "password", fallback="").strip()
        login_shown = bool(_LOGIN_SHAPE_RE.fullmatch(login)) and login != PLACEHOLDER
        rows.append(
            {
                "id": section,
                "editable": True,
                "template": section == EXAMPLE_SECTION,
                "login": login if login_shown else "",
                "login_hidden": bool(login) and not login_shown and login != PLACEHOLDER,
                "host": "" if host == PLACEHOLDER else _shown_host(host),
                "port": port,
                "use_ssl": use_ssl,
                "chat_id": parse_telegram_chat_id(chat_raw) if chat_ok else "",
                "chat_source": "mailbox" if chat_raw else ("default" if default_set else "none"),
                "password_set": bool(password) and password != PLACEHOLDER,
                "provider": detect_provider(host, login),
            }
        )
    return rows


def _telegram_state(parser: configparser.ConfigParser | None) -> dict[str, object]:
    if parser is None or not parser.has_section("telegram"):
        return {"token_set": False, "chat_id": "", "chat_status": "missing"}
    token = parser.get("telegram", "bot_token", fallback="").strip()
    chat_raw = parser.get("telegram", "chat_id", fallback="").strip()
    chat_status = _chat_id_status(chat_raw)[0]
    return {
        "token_set": bool(token) and token.upper() != PLACEHOLDER,
        "chat_id": parse_telegram_chat_id(chat_raw) if chat_status == "set" else "",
        "chat_status": chat_status,
    }


def _settings_path(config_dir: Path) -> tuple[Path, str]:
    """settings.ini to write and its state: ok / missing (create) / legacy."""
    path = config_dir / "settings.ini"
    if path.exists():
        return path, "ok"
    if (config_dir / "config.ini").exists():
        return path, "legacy"
    return path, "missing"


def _web_state(app: Any, config_dir: Path) -> dict[str, object]:
    saved_host = load_web_config(config_dir).host
    running_host = str(app.config.get("WEB_UI_BIND") or LOOPBACK_BIND)
    lan_saved = not web_app._is_loopback_bind(saved_host)
    lan_running = not web_app._is_loopback_bind(running_host)
    return {
        "password_set": bool(web_app._remote_password(app)),
        "env_override": bool(str(os.environ.get("WEB_PASSWORD") or "").strip()),
        "lan_saved": lan_saved,
        "lan_running": lan_running,
        "lan_toggle": bool(app.config.get("SETTINGS_LAN_TOGGLE", False)),
        "restart_required": lan_saved != lan_running,
        "local": _is_local_request(),
        "settings_file": _settings_path(config_dir)[1],
    }


def _editor_state(app: Any, config_dir: Path, locale: str) -> dict[str, object]:
    accounts_path = config_dir / "accounts.ini"
    raw = _read_bytes(accounts_path)
    state: dict[str, object] = {
        "etag": file_etag(raw),
        "file": "ok",
        "accounts": [],
        "telegram": _telegram_state(None),
        "issues": [],
        "backups": list_backups(accounts_path),
        "web": _web_state(app, config_dir),
        "presets": PROVIDER_PRESETS,
    }
    if raw is None:
        state["file"] = "missing"
        return state
    try:
        parser = parse_accounts_text(raw.decode("utf-8-sig"))
    except UnicodeDecodeError:
        state["file"] = "encoding"
        return state
    except configparser.Error:
        state["file"] = "unreadable"
        return state
    settings_present = _settings_path(config_dir)[1] != "missing"
    inspection = inspect_accounts(parser, settings_present=settings_present)
    state["accounts"] = _editor_accounts(parser)
    state["telegram"] = _telegram_state(parser)
    state["issues"] = _issue_dicts(list(all_issues(inspection)), locale)
    return state


# ---------------------------------------------------------------- change plans


class _Issues:
    def __init__(self) -> None:
        self.items: list[ConfigIssue] = []

    def add(self, code: str, section: str = "", field: str = "", blocking: bool = True) -> None:
        issue = ConfigIssue(code, section, field, blocking=blocking)
        if issue not in self.items:
            self.items.append(issue)

    @property
    def blocking(self) -> bool:
        return any(issue.blocking for issue in self.items)


def _text_value(
    value: object,
    issues: _Issues,
    section: str,
    field: str,
    *,
    max_length: int = MAX_VALUE_LENGTH,
    strip: bool = True,
) -> str | None:
    """A clean single-line value, or None with an issue recorded."""
    if not isinstance(value, str):
        issues.add("bad_type", section, field)
        return None
    if _CONTROL_RE.search(value):
        issues.add("bad_characters", section, field)
        return None
    if len(value) > max_length:
        issues.add("too_long", section, field)
        return None
    if strip:
        return value.strip()
    if value != value.strip():
        issues.add("edge_spaces", section, field)
        return None
    return value


def _port_value(value: object, issues: _Issues, section: str) -> int | None:
    if isinstance(value, bool):
        issues.add("bad_port", section, "port")
        return None
    if isinstance(value, str) and value.strip().isdigit():
        value = int(value.strip())
    if isinstance(value, int) and 1 <= value <= 65535:
        return value
    issues.add("bad_port", section, "port")
    return None


def _ssl_value(value: object, issues: _Issues, section: str) -> bool | None:
    if isinstance(value, bool):
        return value
    states = configparser.ConfigParser.BOOLEAN_STATES
    if isinstance(value, str) and value.strip().lower() in states:
        return states[value.strip().lower()]
    issues.add("bad_ssl", section, "use_ssl")
    return None


def _host_value(value: object, issues: _Issues, section: str) -> str | None:
    host = _text_value(value, issues, section, "host", max_length=253)
    if host is None:
        return None
    if not host or not _host_looks_valid(host):
        issues.add("bad_host", section, "host")
        return None
    if detect_provider(host) == "outlook":
        issues.add("outlook_unsupported", section, "host", blocking=False)
    return host


def _chat_value(value: object, issues: _Issues, section: str, field: str) -> str | None:
    chat = _text_value(value, issues, section, field, max_length=64)
    if chat is None or not chat:
        return chat
    status, code = _chat_id_status(chat)
    if status != "set":
        issues.add(code or "chat_format", section, field)
        return None
    return chat


def _secret_value(value: object, issues: _Issues, section: str, field: str) -> str | None:
    secret = _text_value(value, issues, section, field, strip=False)
    if secret is not None and secret.upper() == PLACEHOLDER:
        issues.add("missing_field", section, field)
        return None
    return secret


def _account_fields(
    op: str, account_id: str, change: dict[str, Any], issues: _Issues
) -> list[tuple[str, str | None]]:
    """(key, value) pairs to write; value None removes the key."""
    fields = change.get("fields") or {}
    if not isinstance(fields, dict) or any(key not in EDITABLE_FIELDS for key in fields):
        issues.add("unknown_field", account_id)
        return []
    pairs: list[tuple[str, str | None]] = []
    if "login" in fields or op == "add":
        login = _text_value(
            fields.get("login", ""), issues, account_id, "login", max_length=MAX_LOGIN_LENGTH
        )
        if login == "" or (login or "").upper() == PLACEHOLDER:
            issues.add("required", account_id, "login")
        elif login is not None:
            pairs.append(("login", login))
    password = change.get("password")
    if password not in (None, ""):
        secret = _secret_value(password, issues, account_id, "password")
        if secret is not None:
            pairs.append(("password", secret))
    elif op == "add":
        issues.add("required", account_id, "password")
    if "host" in fields or op == "add":
        host = _host_value(fields.get("host", ""), issues, account_id)
        if host is not None:
            pairs.append(("host", host))
    if "port" in fields or op == "add":
        port = _port_value(fields.get("port", 993), issues, account_id)
        if port is not None:
            pairs.append(("port", str(port)))
    if "use_ssl" in fields or op == "add":
        use_ssl = _ssl_value(fields.get("use_ssl", True), issues, account_id)
        if use_ssl is not None:
            pairs.append(("use_ssl", "true" if use_ssl else "false"))
            if not use_ssl:
                issues.add("plain_imap", account_id, "use_ssl", blocking=False)
    if "telegram_chat_id" in fields:
        chat = _chat_value(fields["telegram_chat_id"], issues, account_id, "telegram_chat_id")
        if chat is not None:
            pairs.append(("telegram_chat_id", chat or None))
    return pairs


def _moves_stored_password(
    parser: configparser.ConfigParser, section: str, pairs: list[tuple[str, str | None]]
) -> bool:
    """True when an update points a stored password at another server: the
    host, port or SSL setting differs from what the worker uses now."""
    stored = parser.get(section, "password", fallback="").strip()
    if not stored or stored.upper() == PLACEHOLDER:
        return False
    port, _ = _port(parser, section)
    use_ssl, _ = _use_ssl(parser, section)
    saved: dict[str, str | None] = {
        "host": parser.get(section, "host", fallback="").strip().lower(),
        "port": None if port is None else str(port),
        "use_ssl": None if use_ssl is None else ("true" if use_ssl else "false"),
    }
    return any(key in saved and (value or "").lower() != saved[key] for key, value in pairs)


def _account_edits(
    parser: configparser.ConfigParser | None, changes: object, issues: _Issues
) -> list[Edit]:
    if not isinstance(changes, list) or len(changes) > MAX_CHANGES:
        issues.add("bad_request")
        return []
    edits: list[Edit] = []
    adding = False
    for change in changes:
        if not isinstance(change, dict):
            issues.add("bad_request")
            continue
        op = change.get("op")
        account_id = change.get("id")
        if (
            not isinstance(account_id, str)
            or not ACCOUNT_ID_PATTERN.fullmatch(account_id)
            or len(account_id) > 64
            or account_id in SYSTEM_SECTIONS
        ):
            issues.add("bad_id", field="id")
            continue
        exists = parser is not None and parser.has_section(account_id)
        if op == "remove":
            if exists:
                edits.append(RemoveSection(account_id))
            else:
                issues.add("unknown_account", account_id)
            continue
        if op not in ("add", "update"):
            issues.add("bad_request", account_id)
            continue
        if op == "add" and exists:
            issues.add("account_exists", account_id, "id")
            continue
        if op == "update" and not exists:
            issues.add("unknown_account", account_id)
            continue
        pairs = _account_fields(op, account_id, change, issues)
        if (
            op == "update"
            and parser is not None
            and change.get("password") in (None, "")
            and _moves_stored_password(parser, account_id, pairs)
        ):
            # A stored password goes only to the server it was saved for (the
            # same rule as the Test button): a new server needs it typed again.
            issues.add("password_required", account_id, "password")
        if op == "add":
            adding = True
            edits.append(
                AddSection(account_id, tuple((k, v) for k, v in pairs if v is not None))
            )
        else:
            edits.extend(
                SetValue(account_id, key, value) if value is not None else RemoveKey(account_id, key)
                for key, value in pairs
            )
    if adding and parser is not None and parser.has_section(EXAMPLE_SECTION):
        template_password = parser.get(EXAMPLE_SECTION, "password", fallback="").strip()
        if not template_password or template_password == PLACEHOLDER:
            # The first real mailbox replaces the untouched init-config template.
            edits.insert(0, RemoveSection(EXAMPLE_SECTION))
    return edits


def _telegram_edits(telegram: object, issues: _Issues) -> list[Edit]:
    if telegram is None:
        return []
    if not isinstance(telegram, dict) or any(k not in ("bot_token", "chat_id") for k in telegram):
        issues.add("bad_request", "telegram")
        return []
    edits: list[Edit] = []
    token = telegram.get("bot_token")
    if token not in (None, ""):
        secret = _secret_value(token, issues, "telegram", "bot_token")
        if secret is not None and not _TOKEN_RE.fullmatch(secret):
            issues.add("token_format", "telegram", "bot_token")
        elif secret is not None:
            edits.append(SetValue("telegram", "bot_token", secret))
    if "chat_id" in telegram:
        chat = _chat_value(telegram["chat_id"], issues, "telegram", "chat_id")
        if chat == "":
            edits.append(RemoveKey("telegram", "chat_id"))
        elif chat is not None:
            edits.append(SetValue("telegram", "chat_id", chat))
    return edits


@dataclass(frozen=True)
class _Plan:
    candidate: bytes | None
    issues: list[ConfigIssue]
    blocking: bool
    changed: list[str]


def _snapshot(raw: bytes) -> dict[str, dict[str, str]]:
    parser = parse_accounts_text(raw.decode("utf-8-sig"))
    return {s: dict(parser.items(s, raw=True)) for s in parser.sections()}


def _changed(before: dict[str, dict[str, str]], after: dict[str, dict[str, str]]) -> list[str]:
    """'[section] key' names (never values) that differ."""
    names: list[str] = []
    for section in sorted(set(before) | set(after)):
        if section not in after:
            names.append(f"[{section}] removed")
        elif section not in before:
            names.append(f"[{section}] added")
        else:
            old, new = before[section], after[section]
            names.extend(
                f"[{section}] {key}"
                for key in sorted(set(old) | set(new))
                if old.get(key) != new.get(key)
            )
    return names


def _template_base() -> bytes:
    """A new accounts.ini: the init-config template without its example mailbox
    and without the CHANGE_ME default chat (the worker refuses that value)."""
    return apply_edits(
        ACCOUNTS_TEMPLATE.encode("utf-8"),
        [RemoveSection(EXAMPLE_SECTION), RemoveKey("telegram", "chat_id")],
    )


def _plan(base: bytes | None, body: dict[str, Any], *, settings_present: bool) -> _Plan:
    issues = _Issues()
    raw = base if base is not None else _template_base()
    parser = parse_accounts_text(raw.decode("utf-8-sig"))
    edits = _account_edits(parser, body.get("changes", []), issues)
    edits += _telegram_edits(body.get("telegram"), issues)
    if issues.blocking or not edits:
        return _Plan(None, issues.items, issues.blocking, [])
    try:
        # Re-parsed with the worker's reader: the result must read back as
        # exactly these changes (catches %, line breaks, layout surprises).
        candidate = apply_edits(raw, edits)
    except IniEditError as exc:
        issues.add("bad_characters" if exc.code == "bad_value" else "layout")
        return _Plan(None, issues.items, True, [])
    changed = _changed(_snapshot(raw), _snapshot(candidate))
    if not changed and base is not None:
        return _Plan(None, issues.items, issues.blocking, [])
    before = set(all_issues(inspect_accounts(parser, settings_present=settings_present)))
    after_parser = parse_accounts_text(candidate.decode("utf-8-sig"))
    after = all_issues(inspect_accounts(after_parser, settings_present=settings_present))
    # A change is refused only for a new problem in a section it touches:
    # what is already wrong elsewhere (a missing token while the first
    # mailbox is added) stays a hint on the status page.
    touched = {edit.section for edit in edits if not isinstance(edit, RemoveSection)}
    new_blocking = [
        issue
        for issue in after
        if issue.blocking and issue not in before and issue.section in touched
    ]
    combined = list(issues.items)
    combined.extend(issue for issue in after if issue not in combined)
    blocking = issues.blocking or bool(new_blocking)
    return _Plan(candidate if not blocking else None, combined, blocking, changed)


# ---------------------------------------------------------------- tests (probes)


class RateLimiter:
    """At most one call per ``interval`` seconds per key (bounded memory)."""

    MAX_KEYS = 1024

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._last: dict[str, float] = {}

    def retry_after(self, key: str, interval: float) -> float:
        """0 and the call is recorded, else the seconds still to wait."""
        now = self._clock()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < interval:
                return interval - (now - last)
            if key not in self._last and len(self._last) >= self.MAX_KEYS:
                del self._last[min(self._last, key=self._last.__getitem__)]
            self._last[key] = now
        return 0.0


def _run_with_deadline(app: Any, label: str, func: Callable[[], str]) -> str:
    """Run a probe off the request thread; TIMEOUT after the deadline, BUSY if full."""
    slots: threading.BoundedSemaphore = app.config["SETTINGS_TEST_SLOTS"]
    if not slots.acquire(blocking=False):
        return "BUSY"
    done = threading.Event()
    box: dict[str, str] = {}

    def runner() -> None:
        try:
            box["code"] = func()
        except Exception as exc:
            logger.warning(
                "settings_test_crashed probe=%s error_class=%s at=%s",
                label,
                type(exc).__name__,
                _where(exc),
            )
            box["code"] = "OTHER"
        finally:
            slots.release()
            done.set()

    threading.Thread(target=runner, name=f"settings-test-{label}", daemon=True).start()
    deadline = float(app.config.get("SETTINGS_TEST_DEADLINE_SECONDS", TEST_DEADLINE_SECONDS))
    if not done.wait(deadline):
        logger.warning("settings_test_deadline probe=%s seconds=%s", label, deadline)
        return "TIMEOUT"
    return box.get("code", "OTHER")


def _rate_limited(app: Any, kind: str) -> Any | None:
    limiter: RateLimiter = app.config["SETTINGS_TEST_LIMITER"]
    interval = float(app.config.get("SETTINGS_TEST_INTERVAL_SECONDS", TEST_INTERVAL_SECONDS))
    wait = limiter.retry_after(f"{_client_key()}:{kind}", interval)
    if wait <= 0:
        return None
    return _error(429, "RATE_LIMIT", retry_after=max(1, int(wait + 0.999)))


def _ip_not_allowed(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        # "0251.0376.0251.0376" or "0x7f.1": an address in disguise that
        # the system resolver may still accept.
        return all(_NUMERIC_LABEL_RE.fullmatch(label) for label in host.rstrip(".").split("."))
    return (
        address.is_unspecified
        or address.is_multicast
        or address.is_link_local
        or address.is_reserved
    )


def _saved_mailboxes(raw: bytes | None) -> dict[str, dict[str, object]]:
    if raw is None:
        return {}
    try:
        parser = parse_accounts_text(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, configparser.Error):
        return {}
    saved: dict[str, dict[str, object]] = {}
    for section in parser.sections():
        if section.lower() in SYSTEM_SECTIONS or not ACCOUNT_ID_PATTERN.fullmatch(section):
            continue
        port, _ = _port(parser, section)
        use_ssl, _ = _use_ssl(parser, section)
        saved[section] = {
            "host": parser.get(section, "host", fallback="").strip().lower(),
            "port": port,
            "use_ssl": use_ssl,
            "login": parser.get(section, "login", fallback="").strip(),
            "password": parser.get(section, "password", fallback="").strip(),
        }
    return saved


def _imap_target(body: dict[str, Any], saved: dict[str, dict[str, object]]) -> tuple[dict, str]:
    """(probe arguments, "") or ({}, refusal code)."""
    issues = _Issues()
    host = _text_value(body.get("host", ""), issues, "", "host", max_length=253)
    port = _port_value(body.get("port", 993), issues, "")
    use_ssl = _ssl_value(body.get("use_ssl", True), issues, "")
    if host is None or not host or not _host_looks_valid(host) or port is None or use_ssl is None:
        return {}, "INVALID"
    if _ip_not_allowed(host):
        return {}, "HOST_NOT_ALLOWED"
    endpoint = (host.lower(), port)
    if port not in IMAP_TEST_PORTS and endpoint not in {
        (str(m["host"]), m["port"]) for m in saved.values()
    }:
        return {}, "PORT_NOT_ALLOWED"
    account = saved.get(str(body.get("id") or ""))
    same_server = account is not None and (
        account["host"],
        account["port"],
        account["use_ssl"],
    ) == (host.lower(), port, use_ssl)
    password = body.get("password")
    if password in (None, ""):
        # A stored secret goes only to the server it is saved for.
        stored = str(account["password"]) if same_server and account else ""
        if not stored or stored == PLACEHOLDER:
            return {}, "PASSWORD_REQUIRED"
        password = stored
    password = _secret_value(password, issues, "", "password")
    login = _text_value(body.get("login", ""), issues, "", "login", max_length=MAX_LOGIN_LENGTH)
    if not login and same_server and account:
        login = str(account["login"])
    if password is None or not login or login == PLACEHOLDER:
        return {}, "LOGIN_REQUIRED" if password is not None else "INVALID"
    return {"host": host, "port": port, "use_ssl": use_ssl, "login": login, "password": password}, ""


def _telegram_http(method: str, token: str, payload: dict | None, timeout: float) -> tuple[int, Any]:
    """One Bot API call to api.telegram.org: (HTTP status, JSON body or None)."""
    import requests

    url = f"{TELEGRAM_API}/bot{token}/{method}"
    if payload is None:
        response = requests.get(url, timeout=timeout)
    else:
        response = requests.post(url, json=payload, timeout=timeout)
    try:
        data = response.json()
    except ValueError:
        data = None
    return int(response.status_code), data


def _telegram_failure_code(exc: BaseException) -> str:
    names = {cls.__name__ for cls in type(exc).__mro__}
    if names & {"Timeout", "TimeoutError", "timeout"}:
        return "TIMEOUT"
    if "SSLError" in names:
        return "TLS"
    if names & {"ConnectionError", "OSError"}:
        return "NETWORK"
    return "OTHER"


def _telegram_probe(action: str, token: str, chat_id: str, message: str, timeout: float) -> dict:
    method, payload = ("getMe", None) if action == "getme" else (
        "sendMessage",
        {"chat_id": chat_id, "text": message, "disable_web_page_preview": True},
    )
    try:
        status, data = _telegram_http(method, token, payload, timeout)
    except Exception as exc:
        # The exception text can hold the request URL, i.e. the token.
        code = _telegram_failure_code(exc)
        logger.info("settings_telegram_test_failed action=%s code=%s error_class=%s", action, code, type(exc).__name__)
        return {"code": code}
    ok = status == 200 and isinstance(data, dict) and data.get("ok") is True
    if ok:
        result: dict[str, object] = {"code": "OK"}
        bot = data.get("result") if action == "getme" else None
        username = bot.get("username") if isinstance(bot, dict) else None
        if isinstance(username, str) and _BOT_USERNAME_RE.fullmatch(username):
            result["bot_username"] = username
        return result
    if status in (401, 404):
        code = "AUTH"
    elif action == "send" and status in (400, 403):
        code = "CHAT"
    else:
        code = "OTHER"
    logger.info("settings_telegram_test_failed action=%s code=%s http_status=%s", action, code, status)
    return {"code": code}


# ---------------------------------------------------------------- routes


def _saved_message_fields(app: Any, backup: str | None) -> dict[str, object]:
    return {"saved_at": _now(app).strftime("%H:%M"), "backup": backup}


def _log_saved(file_name: str, changed: list[str], backup: str | None) -> None:
    logger.info(
        "settings_saved file=%s changed=%s backup=%s client=%s",
        file_name,
        ",".join(changed) or "-",
        backup or "-",
        "loopback" if _is_local_request() else _client_key(),
    )


def _ensure_settings_file(config_dir: Path) -> bool:
    """Write settings.ini from the init-config template when neither it nor
    config.ini exists (the worker needs it). True when created."""
    path, state = _settings_path(config_dir)
    if state != "missing":
        return False
    atomic_write(path, SETTINGS_TEMPLATE.encode("utf-8"))
    logger.info("settings_saved file=settings.ini changed=created backup=-")
    return True


def _save_accounts(app: Any, config_dir: Path, body: dict[str, Any]) -> Any:
    path = config_dir / "accounts.ini"
    with app.config["SETTINGS_WRITE_LOCK"]:
        raw = _read_bytes(path)
        if str(body.get("etag") or "") != file_etag(raw):
            logger.info("settings_save_conflict file=accounts.ini")
            return _error(409, "CONFLICT", etag=file_etag(raw))
        try:
            plan = _plan(raw, body, settings_present=_settings_path(config_dir)[1] != "missing")
        except (UnicodeDecodeError, configparser.Error):
            return _error(409, "FILE_UNREADABLE")
        if plan.blocking:
            return _json(
                {"ok": False, "code": "INVALID", "issues": _issue_dicts(plan.issues, _locale(app))},
                422,
            )
        if plan.candidate is None:
            # Nothing differs: no write, no backup.
            return _json({"ok": True, "unchanged": True, "changed": [], "etag": file_etag(raw)})
        backup = write_with_backup(path, plan.candidate, now=_now(app))
        settings_created = _ensure_settings_file(config_dir)
    _log_saved("accounts.ini", plan.changed, backup)
    return _json(
        {
            "ok": True,
            "etag": file_etag(plan.candidate),
            "changed": plan.changed,
            "settings_created": settings_created,
            "issues": _issue_dicts(plan.issues, _locale(app)),
            **_saved_message_fields(app, backup),
        }
    )


def _write_settings_value(app: Any, config_dir: Path, section: str, key: str, value: str) -> tuple[str | None, Any | None]:
    """Set one settings.ini value; (backup name, None) or (None, error response)."""
    path, state = _settings_path(config_dir)
    if state == "legacy":
        return None, _error(409, "LEGACY_CONFIG")
    with app.config["SETTINGS_WRITE_LOCK"]:
        raw = _read_bytes(path)
        base = raw if raw is not None else SETTINGS_TEMPLATE.encode("utf-8")
        try:
            candidate = apply_edits(base, [SetValue(section, key, value)])
        except (UnicodeDecodeError, configparser.Error):
            return None, _error(409, "FILE_UNREADABLE")
        except IniEditError:
            return None, _error(422, "INVALID")
        backup = write_with_backup(path, candidate, now=_now(app))
    _log_saved("settings.ini", [f"[{section}] {key}"], backup)
    return backup, None


def register_settings_routes(app: Any, *, config_dir: Path | None) -> None:
    """Add the Settings routes and the Home-card Jinja global to ``app``."""
    app.config["SETTINGS_CONFIG_DIR"] = (
        Path(config_dir) if config_dir is not None else None
    )
    app.config.setdefault(
        "SETTINGS_RUNTIME_HEALTH_PATH", default_runtime_health_path()
    )
    app.config.setdefault("SETTINGS_WRITE_LOCK", threading.Lock())
    app.config.setdefault("SETTINGS_TEST_LIMITER", RateLimiter())
    app.config.setdefault("SETTINGS_TEST_SLOTS", threading.BoundedSemaphore(TEST_MAX_CONCURRENT))

    @app.route("/accounts", methods=["GET"])
    def accounts():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        locale = _locale(app)
        status = _status_or_none(app, locale)
        title = text("accounts.title", locale)
        body = web_app._render_template(
            app,
            ACCOUNTS_TEMPLATE_NAME,
            title=f"{title} · LetterBot.ru",
            page_title=title,
            status=status,
            st=partial(text, locale=locale),
            http_warning=_http_warning(),
            hide_limit=True,
            editor_enabled=_config_dir(app) is not None,
            editor_data={"texts": editor_texts(locale), "presets": PROVIDER_PRESETS},
            csrf_token=web_app._get_or_create_csrf_token(),
        )
        return _no_store(
            web_app._text_response(body, 200 if status is not None else 500)
        )

    @app.route("/api/accounts/status", methods=["GET"])
    def api_accounts_status():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        status = _status_or_none(app, _locale(app))
        if status is None:
            response = web_app.jsonify({"error": "status_unavailable"})
            response.status_code = 500
            return _no_store(response)
        return _no_store(web_app.jsonify(status))

    @app.route("/api/settings/accounts", methods=["GET"])
    def api_settings_accounts():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        folder = _config_dir(app)
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        try:
            return _json({"ok": True, **_editor_state(app, folder, _locale(app))})
        except Exception as exc:
            logger.error("settings_state_failed error_class=%s at=%s", type(exc).__name__, _where(exc))
            return _error(500, "STATE_UNAVAILABLE")

    @app.route("/api/settings/accounts/validate", methods=["POST"])
    def api_settings_validate():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        if body is None:
            return _error(400, "BAD_REQUEST")
        try:
            plan = _plan(
                _read_bytes(folder / "accounts.ini"),
                body,
                settings_present=_settings_path(folder)[1] != "missing",
            )
        except (UnicodeDecodeError, configparser.Error):
            return _error(409, "FILE_UNREADABLE")
        return _json(
            {
                "ok": not plan.blocking,
                "issues": _issue_dicts(plan.issues, _locale(app)),
                "changed": plan.changed,
            }
        )

    @app.route("/api/settings/accounts", methods=["POST"])
    def api_settings_save():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        if body is None:
            return _error(400, "BAD_REQUEST")
        try:
            return _save_accounts(app, folder, body)
        except OSError as exc:
            logger.error("settings_save_failed error_class=%s at=%s", type(exc).__name__, _where(exc))
            return _error(500, "WRITE_FAILED")

    @app.route("/api/settings/accounts/restore", methods=["POST"])
    def api_settings_restore():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        name = (body or {}).get("name")
        if not isinstance(name, str):
            return _error(400, "BAD_REQUEST")
        path = folder / "accounts.ini"
        with app.config["SETTINGS_WRITE_LOCK"]:
            raw = _read_bytes(path)
            if str((body or {}).get("etag") or "") != file_etag(raw):
                return _error(409, "CONFLICT", etag=file_etag(raw))
            try:
                backup = restore_backup(path, name, now=_now(app))
            except IniEditError as exc:
                status = 404 if exc.code == "backup_not_found" else 422
                return _error(status, exc.code.upper())
            except OSError as exc:
                logger.error("settings_restore_failed error_class=%s at=%s", type(exc).__name__, _where(exc))
                return _error(500, "WRITE_FAILED")
            etag = file_etag(_read_bytes(path))
        _log_saved("accounts.ini", [f"restored {name}"], backup)
        return _json({"ok": True, "etag": etag, "restored": name, **_saved_message_fields(app, backup)})

    @app.route("/api/settings/applied", methods=["GET"])
    def api_settings_applied():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        folder = _config_dir(app)
        raw = _read_bytes(folder / "accounts.ini") if folder is not None else None
        # S3 adds the worker's applied hash and time.
        return _json(
            {
                "ok": True,
                "saved_sha12": file_etag(raw)[:12] if raw is not None else None,
                "applied_sha12": None,
                "applied_at": None,
            }
        )

    @app.route("/api/settings/accounts/test-imap", methods=["POST"])
    def api_settings_test_imap():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        if body is None:
            return _error(400, "BAD_REQUEST")
        target, refusal = _imap_target(body, _saved_mailboxes(_read_bytes(folder / "accounts.ini")))
        if refusal:
            return _error(422, refusal)
        limited = _rate_limited(app, "imap")
        if limited is not None:
            return limited
        timeout = float(app.config.get("SETTINGS_TEST_DEADLINE_SECONDS", TEST_DEADLINE_SECONDS))
        code = _run_with_deadline(
            app, "imap", partial(mail_accounts.probe_imap_login, timeout_sec=timeout, **target)
        )
        logger.info("settings_imap_test code=%s port=%s", code, target["port"])
        if code == "BUSY":
            return _error(429, "BUSY")
        return _json({"ok": code == "OK", "code": code})

    @app.route("/api/settings/telegram/test", methods=["POST"])
    def api_settings_test_telegram():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        if body is None or body.get("action") not in ("getme", "send"):
            return _error(400, "BAD_REQUEST")
        action = str(body["action"])
        issues = _Issues()
        raw = _read_bytes(folder / "accounts.ini")
        stored = {"token_set": False}
        parser = None
        if raw is not None:
            try:
                parser = parse_accounts_text(raw.decode("utf-8-sig"))
                stored = _telegram_state(parser)
            except (UnicodeDecodeError, configparser.Error):
                parser = None
        token = body.get("bot_token")
        if token in (None, ""):
            token = parser.get("telegram", "bot_token", fallback="").strip() if stored["token_set"] and parser else ""
        else:
            token = _secret_value(token, issues, "telegram", "bot_token") or ""
        if not token or not _TOKEN_RE.fullmatch(token):
            return _error(422, "TOKEN_MISSING" if not token else "TOKEN_FORMAT")
        chat_id = ""
        if action == "send":
            chat = body.get("chat_id")
            if chat in (None, ""):
                chat_id = str(stored.get("chat_id") or "")
            else:
                chat_id = _chat_value(chat, issues, "telegram", "chat_id") or ""
                if not chat_id:
                    return _error(422, "CHAT_FORMAT")
            if not chat_id:
                return _error(422, "CHAT_MISSING")
        limited = _rate_limited(app, f"telegram-{action}")
        if limited is not None:
            return limited
        timeout = float(app.config.get("SETTINGS_TEST_DEADLINE_SECONDS", TEST_DEADLINE_SECONDS))
        box: dict[str, dict] = {}

        def probe() -> str:
            box["result"] = _telegram_probe(
                action, token, chat_id, text("telegram.test_message", _locale(app)), timeout
            )
            return str(box["result"]["code"])

        code = _run_with_deadline(app, "telegram", probe)
        logger.info("settings_telegram_test action=%s code=%s", action, code)
        if code == "BUSY":
            return _error(429, "BUSY")
        result = dict(box.get("result") or {}) if code != "TIMEOUT" else {}
        result["code"] = code
        return _json({"ok": code == "OK", **result})

    @app.route("/api/settings/web-password", methods=["POST"])
    def api_settings_web_password():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        if body is None:
            return _error(400, "BAD_REQUEST")
        if str(os.environ.get("WEB_PASSWORD") or "").strip():
            return _error(409, "ENV_OVERRIDE")
        if not _is_local_request():
            throttle = app.config["WEB_UI_LOGIN_THROTTLE"]
            client = _client_key()
            retry_after = throttle.retry_after(client)
            if retry_after > 0:
                return _error(429, "RATE_LIMIT", retry_after=retry_after)
            if not web_app._secret_matches(body.get("current_password"), web_app._remote_password(app)):
                failures, _ = throttle.record_failure(client)
                logger.warning("settings_web_password_refused reason=current_password client=%s failures=%d", client, failures)
                return _error(403, "CURRENT_PASSWORD_WRONG")
            throttle.reset(client)
        issues = _Issues()
        new_password = _secret_value(body.get("new_password"), issues, "web_ui", "password")
        if new_password is None or issues.items:
            return _error(422, "INVALID")
        if len(new_password) < MIN_WEB_PASSWORD_LENGTH or len(new_password) > MAX_WEB_PASSWORD_LENGTH:
            return _error(422, "LENGTH", min=MIN_WEB_PASSWORD_LENGTH)
        if _is_default_weak_web_password(new_password):
            return _error(422, "WEAK")
        try:
            backup, error = _write_settings_value(app, folder, "web_ui", "password", new_password)
        except OSError as exc:
            logger.error("settings_save_failed error_class=%s at=%s", type(exc).__name__, _where(exc))
            return _error(500, "WRITE_FAILED")
        if error is not None:
            return error
        # Network logins use the new password right away.
        app.config["WEB_PASSWORD"] = new_password
        return _json({"ok": True, **_saved_message_fields(app, backup)})

    @app.route("/api/settings/web-access", methods=["POST"])
    def api_settings_web_access():
        denial = _write_denial(app)
        if denial is not None:
            return denial
        folder, body = _config_dir(app), _body()
        if folder is None:
            return _error(503, "NO_CONFIG_DIR")
        if body is None or not isinstance(body.get("lan"), bool):
            return _error(400, "BAD_REQUEST")
        if not _is_local_request():
            return _error(403, "LOOPBACK_ONLY")
        if not app.config.get("SETTINGS_LAN_TOGGLE", False):
            return _error(409, "LAUNCHER_UNSUPPORTED")
        lan = bool(body["lan"])
        if lan and not web_app._remote_password(app):
            return _error(409, "PASSWORD_REQUIRED")
        saved_lan = not web_app._is_loopback_bind(load_web_config(folder).host)
        backup = None
        if saved_lan != lan:
            try:
                backup, error = _write_settings_value(
                    app, folder, "web", "host", LAN_BIND if lan else LOOPBACK_BIND
                )
            except OSError as exc:
                logger.error("settings_save_failed error_class=%s at=%s", type(exc).__name__, _where(exc))
                return _error(500, "WRITE_FAILED")
            if error is not None:
                return error
        running_lan = not web_app._is_loopback_bind(str(app.config.get("WEB_UI_BIND") or LOOPBACK_BIND))
        return _json(
            {"ok": True, "lan": lan, "restart_required": lan != running_lan, **_saved_message_fields(app, backup)}
        )

    jinja_env = getattr(app, "jinja_env", None)
    if jinja_env is not None:
        jinja_env.globals[HOME_CARD_GLOBAL] = partial(_home_card, app)


__all__ = ["HOME_CARD_GLOBAL", "RateLimiter", "register_settings_routes"]
