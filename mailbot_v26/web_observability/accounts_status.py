"""Read-only mailbox status for the Settings lane (S1: GET /accounts).

Sources, never written: accounts.ini in the config folder, the IMAP_HEALTH
success events in events_v1 (last successful check per login) and the
worker's runtime_health.json (failure bucket, next retry, cooldown).

The rules mirror what the worker does with the same accounts.ini (start.py
preflight, config_loader.load_accounts_config and validate_telegram_contract),
so "Letterbot will not start" is only claimed when the worker would refuse.

Privacy: secrets leave this module only as set/unset booleans; the free-text
IMAP error in runtime_health.json (last_error, last_alert_fingerprint) is
never read into the result; configparser exception text is never logged or
shown because it can quote a line of the file (a password); logins and
e-mail-like section names are masked (a…@gmail.com). A malformed line can
carry a secret into a shown value (an indented line continues the value
above, configparser has no inline comments, a header runs to its line's last
"]", a value on its own line reads as a key), so file text is shown only in
its expected shape: a login's domain for a single address, a host that is a
server name, key names Letterbot reads, a section name up to its first "]",
"=", ":", ";" or "#".
"""

from __future__ import annotations

import configparser
import ipaddress
import json
import logging
import re
import sqlite3
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Iterable, Mapping

from mailbot_v26.account_identity import normalize_login
from mailbot_v26.config_loader import ACCOUNT_ID_PATTERN, parse_telegram_chat_id
from mailbot_v26.events.contract import EventType
from mailbot_v26.storage.sqlite_uri import sqlite_readonly_uri
from mailbot_v26.web_observability.settings_text import (
    fix_card,
    plural_text,
    text,
)

logger = logging.getLogger(__name__)

SYSTEM_SECTIONS = frozenset({"telegram", "cloudflare", "gigachat", "llm"})
EXAMPLE_SECTION = "example_account"
PLACEHOLDER = "CHANGE_ME"
ACCOUNTS_URL = "/accounts"

OK_MAX_AGE = timedelta(minutes=15)
DELAYED_MAX_AGE = timedelta(minutes=60)

# File states
FILE_OK = "ok"
FILE_NO_CONFIG_DIR = "no_config_dir"
FILE_MISSING = "missing"
FILE_NO_MAILBOXES = "no_mailboxes"
FILE_ENCODING = "encoding"
FILE_UNREADABLE = "unreadable"

# Evidence states
EVIDENCE_OK = "ok"
EVIDENCE_MISSING = "missing"
EVIDENCE_UNAVAILABLE = "unavailable"
EVIDENCE_SKIPPED = "skipped"

# start.py emits these with entity_id "imap_health:<subtype>" after a
# successful IMAP session (same subtypes the health page counts as success).
_SUCCESS_ENTITY_IDS = tuple(
    f"imap_health:{subtype}"
    for subtype in ("success", "startup", "reconnect", "uidvalidity_change")
)
_LAST_SUCCESS_SQL = (
    "SELECT ts_utc FROM events_v1 "
    "WHERE event_type = ? AND account_id = ? AND entity_id IN (?, ?, ?, ?) "
    "ORDER BY ts_utc DESC LIMIT 1"
)

# Values the worker reads at startup with %-interpolation: a lone % in one of
# them stops Letterbot (InterpolationSyntaxError). A % elsewhere only warns.
_MAILBOX_READ_KEYS = frozenset(
    {
        "login",
        "username",
        "name",
        "password",
        "host",
        "port",
        "use_ssl",
        "telegram_chat_id",
    }
)
_SYSTEM_READ_KEYS = {
    "telegram": frozenset({"bot_token", "chat_id"}),
    "cloudflare": frozenset({"account_id", "api_token"}),
}
# Key names the page may show: those Letterbot reads from accounts.ini
# (config_loader, the startup preflight, llm/router.py). Any other "key" can be
# part of a value: "password =" with "ab=c%d" on the next line reads as "ab".
_SHOWN_KEYS = _MAILBOX_READ_KEYS.union(
    *_SYSTEM_READ_KEYS.values(),
    {
        "api_key",
        "llm_priority_primary",
        "llm_priority_secondary",
        "priority_llm_primary",
        "priority_llm_secondary",
    },
)

# The worker refuses to start with these, or skips the mailbox.
_SETUP_CODES = frozenset(
    {
        "missing_field",
        "example_section",
        "bad_section_name",
        "bad_port",
        "bad_ssl",
    }
)
_GENERIC_CARD_CODES = frozenset(
    {
        "example_section",
        "bad_section_name",
        "bad_port",
        "bad_ssl",
        "ssl_mismatch",
        "bad_host",
    }
)
_PROVIDER_CARD_STATES = frozenset(
    {
        "setup_incomplete",
        "login_rejected",
        "cant_reach",
        "paused",
        "connection_problem",
        "unsupported",
    }
)
_PROVIDER_CARD_ORDER = ("gmail", "yandex", "mailru", "outlook")

_STATE_SEVERITY = {
    "ok": "success",
    "delayed": "warn",
    "not_checking": "danger",
    "not_checked": "muted",
    "unknown": "muted",
    "setup_incomplete": "danger",
    "login_rejected": "danger",
    "cant_reach": "danger",
    "paused": "danger",
    "connection_problem": "warn",
    "unsupported": "danger",
}
_SEVERITY_RANK = {"muted": 0, "success": 1, "warn": 2, "danger": 3}
_FILE_SEVERITY = {
    FILE_NO_CONFIG_DIR: "muted",
    FILE_MISSING: "warn",
    FILE_NO_MAILBOXES: "warn",
    FILE_ENCODING: "danger",
    FILE_UNREADABLE: "danger",
}

_PROVIDER_HOSTS = (
    ("gmail", ("gmail.com", "googlemail.com")),
    ("yandex", ("yandex.ru", "yandex.com", "yandex.net", "ya.ru")),
    ("mailru", ("mail.ru",)),
    ("outlook", ("outlook.com", "office365.com", "hotmail.com", "live.com")),
)
_PROVIDER_LOGIN_DOMAINS = {
    "gmail.com": "gmail",
    "googlemail.com": "gmail",
    "yandex.ru": "yandex",
    "yandex.com": "yandex",
    "yandex.by": "yandex",
    "yandex.kz": "yandex",
    "ya.ru": "yandex",
    "mail.ru": "mailru",
    "inbox.ru": "mailru",
    "list.ru": "mailru",
    "bk.ru": "mailru",
    "internet.ru": "mailru",
    "outlook.com": "outlook",
    "hotmail.com": "outlook",
    "live.com": "outlook",
    "msn.com": "outlook",
}
# Telegram chat ids: a (negative for groups) number or a public @username.
_CHAT_ID_RE = re.compile(r"^(?:-?\d{1,20}|@[A-Za-z][A-Za-z0-9_]{3,31})$")
_HOST_RE = re.compile(r"^[\w-]+(?:\.[\w-]+)+\.?$")
_HOST_MAX_LENGTH = 253
# A login shown with its domain: one plain address and nothing else (no
# space, ";", "=", "\" ... that could join it to more text).
_ADDRESS_RE = re.compile(r"[\w.+'-]+@[\w-]+(?:\.[\w-]+)+")
_EMAIL_LIKE_RE = re.compile(r"[^\s@\[\]]+@[^\s@\[\]]+")
# From the first of these on, a section name may hold the rest of its line.
_SECTION_CUT_RE = re.compile(r"[\]=:;#]")
_SECTION_DISPLAY_LIMIT = 64

_LOGGED_KEYS: set[tuple[str, ...]] = set()
_LOGGED_KEYS_LIMIT = 256
_LOGGED_KEYS_LOCK = threading.Lock()


@dataclass(frozen=True)
class ConfigIssue:
    code: str
    section: str = ""
    field: str = ""
    blocking: bool = False


@dataclass(frozen=True)
class MailboxEntry:
    """One accounts.ini mailbox section; secrets reduced to booleans."""

    section: str
    login: str
    host: str
    port: int | None
    use_ssl: bool | None
    password_set: bool
    chat_status: str
    chat_source: str
    issues: tuple[ConfigIssue, ...]


@dataclass(frozen=True)
class AccountsFile:
    state: str
    path: Path | None = None
    line: int | None = None
    mailboxes: tuple[MailboxEntry, ...] = ()
    token_set: bool = False
    default_chat: str = "missing"
    issues: tuple[ConfigIssue, ...] = ()


@dataclass(frozen=True)
class RuntimeState:
    failure_bucket: str | None
    next_retry_at: datetime | None
    cooldown_until: datetime | None


def default_runtime_health_path() -> Path:
    """mailbot_v26/data/runtime_health.json, where start.main keeps IMAP retry state."""
    return Path(__file__).resolve().parents[1] / "data" / "runtime_health.json"


def mask_login(login: str) -> str:
    """'a…@gmail.com' for addresses, 'H…' for other logins, '' if unusable.

    The domain is shown only when the whole value is one address: text after
    it (an indented next line, "; password = …") may be a secret.
    """
    value = str(login or "").strip()
    if not value:
        return ""
    local, at, domain = value.partition("@")
    if at and (not local or not domain):
        return ""
    if at and _ADDRESS_RE.fullmatch(value):
        return f"{local[0]}…@{domain}"
    return f"{value[0]}…"


def _login_provider(login: str) -> str:
    domain = str(login or "").strip().lower().rpartition("@")[2]
    return _PROVIDER_LOGIN_DOMAINS.get(domain, "other")


def detect_provider(host: str, login: str = "") -> str:
    """gmail / yandex / mailru / outlook / other, from the IMAP host.

    The login's domain decides only while host is still empty or CHANGE_ME:
    a host of its own (a company server, a local bridge, a typo) is "other".
    """
    hostname = str(host or "").strip().lower().rstrip(".")
    if hostname and hostname != PLACEHOLDER.lower():
        for provider, domains in _PROVIDER_HOSTS:
            if any(
                hostname == domain or hostname.endswith(f".{domain}")
                for domain in domains
            ):
                return provider
        return "other"
    return _login_provider(login)


def _log_degraded(key: tuple[str, ...], message: str, *args: object) -> None:
    """WARNING the first time a read degrades, DEBUG afterwards (pages reload)."""
    with _LOGGED_KEYS_LOCK:
        first = key not in _LOGGED_KEYS
        if first and len(_LOGGED_KEYS) < _LOGGED_KEYS_LIMIT:
            _LOGGED_KEYS.add(key)
    logger.log(logging.WARNING if first else logging.DEBUG, message, *args)


def _looks_like_placeholder(value: str) -> bool:
    # Same rule as start._looks_like_placeholder (the startup preflight).
    token = str(value or "").strip()
    return not token or token == PLACEHOLDER


def _value(
    parser: configparser.ConfigParser, section: str, option: str
) -> tuple[str, bool]:
    """(value as the worker reads it, readable); a broken % gives ('', False)."""
    try:
        return str(parser.get(section, option, fallback="")).strip(), True
    except configparser.InterpolationError:
        return "", False


def _error_line(exc: configparser.Error) -> int | None:
    lineno = getattr(exc, "lineno", None)
    if isinstance(lineno, int):
        return lineno
    errors = getattr(exc, "errors", None)
    if errors and isinstance(errors[0], tuple) and isinstance(errors[0][0], int):
        return errors[0][0]
    return None


def _settings_present(config_dir: Path) -> bool:
    return (config_dir / "settings.ini").exists() or (config_dir / "config.ini").exists()


def _percent_issues(
    parser: configparser.ConfigParser, section: str, read_keys: frozenset[str]
) -> list[ConfigIssue]:
    issues: list[ConfigIssue] = []
    for option in parser.options(section):
        if _value(parser, section, option)[1]:
            continue
        # An unknown key is not named (it may be part of a value): one
        # unnamed hint covers all of them.
        issue = ConfigIssue(
            "percent_sign",
            section,
            option if option in _SHOWN_KEYS else "",
            blocking=option in read_keys,
        )
        if issue not in issues:
            issues.append(issue)
    return issues


def _is_setup_issue(issue: ConfigIssue) -> bool:
    """True when the worker skips this mailbox or refuses to start over it."""
    return issue.code in _SETUP_CODES or (
        issue.code == "percent_sign" and issue.blocking
    )


def _port(
    parser: configparser.ConfigParser, section: str
) -> tuple[int | None, ConfigIssue | None]:
    # load_accounts_config: getint("port", fallback=993) - only a missing key
    # falls back; a non-number skips the mailbox.
    if not parser.has_option(section, "port"):
        return 993, None
    value, readable = _value(parser, section, "port")
    if not readable:
        return None, None
    try:
        port = int(value)
    except ValueError:
        return None, ConfigIssue("bad_port", section, "port")
    if not 1 <= port <= 65535:
        return port, ConfigIssue("bad_port", section, "port")
    return port, None


def _use_ssl(
    parser: configparser.ConfigParser, section: str
) -> tuple[bool | None, ConfigIssue | None]:
    if not parser.has_option(section, "use_ssl"):
        return True, None
    value, readable = _value(parser, section, "use_ssl")
    if not readable:
        return None, None
    states = configparser.ConfigParser.BOOLEAN_STATES
    if value.lower() in states:
        return states[value.lower()], None
    return None, ConfigIssue("bad_ssl", section, "use_ssl")


def _host_looks_valid(host: str) -> bool:
    if len(host) > _HOST_MAX_LENGTH:
        return False
    if host.lower() == "localhost":
        return True
    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return bool(_HOST_RE.fullmatch(host))


def _shown_host(host: str) -> str:
    """The host if it is a server name, an IP address or CHANGE_ME, else ''.

    Other text may run on into another setting (an indented next line, inline
    text); the bad_host issue says why nothing is shown.
    """
    return host if host == PLACEHOLDER or _host_looks_valid(host) else ""


def _chat_id_status(raw: str) -> tuple[str, str]:
    """('set'|'invalid'|'missing', issue code or '') for one raw chat id value."""
    if not raw:
        return "missing", ""
    parsed = parse_telegram_chat_id(raw)
    if not parsed:
        # validate_telegram_contract refuses to start on this value.
        return "invalid", "chat_invalid"
    if not _CHAT_ID_RE.fullmatch(parsed):
        # The worker starts, but Telegram would reject the chat.
        return "invalid", "chat_format"
    return "set", ""


def _mailbox_chat(
    parser: configparser.ConfigParser,
    section: str,
    default_chat: str,
) -> tuple[str, str, list[ConfigIssue]]:
    raw, readable = _value(parser, section, "telegram_chat_id")
    if not readable:
        # Reported as percent_sign.
        return "invalid", "mailbox", []
    status, code = _chat_id_status(raw)
    if raw:
        issues = [
            ConfigIssue(
                code, section, "telegram_chat_id", blocking=code == "chat_invalid"
            )
        ] if code else []
        return status, "mailbox", issues
    if default_chat == "missing":
        return (
            "missing",
            "none",
            [ConfigIssue("chat_missing", section, "telegram_chat_id", blocking=True)],
        )
    # An invalid [telegram] chat_id is reported once, on the telegram section.
    return default_chat, "default", []


def _inspect_mailbox(
    parser: configparser.ConfigParser, section: str, default_chat: str
) -> MailboxEntry:
    login, _ = _value(parser, section, "login")
    host, _ = _value(parser, section, "host")
    raw_password = str(parser.get(section, "password", raw=True, fallback=""))
    password_set = not _looks_like_placeholder(raw_password)
    port, port_issue = _port(parser, section)
    use_ssl, ssl_issue = _use_ssl(parser, section)

    if section.strip().lower() == EXAMPLE_SECTION:
        # The preflight refuses the template section before any other check.
        return MailboxEntry(
            section=section,
            login=login,
            host=host,
            port=port,
            use_ssl=use_ssl,
            password_set=password_set,
            chat_status="missing",
            chat_source="none",
            issues=(ConfigIssue("example_section", section, blocking=True),),
        )

    issues: list[ConfigIssue] = []
    for field in ("login", "password", "host"):
        value, readable = _value(parser, section, field)
        if readable and _looks_like_placeholder(value):
            issues.append(ConfigIssue("missing_field", section, field, blocking=True))
    issues.extend(_percent_issues(parser, section, _MAILBOX_READ_KEYS))
    valid_name = bool(ACCOUNT_ID_PATTERN.fullmatch(section))
    if not valid_name:
        issues.append(ConfigIssue("bad_section_name", section))
    issues.extend(issue for issue in (port_issue, ssl_issue) if issue is not None)

    chat_status, chat_source, chat_issues = _mailbox_chat(parser, section, default_chat)
    # validate_telegram_contract only sees mailboxes load_accounts_config kept.
    loaded = (
        valid_name
        and parser.has_option(section, "login")
        and parser.has_option(section, "password")
        and port is not None
        and use_ssl is not None
    )
    if not loaded:
        chat_issues = [replace(issue, blocking=False) for issue in chat_issues]
    issues.extend(chat_issues)

    if port == 993 and use_ssl is False:
        issues.append(ConfigIssue("ssl_mismatch", section, "use_ssl"))
    if host and not _looks_like_placeholder(host) and not _host_looks_valid(host):
        issues.append(ConfigIssue("bad_host", section, "host"))
    return MailboxEntry(
        section=section,
        login=login,
        host=host,
        port=port,
        use_ssl=use_ssl,
        password_set=password_set,
        chat_status=chat_status,
        chat_source=chat_source,
        issues=tuple(issues),
    )


def _inspect(
    parser: configparser.ConfigParser, path: Path, file_issues: list[ConfigIssue]
) -> AccountsFile:
    system_issues: list[ConfigIssue] = []
    for section in parser.sections():
        # Same test as the startup preflight: lowercased, not stripped.
        if section.lower() in SYSTEM_SECTIONS:
            read_keys = _SYSTEM_READ_KEYS.get(section, frozenset())
            system_issues.extend(_percent_issues(parser, section, read_keys))

    token_raw = ""
    default_raw, default_readable = "", True
    if parser.has_section("telegram"):
        token_raw = str(parser.get("telegram", "bot_token", raw=True, fallback=""))
        default_raw, default_readable = _value(parser, "telegram", "chat_id")
    token = token_raw.strip()
    # validate_telegram_contract: empty or CHANGE_ME (any case) is no token.
    token_set = bool(token) and token.upper() != PLACEHOLDER
    if default_readable:
        default_chat, default_code = _chat_id_status(default_raw)
    else:
        # Reported as percent_sign on [telegram].
        default_chat, default_code = "invalid", ""
    if default_code:
        system_issues.append(
            ConfigIssue(
                default_code,
                "telegram",
                "chat_id",
                blocking=default_code == "chat_invalid",
            )
        )

    mailboxes = tuple(
        _inspect_mailbox(parser, section, default_chat)
        for section in parser.sections()
        if section.lower() not in SYSTEM_SECTIONS
    )
    real = [m for m in mailboxes if m.section.strip().lower() != EXAMPLE_SECTION]
    if not real:
        return AccountsFile(
            state=FILE_NO_MAILBOXES,
            path=path,
            mailboxes=mailboxes,
            token_set=token_set,
            default_chat=default_chat,
            issues=(ConfigIssue("no_mailboxes", blocking=True), *file_issues),
        )
    if not token_set:
        system_issues.append(
            ConfigIssue("token_missing", "telegram", "bot_token", blocking=True)
        )
    return AccountsFile(
        state=FILE_OK,
        path=path,
        mailboxes=mailboxes,
        token_set=token_set,
        default_chat=default_chat,
        issues=(*file_issues, *system_issues),
    )


def read_accounts_file(config_dir: Path | None) -> AccountsFile:
    """Parse accounts.ini like the worker does, without echoing any value."""
    if config_dir is None:
        return AccountsFile(state=FILE_NO_CONFIG_DIR)
    folder = Path(config_dir)
    path = folder / "accounts.ini"
    file_issues = (
        [] if _settings_present(folder) else [ConfigIssue("settings_missing", blocking=True)]
    )
    if not path.exists():
        return AccountsFile(
            state=FILE_MISSING,
            path=path,
            issues=(ConfigIssue("accounts_missing", blocking=True), *file_issues),
        )
    try:
        raw = path.read_bytes()
    except OSError as exc:
        _log_degraded(
            ("read", str(path)),
            "accounts_status_read_failed error_class=%s path=%s",
            type(exc).__name__,
            path,
        )
        return AccountsFile(
            state=FILE_UNREADABLE,
            path=path,
            issues=(ConfigIssue("unreadable", blocking=True), *file_issues),
        )
    try:
        # utf-8-sig and universal newlines, like the worker's read_text().
        content = raw.decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
    except UnicodeDecodeError:
        _log_degraded(("encoding", str(path)), "accounts_status_not_utf8 path=%s", path)
        return AccountsFile(
            state=FILE_ENCODING,
            path=path,
            issues=(ConfigIssue("encoding", blocking=True), *file_issues),
        )
    parser = configparser.ConfigParser()
    try:
        parser.read_string(content, source=path.name)
    except configparser.Error as exc:
        line = _error_line(exc)
        _log_degraded(
            ("parse", str(path), str(line)),
            "accounts_status_unparseable error_class=%s line=%s path=%s",
            type(exc).__name__,
            line,
            path,
        )
        return AccountsFile(
            state=FILE_UNREADABLE,
            path=path,
            line=line,
            issues=(ConfigIssue("unreadable", blocking=True), *file_issues),
        )
    return _inspect(parser, path, file_issues)


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_runtime_states(path: Path | None) -> tuple[dict[str, RuntimeState], str]:
    """Per-section retry state from runtime_health.json (error text is ignored)."""
    if path is None:
        return {}, EVIDENCE_SKIPPED
    try:
        raw_text = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}, EVIDENCE_MISSING
    except (OSError, UnicodeDecodeError) as exc:
        _log_degraded(
            ("runtime", str(path)),
            "accounts_status_runtime_unreadable error_class=%s path=%s",
            type(exc).__name__,
            path,
        )
        return {}, EVIDENCE_UNAVAILABLE
    try:
        payload = json.loads(raw_text)
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        _log_degraded(
            ("runtime_json", str(path)),
            "accounts_status_runtime_invalid path=%s",
            path,
        )
        return {}, EVIDENCE_UNAVAILABLE
    states: dict[str, RuntimeState] = {}
    for account_id, raw in payload.items():
        if not isinstance(account_id, str) or not isinstance(raw, dict):
            continue
        bucket = raw.get("failure_bucket")
        states[account_id] = RuntimeState(
            failure_bucket=bucket if isinstance(bucket, str) and bucket else None,
            next_retry_at=_parse_utc(raw.get("next_retry_at_utc")),
            cooldown_until=_parse_utc(raw.get("cooldown_until_utc")),
        )
    return states, EVIDENCE_OK


def _as_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def load_last_success(
    db_path: Path | None, logins: Iterable[str]
) -> tuple[dict[str, float], str]:
    """Newest successful IMAP check per normalized login, from a read-only connection.

    The worker records the login exactly as accounts.ini spells it; the
    normalized spelling is tried too (one indexed lookup each).
    """
    if db_path is None:
        return {}, EVIDENCE_SKIPPED
    path = Path(db_path)
    if not path.exists():
        return {}, EVIDENCE_MISSING
    variants: dict[str, set[str]] = {}
    for login in logins:
        key = normalize_login(login)
        if key:
            variants.setdefault(key, set()).update({str(login).strip(), key})
    if not variants:
        return {}, EVIDENCE_OK
    result: dict[str, float] = {}
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(path), uri=True)
        try:
            conn.execute("PRAGMA busy_timeout = 750")
            conn.execute("PRAGMA query_only = ON")
            for key, names in variants.items():
                for name in sorted(names):
                    row = conn.execute(
                        _LAST_SUCCESS_SQL,
                        (EventType.IMAP_HEALTH.value, name, *_SUCCESS_ENTITY_IDS),
                    ).fetchone()
                    ts = _as_float(row[0]) if row else None
                    if ts is not None and ts > result.get(key, float("-inf")):
                        result[key] = ts
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            # The worker has not created its tables yet: nothing checked so far.
            return {}, EVIDENCE_MISSING
        return {}, _db_unavailable(path, exc)
    except sqlite3.Error as exc:
        return {}, _db_unavailable(path, exc)
    return result, EVIDENCE_OK


def _db_unavailable(path: Path, exc: sqlite3.Error) -> str:
    _log_degraded(
        ("db", str(path)),
        "accounts_status_db_unavailable error_class=%s path=%s",
        type(exc).__name__,
        path,
    )
    return EVIDENCE_UNAVAILABLE


def _clock(moment: datetime, now: datetime, tz: tzinfo | None) -> str:
    """Machine-local 'HH:MM', with the date when it is not today."""
    local = moment.astimezone(tz) if tz else moment.astimezone()
    local_now = now.astimezone(tz) if tz else now.astimezone()
    if local.date() == local_now.date():
        return local.strftime("%H:%M")
    return local.strftime("%Y-%m-%d %H:%M")


def _age_text(last_ts: float | None, now: datetime, locale: str, db_evidence: str) -> str:
    if last_ts is None:
        key = "age.unknown" if db_evidence == EVIDENCE_UNAVAILABLE else "age.never"
        return text(key, locale)
    seconds = max(0.0, now.timestamp() - last_ts)
    minutes = int(seconds // 60)
    if minutes < 1:
        return text("age.just_now", locale)
    if minutes < 60:
        return text("age.minutes", locale, n=minutes)
    hours = minutes // 60
    if hours < 24:
        return text("age.hours", locale, n=hours)
    return text("age.days", locale, n=hours // 24)


def _mask_emails(value: str) -> str:
    return _EMAIL_LIKE_RE.sub(lambda match: mask_login(match.group(0)) or "…", value)


def _section_label(section: str) -> str:
    """Section name for display: e-mail-like parts masked, long names cut.

    configparser reads a header up to its line's last "]", so the name is
    shown only up to its first "]", "=", ":", ";" or "#" (the rest may be
    another setting written on the same line).
    """
    head = _SECTION_CUT_RE.split(section, maxsplit=1)[0]
    label = _mask_emails(head)
    if head == section and len(label) <= _SECTION_DISPLAY_LIMIT:
        return label
    return label[: _SECTION_DISPLAY_LIMIT - 1] + "…"


def _issue_text(issue: ConfigIssue, locale: str) -> str:
    key = f"issue.{issue.code}"
    if issue.code == "percent_sign" and not issue.field:
        key = "issue.percent_sign_unnamed"
    return text(
        key,
        locale,
        section=_section_label(issue.section),
        field=issue.field,
    )


def _issue_payload(issue: ConfigIssue, locale: str) -> dict[str, object]:
    return {
        "code": issue.code,
        "section": _section_label(issue.section),
        "field": issue.field,
        "blocking": issue.blocking,
        "text": _issue_text(issue, locale),
    }


def _mailbox_state(
    entry: MailboxEntry,
    provider: str,
    runtime: RuntimeState | None,
    last_ts: float | None,
    db_evidence: str,
    now: datetime,
) -> str:
    """First match wins: provider, config, login, network, cooldown, recency."""
    bucket = runtime.failure_bucket if runtime else None
    if provider == "outlook" and (last_ts is None or bucket):
        # Microsoft refuses password IMAP; a success without a later failure
        # (for example through a local bridge) outranks the guess from the host.
        return "unsupported"
    if any(_is_setup_issue(issue) for issue in entry.issues):
        return "setup_incomplete"
    if bucket == "imap_auth":
        return "login_rejected"
    if bucket == "imap_connectivity":
        return "cant_reach"
    if runtime and runtime.cooldown_until and runtime.cooldown_until > now:
        return "paused"
    if bucket in {"imap_network_timeout", "imap_server_unavailable"}:
        return "cant_reach"
    if bucket:
        return "connection_problem"
    if last_ts is not None:
        age = timedelta(seconds=max(0.0, now.timestamp() - last_ts))
        if age <= OK_MAX_AGE:
            return "ok"
        if age <= DELAYED_MAX_AGE:
            return "delayed"
        return "not_checking"
    if db_evidence == EVIDENCE_UNAVAILABLE:
        return "unknown"
    return "not_checked"


def _state_sentence(state: str, provider: str, age: str, locale: str) -> str:
    if state == "login_rejected":
        flavour = provider if provider in {"gmail", "yandex", "mailru"} else "other"
        return text(f"detail.login_rejected.{flavour}", locale)
    if state in {"delayed", "not_checking"}:
        return text(f"detail.{state}", locale, age=age)
    if state in {
        "cant_reach",
        "paused",
        "connection_problem",
        "unsupported",
        "not_checked",
        "unknown",
    }:
        return text(f"detail.{state}", locale)
    return ""


def _server_text(entry: MailboxEntry, locale: str) -> str:
    host = _shown_host(entry.host)
    if not host:
        return "—"
    server = host if entry.port is None else f"{host}:{entry.port}"
    if entry.use_ssl is None:
        return server
    ssl_label = text("server.ssl" if entry.use_ssl else "server.no_ssl", locale)
    return f"{server} · {ssl_label}"


def _chat_label(status: str, source: str, locale: str) -> str:
    if status == "set":
        return text("chat.set_default" if source == "default" else "chat.set_mailbox", locale)
    return text(f"chat.{status}", locale)


def _max_severity(*levels: str) -> str:
    return max(levels, key=lambda level: _SEVERITY_RANK[level])


def _mailbox_row(
    entry: MailboxEntry,
    *,
    last_success: Mapping[str, float],
    runtime_states: Mapping[str, RuntimeState],
    db_evidence: str,
    now: datetime,
    locale: str,
    tz: tzinfo | None,
) -> dict[str, object]:
    provider = detect_provider(entry.host, entry.login)
    login_key = normalize_login(entry.login)
    last_ts = last_success.get(login_key) if login_key else None
    runtime = runtime_states.get(entry.section)
    state = _mailbox_state(entry, provider, runtime, last_ts, db_evidence, now)
    age = _age_text(last_ts, now, locale, db_evidence)

    if state == "paused" and runtime and runtime.cooldown_until:
        state_label = text("state.paused", locale, time=_clock(runtime.cooldown_until, now, tz))
    else:
        state_label = text(f"state.{state}", locale)

    parts = [_state_sentence(state, provider, age, locale)]
    retry_at = runtime.next_retry_at if runtime else None
    if (
        state in {"login_rejected", "cant_reach", "connection_problem"}
        and retry_at is not None
        and retry_at > now
    ):
        parts.append(text("detail.next_try", locale, time=_clock(retry_at, now, tz)))

    severity = _STATE_SEVERITY[state]
    if entry.chat_status != "set" or any(
        not _is_setup_issue(issue) for issue in entry.issues
    ):
        severity = _max_severity(severity, "warn")

    last_success_at = (
        datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()
        if last_ts is not None
        else None
    )
    return {
        "id": _section_label(entry.section),
        "login": mask_login(entry.login),
        "provider": provider,
        "provider_label": text(f"provider.{provider}", locale),
        "host": _shown_host(entry.host),
        "port": entry.port,
        "use_ssl": entry.use_ssl,
        "server": _server_text(entry, locale),
        "password_set": entry.password_set,
        "last_success_at": last_success_at,
        "last_check": age,
        "last_check_title": (
            _clock(datetime.fromtimestamp(last_ts, tz=timezone.utc), now, tz)
            if last_ts is not None
            else ""
        ),
        "chat": {
            "status": entry.chat_status,
            "source": entry.chat_source,
            "label": _chat_label(entry.chat_status, entry.chat_source, locale),
        },
        "state": state,
        "state_label": state_label,
        "severity": severity,
        "detail": " ".join(part for part in parts if part),
        "issues": [_issue_payload(issue, locale) for issue in entry.issues],
        "next_retry_at": retry_at.isoformat() if retry_at is not None else None,
        "paused_until": (
            runtime.cooldown_until.isoformat()
            if state == "paused" and runtime and runtime.cooldown_until
            else None
        ),
    }


def _cards(
    accounts_file: AccountsFile,
    rows: list[dict[str, object]],
    locale: str,
) -> list[dict[str, object]]:
    state = accounts_file.state
    if state in {FILE_MISSING, FILE_NO_MAILBOXES}:
        return [fix_card("first_run", locale)]
    if state == FILE_ENCODING:
        return [fix_card("encoding", locale)]
    if state == FILE_UNREADABLE:
        return [fix_card("unreadable", locale, line=accounts_file.line)]
    if state != FILE_OK:
        return []
    codes = {issue.code for issue in accounts_file.issues}
    for entry in accounts_file.mailboxes:
        codes.update(issue.code for issue in entry.issues)
    problem_providers: set[str] = set()
    for entry, row in zip(accounts_file.mailboxes, rows):
        if row["state"] not in _PROVIDER_CARD_STATES:
            continue
        problem_providers.add(str(row["provider"]))
        if row["provider"] == "other":
            # Often a typo in a known provider's host: show that provider's server.
            problem_providers.add(_login_provider(entry.login))
    card_ids: list[str] = []
    if "percent_sign" in codes:
        card_ids.append("percent")
    card_ids.extend(p for p in _PROVIDER_CARD_ORDER if p in problem_providers)
    if "other" in problem_providers or codes & _GENERIC_CARD_CODES:
        card_ids.append("generic")
    return [fix_card(card_id, locale) for card_id in card_ids]


def _summary(
    file_state: str,
    rows: list[dict[str, object]],
    startup_blocked: bool,
    locale: str,
) -> dict[str, object]:
    severities = [str(row["severity"]) for row in rows]
    ok = severities.count("success")
    waiting = severities.count("muted")
    attention = len(severities) - ok - waiting
    if file_state != FILE_OK:
        label = text(f"summary.{file_state}", locale)
        severity = _FILE_SEVERITY[file_state]
    else:
        parts: list[str] = []
        if ok:
            parts.append(text("summary.ok", locale, n=ok))
        if attention:
            parts.append(plural_text("summary.attention", attention, locale))
        if waiting:
            parts.append(plural_text("summary.waiting", waiting, locale))
        label = " · ".join(parts)
        severity = _max_severity("muted", *severities)
        if startup_blocked:
            severity = "danger"
    return {
        "total": len(rows),
        "ok": ok,
        "attention": attention,
        "waiting": waiting,
        "severity": severity,
        "text": label,
    }


def _telegram(accounts_file: AccountsFile, locale: str) -> dict[str, object]:
    chat_problem = accounts_file.default_chat == "invalid" or any(
        entry.chat_status != "set" for entry in accounts_file.mailboxes
    )
    hints: list[str] = []
    if not accounts_file.token_set:
        hints.append(text("accounts.telegram.hint_token", locale))
    if chat_problem:
        hints.append(text("accounts.telegram.hint_chat", locale))
    if not accounts_file.token_set or accounts_file.default_chat == "invalid":
        severity = "danger"
    elif chat_problem:
        severity = "warn"
    else:
        severity = "success"
    return {
        "token_set": accounts_file.token_set,
        "token_label": text(
            "secret.set" if accounts_file.token_set else "secret.missing", locale
        ),
        "default_chat": {
            "status": accounts_file.default_chat,
            "label": text(f"chat.default.{accounts_file.default_chat}", locale),
        },
        "severity": severity,
        "hints": hints,
    }


def build_accounts_status(
    accounts_file: AccountsFile,
    *,
    last_success: Mapping[str, float],
    runtime_states: Mapping[str, RuntimeState],
    evidence: Mapping[str, str],
    now: datetime,
    locale: str,
    tz: tzinfo | None = None,
) -> dict[str, object]:
    """Pure: the JSON-ready status (rows, summary, blocking issues, fix cards)."""
    db_evidence = str(evidence.get("db", EVIDENCE_SKIPPED))
    rows: list[dict[str, object]] = []
    if accounts_file.state == FILE_OK:
        rows = [
            _mailbox_row(
                entry,
                last_success=last_success,
                runtime_states=runtime_states,
                db_evidence=db_evidence,
                now=now,
                locale=locale,
                tz=tz,
            )
            for entry in accounts_file.mailboxes
        ]
    page_issues = [issue for issue in accounts_file.issues if not issue.section]
    section_issues = [issue for issue in accounts_file.issues if issue.section]
    ordered = list(page_issues)
    if accounts_file.state == FILE_OK:
        for entry in accounts_file.mailboxes:
            ordered.extend(entry.issues)
        ordered.extend(section_issues)
    blocking = [
        {key: value for key, value in _issue_payload(issue, locale).items() if key != "blocking"}
        for issue in ordered
        if issue.blocking
    ]
    startup_blocked = bool(blocking)
    return {
        "generated_at": now.astimezone(timezone.utc).isoformat(),
        "generated_clock": _clock(now, now, tz),
        "locale": locale,
        "file": {
            "state": accounts_file.state,
            "path": str(accounts_file.path) if accounts_file.path else None,
            "line": accounts_file.line,
        },
        "evidence": {
            "db": db_evidence,
            "runtime": str(evidence.get("runtime", EVIDENCE_SKIPPED)),
        },
        "startup_blocked": startup_blocked,
        "blocking": blocking,
        "summary": _summary(accounts_file.state, rows, startup_blocked, locale),
        "telegram": _telegram(accounts_file, locale),
        "accounts": rows,
        "cards": _cards(accounts_file, rows, locale),
    }


def collect_accounts_status(
    *,
    config_dir: Path | None,
    db_path: Path | None,
    runtime_health_path: Path | None,
    locale: str = "en",
    now: datetime | None = None,
    tz: tzinfo | None = None,
) -> dict[str, object]:
    """Read the three sources (read-only) and build the status."""
    moment = now or datetime.now(timezone.utc)
    accounts_file = read_accounts_file(config_dir)
    if accounts_file.state != FILE_OK:
        return build_accounts_status(
            accounts_file,
            last_success={},
            runtime_states={},
            evidence={"db": EVIDENCE_SKIPPED, "runtime": EVIDENCE_SKIPPED},
            now=moment,
            locale=locale,
            tz=tz,
        )
    logins = [entry.login for entry in accounts_file.mailboxes if entry.login]
    last_success, db_evidence = load_last_success(db_path, logins)
    runtime_states, runtime_evidence = load_runtime_states(runtime_health_path)
    return build_accounts_status(
        accounts_file,
        last_success=last_success,
        runtime_states=runtime_states,
        evidence={"db": db_evidence, "runtime": runtime_evidence},
        now=moment,
        locale=locale,
        tz=tz,
    )


def build_home_card(status: Mapping[str, object], locale: str) -> dict[str, object]:
    """Small Home-page card: 'Mailboxes: 2 OK · 1 needs attention'."""
    summary = status.get("summary")
    summary = summary if isinstance(summary, Mapping) else {}
    telegram = status.get("telegram")
    telegram = telegram if isinstance(telegram, Mapping) else {}
    file_info = status.get("file")
    file_state = file_info.get("state") if isinstance(file_info, Mapping) else None
    note = ""
    if status.get("startup_blocked"):
        note = text("home.startup_blocked", locale)
    elif file_state == FILE_OK and not telegram.get("token_set"):
        note = text("home.token_missing", locale)
    return {
        "label": text("home.label", locale),
        "headline": str(summary.get("text") or text("home.unavailable", locale)),
        "severity": str(summary.get("severity") or "muted"),
        "note": note,
        "url": ACCOUNTS_URL,
        "link_text": text("home.link", locale),
    }


def unavailable_home_card(locale: str) -> dict[str, object]:
    """Card shown when the status could not be built (the host page still renders)."""
    return {
        "label": text("home.label", locale),
        "headline": text("home.unavailable", locale),
        "severity": "muted",
        "note": "",
        "url": ACCOUNTS_URL,
        "link_text": text("home.link", locale),
    }


__all__ = [
    "AccountsFile",
    "ConfigIssue",
    "MailboxEntry",
    "RuntimeState",
    "build_accounts_status",
    "build_home_card",
    "collect_accounts_status",
    "default_runtime_health_path",
    "detect_provider",
    "load_last_success",
    "load_runtime_states",
    "mask_login",
    "read_accounts_file",
    "unavailable_home_card",
]
