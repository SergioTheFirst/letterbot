from __future__ import annotations

import configparser
import importlib
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config_loader import (
    ACCOUNT_ID_PATTERN,
    CONFIG_DIR,
    ConfigError,
    load_accounts_config,
    load_general_config,
    load_keys_config,
    load_storage_config,
)
from mailbot_v26.health.mail_accounts import check_mail_accounts
from mailbot_v26.llm import router as llm_router
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.version import __version__
from mailbot_v26.worker.telegram_sender import DeliveryResult, ping_telegram, send_telegram


@dataclass(frozen=True)
class DoctorEntry:
    component: str
    status: str
    details: str


@dataclass(frozen=True)
class DoctorReport:
    entries: list[DoctorEntry]
    telegram_sent: bool
    telegram_error: str | None


DEPENDENCY_IMPORTS = [
    "imapclient",
    "requests",
    "yaml",
    "numpy",
    "pandas",
    "openpyxl",
    "docx",
    "xlrd",
    "pdfminer",
    "pyttsx3",
    "telegram",
    "dotenv",
    "nltk",
    "langdetect",
    "flask",
    "ldap",
]

REQUIRED_TABLES = {
    "emails",
    "events_v1",
    "attachments",
    "commitments",
}

CRITICAL_COMPONENTS = {"SQLite", "Telegram", "IMAP"}


def run_doctor(config_dir: Path | None = None) -> DoctorReport:
    base_dir = config_dir or CONFIG_DIR
    entries: list[DoctorEntry] = []

    entries.append(_check_python())
    entries.append(_check_venv())
    entries.append(_check_dependencies())

    config_entries, config_data = _check_config_files(base_dir)
    entries.extend(config_entries)

    storage_entry = _check_sqlite(config_data.get("db_path"))
    entries.append(storage_entry)

    telegram_entry = _check_telegram(config_data.get("telegram_bot_token"))
    entries.append(telegram_entry)

    entries.extend(_check_llm(base_dir))

    entries.extend(_check_imap(config_data.get("account_configs")))

    report_text = _format_report(entries, base_dir)
    print(report_text)

    telegram_sent, telegram_error = _send_report_to_telegram(
        report_text,
        config_data.get("telegram_bot_token"),
        config_data.get("report_chat_id"),
    )

    if telegram_sent:
        print("[OK] Doctor report sent to Telegram")
    else:
        print(
            "[WARN] Doctor report was not sent to Telegram"
            + (f" ({telegram_error})" if telegram_error else "")
        )

    return DoctorReport(entries=entries, telegram_sent=telegram_sent, telegram_error=telegram_error)


def has_critical_issues(report: DoctorReport) -> bool:
    for entry in report.entries:
        if entry.component in CRITICAL_COMPONENTS and entry.status == "FAIL":
            return True
    return False


def report_exit_code(report: DoctorReport) -> int:
    return 2 if has_critical_issues(report) else 0


def _check_python() -> DoctorEntry:
    version = sys.version_info
    ok = version >= (3, 10)
    status = "OK" if ok else "FAIL"
    details = f"{version.major}.{version.minor}.{version.micro}"
    if not ok:
        details = f"{details} (requires >=3.10)"
    return DoctorEntry("Python", status, details)


def _check_venv() -> DoctorEntry:
    in_venv = sys.prefix != sys.base_prefix or bool(os.environ.get("VIRTUAL_ENV"))
    status = "OK" if in_venv else "WARN"
    details = "active" if in_venv else "not detected"
    return DoctorEntry("Virtualenv", status, details)


def _check_dependencies() -> DoctorEntry:
    missing: list[str] = []
    for module in DEPENDENCY_IMPORTS:
        try:
            importlib.import_module(module)
        except Exception:
            missing.append(module)
    if missing:
        return DoctorEntry("Dependencies", "FAIL", f"missing: {', '.join(sorted(missing))}")
    return DoctorEntry("Dependencies", "OK", "all imports succeeded")


def _check_config_files(base_dir: Path) -> tuple[list[DoctorEntry], dict[str, object]]:
    entries: list[DoctorEntry] = []
    data: dict[str, object] = {"accounts": [], "account_configs": []}

    try:
        general = load_general_config(base_dir)
        entries.append(DoctorEntry("config.ini", "OK", "loaded"))
        data["admin_chat_id"] = general.admin_chat_id
    except ConfigError as exc:
        entries.append(DoctorEntry("config.ini", "FAIL", str(exc)))

    try:
        keys = load_keys_config(base_dir)
        entries.append(DoctorEntry("keys.ini", "OK", "loaded"))
        data["telegram_bot_token"] = keys.telegram_bot_token
    except ConfigError as exc:
        entries.append(DoctorEntry("keys.ini", "FAIL", str(exc)))

    try:
        storage = load_storage_config(base_dir)
        data["db_path"] = storage.db_path
    except ConfigError as exc:
        entries.append(DoctorEntry("storage", "FAIL", str(exc)))

    accounts_entry, accounts = _validate_accounts_ini(base_dir)
    entries.append(accounts_entry)
    data["accounts"] = accounts
    try:
        data["account_configs"] = load_accounts_config(base_dir)
    except ConfigError as exc:
        entries.append(DoctorEntry("accounts.ini (strict)", "FAIL", str(exc)))

    report_chat_id = _resolve_report_chat_id(data)
    data["report_chat_id"] = report_chat_id

    return entries, data


def _validate_accounts_ini(base_dir: Path) -> tuple[DoctorEntry, list[dict[str, object]]]:
    path = base_dir / "accounts.ini"
    parser = configparser.ConfigParser()
    if not path.exists():
        return DoctorEntry("accounts.ini", "FAIL", f"Config file not found: {path}"), []

    parser.read(path, encoding="utf-8")
    accounts: list[dict[str, object]] = []
    issues: list[str] = []
    has_critical = False

    for section_name in parser.sections():
        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            issues.append(f"invalid section id: {section_name}")
            has_critical = True
            continue
        section = parser[section_name]
        account_issues: list[str] = []
        login = section.get("login", "").strip()
        password = section.get("password", "").strip()
        host = section.get("host", "").strip()
        chat_id = section.get("telegram_chat_id", "").strip()

        if not login:
            account_issues.append("missing login")
        if not password:
            account_issues.append("missing password")
        if not host:
            account_issues.append("missing host")

        try:
            port = section.getint("port", fallback=993)
            if not (1 <= port <= 65535):
                account_issues.append("port out of range")
        except ValueError:
            port = None
            account_issues.append("invalid port")

        try:
            use_ssl = section.getboolean("use_ssl", fallback=True)
        except ValueError:
            use_ssl = None
            account_issues.append("invalid use_ssl")

        if not chat_id:
            account_issues.append("missing telegram_chat_id")

        if any(
            issue in account_issues
            for issue in (
                "missing login",
                "missing password",
                "missing host",
                "invalid port",
                "invalid use_ssl",
            )
        ):
            has_critical = True

        if account_issues:
            issues.append(f"{section_name}: {', '.join(account_issues)}")

        accounts.append(
            {
                "account_id": section_name,
                "login": login,
                "password": password,
                "host": host,
                "port": port,
                "use_ssl": use_ssl,
                "telegram_chat_id": chat_id,
            }
        )

    if not accounts:
        issues.append("no accounts defined")
        has_critical = True

    if not issues:
        status = "OK"
    else:
        status = "FAIL" if has_critical else "WARN"
    details = "valid" if not issues else "; ".join(issues)
    return DoctorEntry("accounts.ini", status, details), accounts


def _resolve_report_chat_id(data: dict[str, object]) -> str | None:
    admin_chat_id = str(data.get("admin_chat_id") or "").strip()
    if admin_chat_id:
        return admin_chat_id
    accounts = data.get("accounts") or []
    for account in accounts:
        chat_id = str(account.get("telegram_chat_id") or "").strip()
        if chat_id:
            return chat_id
    return None


def _check_sqlite(db_path: object) -> DoctorEntry:
    if not db_path:
        return DoctorEntry("SQLite", "FAIL", "db_path not configured")
    path = Path(db_path)
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = {row[0] for row in cursor.fetchall()}
    except sqlite3.Error as exc:
        return DoctorEntry("SQLite", "FAIL", f"{path} ({exc})")

    missing = sorted(REQUIRED_TABLES - tables)
    if missing:
        return DoctorEntry("SQLite", "WARN", f"missing tables: {', '.join(missing)}")
    return DoctorEntry("SQLite", "OK", str(path))


def _check_telegram(bot_token: object) -> DoctorEntry:
    token = str(bot_token or "")
    ok, details = ping_telegram(token)
    status = "OK" if ok else "FAIL"
    return DoctorEntry("Telegram", status, details)


def _check_llm(base_dir: Path) -> list[DoctorEntry]:
    entries: list[DoctorEntry] = []
    try:
        config = llm_router._load_llm_config(base_dir)
        router = llm_router.LLMRouter(config)
    except Exception as exc:
        return [DoctorEntry("LLM", "FAIL", f"config error: {exc}")]

    entries.append(_check_provider(router, "gigachat", config.gigachat_enabled, config.gigachat_api_key))
    entries.append(
        _check_provider(
            router,
            "cloudflare",
            config.cloudflare_enabled,
            config.cloudflare_account_id and config.cloudflare_api_key,
        )
    )
    return entries


def _check_provider(
    router: llm_router.LLMRouter,
    name: str,
    enabled_flag: bool,
    has_credentials: object,
) -> DoctorEntry:
    provider = router._providers.get(name)
    if not enabled_flag and not has_credentials:
        return DoctorEntry(name.capitalize(), "WARN", "disabled")
    if not has_credentials:
        return DoctorEntry(name.capitalize(), "FAIL", "missing credentials")
    if not provider:
        return DoctorEntry(name.capitalize(), "FAIL", "provider unavailable")
    ok = provider.healthcheck()
    status = "OK" if ok else "FAIL"
    details = "active" if ok else "healthcheck failed"
    return DoctorEntry(name.capitalize(), status, details)


def _check_imap(accounts: object, *, timeout_sec: float = 10.0) -> list[DoctorEntry]:
    if not accounts:
        return [DoctorEntry("IMAP", "FAIL", "no accounts configured")]

    results = check_mail_accounts(accounts, timeout_sec=timeout_sec)

    failures = [result for result in results if result.status != "OK"]
    if not failures:
        return [DoctorEntry("IMAP", "OK", f"{len(results)} account(s) OK")]

    if len(failures) == len(results):
        status = "FAIL"
    else:
        status = "WARN"

    details = "; ".join(
        f"{result.account_id}: {result.error or 'failed'}" for result in failures
    )
    return [DoctorEntry("IMAP", status, details)]


def _format_report(entries: list[DoctorEntry], base_dir: Path) -> str:
    lines = [
        "=== MAILBOT DOCTOR REPORT ===",
        f"Version: {__version__}",
        f"Config dir: {base_dir}",
        "",
    ]
    for entry in entries:
        detail = f" ({entry.details})" if entry.details else ""
        lines.append(f"- {entry.component}: {entry.status}{detail}")
    return "\n".join(lines)


def _send_report_to_telegram(
    report: str,
    bot_token: object,
    chat_id: object,
) -> tuple[bool, str | None]:
    token = str(bot_token or "")
    chat = str(chat_id or "")
    if not token or not chat:
        return False, "missing token or chat id"
    payload = TelegramPayload(
        html_text=telegram_safe(report),
        priority="🔵",
        metadata={"bot_token": token, "chat_id": chat},
    )
    try:
        result: DeliveryResult = send_telegram(payload)
        return result.delivered, result.error
    except Exception as exc:
        return False, str(exc)


def main() -> None:
    run_doctor()


__all__ = [
    "DoctorEntry",
    "DoctorReport",
    "has_critical_issues",
    "report_exit_code",
    "run_doctor",
    "main",
]
