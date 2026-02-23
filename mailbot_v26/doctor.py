from __future__ import annotations

import configparser
import importlib
import ipaddress
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.deps import DependencyError, require_runtime_for
from mailbot_v26.config_loader import (
    ACCOUNT_ID_PATTERN,
    CONFIG_DIR,
    BotConfig,
    ConfigError,
    load_accounts_config,
    load_general_config,
    load_keys_config,
    load_storage_config,
)
from mailbot_v26.config_yaml import ConfigError as YamlConfigError
from mailbot_v26.config_yaml import build_bot_config
from mailbot_v26.config_yaml import load_config as load_yaml_config
from mailbot_v26.config_yaml import validate_config as validate_yaml_config
from mailbot_v26.health.mail_accounts import check_mail_accounts
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.llm import router as llm_router
from mailbot_v26.priority.priority_engine_v2 import load_priority_v2_config, load_vip_senders
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.version import __version__
from mailbot_v26.worker.telegram_sender import DeliveryResult, ping_telegram, send_telegram
from mailbot_v26.tools.networking import get_primary_ipv4


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


REQUIRED_IMPORTS = [
    "imapclient",
    "requests",
    "yaml",
    "openpyxl",
    "docx",
    "pdfminer",
    "pyttsx3",
    "telegram",
    "dotenv",
    "nltk",
    "langdetect",
    "flask",
    "ldap",
]

OPTIONAL_IMPORTS = [
    "numpy",
    "pandas",
    "xlrd",
]

DEPENDENCY_IMPORTS = REQUIRED_IMPORTS + OPTIONAL_IMPORTS

REQUIRED_TABLES = {
    "emails",
    "events_v1",
    "attachments",
    "commitments",
}

CRITICAL_COMPONENTS = {"SQLite", "Telegram", "IMAP"}

_STATUS_LABELS_RU = {
    "OK": "ОК",
    "WARN": "ПРЕДУПРЕЖДЕНИЕ",
    "FAIL": "ОШИБКА",
}


logger = logging.getLogger(__name__)


def run_doctor(config_dir: Path | None = None) -> DoctorReport:
    require_runtime_for("doctor")
    base_dir = config_dir or CONFIG_DIR
    entries, config_data = run_doctor_checks(config_dir=base_dir, return_config=True)

    report_text = _format_report(entries, base_dir)
    print(report_text)

    telegram_sent, telegram_error = _send_report_to_telegram(
        report_text,
        config_data.get("telegram_bot_token"),
        config_data.get("report_chat_id"),
    )

    if telegram_sent:
        print("[ОК] Отчёт доктора отправлен в Telegram")
    else:
        print(
            "[ПРЕДУПРЕЖДЕНИЕ] Отчёт доктора не отправлен в Telegram"
            + (f" ({telegram_error})" if telegram_error else "")
        )

    return DoctorReport(entries=entries, telegram_sent=telegram_sent, telegram_error=telegram_error)


def print_lan_url(config_dir: Path | None = None) -> int:
    raw, _config, errors = _load_doctor_bot_config(config_dir)
    if errors:
        for error in errors:
            print(error)
        return 2
    web_ui = raw.get("web_ui") if isinstance(raw.get("web_ui"), dict) else {}
    enabled = bool(web_ui.get("enabled", False))
    bind = str(web_ui.get("bind", "127.0.0.1")).strip() or "127.0.0.1"
    port = int(web_ui.get("port", 8080))
    if not enabled:
        print("Web UI disabled in config. Enable web_ui.enabled=true.")
        return 2

    if _is_loopback_bind(bind):
        print(f"Local only: http://127.0.0.1:{port}/")
        print("Not reachable from LAN. Set bind: '0.0.0.0' (or your LAN IP) to allow LAN access.")
        return 0

    if bind == "0.0.0.0":
        detected = get_primary_ipv4()
        if detected:
            print(f"Open from another device: http://{detected}:{port}/")
        else:
            print(f"Open from another device: http://<PC IPv4>:{port}/")
            print("Could not detect LAN IPv4 automatically. Run ipconfig and use IPv4 Address.")
        print("Firewall may block incoming connections; see docs.")
        return 0

    print(f"Open from another device: http://{bind}:{port}/")
    return 0


def run_doctor_checks(
    config_dir: Path | None = None,
    *,
    return_config: bool = False,
) -> list[DoctorEntry] | tuple[list[DoctorEntry], dict[str, object]]:
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

    if return_config:
        return entries, config_data
    return entries


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
        details = f"{details} (требуется >=3.10)"
    return DoctorEntry("Python", status, details)


def _check_venv() -> DoctorEntry:
    in_venv = sys.prefix != sys.base_prefix or bool(os.environ.get("VIRTUAL_ENV"))
    status = "OK" if in_venv else "WARN"
    details = "активирован" if in_venv else "не обнаружен"
    return DoctorEntry("Virtualenv", status, details)


def _check_dependencies() -> DoctorEntry:
    missing_required: list[str] = []
    missing_optional: list[str] = []
    for module in REQUIRED_IMPORTS:
        try:
            importlib.import_module(module)
        except Exception:
            missing_required.append(module)
    for module in OPTIONAL_IMPORTS:
        try:
            importlib.import_module(module)
        except Exception:
            missing_optional.append(module)
    if missing_required:
        details = f"отсутствуют: {', '.join(sorted(missing_required))}"
        if missing_optional:
            details = (
                f"{details}; опционально отсутствуют: {', '.join(sorted(missing_optional))}"
            )
        return DoctorEntry("Dependencies", "FAIL", details)
    if missing_optional:
        return DoctorEntry(
            "Dependencies",
            "WARN",
            f"опционально отсутствуют: {', '.join(sorted(missing_optional))}",
        )
    return DoctorEntry("Dependencies", "OK", "импорты успешны")


def _check_config_files(base_dir: Path) -> tuple[list[DoctorEntry], dict[str, object]]:
    entries: list[DoctorEntry] = []
    data: dict[str, object] = {"accounts": [], "account_configs": []}

    raw_config, bot_config, yaml_errors = _load_doctor_bot_config(base_dir)
    if yaml_errors:
        entries.append(DoctorEntry("config.yaml", "WARN", "; ".join(yaml_errors)))
    else:
        entries.append(DoctorEntry("config.yaml", "OK", "загружен"))
    data["raw_config"] = raw_config
    data["bot_config"] = bot_config

    if not (base_dir / "config.ini").exists():
        entries.append(DoctorEntry("config.ini", "WARN", _config_template_hint(base_dir / "config.ini")))

    data["priority_v2_config"] = load_priority_v2_config(base_dir)
    data["vip_senders"] = load_vip_senders(base_dir)

    try:
        general = load_general_config(base_dir)
        entries.append(DoctorEntry("config.ini (general)", "OK", "загружен"))
        data["admin_chat_id"] = general.admin_chat_id
    except ConfigError as exc:
        entries.append(DoctorEntry("config.ini", "FAIL", str(exc)))

    try:
        keys = load_keys_config(base_dir)
        entries.append(DoctorEntry("keys.ini", "OK", "загружен"))
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
    if not path.exists():
        return DoctorEntry("accounts.ini", "FAIL", f"Файл не найден: {path}"), []

    parser = read_user_ini_with_defaults(
        path,
        logger=logger,
        scope_label="doctor accounts.ini check",
    )
    accounts: list[dict[str, object]] = []
    issues: list[str] = []
    has_critical = False

    for section_name in parser.sections():
        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            issues.append(f"некорректный id секции: {section_name}")
            has_critical = True
            continue
        section = parser[section_name]
        account_issues: list[str] = []
        login = section.get("login", "").strip()
        password = section.get("password", "").strip()
        host = section.get("host", "").strip()
        chat_id = section.get("telegram_chat_id", "").strip()

        if not login:
            account_issues.append("нет login")
        if not password:
            account_issues.append("нет password")
        if not host:
            account_issues.append("нет host")

        try:
            port = section.getint("port", fallback=993)
            if not (1 <= port <= 65535):
                account_issues.append("порт вне диапазона")
        except ValueError:
            port = None
            account_issues.append("некорректный port")

        try:
            use_ssl = section.getboolean("use_ssl", fallback=True)
        except ValueError:
            use_ssl = None
            account_issues.append("некорректный use_ssl")

        if not chat_id:
            account_issues.append("нет telegram_chat_id")

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
        issues.append("учётные записи не заданы")
        has_critical = True

    if not issues:
        status = "OK"
    else:
        status = "FAIL" if has_critical else "WARN"
    details = "валидно" if not issues else "; ".join(issues)
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


def _resolve_yaml_config_path(config_dir: Path | None) -> Path | None:
    return resolve_config_paths(config_dir).yaml_path


def _config_template_hint(config_path: Path) -> str:
    example_path = config_path.with_name(f"{config_path.name}.example")
    compact_example_path = config_path.with_name("config.ini.compact.example")
    return (
        f"Файл не найден: {config_path}. Используйте шаблон {example_path} "
        f"или начните с компактного {compact_example_path}. "
        f"Скопировать (полный): copy {example_path.name} {config_path.name}. "
        f"Скопировать (compact): copy mailbot_v26\\config\\config.ini.compact.example "
        f"mailbot_v26\\config\\config.ini"
    )


def _build_default_bot_config() -> BotConfig:
    repo_root = Path(__file__).resolve().parents[1]
    return build_bot_config({}, repo_root=repo_root)


def _yaml_template_hint(config_dir: Path | None) -> str:
    if config_dir is not None and config_dir.is_file():
        config_path = config_dir
    else:
        base_dir = config_dir or Path(__file__).resolve().parent
        config_path = base_dir / "config.yaml"
    example_candidates = [
        config_path.with_name("config.example.yaml"),
        config_path.with_name("config.yaml.example"),
    ]
    example = next((item for item in example_candidates if item.exists()), example_candidates[0])
    return (
        f"Файл не найден: {config_path}. Используйте шаблон {example} "
        f"и скопируйте: copy {example.name} {config_path.name}"
    )


def _load_doctor_bot_config(config_dir: Path | None) -> tuple[dict[str, object], BotConfig, list[str]]:
    config_path = _resolve_yaml_config_path(config_dir)
    if config_path is None:
        message = _yaml_template_hint(config_dir)
        logger.warning(message)
        return {}, _build_default_bot_config(), [message]

    try:
        raw = load_yaml_config(config_path)
    except YamlConfigError as exc:
        message = str(exc)
        logger.warning(message)
        return {}, _build_default_bot_config(), [message]

    ok, error = validate_yaml_config(raw)
    if not ok:
        message = error or "Invalid config.yaml"
        logger.warning("doctor config.yaml invalid: %s; using defaults", message)
        return raw, _build_default_bot_config(), [message]
    return raw, build_bot_config(raw, repo_root=Path(__file__).resolve().parents[1]), []


def _is_loopback_bind(bind: str) -> bool:
    if not bind:
        return False
    if bind.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(bind).is_loopback
    except ValueError:
        return False


def _check_sqlite(db_path: object) -> DoctorEntry:
    if not db_path:
        return DoctorEntry("SQLite", "FAIL", "db_path не указан")
    path = Path(db_path)
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = {row[0] for row in cursor.fetchall()}
    except sqlite3.Error as exc:
        return DoctorEntry("SQLite", "FAIL", f"{path} ({exc})")

    missing = sorted(REQUIRED_TABLES - tables)
    if missing:
        return DoctorEntry("SQLite", "WARN", f"нет таблиц: {', '.join(missing)}")
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
        return [DoctorEntry("LLM", "FAIL", f"ошибка конфигурации: {exc}")]

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
        return DoctorEntry(name.capitalize(), "WARN", "отключен")
    if not has_credentials:
        return DoctorEntry(name.capitalize(), "FAIL", "нет учётных данных")
    if not provider:
        return DoctorEntry(name.capitalize(), "FAIL", "провайдер недоступен")
    ok = provider.healthcheck()
    status = "OK" if ok else "FAIL"
    details = "активен" if ok else "проверка не пройдена"
    return DoctorEntry(name.capitalize(), status, details)


def _check_imap(accounts: object, *, timeout_sec: float = 10.0) -> list[DoctorEntry]:
    if not accounts:
        return [DoctorEntry("IMAP", "FAIL", "нет настроенных аккаунтов")]

    results = check_mail_accounts(accounts, timeout_sec=timeout_sec)

    failures = [result for result in results if result.status != "OK"]
    if not failures:
        return [DoctorEntry("IMAP", "OK", f"{len(results)} аккаунтов в порядке")]

    if len(failures) == len(results):
        status = "FAIL"
    else:
        status = "WARN"

    details = "; ".join(
        f"{result.account_id}: {result.error or 'ошибка'}" for result in failures
    )
    return [DoctorEntry("IMAP", status, details)]


def _format_report(entries: list[DoctorEntry], base_dir: Path) -> str:
    lines = [
        "=== ОТЧЁТ ДОКТОРА LETTERBOT ===",
        f"Версия: {__version__}",
        f"Каталог конфигурации: {base_dir}",
        "",
    ]
    for entry in entries:
        detail = f" ({entry.details})" if entry.details else ""
        status_display = _STATUS_LABELS_RU.get(entry.status, entry.status)
        lines.append(f"- {entry.component}: {status_display}{detail}")
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
    try:
        run_doctor()
    except DependencyError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)


__all__ = [
    "DoctorEntry",
    "DoctorReport",
    "has_critical_issues",
    "report_exit_code",
    "run_doctor",
    "run_doctor_checks",
    "main",
    "print_lan_url",
]
