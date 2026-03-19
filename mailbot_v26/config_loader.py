"""Configuration loading utilities for LetterBot.ru v26."""

from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass, field
from functools import lru_cache
import re
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.account_identity import normalize_login

# Canonical project support URLs - immutable, not overridable by config.
_PROJECT_SUPPORT_URL = "https://boosty.to/personalbot/donate?qr=true"
_PROJECT_SUPPORT_URL_ALT = "https://pay.cloudtips.ru/p/00d77c6a"
_PROJECT_HOMEPAGE = "https://letterbot.ru"

CONFIG_DIR = resolve_config_paths().config_dir
SETTINGS_INI_NAME = "settings.ini"


@dataclass
class GeneralConfig:
    """Top-level bot settings."""

    check_interval: int
    max_email_mb: int
    max_attachment_mb: int
    max_zip_uncompressed_mb: int
    max_extracted_chars: int
    max_extracted_total_chars: int
    admin_chat_id: str


@dataclass
class AccountConfig:
    """Configuration for a single IMAP account."""

    account_id: str
    login: str
    password: str
    host: str
    port: int
    use_ssl: bool
    telegram_chat_id: str
    username: str = ""
    name: str = ""
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class AccountScope:
    chat_id: str
    account_emails: list[str]


@dataclass
class KeysConfig:
    """External service tokens."""

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    cf_account_id: str = ""
    cf_api_token: str = ""


@dataclass
class IngestConfig:
    """Mail ingest policies."""

    allow_prestart_emails: bool = False
    first_run_bootstrap_hours: int = 24
    first_run_bootstrap_max_messages: int = 20


@dataclass
class MaintenanceConfig:
    """Maintenance mode settings."""

    maintenance_mode: bool


@dataclass
class WebConfig:
    """Web UI bind settings."""

    host: str
    port: int


@dataclass
class TelegramUIConfig:
    """Telegram UX toggles from settings.ini."""

    show_decision_trace: bool


@dataclass
class BrandingConfig:
    """Telegram branding toggles from settings.ini."""

    show_watermark: bool = True


@dataclass
class SupportSettings:
    """Telegram support banner settings."""

    enabled: bool
    text: str
    url: str
    label: str
    frequency_days: int


@dataclass
class BotConfig:
    """Aggregate configuration bundle."""

    general: GeneralConfig
    accounts: List[AccountConfig]
    keys: KeysConfig
    storage: "StorageConfig"
    ingest: IngestConfig = field(
        default_factory=lambda: IngestConfig(allow_prestart_emails=False)
    )
    maintenance: MaintenanceConfig = field(
        default_factory=lambda: MaintenanceConfig(maintenance_mode=False)
    )
    web: WebConfig = field(
        default_factory=lambda: WebConfig(host="127.0.0.1", port=8787)
    )
    llm_call: Optional[Callable[[str], str]] = None


@dataclass
class StorageConfig:
    """Database connection settings."""

    db_path: Path


class ConfigError(Exception):
    """Raised when configuration files are missing or invalid."""


class InvalidAccountIdError(ConfigError):
    """Raised when account IDs do not match the allowed pattern."""

    def __init__(self, invalid_ids: list[str]) -> None:
        super().__init__(
            f"Invalid account_id(s) in accounts.ini: {', '.join(invalid_ids)}"
        )
        self.invalid_ids = invalid_ids


ACCOUNT_ID_PATTERN = re.compile(r"^[a-z0-9_]+$")
_LOGGER = logging.getLogger(__name__)


def _read_config_file(path: Path) -> configparser.ConfigParser:
    parser = read_user_ini_with_defaults(
        path,
        logger=_LOGGER,
        scope_label=f"{path.name} settings",
    )
    return parser


def _resolve_settings_path(base_dir: Path) -> Path:
    resolved = resolve_config_paths(base_dir)
    if resolved.two_file_mode:
        return resolved.settings_path
    settings_path = resolved.settings_path
    if settings_path.exists():
        return settings_path
    return resolved.legacy_ini_path


def _read_settings_with_legacy_fallback(
    base_dir: Path,
    *,
    section: str | None = None,
) -> configparser.ConfigParser:
    resolved = resolve_config_paths(base_dir)
    parser = _read_config_file(resolved.settings_path)
    if section is None or section in parser:
        return parser
    if resolved.legacy_ini_path.exists():
        legacy_parser = _read_config_file(resolved.legacy_ini_path)
        if section in legacy_parser:
            _LOGGER.info(
                "settings.ini missing [%s], using legacy config.ini fallback", section
            )
            return legacy_parser
    return parser


def load_general_config(base_dir: Path = CONFIG_DIR) -> GeneralConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="general")

    if "general" not in parser:
        return GeneralConfig(
            check_interval=120,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
            admin_chat_id="",
        )

    section = parser["general"]
    try:
        return GeneralConfig(
            check_interval=section.getint("check_interval", fallback=120),
            max_email_mb=section.getint("max_email_mb", fallback=15),
            max_attachment_mb=section.getint("max_attachment_mb", fallback=15),
            max_zip_uncompressed_mb=section.getint(
                "max_zip_uncompressed_mb",
                fallback=80,
            ),
            max_extracted_chars=section.getint(
                "max_extracted_chars",
                fallback=50_000,
            ),
            max_extracted_total_chars=section.getint(
                "max_extracted_total_chars",
                fallback=120_000,
            ),
            admin_chat_id=section.get("admin_chat_id", fallback=""),
        )
    except ValueError as exc:  # invalid numbers
        _LOGGER.warning("Invalid [general] values, using defaults: %s", exc)
        return GeneralConfig(
            check_interval=120,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
            admin_chat_id="",
        )


def load_accounts_config(base_dir: Path = CONFIG_DIR) -> List[AccountConfig]:
    parser = _read_config_file(base_dir / "accounts.ini")
    telegram_section = parser["telegram"] if "telegram" in parser else {}
    global_chat_id = str(telegram_section.get("chat_id", "")).strip()
    accounts: List[AccountConfig] = []
    invalid_ids: list[str] = []
    for section_name in parser.sections():
        if section_name in {"telegram", "cloudflare", "gigachat", "llm"}:
            continue
        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            invalid_ids.append(section_name)
            continue
        section = parser[section_name]
        try:
            account = AccountConfig(
                account_id=section_name,
                login=section["login"],
                username=section.get("username", section["login"]),
                name=section.get("name", section_name),
                password=section["password"],
                host=section.get("host", ""),
                port=section.getint("port", fallback=993),
                use_ssl=section.getboolean("use_ssl", fallback=True),
                telegram_chat_id=(
                    section.get("telegram_chat_id", fallback="") or global_chat_id
                ).strip(),
            )
        except KeyError as exc:
            _LOGGER.warning(
                "Skipping account [%s]: missing required field %s", section_name, exc
            )
            continue
        except ValueError as exc:
            _LOGGER.warning(
                "Skipping account [%s]: invalid field (%s)", section_name, exc
            )
            continue
        accounts.append(account)

    if invalid_ids:
        _LOGGER.warning("Ignoring invalid account_id(s): %s", ", ".join(invalid_ids))

    return accounts


@lru_cache(maxsize=8)
def _load_account_scopes(base_dir: str) -> dict[str, AccountScope]:
    scopes: dict[str, AccountScope] = {}
    try:
        accounts = load_accounts_config(Path(base_dir))
    except ConfigError:
        return scopes
    chat_groups: dict[str, list[str]] = {}
    for account in accounts:
        if not account.telegram_chat_id:
            continue
        chat_groups.setdefault(account.telegram_chat_id, []).append(account.login)
    for account in accounts:
        if not account.telegram_chat_id:
            continue
        scopes[normalize_login(account.login)] = AccountScope(
            chat_id=account.telegram_chat_id,
            account_emails=chat_groups.get(account.telegram_chat_id, []),
        )
    return scopes


def resolve_account_scope(
    account_email: str,
    base_dir: Path | None = None,
) -> AccountScope | None:
    if not account_email:
        return None
    config_dir = base_dir or CONFIG_DIR
    scopes = _load_account_scopes(str(config_dir))
    return scopes.get(normalize_login(account_email))


def _normalize_account_emails(account_emails: Iterable[str] | None) -> list[str]:
    if not account_emails:
        return []
    cleaned = {
        str(email).strip() for email in account_emails if str(email or "").strip()
    }
    return sorted(cleaned)


def get_account_scope(
    *,
    chat_id: str | None,
    account_email: str | None = None,
    account_emails: Iterable[str] | None = None,
) -> dict[str, object]:
    if not chat_id or not str(chat_id).strip():
        return {}
    scope: dict[str, object] = {"chat_scope": f"tg:{chat_id}"}
    if account_email:
        scope["account_email"] = account_email
    normalized_emails = _normalize_account_emails(account_emails)
    if normalized_emails:
        scope["account_emails"] = normalized_emails
    return scope


def load_keys_config(base_dir: Path = CONFIG_DIR) -> KeysConfig:
    resolved = resolve_config_paths(base_dir)
    accounts_parser = _read_config_file(resolved.accounts_path)
    parser = accounts_parser
    if not resolved.two_file_mode and not any(
        section in accounts_parser for section in ("telegram", "cloudflare")
    ):
        keys_path = resolved.keys_path
        if keys_path.exists():
            _LOGGER.info("keys.ini legacy fallback is used")
            parser = _read_config_file(keys_path)

    telegram = parser["telegram"] if "telegram" in parser else {}
    cloudflare = parser["cloudflare"] if "cloudflare" in parser else {}
    return KeysConfig(
        telegram_bot_token=str(telegram.get("bot_token", "")),
        telegram_chat_id=str(telegram.get("chat_id", "")).strip(),
        cf_account_id=str(cloudflare.get("account_id", "")),
        cf_api_token=str(cloudflare.get("api_token", "")),
    )


def parse_telegram_chat_id(raw_chat_id: object) -> str:
    value = str(raw_chat_id or "").strip()
    if not value or value.upper() == "CHANGE_ME" or value.startswith("="):
        return ""
    if value.startswith("+") and len(value) > 1:
        value = value[1:]
    return value


def _telegram_chat_id_format_hint() -> str:
    return "use a numeric chat id like 272250747 or -1001234567890 without '='"


def validate_telegram_contract(config: BotConfig, *, config_dir: Path) -> list[str]:
    errors: list[str] = []
    token = str(config.keys.telegram_bot_token or "").strip()

    global_chat_id = parse_telegram_chat_id(config.keys.telegram_chat_id)
    if str(config.keys.telegram_chat_id or "").strip() and not global_chat_id:
        raw_global_chat_id = str(config.keys.telegram_chat_id or "").strip()
        errors.append(
            f"Invalid [telegram].chat_id in {config_dir / 'accounts.ini'} "
            f"(got '{raw_global_chat_id}'; {_telegram_chat_id_format_hint()})."
        )

    telegram_targets = [
        account
        for account in config.accounts
        if account.enabled
        and (
            str(account.telegram_chat_id or "").strip()
            or str(config.keys.telegram_chat_id or "").strip()
        )
    ]
    if telegram_targets and (not token or token.upper() == "CHANGE_ME"):
        errors.append(
            f"Missing/invalid [telegram].bot_token in {config_dir / 'accounts.ini'} (set a non-empty token)."
        )

    for account in config.accounts:
        account_chat_raw = str(account.telegram_chat_id or "").strip()
        account_chat_id = parse_telegram_chat_id(account_chat_raw)
        if account_chat_raw and not account_chat_id:
            errors.append(
                f"Invalid [{account.account_id}].telegram_chat_id in {config_dir / 'accounts.ini'} "
                f"(got '{account_chat_raw}'; {_telegram_chat_id_format_hint()})."
            )
        resolved_chat_id = account_chat_id or global_chat_id
        if not account.enabled:
            account.telegram_chat_id = resolved_chat_id
            continue
        if not resolved_chat_id:
            errors.append(
                "Missing Telegram chat_id for account "
                f"[{account.account_id}] in {config_dir / 'accounts.ini'} "
                "(set account.telegram_chat_id or [telegram].chat_id)."
            )
            continue
        account.telegram_chat_id = resolved_chat_id
    return errors


def load_storage_config(base_dir: Path = CONFIG_DIR) -> StorageConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="storage")
    default_path = Path(__file__).resolve().parents[1] / "data" / "mailbot.sqlite"
    db_path_raw = parser.get("storage", "db_path", fallback=str(default_path))
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        project_root = Path(__file__).resolve().parents[1]
        db_path = (project_root / db_path).resolve()
    return StorageConfig(db_path=db_path)


def load_ingest_config(base_dir: Path = CONFIG_DIR) -> IngestConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="ingest")
    if "ingest" not in parser:
        return IngestConfig()
    section = parser["ingest"]
    try:
        hours = section.getint("first_run_bootstrap_hours", fallback=24)
        max_messages = section.getint("first_run_bootstrap_max_messages", fallback=20)
        return IngestConfig(
            allow_prestart_emails=section.getboolean(
                "allow_prestart_emails",
                fallback=False,
            ),
            first_run_bootstrap_hours=max(0, hours),
            first_run_bootstrap_max_messages=max(0, max_messages),
        )
    except ValueError as exc:
        _LOGGER.warning("Invalid [ingest] config, using defaults: %s", exc)
        return IngestConfig()


def load_maintenance_config(base_dir: Path = CONFIG_DIR) -> MaintenanceConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="maintenance")
    if "maintenance" not in parser:
        return MaintenanceConfig(maintenance_mode=False)
    section = parser["maintenance"]
    try:
        return MaintenanceConfig(
            maintenance_mode=section.getboolean(
                "maintenance_mode",
                fallback=False,
            )
        )
    except ValueError as exc:
        _LOGGER.warning("Invalid [maintenance] config, using defaults: %s", exc)
        return MaintenanceConfig(maintenance_mode=False)


def _support_defaults() -> SupportSettings:
    return SupportSettings(
        enabled=False,
        text="Если LetterBot.ru помогает, проект можно поддержать",
        url=_PROJECT_SUPPORT_URL,
        label="Поддержать LetterBot.ru",
        frequency_days=30,
    )


def _normalize_support_frequency(raw: object, *, default: int = 30) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(default, value)


def _load_support_from_ini(parser: configparser.ConfigParser) -> SupportSettings:
    defaults = _support_defaults()
    if "support" not in parser:
        return defaults
    section = parser["support"]
    try:
        enabled = section.getboolean("enabled", fallback=defaults.enabled)
    except ValueError:
        enabled = defaults.enabled
    text = str(section.get("text", fallback=defaults.text)).strip() or defaults.text
    url = _PROJECT_SUPPORT_URL  # Immutable: always points to official project support.
    label = str(section.get("label", fallback=defaults.label)).strip() or defaults.label
    frequency_days = _normalize_support_frequency(
        section.get("frequency_days", fallback=str(defaults.frequency_days)),
        default=defaults.frequency_days,
    )
    return SupportSettings(
        enabled=enabled,
        text=text,
        url=url,
        label=label,
        frequency_days=frequency_days,
    )


def _load_support_from_yaml(raw: Any) -> SupportSettings:
    defaults = _support_defaults()
    if not isinstance(raw, dict):
        return defaults
    support = raw.get("support")
    if not isinstance(support, dict):
        return defaults

    telegram = support.get("telegram")
    telegram = telegram if isinstance(telegram, dict) else {}
    methods = support.get("methods")
    method0 = (
        methods[0]
        if isinstance(methods, list) and methods and isinstance(methods[0], dict)
        else {}
    )

    text = (
        str(telegram.get("text") or support.get("text") or defaults.text).strip()
        or defaults.text
    )
    url = _PROJECT_SUPPORT_URL  # Immutable: always points to official project support.
    label = (
        str(support.get("label") or method0.get("label") or defaults.label).strip()
        or defaults.label
    )
    return SupportSettings(
        enabled=bool(support.get("enabled", False)),
        text=text,
        url=url,
        label=label,
        frequency_days=_normalize_support_frequency(
            telegram.get(
                "frequency_days", support.get("frequency_days", defaults.frequency_days)
            ),
            default=defaults.frequency_days,
        ),
    )


def load_support_settings(base_dir: Path = CONFIG_DIR) -> SupportSettings:
    resolved = resolve_config_paths(base_dir)
    if resolved.two_file_mode:
        return _load_support_from_ini(_read_config_file(resolved.settings_path))

    settings_path = (
        resolved.settings_path
        if resolved.settings_path.exists()
        else resolved.legacy_ini_path
    )
    parser = _read_config_file(settings_path)
    ini_settings = _load_support_from_ini(parser)
    if "support" in parser:
        return ini_settings

    yaml_path = resolved.yaml_path
    if not yaml_path or not yaml_path.exists():
        return ini_settings
    try:
        from mailbot_v26.config_yaml import load_config as load_yaml_config
    except Exception as exc:
        _LOGGER.warning("support_yaml_loader_unavailable: %s", exc)
        return ini_settings
    try:
        raw = load_yaml_config(yaml_path)
    except Exception as exc:
        _LOGGER.warning("support_yaml_load_failed: %s", exc)
        return ini_settings
    return _load_support_from_yaml(raw)


def load_web_config(base_dir: Path = CONFIG_DIR) -> WebConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="web")

    host = "127.0.0.1"
    port = 8787

    if "web" in parser:
        section = parser["web"]
        host = str(section.get("host", fallback=host)).strip() or host
        try:
            parsed_port = section.getint("port", fallback=port)
            if 1 <= parsed_port <= 65535:
                port = parsed_port
            else:
                _LOGGER.warning(
                    "Invalid [web] port out of range (%s), using default %s",
                    parsed_port,
                    port,
                )
        except ValueError as exc:
            _LOGGER.warning("Invalid [web] port value, using default %s: %s", port, exc)
    elif "web_ui" in parser:
        # Legacy fallback for config.ini users.
        section = parser["web_ui"]
        host = str(section.get("bind", fallback=host)).strip() or host
        try:
            parsed_port = section.getint("port", fallback=port)
            if 1 <= parsed_port <= 65535:
                port = parsed_port
            else:
                _LOGGER.warning(
                    "Invalid [web_ui] port out of range (%s), using default %s",
                    parsed_port,
                    port,
                )
        except ValueError as exc:
            _LOGGER.warning(
                "Invalid [web_ui] port value, using default %s: %s", port, exc
            )

    return WebConfig(host=host, port=port)


def load_telegram_ui_config(base_dir: Path = CONFIG_DIR) -> TelegramUIConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="telegram_ui")
    show_decision_trace = False
    if "telegram_ui" in parser:
        section = parser["telegram_ui"]
        try:
            show_decision_trace = section.getboolean(
                "show_decision_trace", fallback=False
            )
        except ValueError as exc:
            _LOGGER.warning(
                "Invalid [telegram_ui] show_decision_trace value, using default false: %s",
                exc,
            )
    return TelegramUIConfig(show_decision_trace=show_decision_trace)


def load_branding_config(base_dir: Path = CONFIG_DIR) -> BrandingConfig:
    parser = _read_settings_with_legacy_fallback(base_dir, section="branding")
    show_watermark = True
    if "branding" in parser:
        section = parser["branding"]
        try:
            show_watermark = section.getboolean("show_watermark", fallback=True)
        except ValueError as exc:
            _LOGGER.warning(
                "Invalid [branding] show_watermark value, using default true: %s",
                exc,
            )
    return BrandingConfig(show_watermark=show_watermark)


def load_web_ui_password_from_ini(base_dir: Path = CONFIG_DIR) -> str:
    parser = _read_settings_with_legacy_fallback(base_dir, section="web_ui")
    if "web_ui" not in parser:
        return ""
    return str(parser["web_ui"].get("password", fallback="")).strip()


def load_config(base_dir: Path = CONFIG_DIR) -> BotConfig:
    """Load and validate all configuration files.

    Parameters
    ----------
    base_dir:
        Optional base directory override. Defaults to repo root ``.``.
    """

    general = load_general_config(base_dir)
    ingest = load_ingest_config(base_dir)
    maintenance = load_maintenance_config(base_dir)
    web = load_web_config(base_dir)
    accounts = load_accounts_config(base_dir)
    keys = load_keys_config(base_dir)
    storage = load_storage_config(base_dir)
    return BotConfig(
        general=general,
        ingest=ingest,
        maintenance=maintenance,
        web=web,
        accounts=accounts,
        keys=keys,
        storage=storage,
    )


__all__ = [
    "AccountConfig",
    "AccountScope",
    "BotConfig",
    "BrandingConfig",
    "ConfigError",
    "GeneralConfig",
    "IngestConfig",
    "MaintenanceConfig",
    "TelegramUIConfig",
    "WebConfig",
    "InvalidAccountIdError",
    "KeysConfig",
    "StorageConfig",
    "SupportSettings",
    "get_account_scope",
    "load_config",
    "load_accounts_config",
    "load_general_config",
    "load_ingest_config",
    "load_maintenance_config",
    "load_branding_config",
    "load_telegram_ui_config",
    "load_web_config",
    "load_web_ui_password_from_ini",
    "load_keys_config",
    "load_storage_config",
    "load_support_settings",
    "parse_telegram_chat_id",
    "validate_telegram_contract",
    "resolve_account_scope",
]
