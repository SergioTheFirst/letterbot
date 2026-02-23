"""Configuration loading utilities for MailBot Premium v26.

This module follows the project Constitution by favoring clarity and
strict validation over implicit defaults. All configuration files are
stored under ``mailbot_v26/config`` and are separated to keep secrets
and per-account settings isolated.
"""

from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass, field
from functools import lru_cache
import re
from pathlib import Path
from typing import Callable, Iterable, List, Optional

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.account_identity import normalize_login

CONFIG_DIR = Path(__file__).resolve().parent / "config"
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

    telegram_bot_token: str
    cf_account_id: str
    cf_api_token: str


@dataclass
class IngestConfig:
    """Mail ingest policies."""

    allow_prestart_emails: bool


@dataclass
class MaintenanceConfig:
    """Maintenance mode settings."""

    maintenance_mode: bool


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
        super().__init__(f"Invalid account_id(s) in accounts.ini: {', '.join(invalid_ids)}")
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


def load_general_config(base_dir: Path = CONFIG_DIR) -> GeneralConfig:
    parser = _read_config_file(_resolve_settings_path(base_dir))

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
                telegram_chat_id=section.get("telegram_chat_id", fallback=""),
            )
        except KeyError as exc:
            _LOGGER.warning("Skipping account [%s]: missing required field %s", section_name, exc)
            continue
        except ValueError as exc:
            _LOGGER.warning("Skipping account [%s]: invalid field (%s)", section_name, exc)
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
        str(email).strip()
        for email in account_emails
        if str(email or "").strip()
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
    if not resolved.two_file_mode and not any(section in accounts_parser for section in ("telegram", "cloudflare")):
        keys_path = resolved.keys_path
        if keys_path.exists():
            _LOGGER.info("keys.ini legacy fallback is used")
            parser = _read_config_file(keys_path)

    telegram = parser["telegram"] if "telegram" in parser else {}
    cloudflare = parser["cloudflare"] if "cloudflare" in parser else {}
    return KeysConfig(
        telegram_bot_token=str(telegram.get("bot_token", "")),
        cf_account_id=str(cloudflare.get("account_id", "")),
        cf_api_token=str(cloudflare.get("api_token", "")),
    )


def load_storage_config(base_dir: Path = CONFIG_DIR) -> StorageConfig:
    parser = _read_config_file(_resolve_settings_path(base_dir))
    default_path = Path(__file__).resolve().parents[1] / "data" / "mailbot.sqlite"
    db_path_raw = parser.get("storage", "db_path", fallback=str(default_path))
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        project_root = Path(__file__).resolve().parents[1]
        db_path = (project_root / db_path).resolve()
    return StorageConfig(db_path=db_path)


def load_ingest_config(base_dir: Path = CONFIG_DIR) -> IngestConfig:
    parser = _read_config_file(_resolve_settings_path(base_dir))
    if "ingest" not in parser:
        return IngestConfig(allow_prestart_emails=False)
    section = parser["ingest"]
    try:
        return IngestConfig(
            allow_prestart_emails=section.getboolean(
                "allow_prestart_emails",
                fallback=False,
            )
        )
    except ValueError as exc:
        _LOGGER.warning("Invalid [ingest] config, using defaults: %s", exc)
        return IngestConfig(allow_prestart_emails=False)


def load_maintenance_config(base_dir: Path = CONFIG_DIR) -> MaintenanceConfig:
    parser = _read_config_file(_resolve_settings_path(base_dir))
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


def load_config(base_dir: Path = CONFIG_DIR) -> BotConfig:
    """Load and validate all configuration files.

    Parameters
    ----------
    base_dir:
        Optional base directory override. Defaults to ``mailbot_v26/config``.
    """

    general = load_general_config(base_dir)
    ingest = load_ingest_config(base_dir)
    maintenance = load_maintenance_config(base_dir)
    accounts = load_accounts_config(base_dir)
    keys = load_keys_config(base_dir)
    storage = load_storage_config(base_dir)
    return BotConfig(
        general=general,
        ingest=ingest,
        maintenance=maintenance,
        accounts=accounts,
        keys=keys,
        storage=storage,
    )


__all__ = [
    "AccountConfig",
    "AccountScope",
    "BotConfig",
    "ConfigError",
    "GeneralConfig",
    "IngestConfig",
    "MaintenanceConfig",
    "InvalidAccountIdError",
    "KeysConfig",
    "StorageConfig",
    "get_account_scope",
    "load_config",
    "load_accounts_config",
    "load_general_config",
    "load_ingest_config",
    "load_maintenance_config",
    "load_keys_config",
    "load_storage_config",
    "resolve_account_scope",
]
