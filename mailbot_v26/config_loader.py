"""Configuration loading utilities for MailBot Premium v26.

This module follows the project Constitution by favoring clarity and
strict validation over implicit defaults. All configuration files are
stored under ``mailbot_v26/config`` and are separated to keep secrets
and per-account settings isolated.
"""

from __future__ import annotations

import configparser
from dataclasses import dataclass
from functools import lru_cache
import re
from pathlib import Path
from typing import Callable, List, Optional

CONFIG_DIR = Path(__file__).resolve().parent / "config"


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
class BotConfig:
    """Aggregate configuration bundle."""

    general: GeneralConfig
    accounts: List[AccountConfig]
    keys: KeysConfig
    storage: "StorageConfig"
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


def _read_config_file(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    parser.read(path, encoding="utf-8")
    return parser


def load_general_config(base_dir: Path = CONFIG_DIR) -> GeneralConfig:
    parser = _read_config_file(base_dir / "config.ini")
    if "general" not in parser:
        raise ConfigError("[general] section missing in config.ini")

    section = parser["general"]
    try:
        return GeneralConfig(
            check_interval=section.getint("check_interval", fallback=180),
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
        raise ConfigError(f"Invalid value in config.ini: {exc}") from exc


def load_accounts_config(base_dir: Path = CONFIG_DIR) -> List[AccountConfig]:
    parser = _read_config_file(base_dir / "accounts.ini")
    accounts: List[AccountConfig] = []
    invalid_ids: list[str] = []
    for section_name in parser.sections():
        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            invalid_ids.append(section_name)
            continue
        section = parser[section_name]
        try:
            account = AccountConfig(
                account_id=section_name,
                login=section["login"],
                password=section["password"],
                host=section.get("host", ""),
                port=section.getint("port", fallback=993),
                use_ssl=section.getboolean("use_ssl", fallback=True),
                telegram_chat_id=section.get("telegram_chat_id", fallback=""),
            )
        except KeyError as exc:
            raise ConfigError(f"Missing required field {exc!s} in accounts.ini:{section_name}") from exc
        except ValueError as exc:
            raise ConfigError(f"Invalid numeric field in accounts.ini:{section_name}: {exc}") from exc
        accounts.append(account)

    if invalid_ids:
        raise InvalidAccountIdError(invalid_ids)

    if not accounts:
        raise ConfigError("No accounts defined in accounts.ini")
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
        scopes[account.login] = AccountScope(
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
    return scopes.get(account_email)


def get_account_scope(account_email: str) -> tuple[str, list[str]] | None:
    scope = resolve_account_scope(account_email)
    if scope is None or not scope.chat_id:
        return None
    account_emails = list(scope.account_emails)
    if account_email and account_email not in account_emails:
        account_emails.append(account_email)
    return (f"tg:{scope.chat_id}", account_emails)


def load_keys_config(base_dir: Path = CONFIG_DIR) -> KeysConfig:
    parser = _read_config_file(base_dir / "keys.ini")
    if "telegram" not in parser or "cloudflare" not in parser:
        raise ConfigError("keys.ini must contain [telegram] and [cloudflare] sections")

    telegram = parser["telegram"]
    cloudflare = parser["cloudflare"]
    try:
        return KeysConfig(
            telegram_bot_token=telegram["bot_token"],
            cf_account_id=cloudflare["account_id"],
            cf_api_token=cloudflare["api_token"],
        )
    except KeyError as exc:
        raise ConfigError(f"Missing key in keys.ini: {exc!s}") from exc


def load_storage_config(base_dir: Path = CONFIG_DIR) -> StorageConfig:
    parser = _read_config_file(base_dir / "config.ini")
    default_path = Path(__file__).resolve().parents[1] / "data" / "mailbot.sqlite"
    db_path_raw = parser.get("storage", "db_path", fallback=str(default_path))
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        project_root = Path(__file__).resolve().parents[1]
        db_path = (project_root / db_path).resolve()
    return StorageConfig(db_path=db_path)


def load_config(base_dir: Path = CONFIG_DIR) -> BotConfig:
    """Load and validate all configuration files.

    Parameters
    ----------
    base_dir:
        Optional base directory override. Defaults to ``mailbot_v26/config``.
    """

    general = load_general_config(base_dir)
    accounts = load_accounts_config(base_dir)
    keys = load_keys_config(base_dir)
    storage = load_storage_config(base_dir)
    return BotConfig(general=general, accounts=accounts, keys=keys, storage=storage)


__all__ = [
    "AccountConfig",
    "AccountScope",
    "BotConfig",
    "ConfigError",
    "GeneralConfig",
    "InvalidAccountIdError",
    "KeysConfig",
    "StorageConfig",
    "get_account_scope",
    "load_config",
    "load_accounts_config",
    "load_general_config",
    "load_keys_config",
    "load_storage_config",
    "resolve_account_scope",
]
