from __future__ import annotations

class ConfigError(RuntimeError):
    """Публичное исключение для ошибок загрузки/валидации конфигурации."""


from pathlib import Path
import ipaddress
from typing import Any, Dict, Optional, Tuple

import yaml


SCHEMA_NEWER_MESSAGE = (
    "Файл конфигурации использует более новую версию схемы, чем поддерживает текущая версия бота. "
    "Обновите проект/код mailbot."
)


def _as_path(v: Any) -> Path:
    if isinstance(v, Path):
        return v
    if isinstance(v, str) and v.strip():
        return Path(v)
    raise ConfigError("Ожидалась строка-путь (path).")


def _as_int(v: Any, *, name: str) -> int:
    if isinstance(v, bool):
        raise ConfigError(f"{name}: ожидалось целое число, а не bool.")
    try:
        iv = int(v)
    except Exception as e:
        raise ConfigError(f"{name}: ожидалось целое число.") from e
    return iv


def _as_float(v: Any, *, name: str) -> float:
    if isinstance(v, bool):
        raise ConfigError(f"{name}: ожидалось число, а не bool.")
    try:
        fv = float(v)
    except Exception as e:
        raise ConfigError(f"{name}: ожидалось число.") from e
    return fv


def _as_str(v: Any, *, name: str) -> str:
    if isinstance(v, str) and v.strip():
        return v.strip()
    raise ConfigError(f"{name}: ожидалась непустая строка.")


def _as_bool(v: Any, *, name: str) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    raise ConfigError(f"{name}: ожидался bool (true/false).")


def _as_ip(v: Any, *, name: str) -> ipaddress.IPv4Address:
    try:
        return ipaddress.ip_address(v)  # type: ignore[arg-type]
    except Exception as e:
        raise ConfigError(f"{name}: ожидался IP-адрес.") from e


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Файл конфигурации не найден: {path}")
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as e:
        raise ConfigError(f"Не удалось прочитать файл конфигурации: {path}") from e
    try:
        data = yaml.safe_load(raw) or {}
    except Exception as e:
        raise ConfigError(f"YAML синтаксическая ошибка в {path}") from e
    if not isinstance(data, dict):
        raise ConfigError("Корневой YAML должен быть словарём (mapping).")
    return data


def load_config(config_path: str | Path) -> Dict[str, Any]:
    """Загружает YAML и возвращает dict (сырые данные)."""
    path = _as_path(config_path)
    return _load_yaml(path)


def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводит типы и валидирует обязательные поля.
    Возвращает нормализованный конфиг.
    """
    schema_version = cfg.get("schema_version", 1)
    schema_version = _as_int(schema_version, name="schema_version")
    # если у тебя в проекте есть ограничение по версиям — оставим мягко:
    # (если в исходнике было иначе — поправишь потом, но сейчас главное убрать SyntaxError)
    if schema_version > 9999:
        raise ConfigError(SCHEMA_NEWER_MESSAGE)

    imap = cfg.get("imap") or {}
    if not isinstance(imap, dict):
        raise ConfigError("imap: ожидался словарь.")

    bot = cfg.get("bot") or {}
    if not isinstance(bot, dict):
        raise ConfigError("bot: ожидался словарь.")

    # обязательные: IMAP
    imap_host = _as_str(imap.get("host"), name="imap.host")
    imap_user = _as_str(imap.get("user"), name="imap.user")
    imap_pass = _as_str(imap.get("password"), name="imap.password")

    imap_port = imap.get("port", 993)
    imap_port = _as_int(imap_port, name="imap.port")
    if not (1 <= imap_port <= 65535):
        raise ConfigError("imap.port: недопустимый порт.")

    imap_ssl = imap.get("ssl", True)
    imap_ssl = _as_bool(imap_ssl, name="imap.ssl")

    mailbox = imap.get("mailbox", "INBOX")
    mailbox = _as_str(mailbox, name="imap.mailbox")

    max_email_mb = imap.get("max_email_mb", 15)
    max_email_mb = _as_int(max_email_mb, name="imap.max_email_mb")
    if max_email_mb <= 0:
        raise ConfigError("imap.max_email_mb: должно быть > 0.")

    # bot settings (минимально)
    tg_token = _as_str(bot.get("telegram_token"), name="bot.telegram_token")
    admin_chat_id = _as_int(bot.get("admin_chat_id"), name="bot.admin_chat_id")

    # остальное — оставим как есть, но нормализуем структуру
    normalized: Dict[str, Any] = dict(cfg)
    normalized["schema_version"] = schema_version
    normalized["imap"] = dict(imap)
    normalized["imap"].update(
        {
            "host": imap_host,
            "port": imap_port,
            "ssl": imap_ssl,
            "user": imap_user,
            "password": imap_pass,
            "mailbox": mailbox,
            "max_email_mb": max_email_mb,
        }
    )
    normalized["bot"] = dict(bot)
    normalized["bot"].update(
        {
            "telegram_token": tg_token,
            "admin_chat_id": admin_chat_id,
        }
    )
    return normalized


def load_and_validate_config(config_path: str | Path) -> Dict[str, Any]:
    cfg = load_config(config_path)
    return validate_config(cfg)
