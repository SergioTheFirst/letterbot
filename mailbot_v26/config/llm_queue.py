from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LLMQueueConfig:
    """EN: LLM queue configuration. RU: Конфигурация очереди LLM."""

    max_concurrent_llm_calls: int = 1
    llm_request_queue_enabled: bool = False
    llm_request_queue_size: int = 50
    llm_request_queue_timeout_sec: float = 300.0


def load_llm_queue_config(base_dir: Path | None = None) -> LLMQueueConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="llm queue settings",
    )
    section = parser["llm_queue"] if "llm_queue" in parser else parser["threading"] if "threading" in parser else None
    return LLMQueueConfig(
        max_concurrent_llm_calls=_get_int(section, "max_concurrent_llm_calls", 1),
        llm_request_queue_enabled=_get_bool(section, "llm_request_queue_enabled", False),
        llm_request_queue_size=_get_int(section, "llm_request_queue_size", 50),
        llm_request_queue_timeout_sec=_get_float(section, "llm_request_queue_timeout_sec", 300.0),
    )


def _get_int(section: configparser.SectionProxy | None, key: str, default: int) -> int:
    if section is None:
        return default
    try:
        return int(section.get(key, fallback=default))
    except (TypeError, ValueError):
        return default


def _get_float(section: configparser.SectionProxy | None, key: str, default: float) -> float:
    if section is None:
        return default
    try:
        return float(section.get(key, fallback=default))
    except (TypeError, ValueError):
        return default


def _get_bool(section: configparser.SectionProxy | None, key: str, default: bool) -> bool:
    if section is None:
        return default
    try:
        return section.getboolean(key, fallback=default)
    except ValueError:
        return default


__all__ = ["LLMQueueConfig", "load_llm_queue_config"]
