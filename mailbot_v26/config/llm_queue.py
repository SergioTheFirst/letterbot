from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LLMQueueConfig:
    """EN: LLM queue configuration. RU: Конфигурация очереди LLM."""

    max_concurrent_llm_calls: int = 1
    llm_request_queue_enabled: bool = True
    llm_request_queue_size: int = 50
    llm_request_queue_timeout_sec: float = 300.0


def load_llm_queue_config(base_dir: Path | None = None) -> LLMQueueConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = configparser.ConfigParser()
    if not config_path.exists():
        return LLMQueueConfig()
    parser.read(config_path, encoding="utf-8")
    section = parser["threading"] if "threading" in parser else None
    return LLMQueueConfig(
        max_concurrent_llm_calls=_get_int(section, "max_concurrent_llm_calls", 1),
        llm_request_queue_enabled=_get_bool(section, "llm_request_queue_enabled", True),
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
