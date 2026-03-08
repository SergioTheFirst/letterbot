from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SilencePolicyConfig:
    lookback_days: int = 60
    min_messages: int = 5
    silence_factor: float = 3.0
    min_silence_days: int = 7
    cooldown_hours: int = 336
    max_per_run: int = 20


def load_silence_policy_config(
    base_dir: Path | None = None,
) -> SilencePolicyConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="silence policy settings",
    )
    if not parser.has_section("silence_policy"):
        return SilencePolicyConfig()

    section = parser["silence_policy"]
    return SilencePolicyConfig(
        lookback_days=_get_int(section, "lookback_days", default=60),
        min_messages=_get_int(section, "min_messages", default=5),
        silence_factor=_get_float(section, "silence_factor", default=3.0),
        min_silence_days=_get_int(section, "min_silence_days", default=7),
        cooldown_hours=_get_int(section, "cooldown_hours", default=336),
        max_per_run=_get_int(section, "max_per_run", default=20),
    )


def _get_int(section: configparser.SectionProxy, name: str, *, default: int) -> int:
    try:
        return section.getint(name, fallback=default)
    except ValueError:
        return default


def _get_float(
    section: configparser.SectionProxy, name: str, *, default: float
) -> float:
    try:
        return section.getfloat(name, fallback=default)
    except ValueError:
        return default


__all__ = ["SilencePolicyConfig", "load_silence_policy_config"]
