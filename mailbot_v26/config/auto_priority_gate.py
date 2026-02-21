from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AutoPriorityGateConfig:
    enabled: bool = False
    window_days: int = 30
    min_samples: int = 30
    max_correction_rate: float = 0.15
    cooldown_hours: int = 24


def load_auto_priority_gate_config(
    base_dir: Path | None = None,
) -> AutoPriorityGateConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="auto-priority gate settings",
    )
    if not parser.has_section("auto_priority_gate"):
        return AutoPriorityGateConfig()

    section = parser["auto_priority_gate"]
    return AutoPriorityGateConfig(
        enabled=_get_bool(section, "enabled", default=False),
        window_days=_get_int(section, "window_days", default=30),
        min_samples=_get_int(section, "min_samples", default=30),
        max_correction_rate=_get_float(
            section, "max_correction_rate", default=0.15
        ),
        cooldown_hours=_get_int(section, "cooldown_hours", default=24),
    )


def _get_bool(section: configparser.SectionProxy, name: str, *, default: bool) -> bool:
    try:
        return section.getboolean(name, fallback=default)
    except ValueError:
        return default


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


__all__ = ["AutoPriorityGateConfig", "load_auto_priority_gate_config"]
