from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


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
    parser = configparser.ConfigParser()
    if not config_path.exists():
        return AutoPriorityGateConfig()

    parser.read(config_path, encoding="utf-8")
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
