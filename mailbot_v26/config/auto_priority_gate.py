from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AutoPriorityGateConfig:
    enabled: bool = True
    window_days: int = 30
    min_samples: int = 10
    max_correction_rate: float = 0.15
    cooldown_hours: int = 24


def load_auto_priority_gate_config(
    base_dir: Path | None = None,
) -> AutoPriorityGateConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = _load_auto_priority_gate_parser(config_path)
    if not parser.has_section("auto_priority_gate"):
        return AutoPriorityGateConfig()

    section = parser["auto_priority_gate"]
    return AutoPriorityGateConfig(
        enabled=_get_bool(section, "enabled", default=True),
        window_days=_get_int(section, "window_days", default=30),
        min_samples=_get_int(section, "min_samples", default=10),
        max_correction_rate=_get_float(
            section, "max_correction_rate", default=0.15
        ),
        cooldown_hours=_get_int(section, "cooldown_hours", default=24),
    )


def _load_auto_priority_gate_parser(config_path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(inline_comment_prefixes=(";", "#", "'"))
    template_path = config_path.parent / "config.ini.example"

    if not config_path.exists():
        _LOGGER.warning(
            "%s missing at %s; using deterministic defaults for auto-priority gate settings. "
            "Template: %s. Windows command: copy %s %s",
            config_path.name,
            config_path,
            template_path,
            template_path,
            config_path,
        )
        return parser

    try:
        parser.read(config_path, encoding="utf-8")
        return parser
    except (configparser.MissingSectionHeaderError, configparser.ParsingError) as exc:
        _LOGGER.warning(
            "%s is invalid at %s; trying legacy no-section mode. "
            "If this persists, restore from %s (Windows: copy %s %s).",
            config_path.name,
            config_path,
            template_path,
            template_path,
            config_path,
        )
        _LOGGER.debug("auto_priority_gate_ini_parse_failed", exc_info=exc)
        try:
            raw_text = config_path.read_text(encoding="utf-8")
        except OSError as read_exc:
            _LOGGER.warning(
                "Failed to read %s after parse error; using deterministic defaults.",
                config_path,
            )
            _LOGGER.debug("auto_priority_gate_ini_read_failed", exc_info=read_exc)
            return configparser.ConfigParser(inline_comment_prefixes=(";", "#", "'"))

        legacy_parser = configparser.ConfigParser(
            inline_comment_prefixes=(";", "#", "'")
        )
        try:
            legacy_parser.read_string(f"[main]\n{raw_text}")
        except (configparser.MissingSectionHeaderError, configparser.ParsingError) as legacy_exc:
            _LOGGER.warning(
                "Legacy parse also failed for %s; using deterministic defaults.",
                config_path,
            )
            _LOGGER.debug("auto_priority_gate_legacy_parse_failed", exc_info=legacy_exc)
            return configparser.ConfigParser(inline_comment_prefixes=(";", "#", "'"))

        if "main" not in legacy_parser:
            return configparser.ConfigParser(inline_comment_prefixes=(";", "#", "'"))

        for key, value in legacy_parser["main"].items():
            normalized = key.strip().lower()
            if normalized.startswith("auto_priority_gate."):
                normalized = normalized.split(".", 1)[1]
            if normalized in {
                "enabled",
                "window_days",
                "min_samples",
                "max_correction_rate",
                "cooldown_hours",
            }:
                if "auto_priority_gate" not in parser:
                    parser["auto_priority_gate"] = {}
                parser["auto_priority_gate"][normalized] = value
        return parser
    except OSError as exc:
        _LOGGER.warning(
            "Failed to read %s; using deterministic defaults for auto-priority gate settings.",
            config_path,
        )
        _LOGGER.debug("auto_priority_gate_ini_oserror", exc_info=exc)
        return configparser.ConfigParser(inline_comment_prefixes=(";", "#", "'"))


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
