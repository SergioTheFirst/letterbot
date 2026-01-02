from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DeliveryPolicyConfig:
    night_start_hour: int = 21
    night_end_hour: int = 7
    immediate_value_threshold: int = 60
    batch_value_threshold: int = 20
    critical_risk_threshold: int = 80
    max_immediate_per_hour: int = 5


def load_delivery_policy_config(base_dir: Path | None = None) -> DeliveryPolicyConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = configparser.ConfigParser()
    if not config_path.exists():
        return DeliveryPolicyConfig()
    parser.read(config_path, encoding="utf-8")
    section = parser["delivery_policy"] if "delivery_policy" in parser else None
    if section is None:
        return DeliveryPolicyConfig()
    night_start, night_end = _parse_hours(section.get("night_hours", fallback="21-7"))
    return DeliveryPolicyConfig(
        night_start_hour=night_start,
        night_end_hour=night_end,
        immediate_value_threshold=_get_int(section, "immediate_value_threshold", 60),
        batch_value_threshold=_get_int(section, "batch_value_threshold", 20),
        critical_risk_threshold=_get_int(section, "critical_risk_threshold", 80),
        max_immediate_per_hour=_get_int(section, "max_immediate_per_hour", 5),
    )


def _parse_hours(raw: str) -> tuple[int, int]:
    if not raw:
        return 21, 7
    value = raw.strip()
    if "-" not in value:
        return 21, 7
    start_raw, end_raw = value.split("-", 1)
    return _clamp_hour(start_raw, default=21), _clamp_hour(end_raw, default=7)


def _clamp_hour(raw: str, *, default: int) -> int:
    try:
        hour = int(raw)
    except (TypeError, ValueError):
        return default
    return max(0, min(23, hour))


def _get_int(section: configparser.SectionProxy, key: str, default: int) -> int:
    try:
        return int(section.get(key, fallback=default))
    except (TypeError, ValueError):
        return default


__all__ = ["DeliveryPolicyConfig", "load_delivery_policy_config"]
