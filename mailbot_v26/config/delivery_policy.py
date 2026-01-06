from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DeliveryPolicyConfig:
    immediate_value_threshold: int = 60
    critical_risk_threshold: int = 80
    max_immediate_per_hour: int = 0


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
    return DeliveryPolicyConfig(
        immediate_value_threshold=_get_int(section, "immediate_value_threshold", 60),
        critical_risk_threshold=_get_int(section, "critical_risk_threshold", 80),
        max_immediate_per_hour=0,
    )


def _get_int(section: configparser.SectionProxy, key: str, default: int) -> int:
    try:
        return int(section.get(key, fallback=default))
    except (TypeError, ValueError):
        return default


__all__ = ["DeliveryPolicyConfig", "load_delivery_policy_config"]
