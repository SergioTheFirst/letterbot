from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FlowProtectionConfig:
    focus_start_hour: int = 9
    focus_end_hour: int = 12


def load_flow_protection_config(base_dir: Path | None = None) -> FlowProtectionConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="flow protection settings",
    )
    section = parser["flow_protection"] if "flow_protection" in parser else None
    if section is None:
        return FlowProtectionConfig()
    focus_start, focus_end = _parse_hours(section.get("focus_hours", fallback="9-12"))
    return FlowProtectionConfig(
        focus_start_hour=focus_start,
        focus_end_hour=focus_end,
    )


def _parse_hours(raw: str) -> tuple[int, int]:
    if not raw:
        return 9, 12
    value = raw.strip()
    if "-" not in value:
        return 9, 12
    start_raw, end_raw = value.split("-", 1)
    return _clamp_hour(start_raw, default=9), _clamp_hour(end_raw, default=12)


def _clamp_hour(raw: str, *, default: int) -> int:
    try:
        hour = int(raw)
    except (TypeError, ValueError):
        return default
    return max(0, min(23, hour))


__all__ = ["FlowProtectionConfig", "load_flow_protection_config"]
