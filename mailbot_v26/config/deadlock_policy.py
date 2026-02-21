from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DeadlockPolicyConfig:
    window_days: int = 5
    min_messages: int = 10
    cooldown_hours: int = 24
    max_per_run: int = 20


def load_deadlock_policy_config(
    base_dir: Path | None = None,
) -> DeadlockPolicyConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="deadlock policy settings",
    )
    if not parser.has_section("deadlock_policy"):
        return DeadlockPolicyConfig()

    section = parser["deadlock_policy"]
    return DeadlockPolicyConfig(
        window_days=_get_int(section, "window_days", default=5),
        min_messages=_get_int(section, "min_messages", default=10),
        cooldown_hours=_get_int(section, "cooldown_hours", default=24),
        max_per_run=_get_int(section, "max_per_run", default=20),
    )


def _get_int(section: configparser.SectionProxy, name: str, *, default: int) -> int:
    try:
        return section.getint(name, fallback=default)
    except ValueError:
        return default


__all__ = ["DeadlockPolicyConfig", "load_deadlock_policy_config"]
