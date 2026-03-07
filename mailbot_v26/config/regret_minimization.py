from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RegretMinimizationConfig:
    window_days: int = 90
    trust_drop_window_days: int = 7
    min_samples: int = 5


def load_regret_minimization_config(
    config_dir: Path | None = None,
) -> RegretMinimizationConfig:
    config_path = (config_dir or Path(__file__).resolve().parent) / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="regret minimization settings",
    )
    section = parser["regret_minimization"] if "regret_minimization" in parser else None

    window_days = 90
    trust_drop_window_days = 7
    min_samples = 5

    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=90))
        except ValueError:
            window_days = 90
        try:
            trust_drop_window_days = max(
                1, section.getint("trust_drop_window_days", fallback=7)
            )
        except ValueError:
            trust_drop_window_days = 7
        try:
            min_samples = max(1, section.getint("min_samples", fallback=5))
        except ValueError:
            min_samples = 5

    return RegretMinimizationConfig(
        window_days=window_days,
        trust_drop_window_days=trust_drop_window_days,
        min_samples=min_samples,
    )


__all__ = ["RegretMinimizationConfig", "load_regret_minimization_config"]
