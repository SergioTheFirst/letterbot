from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RegretMinimizationConfig:
    window_days: int = 90
    trust_drop_window_days: int = 7
    min_samples: int = 5


def load_regret_minimization_config(
    config_dir: Path | None = None,
) -> RegretMinimizationConfig:
    config_path = (config_dir or Path(__file__).resolve().parent) / "config.ini"
    parser = configparser.ConfigParser()
    if config_path.exists():
        parser.read(config_path, encoding="utf-8")
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
