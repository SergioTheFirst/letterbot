from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class UncertaintyQueueConfig:
    window_days: int = 1
    min_confidence: int = 70
    max_items: int = 5


def load_uncertainty_queue_config(
    config_dir: Path | None = None,
) -> UncertaintyQueueConfig:
    config_path = (config_dir or Path(__file__).resolve().parent) / "config.ini"
    parser = configparser.ConfigParser()
    if config_path.exists():
        parser.read(config_path, encoding="utf-8")
    section = parser["uncertainty_queue"] if "uncertainty_queue" in parser else None

    window_days = 1
    min_confidence = 70
    max_items = 5

    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=1))
        except ValueError:
            window_days = 1
        try:
            min_confidence = max(0, min(100, section.getint("min_confidence", fallback=70)))
        except ValueError:
            min_confidence = 70
        try:
            max_items = max(0, section.getint("max_items", fallback=5))
        except ValueError:
            max_items = 5

    return UncertaintyQueueConfig(
        window_days=window_days,
        min_confidence=min_confidence,
        max_items=max_items,
    )


__all__ = ["UncertaintyQueueConfig", "load_uncertainty_queue_config"]
