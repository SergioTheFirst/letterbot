from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TrustBootstrapConfig:
    learning_days: int = 14
    min_samples: int = 50
    max_allowed_surprise_rate: float = 0.30
    hide_action_templates_until_ready: bool = True
    templates_window_days: int = 7
    templates_min_corrections: int = 20
    templates_max_surprise_rate: float = 0.15


def load_trust_bootstrap_config(
    config_dir: Path | None = None,
) -> TrustBootstrapConfig:
    config_path = (config_dir or Path(__file__).resolve().parent) / "config.ini"
    parser = read_user_ini_with_defaults(
        config_path,
        logger=_LOGGER,
        scope_label="trust bootstrap settings",
    )
    section = parser["trust_bootstrap"] if "trust_bootstrap" in parser else None

    learning_days = 14
    min_samples = 50
    max_allowed_surprise_rate = 0.30
    hide_action_templates_until_ready = True
    templates_window_days = 7
    templates_min_corrections = 20
    templates_max_surprise_rate = 0.15

    if section is not None:
        try:
            learning_days = max(1, section.getint("learning_days", fallback=14))
        except ValueError:
            learning_days = 14
        try:
            min_samples = max(1, section.getint("min_samples", fallback=50))
        except ValueError:
            min_samples = 50
        try:
            max_allowed_surprise_rate = max(
                0.0,
                min(1.0, section.getfloat("max_allowed_surprise_rate", fallback=0.30)),
            )
        except ValueError:
            max_allowed_surprise_rate = 0.30
        try:
            hide_action_templates_until_ready = section.getboolean(
                "hide_action_templates_until_ready",
                fallback=True,
            )
        except ValueError:
            hide_action_templates_until_ready = True
        try:
            templates_window_days = max(
                1, section.getint("templates_window_days", fallback=7)
            )
        except ValueError:
            templates_window_days = 7
        try:
            templates_min_corrections = max(
                1, section.getint("templates_min_corrections", fallback=20)
            )
        except ValueError:
            templates_min_corrections = 20
        try:
            templates_max_surprise_rate = max(
                0.0,
                min(
                    1.0,
                    section.getfloat("templates_max_surprise_rate", fallback=0.15),
                ),
            )
        except ValueError:
            templates_max_surprise_rate = 0.15

    return TrustBootstrapConfig(
        learning_days=learning_days,
        min_samples=min_samples,
        max_allowed_surprise_rate=max_allowed_surprise_rate,
        hide_action_templates_until_ready=hide_action_templates_until_ready,
        templates_window_days=templates_window_days,
        templates_min_corrections=templates_min_corrections,
        templates_max_surprise_rate=templates_max_surprise_rate,
    )


__all__ = ["TrustBootstrapConfig", "load_trust_bootstrap_config"]
