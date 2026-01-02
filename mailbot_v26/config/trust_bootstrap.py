from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TrustBootstrapConfig:
    learning_days: int = 14
    min_samples: int = 50
    max_allowed_surprise_rate: float = 0.30
    hide_action_templates_until_ready: bool = True


def load_trust_bootstrap_config(
    config_dir: Path | None = None,
) -> TrustBootstrapConfig:
    config_path = (config_dir or Path(__file__).resolve().parent) / "config.ini"
    parser = configparser.ConfigParser()
    if config_path.exists():
        parser.read(config_path, encoding="utf-8")
    section = parser["trust_bootstrap"] if "trust_bootstrap" in parser else None

    learning_days = 14
    min_samples = 50
    max_allowed_surprise_rate = 0.30
    hide_action_templates_until_ready = True

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

    return TrustBootstrapConfig(
        learning_days=learning_days,
        min_samples=min_samples,
        max_allowed_surprise_rate=max_allowed_surprise_rate,
        hide_action_templates_until_ready=hide_action_templates_until_ready,
    )


__all__ = ["TrustBootstrapConfig", "load_trust_bootstrap_config"]
