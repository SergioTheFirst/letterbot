from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PremiumClarityConfig:
    confidence_dots_mode: str = "auto"
    confidence_dots_threshold: int = 75
    confidence_dots_scale: int = 10


def load_premium_clarity_config(
    config_dir: Path | None = None,
) -> PremiumClarityConfig:
    base_dir = config_dir or Path(__file__).resolve().parent
    config_path = base_dir / "config.ini"
    if not config_path.exists():
        return PremiumClarityConfig()
    parser = configparser.ConfigParser()
    parser.read(config_path, encoding="utf-8")
    if "premium_clarity" not in parser:
        return PremiumClarityConfig()
    section = parser["premium_clarity"]
    mode_raw = section.get("premium_clarity_confidence_dots", fallback="auto")
    mode = (mode_raw or "").strip().lower()
    if mode not in {"auto", "always", "never"}:
        mode = "auto"
    try:
        threshold = section.getint(
            "premium_clarity_confidence_threshold", fallback=75
        )
    except ValueError:
        threshold = 75
    threshold = max(0, min(100, threshold))
    try:
        scale = section.getint(
            "premium_clarity_confidence_dots_scale", fallback=10
        )
    except ValueError:
        scale = 10
    if scale not in {5, 10}:
        scale = 10
    return PremiumClarityConfig(
        confidence_dots_mode=mode,
        confidence_dots_threshold=threshold,
        confidence_dots_scale=scale,
    )


__all__ = ["PremiumClarityConfig", "load_premium_clarity_config"]
