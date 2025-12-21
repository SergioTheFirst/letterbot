from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TemporalRuleConfig:
    commitment_warning_hours: int = 24
    commitment_grace_hours: int = 24
    commitment_max_window_days: int = 30

    response_window_days: int = 90
    response_multiplier: float = 2.0
    response_min_hours: float = 2.0
    response_severity_medium: float = 1.5
    response_severity_high: float = 2.5

    silence_window_days: int = 14
    silence_baseline_weekly: float = 1.0


DEFAULT_TEMPORAL_RULES = TemporalRuleConfig()


__all__ = ["TemporalRuleConfig", "DEFAULT_TEMPORAL_RULES"]
