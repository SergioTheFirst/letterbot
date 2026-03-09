"""Learning-mode configuration for safe template-promotion activation."""

from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True)
class LearningConfig:
    template_promotion_shadow: bool = True
    template_promotion_runtime: bool = False
    template_promotion_min_corrections: int = 5
    template_promotion_min_consistency: float = 0.80
    template_promotion_require_content_agreement: bool = True


_CURRENT_LEARNING_CONFIG = LearningConfig()


def get_learning_config() -> LearningConfig:
    return _CURRENT_LEARNING_CONFIG


def configure_learning_config(
    config: LearningConfig | None = None,
    **overrides: object,
) -> LearningConfig:
    global _CURRENT_LEARNING_CONFIG
    candidate = config or _CURRENT_LEARNING_CONFIG
    if overrides:
        candidate = replace(candidate, **overrides)
    _CURRENT_LEARNING_CONFIG = candidate
    return _CURRENT_LEARNING_CONFIG


def reset_learning_config() -> LearningConfig:
    global _CURRENT_LEARNING_CONFIG
    _CURRENT_LEARNING_CONFIG = LearningConfig()
    return _CURRENT_LEARNING_CONFIG


__all__ = [
    "LearningConfig",
    "configure_learning_config",
    "get_learning_config",
    "reset_learning_config",
]
