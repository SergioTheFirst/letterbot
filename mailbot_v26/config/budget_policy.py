from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.budgets.gate import BudgetGateConfig


@dataclass(frozen=True, slots=True)
class BudgetUsageConfig:
    """EN: LLM usage policy. RU: Политика использования LLM."""

    llm_percentile_threshold: int = 80
    window_days: int = 7


def load_budget_gate_config(base_dir: Path | None = None) -> BudgetGateConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = configparser.ConfigParser()
    if not config_path.exists():
        return BudgetGateConfig()
    parser.read(config_path, encoding="utf-8")
    budgets = parser["budgets"] if "budgets" in parser else None
    gates = parser["gates"] if "gates" in parser else None
    return BudgetGateConfig(
        enable_budget_tracking=_get_bool(budgets, "enable_budget_tracking", True),
        enable_no_ai_gate=_get_bool(gates, "enable_no_ai_gate", True),
        default_llm_budget_tokens_per_year=_get_int(
            budgets, "default_llm_budget_tokens_per_year", 900000
        ),
        default_llm_budget_tokens_per_month=_get_int(
            budgets, "default_llm_budget_tokens_per_month", 75000
        ),
        default_llm_budget_tokens_per_day=_get_int(
            budgets, "default_llm_budget_tokens_per_day", 2466
        ),
        default_llm_budget_period=_get_str(
            budgets, "default_llm_budget_period", "yearly"
        ),
    )


def load_budget_usage_config(base_dir: Path | None = None) -> BudgetUsageConfig:
    config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
    config_path = config_dir / "config.ini"
    parser = configparser.ConfigParser()
    if not config_path.exists():
        return BudgetUsageConfig()
    parser.read(config_path, encoding="utf-8")
    usage = parser["llm_usage"] if "llm_usage" in parser else None
    return BudgetUsageConfig(
        llm_percentile_threshold=_get_int(usage, "llm_percentile_threshold", 80),
        window_days=_get_int(usage, "llm_usage_window_days", 7),
    )


def _get_int(section: configparser.SectionProxy | None, key: str, default: int) -> int:
    if section is None:
        return default
    try:
        return int(section.get(key, fallback=default))
    except (TypeError, ValueError):
        return default


def _get_bool(section: configparser.SectionProxy | None, key: str, default: bool) -> bool:
    if section is None:
        return default
    try:
        return section.getboolean(key, fallback=default)
    except ValueError:
        return default


def _get_str(section: configparser.SectionProxy | None, key: str, default: str) -> str:
    if section is None:
        return default
    raw = section.get(key, fallback=default)
    return str(raw or default)


__all__ = ["BudgetGateConfig", "BudgetUsageConfig", "load_budget_gate_config", "load_budget_usage_config"]
