from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional


class BudgetType(str, Enum):
    """EN: Budget type enumeration. RU: Перечень типов бюджетов."""

    LLM_TOKENS = "llm_tokens"


class BudgetPeriod(str, Enum):
    """EN: Budget period enumeration. RU: Перечень периодов бюджета."""

    DAILY = "DAILY"
    MONTHLY = "MONTHLY"
    YEARLY = "YEARLY"


@dataclass(frozen=True, slots=True)
class ResourceBudget:
    """EN: Resource budget definition. RU: Описание лимита ресурсов."""

    account_email: str
    budget_type: BudgetType
    limit_value: int
    period: BudgetPeriod
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    def reset_date(self, *, now: Optional[datetime] = None) -> datetime:
        """EN: Next reset time. RU: Время следующего сброса."""

        current = now or datetime.now(timezone.utc)
        if self.period == BudgetPeriod.DAILY:
            next_day = current + timedelta(days=1)
            return next_day.replace(hour=0, minute=0, second=0, microsecond=0)
        if self.period == BudgetPeriod.MONTHLY:
            month_start = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            next_month = month_start + timedelta(days=32)
            return next_month.replace(day=1)
        year_start = current.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        next_year = year_start.replace(year=year_start.year + 1)
        return next_year


@dataclass(frozen=True, slots=True)
class BudgetStatus:
    """EN: Budget status snapshot. RU: Снимок статуса бюджета."""

    budget: ResourceBudget
    consumed: int
    remaining: int
    percentage: float
    reset_at: datetime
    is_exhausted: bool


__all__ = [
    "BudgetType",
    "BudgetPeriod",
    "ResourceBudget",
    "BudgetStatus",
]
