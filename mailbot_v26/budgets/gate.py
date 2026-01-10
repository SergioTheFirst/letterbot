from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from mailbot_v26.budgets.contract import BudgetPeriod, BudgetStatus, BudgetType, ResourceBudget
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class BudgetGateConfig:
    """EN: Budget gate configuration. RU: Конфигурация бюджетного гейта."""

    enable_budget_tracking: bool = True
    enable_no_ai_gate: bool = True
    default_llm_budget_tokens_per_year: int = 900000
    default_llm_budget_tokens_per_month: int = 75000
    default_llm_budget_tokens_per_day: int = 2466
    default_llm_budget_period: str = "yearly"

    def resolved_period(self) -> BudgetPeriod:
        raw = (self.default_llm_budget_period or "yearly").strip().lower()
        if raw == "daily":
            return BudgetPeriod.DAILY
        if raw == "monthly":
            return BudgetPeriod.MONTHLY
        return BudgetPeriod.YEARLY

    def resolved_limit(self) -> int:
        period = self.resolved_period()
        if period == BudgetPeriod.DAILY:
            return int(self.default_llm_budget_tokens_per_day)
        if period == BudgetPeriod.MONTHLY:
            return int(self.default_llm_budget_tokens_per_month)
        return int(self.default_llm_budget_tokens_per_year)


class BudgetGate:
    """EN: Budget gate for deterministic LLM usage. RU: Гейт бюджета для LLM."""

    def __init__(
        self,
        db_path: Path,
        config: BudgetGateConfig,
        emitter: Optional[EventEmitter] = None,
    ) -> None:
        self._db_path = db_path
        self._config = config
        self._emitter = emitter or EventEmitter(db_path)

    def can_use_llm(self, account_email: str) -> bool:
        """EN: Check if LLM budget is available. RU: Проверка доступности LLM."""

        if not self._config.enable_budget_tracking:
            return True
        try:
            status = self.get_budget_status(account_email, BudgetType.LLM_TOKENS)
            if status.is_exhausted:
                self._emit_event(
                    EventType.BUDGET_LIMIT_EXCEEDED,
                    account_email=account_email,
                    payload={
                        "budget_type": BudgetType.LLM_TOKENS.value,
                        "limit": status.budget.limit_value,
                        "consumed": status.consumed,
                        "period": status.budget.period.value,
                    },
                )
                if self._config.enable_no_ai_gate:
                    self._emit_event(
                        EventType.GATE_FLIPPED,
                        account_email=account_email,
                        payload={
                            "feature_name": "llm",
                            "old_mode": "enabled",
                            "new_mode": "disabled",
                            "reason": "budget_exhausted",
                        },
                    )
                return False
            return True
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("budget_gate_failed", account=account_email, error=str(exc))
            self._emit_event(
                EventType.BUDGET_GATE_ERROR,
                account_email=account_email,
                payload={"error": str(exc)},
            )
            return False

    def get_budget_status(self, account_email: str, budget_type: BudgetType) -> BudgetStatus:
        """EN: Get budget status. RU: Получить статус бюджета."""

        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT limit_value, period, created_at, updated_at
                FROM account_budgets
                WHERE account_email = ? AND budget_type = ?
                """,
                (account_email, budget_type.value),
            ).fetchone()
            if row is None:
                self._create_default_budget(conn, account_email, budget_type)
                row = conn.execute(
                    """
                    SELECT limit_value, period, created_at, updated_at
                    FROM account_budgets
                    WHERE account_email = ? AND budget_type = ?
                    """,
                    (account_email, budget_type.value),
                ).fetchone()
            if row is None:
                raise RuntimeError("budget_missing_after_create")
            budget = ResourceBudget(
                account_email=account_email,
                budget_type=budget_type,
                limit_value=int(row["limit_value"]),
                period=BudgetPeriod[row["period"]],
                created_at=_parse_dt(row["created_at"]),
                updated_at=_parse_dt(row["updated_at"]),
            )
            consumed = self._get_period_consumption(conn, account_email, budget_type, budget.period)
            remaining = max(0, budget.limit_value - consumed)
            percentage = consumed / budget.limit_value if budget.limit_value else 1.0
            reset_at = budget.reset_date()
            return BudgetStatus(
                budget=budget,
                consumed=consumed,
                remaining=remaining,
                percentage=percentage,
                reset_at=reset_at,
                is_exhausted=remaining <= 0,
            )

    def consume_budget(
        self,
        account_email: str,
        budget_type: BudgetType,
        amount: int,
        *,
        reason: str,
    ) -> bool:
        """EN: Record budget consumption. RU: Учёт расхода бюджета."""

        if not self._config.enable_budget_tracking:
            return True
        if amount <= 0:
            return False
        try:
            event_id = f"budget_{account_email}_{budget_type.value}_{datetime.now(timezone.utc).timestamp()}"
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO budget_consumption (
                        account_email, budget_type, consumed, reason, event_id, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_email,
                        budget_type.value,
                        amount,
                        reason,
                        event_id,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                conn.commit()
            self._emit_event(
                EventType.BUDGET_CONSUMED,
                account_email=account_email,
                payload={
                    "budget_type": budget_type.value,
                    "amount": amount,
                    "reason": reason,
                    "event_id": event_id,
                },
            )
            return True
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("budget_consume_failed", account=account_email, error=str(exc))
            return False

    def _create_default_budget(
        self, conn: sqlite3.Connection, account_email: str, budget_type: BudgetType
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        period = self._config.resolved_period()
        limit_value = self._config.resolved_limit()
        conn.execute(
            """
            INSERT OR IGNORE INTO account_budgets (
                account_email, budget_type, limit_value, period, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                account_email,
                budget_type.value,
                limit_value,
                period.value,
                now_iso,
                now_iso,
            ),
        )
        conn.commit()

    @staticmethod
    def _get_period_consumption(
        conn: sqlite3.Connection,
        account_email: str,
        budget_type: BudgetType,
        period: BudgetPeriod,
    ) -> int:
        start = _period_start(period)
        row = conn.execute(
            """
            SELECT COALESCE(SUM(consumed), 0) AS total
            FROM budget_consumption
            WHERE account_email = ? AND budget_type = ? AND timestamp >= ?
            """,
            (account_email, budget_type.value, start.isoformat()),
        ).fetchone()
        if row is None:
            return 0
        return int(row["total"] or 0)

    def _emit_event(self, event_type: EventType, *, account_email: str, payload: dict) -> None:
        event = EventV1(
            event_type=event_type,
            ts_utc=datetime.now(timezone.utc).timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload=payload,
        )
        self._emitter.emit(event)


def _period_start(period: BudgetPeriod) -> datetime:
    now = datetime.now(timezone.utc)
    if period == BudgetPeriod.DAILY:
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == BudgetPeriod.MONTHLY:
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


def _parse_dt(raw: object) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw))
    except ValueError:
        return None


__all__ = ["BudgetGate", "BudgetGateConfig"]
