from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from mailbot_v26.budgets.contract import BudgetType
from mailbot_v26.budgets.gate import BudgetGate


@dataclass(frozen=True, slots=True)
class BudgetConsumption:
    """EN: Budget consumption details. RU: Детали расхода бюджета."""

    tokens_used: int
    reason: str


class BudgetConsumer:
    """EN: Logs budget usage. RU: Фиксирует расход бюджета."""

    def __init__(self, gate: BudgetGate) -> None:
        self._gate = gate

    def on_llm_call(
        self,
        *,
        account_email: str,
        tokens_used: Optional[int],
        input_chars: int,
        model: str,
        success: bool,
    ) -> bool:
        """EN: Handle LLM usage. RU: Учёт LLM вызова."""

        if not success:
            return False
        resolved_tokens = _resolve_tokens(tokens_used, input_chars)
        if resolved_tokens <= 0:
            return False
        reason = f"llm_call:{model or 'gigachat'}"
        return self._gate.consume_budget(
            account_email,
            BudgetType.LLM_TOKENS,
            resolved_tokens,
            reason=reason,
        )


def _resolve_tokens(tokens_used: Optional[int], input_chars: int) -> int:
    if tokens_used is not None and tokens_used > 0:
        return int(tokens_used)
    if input_chars <= 0:
        return 0
    return max(1, int(input_chars / 4))


__all__ = ["BudgetConsumer", "BudgetConsumption"]
