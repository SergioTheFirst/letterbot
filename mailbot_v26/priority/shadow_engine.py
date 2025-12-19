from __future__ import annotations

import logging
from typing import Any

from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = logging.getLogger(__name__)


class ShadowPriorityEngine:
    """
    Dry-run priority evaluator that relies solely on the analytics layer.

    It never writes to the database and only returns a reason when
    the computed shadow priority differs from the current one.
    """

    def __init__(self, analytics: KnowledgeAnalytics) -> None:
        self.analytics = analytics

    def compute(
        self,
        *,
        llm_priority: str,
        from_email: str,
    ) -> tuple[str, str | None]:
        shadow_priority = llm_priority
        reason: str | None = None

        if llm_priority == "🔴" or not from_email:
            return shadow_priority, reason

        try:
            stats = self._fetch_sender_stats(from_email)
            if not stats:
                return shadow_priority, reason

            red_count = int(stats.get("red_count") or 0)
            yellow_count = int(stats.get("yellow_count") or 0)
            hot_total = red_count + yellow_count

            if llm_priority == "🟡" and red_count >= 3:
                shadow_priority = "🔴"
                reason = "Повышен до 🔴: 3+ писем с 🔴 от отправителя (analytics)"
            elif llm_priority == "🔵" and hot_total >= 2:
                shadow_priority = "🟡"
                reason = "Повышен до 🟡: 2+ писем с 🟡/🔴 от отправителя (analytics)"

        except Exception as exc:  # pragma: no cover - defensive
            logger.error("ShadowPriorityEngine failed to read analytics: %s", exc, exc_info=True)

        return shadow_priority, reason

    def _fetch_sender_stats(self, from_email: str) -> dict[str, Any] | None:
        normalized = (from_email or "").strip().lower()
        if not normalized:
            return None

        stats = self.analytics.sender_stats()
        for row in stats:
            sender = str(row.get("sender_email") or "").strip().lower()
            if sender == normalized:
                return row
        return None
