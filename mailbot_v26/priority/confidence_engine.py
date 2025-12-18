from __future__ import annotations

from typing import Any


PRIORITY_ORDER = {"🔵": 0, "🟡": 1, "🔴": 2}


class PriorityConfidenceEngine:
    def score(
        self,
        *,
        llm_priority: str,
        shadow_priority: str,
        sender_stats: dict[str, Any],
        recent_history: dict[str, Any],
    ) -> float:
        """
        Возвращает confidence ∈ [0.0, 1.0]
        """

        if PRIORITY_ORDER.get(shadow_priority, 0) <= PRIORITY_ORDER.get(
            llm_priority, 0
        ):
            return 0.0

        score = 0.0

        red_count = int(sender_stats.get("red_count") or 0)
        emails_total = int(sender_stats.get("emails_total") or 0)
        if red_count >= 3 or (emails_total and red_count / max(emails_total, 1) >= 0.5):
            score += 0.3

        escalations = int(recent_history.get("escalations") or 0)
        if escalations > 0:
            score += 0.2

        if bool(recent_history.get("is_trending_up")):
            score += 0.2

        underestimate_rate = float(sender_stats.get("llm_underestimate_rate") or 0.0)
        underestimate_count = int(sender_stats.get("llm_underestimation_count") or 0)
        if bool(sender_stats.get("llm_underestimates_often")) or underestimate_rate >= 0.3:
            score += 0.3
        elif underestimate_count >= 3:
            score += 0.3

        return max(0.0, min(1.0, score))


__all__ = ["PriorityConfidenceEngine"]
