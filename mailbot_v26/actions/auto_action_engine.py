from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AutoActionEngine:
    confidence_threshold: float

    def propose(
        self,
        *,
        llm_action_line: str,
        shadow_action: str | None,
        priority: str,
        confidence: float,
    ) -> dict | None:
        """Возвращает proposed_action или None."""

        if confidence < self.confidence_threshold:
            return None

        if priority != "🔴":
            return None

        if not shadow_action:
            return None

        normalized_shadow = shadow_action.strip()
        normalized_llm = (llm_action_line or "").strip()

        if normalized_shadow == normalized_llm:
            return None

        action_type = self._detect_type(normalized_shadow)
        return {
            "type": action_type,
            "text": normalized_shadow,
            "source": "shadow",
            "confidence": confidence,
        }

    def _detect_type(self, action_text: str) -> str:
        lowered = action_text.lower()
        if any(keyword in lowered for keyword in ("оплат", "invoice", "счёт", "счет")):
            return "PAYMENT"
        if any(keyword in lowered for keyword in ("review", "провер", "проверь")):
            return "REVIEW"
        return "FOLLOW_UP"


__all__ = ["AutoActionEngine"]
