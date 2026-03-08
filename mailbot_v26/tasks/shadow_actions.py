from __future__ import annotations

import logging
from typing import Any

from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = logging.getLogger(__name__)


class ShadowActionEngine:
    """
    Dry-run task detector that relies solely on the analytics layer.

    It never writes to the database and only surfaces hypothetical
    actions that could be taken based on historical patterns.
    """

    def __init__(self, analytics: KnowledgeAnalytics) -> None:
        self.analytics = analytics

    def compute(self, *, account_email: str, from_email: str) -> list[tuple[str, str]]:
        tasks: list[tuple[str, str]] = []
        try:
            sender_row = self._find_sender_row(from_email)
            account_row = self._find_account_row(account_email)
            latest_escalation = self._latest_escalation(account_email, from_email)

            if sender_row:
                red_count = int(sender_row.get("red_count") or 0)
                yellow_count = int(sender_row.get("yellow_count") or 0)
                escalations = int(sender_row.get("escalations") or 0)
                hot_total = red_count + yellow_count

                if red_count >= 3:
                    tasks.append(
                        (
                            f"Усилить контроль переписки с {from_email}",
                            "3+ писем с 🔴 от отправителя (analytics)",
                        )
                    )
                if escalations >= 2:
                    tasks.append(
                        (
                            f"Проверить частые эскалации с {from_email}",
                            "2+ эскалации по отправителю (analytics)",
                        )
                    )
                if hot_total >= 5:
                    tasks.append(
                        (
                            f"Запланировать звонок отправителю {from_email}",
                            "5+ писем с 🟡/🔴 от отправителя (analytics)",
                        )
                    )

            if account_row:
                account_escalations = int(account_row.get("escalations") or 0)
                red_count = int(account_row.get("red_count") or 0)

                if account_escalations >= 3:
                    tasks.append(
                        (
                            f"Согласовать план по аккаунту {account_email}",
                            "3+ эскалации в аккаунте (analytics)",
                        )
                    )
                elif red_count >= 5:
                    tasks.append(
                        (
                            f"Проверить SLA по аккаунту {account_email}",
                            "5+ писем с 🔴 в аккаунте (analytics)",
                        )
                    )

            if latest_escalation:
                subject = (
                    latest_escalation.get("subject") or ""
                ).strip() or "(без темы)"
                sender = (
                    latest_escalation.get("from_email") or from_email or ""
                ).strip()
                tasks.append(
                    (
                        f"Держать эскалацию в фокусе: {subject}",
                        f"Свежая эскалация от {sender or 'неизвестно'} (analytics)",
                    )
                )

        except Exception as exc:  # pragma: no cover - defensive
            logger.error(
                "ShadowActionEngine failed to read analytics: %s", exc, exc_info=True
            )

        return tasks

    def _find_sender_row(self, from_email: str) -> dict[str, Any] | None:
        normalized = (from_email or "").strip().lower()
        if not normalized:
            return None

        for row in self.analytics.sender_stats():
            sender = str(row.get("sender_email") or "").strip().lower()
            if sender == normalized:
                return row
        return None

    def _find_account_row(self, account_email: str) -> dict[str, Any] | None:
        normalized = (account_email or "").strip().lower()
        if not normalized:
            return None

        for row in self.analytics.account_stats():
            account = str(row.get("account_email") or "").strip().lower()
            if account == normalized:
                return row
        return None

    def _latest_escalation(
        self,
        account_email: str,
        from_email: str,
    ) -> dict[str, Any] | None:
        normalized_account = (account_email or "").strip().lower()
        normalized_sender = (from_email or "").strip().lower()

        for row in self.analytics.priority_escalations():
            account_match = (
                normalized_account
                and str(row.get("account_email") or "").strip().lower()
                == normalized_account
            )
            sender_match = (
                normalized_sender
                and str(row.get("from_email") or "").strip().lower()
                == normalized_sender
            )
            if account_match or sender_match:
                return row
        return None


__all__ = ["ShadowActionEngine"]
