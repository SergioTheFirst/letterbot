from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class PriorityEngine:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)

    def adjust_priority(
        self,
        *,
        llm_priority: str,
        from_email: str,
        received_at: datetime,
    ) -> tuple[str, str | None]:
        """
        Passive, read-only priority adjustment based on recent history.

        Rules:
        - 🔴 stays 🔴.
        - 🟡 → 🔴 if there are at least 3 🔴 emails from the sender within 30 days.
        - 🔵 → 🟡 if there are at least 2 🟡/🔴 emails from the sender within 14 days.

        Fallback: on any DB issue returns the original llm_priority.
        """

        final_priority = llm_priority
        priority_reason: str | None = None

        if llm_priority == "🔴" or not from_email:
            return final_priority, priority_reason

        try:
            with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True) as conn:
                if llm_priority == "🟡":
                    if self._count_red_recent(conn, from_email, received_at) >= 3:
                        final_priority = "🔴"
                        priority_reason = (
                            "Повышен до 🔴: 3+ писем с 🔴 от отправителя за 30 дней"
                        )

                if llm_priority == "🔵":
                    if self._count_hot_recent(conn, from_email, received_at) >= 2:
                        final_priority = "🟡"
                        priority_reason = (
                            "Повышен до 🟡: 2+ писем с 🟡/🔴 от отправителя за 14 дней"
                        )

        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "PriorityEngine failed to read history: %s", exc, exc_info=True
            )

        return final_priority, priority_reason

    def _count_red_recent(
        self, conn: sqlite3.Connection, from_email: str, received_at: datetime
    ) -> int:
        since = (received_at - timedelta(days=30)).isoformat()
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM emails
            WHERE from_email = ?
              AND priority = '🔴'
              AND received_at >= ?
            """,
            (from_email, since),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def _count_hot_recent(
        self, conn: sqlite3.Connection, from_email: str, received_at: datetime
    ) -> int:
        since = (received_at - timedelta(days=14)).isoformat()
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM emails
            WHERE from_email = ?
              AND priority IN ('🟡', '🔴')
              AND received_at >= ?
            """,
            (from_email, since),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
