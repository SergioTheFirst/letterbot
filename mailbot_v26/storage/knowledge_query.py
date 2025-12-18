from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


class KnowledgeQuery:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _connect_readonly(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)

    def _execute_select(
        self, query: str, params: Iterable[object] | None = None
    ) -> list[dict[str, object]]:
        try:
            with self._connect_readonly() as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA query_only=ON;")
                cur = conn.execute(query, tuple(params or ()))
                return [dict(row) for row in cur.fetchall()]
        except sqlite3.OperationalError:
            logger.warning("KnowledgeQuery: database unavailable for %s", self.path)
            return []
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("KnowledgeQuery failed: %s", exc)
            return []

    def top_senders(self, limit: int = 10) -> list[dict]:
        rows = self._execute_select(
            """
            SELECT
                sender_email AS from_email,
                emails_total AS total_emails,
                red_count,
                yellow_count,
                blue_count
            FROM v_sender_stats
            ORDER BY emails_total DESC, sender_email ASC
            LIMIT ?
            """,
            (limit,),
        )
        return [
            {
                "from_email": row.get("from_email"),
                "total_emails": int(row.get("total_emails", 0) or 0),
                "red_count": int(row.get("red_count", 0) or 0),
                "yellow_count": int(row.get("yellow_count", 0) or 0),
                "blue_count": int(row.get("blue_count", 0) or 0),
            }
            for row in rows
        ]

    def priority_distribution(self) -> dict:
        result = {"🔴": 0, "🟡": 0, "🔵": 0}
        rows = self._execute_select(
            """
            SELECT priority, COUNT(*) AS total
            FROM emails
            WHERE priority IN ('🔴', '🟡', '🔵')
            GROUP BY priority
            """
        )
        for row in rows:
            priority = row.get("priority")
            if priority in result:
                result[priority] = int(row.get("total", 0) or 0)
        return result

    def shadow_vs_llm_stats(self) -> dict:
        rows = self._execute_select(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN shadow_priority != priority THEN 1 ELSE 0 END) AS diff_count,
                SUM(
                    CASE
                        WHEN shadow_priority IN ('🔴', '🟡', '🔵')
                             AND priority IN ('🔴', '🟡', '🔵')
                             AND (
                                CASE shadow_priority
                                    WHEN '🔴' THEN 2
                                    WHEN '🟡' THEN 1
                                    ELSE 0
                                END
                                >
                                CASE priority
                                    WHEN '🔴' THEN 2
                                    WHEN '🟡' THEN 1
                                    ELSE 0
                                END
                             )
                        THEN 1 ELSE 0 END
                ) AS higher_count
            FROM emails
            WHERE TRIM(COALESCE(shadow_priority, '')) != ''
              AND TRIM(COALESCE(priority, '')) != ''
            """
        )
        totals = rows[0] if rows else {"total": 0, "diff_count": 0, "higher_count": 0}

        total = int(totals.get("total", 0) or 0)
        diff_count = int(totals.get("diff_count", 0) or 0)
        higher_count = int(totals.get("higher_count", 0) or 0)

        def _pct(part: int, whole: int) -> float:
            if whole <= 0:
                return 0.0
            return round((part / whole) * 100, 2)

        return {
            "total": total,
            "shadow_diff_pct": _pct(diff_count, total),
            "shadow_higher_pct": _pct(higher_count, total),
        }

    def recent_actions(self, limit: int = 10) -> list[dict]:
        rows = self._execute_select(
            """
            SELECT
                subject,
                action_line,
                priority,
                created_at
            FROM emails
            WHERE TRIM(COALESCE(action_line, '')) != ''
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [
            {
                "subject": row.get("subject"),
                "action_line": row.get("action_line"),
                "priority": row.get("priority"),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ]


__all__ = ["KnowledgeQuery"]
