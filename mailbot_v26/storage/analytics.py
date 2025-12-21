from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


class KnowledgeAnalytics:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def _connect_readonly(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)

    def _execute_select(self, query: str, params: Iterable[object] | None = None) -> list[dict[str, object]]:
        with self._connect_readonly() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(query, tuple(params or ()))
            return [dict(row) for row in cur.fetchall()]

    def sender_stats(self, limit: int | None = None) -> list[dict[str, object]]:
        query = """
        SELECT
            sender_email,
            emails_total,
            account_count,
            red_count,
            yellow_count,
            blue_count,
            escalations,
            first_received_at,
            last_received_at
        FROM v_sender_stats
        ORDER BY emails_total DESC, sender_email ASC
        """
        params: list[object] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        return self._execute_select(query, params)

    def account_stats(self, limit: int | None = None) -> list[dict[str, object]]:
        query = """
        SELECT
            account_email,
            emails_total,
            sender_count,
            red_count,
            yellow_count,
            blue_count,
            escalations,
            first_received_at,
            last_received_at
        FROM v_account_stats
        ORDER BY emails_total DESC, account_email ASC
        """
        params: list[object] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        return self._execute_select(query, params)

    def priority_escalations(self, limit: int | None = None) -> list[dict[str, object]]:
        query = """
        SELECT
            email_id,
            account_email,
            from_email,
            subject,
            received_at,
            priority,
            priority_reason,
            created_at
        FROM v_priority_escalations
        ORDER BY received_at DESC
        """
        params: list[object] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        return self._execute_select(query, params)

    def commitment_stats_by_sender(
        self,
        *,
        from_email: str,
        days: int = 30,
    ) -> dict[str, int]:
        if not from_email:
            return {
                "total_commitments": 0,
                "fulfilled_count": 0,
                "expired_count": 0,
                "unknown_count": 0,
            }
        query = """
        SELECT
            COUNT(*) AS total_commitments,
            SUM(CASE WHEN c.status = 'fulfilled' THEN 1 ELSE 0 END) AS fulfilled_count,
            SUM(CASE WHEN c.status = 'expired' THEN 1 ELSE 0 END) AS expired_count,
            SUM(CASE WHEN c.status = 'unknown' THEN 1 ELSE 0 END) AS unknown_count
        FROM commitments c
        JOIN emails e ON e.id = c.email_row_id
        WHERE lower(e.from_email) = lower(?)
          AND c.created_at >= datetime('now', ?)
        """
        window = f"-{days} days"
        rows = self._execute_select(query, (from_email, window))
        row = rows[0] if rows else {}
        return {
            "total_commitments": int(row.get("total_commitments") or 0),
            "fulfilled_count": int(row.get("fulfilled_count") or 0),
            "expired_count": int(row.get("expired_count") or 0),
            "unknown_count": int(row.get("unknown_count") or 0),
        }

    def shadow_accuracy(self, *, days: int) -> dict[str, float | int]:
        query = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN shadow_priority = priority THEN 1 ELSE 0 END) AS match_count
        FROM emails
        WHERE TRIM(COALESCE(shadow_priority, '')) != ''
          AND TRIM(COALESCE(priority, '')) != ''
          AND created_at >= datetime('now', ?)
        """
        rows = self._execute_select(query, (f"-{days} days",))
        stats = rows[0] if rows else {"total": 0, "match_count": 0}
        total = int(stats.get("total", 0) or 0)
        match_count = int(stats.get("match_count", 0) or 0)
        accuracy = (match_count / total) if total > 0 else 0.0
        return {"total": total, "accuracy": accuracy}

    def auto_priority_reject_rate(
        self, *, days: int | None = None, hours: int | None = None
    ) -> dict[str, float | int]:
        window = "7 days"
        if hours is not None:
            window = f"{hours} hours"
        elif days is not None:
            window = f"{days} days"
        query = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN confidence_decision = 'SKIPPED' THEN 1 ELSE 0 END) AS rejected
        FROM emails
        WHERE confidence_decision IN ('APPLIED', 'SKIPPED')
          AND created_at >= datetime('now', ?)
        """
        rows = self._execute_select(query, (f"-{window}",))
        stats = rows[0] if rows else {"total": 0, "rejected": 0}
        total = int(stats.get("total", 0) or 0)
        rejected = int(stats.get("rejected", 0) or 0)
        rate = (rejected / total) if total > 0 else 0.0
        return {"total": total, "reject_rate": rate}

    def auto_priority_confidence_scores(self, *, hours: int) -> list[float]:
        query = """
        SELECT confidence_score
        FROM emails
        WHERE confidence_score IS NOT NULL
          AND created_at >= datetime('now', ?)
        """
        rows = self._execute_select(query, (f"-{hours} hours",))
        scores: list[float] = []
        for row in rows:
            value = row.get("confidence_score")
            if value is None:
                continue
            try:
                scores.append(float(value))
            except (TypeError, ValueError):
                continue
        return scores

    def interaction_event_times(
        self,
        *,
        entity_id: str,
        event_type: str,
        days: int,
    ) -> list[datetime]:
        if not entity_id:
            return []
        query = """
        SELECT event_time
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
          AND event_time >= datetime('now', ?)
        ORDER BY event_time ASC
        """
        rows = self._execute_select(query, (entity_id, event_type, f"-{days} days"))
        timestamps: list[datetime] = []
        for row in rows:
            value = row.get("event_time")
            if not value:
                continue
            try:
                timestamps.append(datetime.fromisoformat(str(value)))
            except ValueError:
                continue
        return timestamps

    def interaction_event_counts(
        self,
        *,
        entity_id: str,
        event_type: str,
        recent_days: int,
        previous_days: int,
    ) -> dict[str, int]:
        if not entity_id:
            return {"recent": 0, "previous": 0}
        query = """
        SELECT
            SUM(CASE WHEN event_time >= datetime('now', ?) THEN 1 ELSE 0 END) AS recent,
            SUM(
                CASE
                    WHEN event_time >= datetime('now', ?)
                     AND event_time < datetime('now', ?)
                    THEN 1
                    ELSE 0
                END
            ) AS previous
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
        """
        rows = self._execute_select(
            query,
            (
                f"-{recent_days} days",
                f"-{recent_days + previous_days} days",
                f"-{recent_days} days",
                entity_id,
                event_type,
            ),
        )
        row = rows[0] if rows else {}
        return {
            "recent": int(row.get("recent") or 0),
            "previous": int(row.get("previous") or 0),
        }
