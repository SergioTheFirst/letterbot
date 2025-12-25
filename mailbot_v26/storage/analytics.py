from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime

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

    def latest_interaction_event_time(
        self,
        *,
        entity_id: str,
        event_type: str,
    ) -> datetime | None:
        if not entity_id:
            return None
        query = """
        SELECT event_time
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
        ORDER BY event_time DESC
        LIMIT 1
        """
        rows = self._execute_select(query, (entity_id, event_type))
        if not rows:
            return None
        value = rows[0].get("event_time")
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

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

    def interaction_event_count(
        self,
        *,
        entity_id: str,
        event_type: str,
        days: int,
    ) -> int:
        if not entity_id:
            return 0
        query = """
        SELECT COUNT(*) AS total
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
          AND event_time >= datetime('now', ?)
        """
        rows = self._execute_select(query, (entity_id, event_type, f"-{days} days"))
        row = rows[0] if rows else {}
        return int(row.get("total") or 0)

    def interaction_event_response_times(
        self,
        *,
        entity_id: str,
        event_type: str,
        days: int,
        metadata_key: str = "response_time_hours",
    ) -> list[float]:
        if not entity_id:
            return []
        query = """
        SELECT metadata
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
          AND event_time >= datetime('now', ?)
        """
        rows = self._execute_select(query, (entity_id, event_type, f"-{days} days"))
        values: list[float] = []
        for row in rows:
            raw = row.get("metadata")
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            value = payload.get(metadata_key)
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if number >= 0:
                values.append(number)
        return values

    def get_avg_response_time(
        self,
        *,
        entity_id: str,
        window: int = 30,
        end_dt: datetime | None = None,
    ) -> dict[str, float | int | None]:
        if not entity_id:
            return {"avg_hours": None, "sample_size": 0}
        params: list[object] = [entity_id, "response_time"]
        query = """
        SELECT metadata
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
          AND event_time >= datetime(?, ?)
        """
        anchor = end_dt or datetime.utcnow()
        params.extend([anchor.isoformat(), f"-{window} days"])
        if end_dt is not None:
            query += " AND event_time < ?"
            params.append(end_dt.isoformat())
        rows = self._execute_select(query, params)
        values: list[float] = []
        for row in rows:
            raw = row.get("metadata")
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            value = payload.get("response_time_hours")
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if number >= 0:
                values.append(number)
        if not values:
            return {"avg_hours": None, "sample_size": 0}
        avg = sum(values) / len(values)
        return {"avg_hours": avg, "sample_size": len(values)}

    def get_latest_response_time(
        self,
        *,
        entity_id: str,
        now_dt: datetime | None = None,
    ) -> dict[str, object] | None:
        if not entity_id:
            return None
        params: list[object] = [entity_id, "response_time"]
        query = """
        SELECT event_time, metadata
        FROM interaction_events
        WHERE entity_id = ?
          AND event_type = ?
        """
        if now_dt is not None:
            query += " AND event_time <= ?"
            params.append(now_dt.isoformat())
        query += " ORDER BY event_time DESC LIMIT 1"
        rows = self._execute_select(query, params)
        if not rows:
            return None
        row = rows[0]
        event_time_raw = row.get("event_time")
        if not event_time_raw:
            return None
        event_time = parse_sqlite_datetime(str(event_time_raw))
        if event_time is None:
            return None
        raw = row.get("metadata")
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return None
        value = payload.get("response_time_hours")
        try:
            response_time = float(value)
        except (TypeError, ValueError):
            return None
        return {"event_time": event_time, "response_time_hours": response_time}

    def get_rolling_frequency(
        self,
        *,
        entity_id: str,
        window_short: int = 7,
        window_long: int = 30,
        now_dt: datetime | None = None,
    ) -> dict[str, int]:
        if not entity_id:
            return {"count_short": 0, "count_long": 0, "history_days": 0}
        anchor = now_dt or datetime.utcnow()
        rows = self._execute_select(
            """
            SELECT
                SUM(CASE WHEN event_time >= datetime(?, ?) THEN 1 ELSE 0 END) AS count_short,
                SUM(CASE WHEN event_time >= datetime(?, ?) THEN 1 ELSE 0 END) AS count_long
            FROM interaction_events
            WHERE entity_id = ?
              AND event_type = 'email_received'
            """,
            (
                anchor.isoformat(),
                f"-{window_short} days",
                anchor.isoformat(),
                f"-{window_long} days",
                entity_id,
            ),
        )
        row = rows[0] if rows else {}
        history_rows = self._execute_select(
            """
            SELECT MIN(event_time) AS first_seen
            FROM interaction_events
            WHERE entity_id = ?
              AND event_type = 'email_received'
            """,
            (entity_id,),
        )
        history_days = 0
        first_seen_raw = history_rows[0].get("first_seen") if history_rows else None
        if first_seen_raw:
            first_seen = parse_sqlite_datetime(str(first_seen_raw))
            if first_seen is not None:
                history_days = max(0, int((anchor - first_seen).days))
        return {
            "count_short": int(row.get("count_short") or 0),
            "count_long": int(row.get("count_long") or 0),
            "history_days": history_days,
        }

    def get_upcoming_commitments(
        self,
        *,
        entity_id: str,
        hours: int = 48,
        now_dt: datetime | None = None,
    ) -> list[dict[str, object]]:
        if not entity_id:
            return []
        from_email = self._entity_from_email(entity_id)
        if not from_email:
            return []
        anchor = now_dt or datetime.utcnow()
        rows = self._execute_select(
            """
            SELECT
                c.id AS commitment_id,
                c.commitment_text,
                c.deadline_iso,
                c.status,
                c.created_at
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            WHERE lower(e.from_email) = lower(?)
              AND c.deadline_iso IS NOT NULL
              AND c.status NOT IN ('fulfilled', 'expired')
              AND datetime(c.deadline_iso) >= datetime(?)
              AND datetime(c.deadline_iso) <= datetime(?, ?)
            ORDER BY c.deadline_iso ASC
            """,
            (
                from_email,
                anchor.isoformat(),
                anchor.isoformat(),
                f"+{hours} hours",
            ),
        )
        return rows

    def recent_entity_activity(
        self,
        *,
        days: int = 7,
        limit: int = 5,
    ) -> list[dict[str, object]]:
        rows = self._execute_select(
            """
            SELECT entity_id, COUNT(*) AS total
            FROM interaction_events
            WHERE event_type = 'email_received'
              AND event_time >= datetime('now', ?)
            GROUP BY entity_id
            ORDER BY total DESC
            LIMIT ?
            """,
            (f"-{days} days", limit),
        )
        return rows

    def entity_label(self, *, entity_id: str) -> str | None:
        if not entity_id:
            return None
        rows = self._execute_select(
            "SELECT name, metadata FROM entities WHERE id = ? LIMIT 1",
            (entity_id,),
        )
        if not rows:
            return None
        row = rows[0]
        name = str(row.get("name") or "").strip()
        if name:
            return name
        raw = row.get("metadata")
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return None
        for key in ("from_name", "from_email"):
            value = payload.get(key)
            if value:
                cleaned = str(value).strip()
                if cleaned:
                    return cleaned
        return None

    def _entity_from_email(self, entity_id: str) -> str | None:
        rows = self._execute_select(
            "SELECT metadata FROM entities WHERE id = ? LIMIT 1",
            (entity_id,),
        )
        if not rows:
            return None
        raw = rows[0].get("metadata")
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return None
        email = payload.get("from_email")
        if not email:
            return None
        cleaned = str(email).strip()
        return cleaned or None

    def entity_baseline(
        self,
        *,
        entity_id: str,
        metric: str,
    ) -> dict[str, float | int | None]:
        if not entity_id:
            return {"baseline_value": None, "sample_size": 0}
        query = """
        SELECT baseline_value, sample_size
        FROM entity_baselines
        WHERE entity_id = ?
          AND metric = ?
        LIMIT 1
        """
        rows = self._execute_select(query, (entity_id, metric))
        row = rows[0] if rows else {}
        return {
            "baseline_value": row.get("baseline_value"),
            "sample_size": int(row.get("sample_size") or 0),
        }

    def pending_commitments_with_deadline(
        self,
        *,
        from_email: str,
        days_ahead: int | None = None,
    ) -> list[dict[str, object]]:
        if not from_email:
            return []
        query = """
        SELECT
            c.id AS commitment_id,
            c.commitment_text,
            c.deadline_iso,
            c.status,
            c.created_at
        FROM commitments c
        JOIN emails e ON e.id = c.email_row_id
        WHERE lower(e.from_email) = lower(?)
          AND c.deadline_iso IS NOT NULL
          AND c.status NOT IN ('fulfilled', 'expired')
        """
        params: list[object] = [from_email]
        if days_ahead is not None:
            query += " AND date(c.deadline_iso) <= date('now', ?)"
            params.append(f"+{days_ahead} days")
        query += " ORDER BY c.deadline_iso ASC"
        return self._execute_select(query, params)

    def deferred_digest_counts(self, *, account_email: str) -> dict[str, int]:
        if not account_email:
            return {
                "total": 0,
                "attachments_only": 0,
                "informational": 0,
            }
        total_rows = self._execute_select(
            """
            SELECT COUNT(*) AS total
            FROM emails
            WHERE deferred_for_digest = 1
              AND account_email = ?
            """,
            (account_email,),
        )
        total = int(total_rows[0].get("total") or 0) if total_rows else 0
        attachments_rows = self._execute_select(
            """
            SELECT COUNT(DISTINCT e.id) AS attachments_only
            FROM emails e
            JOIN attachments a ON a.email_id = e.id
            WHERE e.deferred_for_digest = 1
              AND e.account_email = ?
              AND COALESCE(e.raw_body_hash, '') = ''
            """,
            (account_email,),
        )
        attachments_only = (
            int(attachments_rows[0].get("attachments_only") or 0)
            if attachments_rows
            else 0
        )
        informational = max(total - attachments_only, 0)
        return {
            "total": total,
            "attachments_only": attachments_only,
            "informational": informational,
        }

    def commitment_status_counts(self, *, account_email: str) -> dict[str, int]:
        if not account_email:
            return {"pending": 0, "expired": 0}
        rows = self._execute_select(
            """
            SELECT
                SUM(CASE WHEN c.status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN c.status = 'expired' THEN 1 ELSE 0 END) AS expired_count
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            WHERE e.account_email = ?
            """,
            (account_email,),
        )
        row = rows[0] if rows else {}
        return {
            "pending": int(row.get("pending_count") or 0),
            "expired": int(row.get("expired_count") or 0),
        }

    def weekly_email_volume(self, *, account_email: str, days: int = 7) -> dict[str, int]:
        if not account_email:
            return {"total": 0, "deferred": 0}
        rows = self._execute_select(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN deferred_for_digest = 1 THEN 1 ELSE 0 END) AS deferred
            FROM emails
            WHERE account_email = ?
              AND created_at >= datetime('now', ?)
            """,
            (account_email, f"-{days} days"),
        )
        row = rows[0] if rows else {}
        return {
            "total": int(row.get("total") or 0),
            "deferred": int(row.get("deferred") or 0),
        }

    def weekly_attention_entities(
        self, *, account_email: str, days: int = 7
    ) -> list[dict[str, object]]:
        if not account_email:
            return []
        rows = self._execute_select(
            """
            SELECT from_email, subject, body_summary
            FROM emails
            WHERE account_email = ?
              AND created_at >= datetime('now', ?)
            """,
            (account_email, f"-{days} days"),
        )
        totals: dict[str, int] = {}
        for row in rows:
            sender = str(row.get("from_email") or "").strip()
            if not sender:
                continue
            summary = str(row.get("body_summary") or "").strip()
            subject = str(row.get("subject") or "").strip()
            text = summary or subject
            if not text:
                continue
            words = re.findall(r"\b\w+\b", text)
            totals[sender] = totals.get(sender, 0) + len(words)
        results = [
            {"entity": sender, "words": words}
            for sender, words in totals.items()
        ]
        results.sort(key=lambda item: (-int(item["words"]), str(item["entity"]).lower()))
        return results

    def weekly_commitment_counts(
        self, *, account_email: str, days: int = 7
    ) -> dict[str, int]:
        if not account_email:
            return {"created": 0, "fulfilled": 0, "overdue": 0}
        rows = self._execute_select(
            """
            SELECT
                COUNT(*) AS created_count,
                SUM(CASE WHEN c.status = 'fulfilled' THEN 1 ELSE 0 END) AS fulfilled_count,
                SUM(CASE WHEN c.status = 'expired' THEN 1 ELSE 0 END) AS expired_count
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            WHERE e.account_email = ?
              AND c.created_at >= datetime('now', ?)
            """,
            (account_email, f"-{days} days"),
        )
        row = rows[0] if rows else {}
        return {
            "created": int(row.get("created_count") or 0),
            "fulfilled": int(row.get("fulfilled_count") or 0),
            "overdue": int(row.get("expired_count") or 0),
        }

    def weekly_overdue_commitments(
        self, *, account_email: str, days: int = 7, limit: int = 5
    ) -> list[dict[str, object]]:
        if not account_email:
            return []
        rows = self._execute_select(
            """
            SELECT e.from_email, c.commitment_text, c.deadline_iso
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            WHERE e.account_email = ?
              AND c.status = 'expired'
              AND c.deadline_iso IS NOT NULL
              AND date(c.deadline_iso) >= date('now', ?)
            ORDER BY date(c.deadline_iso) ASC, lower(e.from_email) ASC
            LIMIT ?
            """,
            (account_email, f"-{days} days", limit),
        )
        return rows

    def weekly_trust_score_deltas(
        self, *, days: int = 7
    ) -> dict[str, list[dict[str, object]]]:
        try:
            rows = self._execute_select(
                """
                SELECT
                    t.entity_id,
                    t.trust_score,
                    t.created_at,
                    e.name AS entity_name
                FROM trust_snapshots t
                LEFT JOIN entities e ON e.id = t.entity_id
                WHERE t.trust_score IS NOT NULL
                  AND t.created_at >= datetime('now', ?)
                ORDER BY t.entity_id ASC, t.created_at ASC
                """,
                (f"-{days} days",),
            )
        except sqlite3.OperationalError:
            return {"up": [], "down": []}

        per_entity: dict[str, list[tuple[datetime, float, str]]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            score_raw = row.get("trust_score")
            try:
                score = float(score_raw)
            except (TypeError, ValueError):
                continue
            created_at = parse_sqlite_datetime(str(row.get("created_at") or ""))
            if created_at is None:
                continue
            name = str(row.get("entity_name") or entity_id)
            per_entity.setdefault(entity_id, []).append((created_at, score, name))

        deltas: list[dict[str, object]] = []
        for entity_id, entries in per_entity.items():
            if len(entries) < 2:
                continue
            entries.sort(key=lambda item: item[0])
            first = entries[0]
            last = entries[-1]
            delta = last[1] - first[1]
            deltas.append(
                {
                    "entity_id": entity_id,
                    "entity_name": last[2],
                    "delta": delta,
                }
            )

        deltas.sort(key=lambda item: float(item["delta"]), reverse=True)
        ups = [item for item in deltas if float(item["delta"]) > 0][:3]
        down_sorted = sorted(deltas, key=lambda item: float(item["delta"]))
        downs = [item for item in down_sorted if float(item["delta"]) < 0][:3]
        return {"up": ups, "down": downs}

    def latest_trust_score_delta(self, *, limit: int = 50) -> dict[str, object] | None:
        try:
            rows = self._execute_select(
                """
                SELECT entity_id, trust_score, created_at
                FROM trust_snapshots
                WHERE trust_score IS NOT NULL
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        except sqlite3.OperationalError:
            return None
        latest_by_entity: dict[str, dict[str, object]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            score_value = row.get("trust_score")
            if score_value is None:
                continue
            try:
                score = float(score_value)
            except (TypeError, ValueError):
                continue
            created_at = parse_sqlite_datetime(str(row.get("created_at") or "")) or None
            if entity_id not in latest_by_entity:
                latest_by_entity[entity_id] = {
                    "current_score": score,
                    "current_at": created_at,
                }
                continue
            previous = latest_by_entity[entity_id]
            previous_score = float(previous["current_score"])
            delta = score - previous_score
            return {
                "entity_id": entity_id,
                "current_score": score,
                "previous_score": previous_score,
                "delta": delta,
                "current_at": created_at,
            }
        return None

    def latest_relationship_health_delta(
        self, *, limit: int = 50
    ) -> dict[str, object] | None:
        try:
            rows = self._execute_select(
                """
                SELECT entity_id, health_score, created_at
                FROM relationship_health_snapshots
                WHERE health_score IS NOT NULL
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        except sqlite3.OperationalError:
            return None
        latest_by_entity: dict[str, dict[str, object]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            score_value = row.get("health_score")
            if score_value is None:
                continue
            try:
                score = float(score_value)
            except (TypeError, ValueError):
                continue
            created_at = parse_sqlite_datetime(str(row.get("created_at") or "")) or None
            if entity_id not in latest_by_entity:
                latest_by_entity[entity_id] = {
                    "current_score": score,
                    "current_at": created_at,
                }
                continue
            previous = latest_by_entity[entity_id]
            previous_score = float(previous["current_score"])
            delta = score - previous_score
            return {
                "entity_id": entity_id,
                "current_score": score,
                "previous_score": previous_score,
                "delta": delta,
                "current_at": created_at,
            }
        return None
