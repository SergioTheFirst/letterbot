from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
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

    def _event_payload(self, row: dict[str, object]) -> dict[str, object]:
        raw = row.get("payload_json") or row.get("payload")
        if not raw:
            return {}
        try:
            return json.loads(str(raw))
        except (TypeError, ValueError):
            return {}

    def event_payload(self, row: dict[str, object]) -> dict[str, object]:
        return self._event_payload(row)

    def _window_start_ts(self, days: int) -> float:
        anchor = datetime.now(timezone.utc).timestamp()
        return anchor - (days * 24 * 60 * 60)

    def _event_rows(
        self,
        *,
        account_id: str | None,
        event_type: str,
        since_ts: float | None = None,
    ) -> list[dict[str, object]]:
        query = """
        SELECT event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
        FROM events_v1
        WHERE event_type = ?
        """
        params: list[object] = [event_type]
        if account_id:
            query += " AND account_id = ?"
            params.append(account_id)
        if since_ts is not None:
            query += " AND ts_utc >= ?"
            params.append(since_ts)
        try:
            return self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

    def recent_email_events(
        self,
        *,
        days: int,
        now_dt: datetime | None = None,
    ) -> list[dict[str, object]]:
        threshold = (now_dt or datetime.now(timezone.utc)) - timedelta(days=days)
        try:
            rows = self._execute_select(
                """
                SELECT type, timestamp, payload
                FROM events
                WHERE type = ?
                  AND timestamp >= ?
                """,
                ("email_received", threshold.isoformat()),
            )
        except sqlite3.OperationalError:
            return []
        return rows

    def event_rows_for_entity(
        self,
        *,
        entity_id: str,
        event_type: str,
        since_ts: float | None = None,
    ) -> list[dict[str, object]]:
        if not entity_id:
            return []
        rows = self._event_rows(
            account_id=None,
            event_type=event_type,
            since_ts=since_ts,
        )
        entity_id = entity_id.strip()
        return [
            row
            for row in rows
            if str(row.get("entity_id") or "").strip() == entity_id
        ]

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

    def deferred_digest_counts(self, *, account_email: str, days: int = 1) -> dict[str, int]:
        if not account_email:
            return {
                "total": 0,
                "attachments_only": 0,
                "informational": 0,
            }
        since_ts = self._window_start_ts(days)
        rows = self._event_rows(
            account_id=account_email,
            event_type="attention_deferred_for_digest",
            since_ts=since_ts,
        )
        total = 0
        attachments_only = 0
        for row in rows:
            total += 1
            payload = self._event_payload(row)
            if payload.get("attachments_only") is True:
                attachments_only += 1
        informational = max(total - attachments_only, 0)
        return {
            "total": total,
            "attachments_only": attachments_only,
            "informational": informational,
        }

    def deferred_digest_items(
        self, *, account_email: str, limit: int = 5
    ) -> list[dict[str, str]]:
        if not account_email or limit <= 0:
            return []
        try:
            rows = self._execute_select(
                """
                SELECT subject, from_email, received_at, body_summary
                FROM emails
                WHERE account_email = ?
                  AND deferred_for_digest = 1
                ORDER BY datetime(received_at) DESC
                LIMIT ?
                """,
                (account_email, limit),
            )
        except sqlite3.OperationalError:
            return []
        items: list[dict[str, str]] = []
        for row in rows:
            subject = str(row.get("subject") or "").strip()
            sender = str(row.get("from_email") or "").strip()
            summary = str(row.get("body_summary") or "").strip()
            items.append(
                {
                    "subject": subject,
                    "sender": sender,
                    "summary": summary,
                }
            )
        return items

    def commitment_status_counts(self, *, account_email: str) -> dict[str, int]:
        if not account_email:
            return {"pending": 0, "expired": 0}
        created_rows = self._event_rows(
            account_id=account_email,
            event_type="commitment_created",
        )
        status_rows = self._event_rows(
            account_id=account_email,
            event_type="commitment_status_changed",
        )
        created_count = len(created_rows)
        expired_count = 0
        fulfilled_count = 0
        for row in status_rows:
            payload = self._event_payload(row)
            status = str(payload.get("new_status") or payload.get("status") or "").lower()
            if status == "expired":
                expired_count += 1
            elif status == "fulfilled":
                fulfilled_count += 1
        pending_count = max(created_count - expired_count - fulfilled_count, 0)
        row = {"pending_count": pending_count, "expired_count": expired_count}
        return {
            "pending": int(row.get("pending_count") or 0),
            "expired": int(row.get("expired_count") or 0),
        }

    def has_daily_digest_sent(self, *, account_email: str, day: datetime) -> bool:
        if not account_email:
            return False
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
        end = start + 24 * 60 * 60
        try:
            rows = self._execute_select(
                """
                SELECT 1
                FROM events_v1
                WHERE account_id = ?
                  AND event_type = 'daily_digest_sent'
                  AND ts_utc >= ?
                  AND ts_utc < ?
                LIMIT 1
                """,
                (account_email, start, end),
            )
        except sqlite3.OperationalError:
            return False
        return bool(rows)

    def has_weekly_digest_sent(self, *, account_email: str, week_key: str) -> bool:
        if not account_email or not week_key:
            return False
        rows = self._event_rows(
            account_id=account_email,
            event_type="weekly_digest_sent",
        )
        for row in rows:
            payload = self._event_payload(row)
            if str(payload.get("week_key") or "") == week_key:
                return True
        return False

    def weekly_email_volume(self, *, account_email: str, days: int = 7) -> dict[str, int]:
        if not account_email:
            return {"total": 0, "deferred": 0}
        since_ts = self._window_start_ts(days)
        try:
            total_rows = self._execute_select(
                """
                SELECT COUNT(*) AS total
                FROM events_v1
                WHERE account_id = ?
                  AND event_type = 'email_received'
                  AND ts_utc >= ?
                """,
                (account_email, since_ts),
            )
            deferred_rows = self._execute_select(
                """
                SELECT COUNT(*) AS total
                FROM events_v1
                WHERE account_id = ?
                  AND event_type = 'attention_deferred_for_digest'
                  AND ts_utc >= ?
                """,
                (account_email, since_ts),
            )
        except sqlite3.OperationalError:
            return {"total": 0, "deferred": 0}
        row = {
            "total": int(total_rows[0].get("total") or 0) if total_rows else 0,
            "deferred": int(deferred_rows[0].get("total") or 0) if deferred_rows else 0,
        }
        return {
            "total": int(row.get("total") or 0),
            "deferred": int(row.get("deferred") or 0),
        }

    def weekly_attention_entities(
        self, *, account_email: str, days: int = 7
    ) -> list[dict[str, object]]:
        if not account_email:
            return []
        since_ts = self._window_start_ts(days)
        rows = self._event_rows(
            account_id=account_email,
            event_type="email_received",
            since_ts=since_ts,
        )
        totals: dict[str, int] = {}
        for row in rows:
            payload = self._event_payload(row)
            sender = str(payload.get("from_email") or "").strip()
            if not sender:
                continue
            summary = str(payload.get("body_summary") or "").strip()
            subject = str(payload.get("subject") or "").strip()
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

    def attention_entity_metrics(
        self, *, account_email: str, days: int = 7
    ) -> list[dict[str, object]]:
        if not account_email:
            return []
        since_ts = self._window_start_ts(days)
        deferred_rows = self._event_rows(
            account_id=account_email,
            event_type="attention_deferred_for_digest",
            since_ts=since_ts,
        )
        deferred_ids = {
            str(row.get("email_id"))
            for row in deferred_rows
            if row.get("email_id") is not None
        }
        rows = self._event_rows(
            account_id=account_email,
            event_type="email_received",
            since_ts=since_ts,
        )
        aggregates: dict[str, dict[str, float]] = {}
        for row in rows:
            payload = self._event_payload(row)
            sender = str(payload.get("from_email") or "").strip()
            if not sender:
                continue
            entity_id = sender.lower()
            text_content = str(payload.get("body_summary") or payload.get("subject") or "").strip()
            word_count = len(re.findall(r"\b\w+\b", text_content)) if text_content else 0
            attachment_count = int(payload.get("attachments_count") or 0)
            read_minutes = 1.0 if word_count <= 0 else max(1.0, word_count / 200.0)
            read_minutes += attachment_count * 1.5
            entry = aggregates.setdefault(
                entity_id,
                {
                    "message_count": 0.0,
                    "deferred_count": 0.0,
                    "attachment_count": 0.0,
                    "estimated_read_minutes": 0.0,
                },
            )
            entry["message_count"] += 1.0
            entry["attachment_count"] += float(attachment_count)
            entry["estimated_read_minutes"] += float(read_minutes)
            if row.get("email_id") is not None and str(row.get("email_id")) in deferred_ids:
                entry["deferred_count"] += 1.0

        results: list[dict[str, object]] = []
        for entity_id, totals in aggregates.items():
            entity_id = str(entity_id or "").strip()
            if not entity_id:
                continue
            results.append(
                {
                    "entity_id": entity_id,
                    "message_count": int(totals.get("message_count") or 0),
                    "deferred_count": int(totals.get("deferred_count") or 0),
                    "attachment_count": int(totals.get("attachment_count") or 0),
                    "estimated_read_minutes": float(totals.get("estimated_read_minutes") or 0.0),
                }
            )
        results.sort(
            key=lambda item: (-float(item["estimated_read_minutes"]), str(item["entity_id"]).lower())
        )
        return results

    def weekly_commitment_counts(
        self, *, account_email: str, days: int = 7
    ) -> dict[str, int]:
        if not account_email:
            return {"created": 0, "fulfilled": 0, "overdue": 0}
        since_ts = self._window_start_ts(days)
        created_rows = self._event_rows(
            account_id=account_email,
            event_type="commitment_created",
            since_ts=since_ts,
        )
        status_rows = self._event_rows(
            account_id=account_email,
            event_type="commitment_status_changed",
            since_ts=since_ts,
        )
        fulfilled_count = 0
        expired_count = 0
        for row in status_rows:
            payload = self._event_payload(row)
            status = str(payload.get("new_status") or payload.get("status") or "").lower()
            if status == "fulfilled":
                fulfilled_count += 1
            elif status == "expired":
                expired_count += 1
        row = {
            "created_count": len(created_rows),
            "fulfilled_count": fulfilled_count,
            "expired_count": expired_count,
        }
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
        since_ts = self._window_start_ts(days)
        rows = self._event_rows(
            account_id=account_email,
            event_type="commitment_status_changed",
            since_ts=since_ts,
        )
        overdue: list[dict[str, object]] = []
        for row in rows:
            payload = self._event_payload(row)
            status = str(payload.get("new_status") or payload.get("status") or "").lower()
            if status != "expired":
                continue
            overdue.append(
                {
                    "from_email": payload.get("from_email") or "",
                    "commitment_text": payload.get("commitment_text") or "",
                    "deadline_iso": payload.get("deadline_iso") or "",
                }
            )
        overdue.sort(
            key=lambda item: (
                str(item.get("deadline_iso") or ""),
                str(item.get("from_email") or "").lower(),
            )
        )
        return overdue[:limit]

    def weekly_trust_score_deltas(
        self, *, days: int = 7
    ) -> dict[str, list[dict[str, object]]]:
        since_ts = self._window_start_ts(days)
        try:
            rows = self._execute_select(
                """
                SELECT
                    ev.entity_id,
                    ev.ts_utc,
                    ev.payload,
                    ev.payload_json,
                    e.name AS entity_name
                FROM events_v1 ev
                LEFT JOIN entities e ON e.id = ev.entity_id
                WHERE ev.event_type = 'trust_score_updated'
                  AND ev.ts_utc >= ?
                ORDER BY ev.entity_id ASC, ev.ts_utc ASC
                """,
                (since_ts,),
            )
        except sqlite3.OperationalError:
            return {"up": [], "down": []}

        per_entity: dict[str, dict[str, list[tuple[float, float, str]]]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            payload = self._event_payload(row)
            score_raw = payload.get("trust_score")
            try:
                score = float(score_raw)
            except (TypeError, ValueError):
                continue
            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc <= 0:
                continue
            name = str(row.get("entity_name") or entity_id)
            version = self._trust_version(payload)
            per_entity.setdefault(entity_id, {}).setdefault(version, []).append(
                (ts_utc, score, name)
            )

        deltas: list[dict[str, object]] = []
        for entity_id, versioned in per_entity.items():
            entries = versioned.get("v2") or versioned.get("v1") or []
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

    def trust_and_health_deltas(self, *, days: int = 7) -> dict[str, dict[str, float]]:
        since_ts = self._window_start_ts(days)
        trust_rows = self._event_rows(
            account_id=None,
            event_type="trust_score_updated",
            since_ts=since_ts,
        )
        health_rows = self._event_rows(
            account_id=None,
            event_type="relationship_health_updated",
            since_ts=since_ts,
        )
        trust_deltas = self._trust_event_deltas(trust_rows, "trust_score")
        health_deltas = self._event_deltas(health_rows, "health_score")

        all_entities = set(trust_deltas.keys()) | set(health_deltas.keys())
        combined: dict[str, dict[str, float]] = {}
        for entity_id in all_entities:
            entry: dict[str, float] = {}
            if entity_id in trust_deltas:
                entry["trust_delta"] = float(trust_deltas[entity_id])
            if entity_id in health_deltas:
                entry["health_delta"] = float(health_deltas[entity_id])
            combined[entity_id] = entry
        return combined

    def latest_trust_score_delta(self, *, limit: int = 50) -> dict[str, object] | None:
        try:
            rows = self._execute_select(
                """
                SELECT entity_id, ts_utc, payload, payload_json
                FROM events_v1
                WHERE event_type = 'trust_score_updated'
                ORDER BY ts_utc DESC
                LIMIT ?
                """,
                (limit,),
            )
        except sqlite3.OperationalError:
            return None
        versioned: dict[str, dict[str, list[tuple[float, float]]]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            payload = self._event_payload(row)
            score_value = payload.get("trust_score")
            if score_value is None:
                continue
            try:
                score = float(score_value)
            except (TypeError, ValueError):
                continue
            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc <= 0:
                continue
            version = self._trust_version(payload)
            versioned.setdefault(entity_id, {}).setdefault(version, []).append(
                (ts_utc, score)
            )

        delta_candidates = self._latest_trust_delta(versioned, version="v2")
        if not delta_candidates:
            delta_candidates = self._latest_trust_delta(versioned, version="v1")
        if not delta_candidates:
            return None

        entity_id, current_score, previous_score, current_ts = max(
            delta_candidates,
            key=lambda item: item[3],
        )
        return {
            "entity_id": entity_id,
            "current_score": current_score,
            "previous_score": previous_score,
            "delta": current_score - previous_score,
            "current_at": datetime.fromtimestamp(current_ts, tz=timezone.utc),
        }

    def latest_relationship_health_delta(
        self, *, limit: int = 50
    ) -> dict[str, object] | None:
        try:
            rows = self._execute_select(
                """
                SELECT entity_id, ts_utc, payload, payload_json
                FROM events_v1
                WHERE event_type = 'relationship_health_updated'
                ORDER BY ts_utc DESC
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
            payload = self._event_payload(row)
            score_value = payload.get("health_score")
            if score_value is None:
                continue
            try:
                score = float(score_value)
            except (TypeError, ValueError):
                continue
            created_at = datetime.fromtimestamp(
                float(row.get("ts_utc") or 0.0), tz=timezone.utc
            )
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

    def _event_deltas(
        self,
        rows: list[dict[str, object]],
        field: str,
    ) -> dict[str, float]:
        per_entity: dict[str, list[tuple[float, float]]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "").strip()
            if not entity_id:
                continue
            payload = self._event_payload(row)
            raw_value = payload.get(field)
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc <= 0:
                continue
            per_entity.setdefault(entity_id, []).append((ts_utc, value))

        deltas: dict[str, float] = {}
        for entity_id, entries in per_entity.items():
            if len(entries) < 2:
                continue
            entries.sort(key=lambda item: item[0])
            deltas[entity_id] = entries[-1][1] - entries[0][1]
        return deltas

    def _trust_version(self, payload: dict[str, object]) -> str:
        version = payload.get("model_version") or payload.get("version") or "v1"
        return str(version).lower()

    def _trust_event_deltas(
        self,
        rows: list[dict[str, object]],
        field: str,
    ) -> dict[str, float]:
        versioned: dict[str, dict[str, list[tuple[float, float]]]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id") or "").strip()
            if not entity_id:
                continue
            payload = self._event_payload(row)
            raw_value = payload.get(field)
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc <= 0:
                continue
            version = self._trust_version(payload)
            versioned.setdefault(entity_id, {}).setdefault(version, []).append(
                (ts_utc, value)
            )

        deltas: dict[str, float] = {}
        for entity_id, versions in versioned.items():
            entries = versions.get("v2") or versions.get("v1") or []
            if len(entries) < 2:
                continue
            entries.sort(key=lambda item: item[0])
            deltas[entity_id] = entries[-1][1] - entries[0][1]
        return deltas

    def _latest_trust_delta(
        self,
        versioned: dict[str, dict[str, list[tuple[float, float]]]],
        *,
        version: str,
    ) -> list[tuple[str, float, float, float]]:
        candidates: list[tuple[str, float, float, float]] = []
        for entity_id, versions in versioned.items():
            entries = versions.get(version) or []
            if len(entries) < 2:
                continue
            entries.sort(key=lambda item: item[0], reverse=True)
            current_ts, current_score = entries[0]
            previous_ts, previous_score = entries[1]
            candidates.append((entity_id, current_score, previous_score, current_ts))
        return candidates
