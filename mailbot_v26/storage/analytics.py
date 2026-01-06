from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from mailbot_v26.events.contract import EventType
from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WeeklyAccuracyProgress:
    current_surprise_rate_pp: int
    prev_surprise_rate_pp: int
    delta_pp: int
    current_decisions: int
    prev_decisions: int
    current_corrections: int


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

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float | None:
        if not values:
            return None
        if percentile <= 0:
            return float(sorted(values)[0])
        if percentile >= 100:
            return float(sorted(values)[-1])
        ordered = sorted(values)
        if len(ordered) == 1:
            return float(ordered[0])
        position = (percentile / 100) * (len(ordered) - 1)
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return float(ordered[lower])
        fraction = position - lower
        return float(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction)

    @staticmethod
    def _normalize_account_scope(
        account_email: str,
        account_emails: Iterable[str] | None,
    ) -> list[str]:
        normalized: set[str] = set()
        if account_emails is not None:
            for email in account_emails:
                if email is None:
                    continue
                cleaned = str(email).strip()
                if cleaned:
                    normalized.add(cleaned)
            if normalized:
                return sorted(normalized)
        cleaned_primary = account_email.strip() if account_email else ""
        if cleaned_primary:
            return [cleaned_primary]
        return []

    @staticmethod
    def _parse_json_dict(raw: object) -> dict[str, object]:
        if raw is None:
            return {}
        try:
            loaded = json.loads(str(raw))
            if isinstance(loaded, dict):
                return loaded
        except (TypeError, ValueError):
            return {}
        return {}

    @staticmethod
    def _account_scope_clause(account_ids: Sequence[str]) -> tuple[str, list[object]]:
        if not account_ids:
            return "", []
        if len(account_ids) == 1:
            return " AND account_id = ?", [account_ids[0]]
        placeholders = ", ".join(["?"] * len(account_ids))
        return f" AND account_id IN ({placeholders})", list(account_ids)

    @staticmethod
    def _contact_key(raw: str) -> str:
        cleaned = (raw or "").strip()
        if cleaned.lower().startswith("contact:"):
            return cleaned[len("contact:") :]
        return cleaned

    @staticmethod
    def _contact_label(entity_id: str, payload: Mapping[str, object] | None = None) -> tuple[str, str]:
        domain_hint = ""
        if payload:
            raw_domain = payload.get("sender_domain") or payload.get("domain")
            if isinstance(raw_domain, str) and raw_domain.strip():
                domain_hint = raw_domain.strip().lower()
        if "@" in entity_id:
            domain = entity_id.split("@", 1)[1].lower()
            return domain, domain
        if domain_hint:
            return domain_hint, domain_hint
        return entity_id, ""

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

    def _event_rows_scoped(
        self,
        *,
        account_ids: Sequence[str],
        event_type: str,
        since_ts: float | None = None,
    ) -> list[dict[str, object]]:
        if not account_ids:
            return []
        query = """
        SELECT event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
        FROM events_v1
        WHERE event_type = ?
        """
        params: list[object] = [event_type]
        if since_ts is not None:
            query += " AND ts_utc >= ?"
            params.append(since_ts)
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        params.extend(clause_params)
        try:
            return self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

    def _event_count(
        self,
        *,
        account_id: str | None,
        event_type: str,
        since_ts: float | None = None,
    ) -> int:
        query = """
        SELECT COUNT(*) AS total
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
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return 0
        if not rows:
            return 0
        return int(rows[0].get("total") or 0)

    def _event_count_scoped(
        self,
        *,
        account_ids: Sequence[str],
        event_type: str,
        since_ts: float | None = None,
        start_ts: float | None = None,
        end_ts: float | None = None,
    ) -> int:
        if not account_ids:
            return 0
        query = """
        SELECT COUNT(*) AS total
        FROM events_v1
        WHERE event_type = ?
        """
        params: list[object] = [event_type]
        if start_ts is not None:
            query += " AND ts_utc >= ?"
            params.append(start_ts)
            if end_ts is not None:
                query += " AND ts_utc < ?"
                params.append(end_ts)
        elif since_ts is not None:
            query += " AND ts_utc >= ?"
            params.append(since_ts)
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        params.extend(clause_params)
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return 0
        if not rows:
            return 0
        return int(rows[0].get("total") or 0)

    def _event_count_between(
        self,
        *,
        account_id: str | None,
        event_type: str,
        start_ts: float,
        end_ts: float,
    ) -> int:
        query = """
        SELECT COUNT(*) AS total
        FROM events_v1
        WHERE event_type = ?
          AND ts_utc >= ?
          AND ts_utc < ?
        """
        params: list[object] = [event_type, start_ts, end_ts]
        if account_id:
            query += " AND account_id = ?"
            params.append(account_id)
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return 0
        if not rows:
            return 0
        return int(rows[0].get("total") or 0)

    @staticmethod
    def _percent_pp(numerator: int, denominator: int) -> int:
        safe_denominator = max(1, int(denominator))
        safe_numerator = max(0, int(numerator))
        return int((safe_numerator * 100 + (safe_denominator // 2)) // safe_denominator)

    @staticmethod
    def _priority_emoji(value: object) -> str | None:
        normalized = str(value or "").strip().lower()
        mapping = {
            "🔴": "🔴",
            "🟡": "🟡",
            "🔵": "🔵",
            "high": "🔴",
            "medium": "🟡",
            "low": "🔵",
            "red": "🔴",
            "yellow": "🟡",
            "blue": "🔵",
        }
        return mapping.get(normalized)

    @staticmethod
    def _priority_rank(value: str | None) -> int | None:
        mapping = {"🔵": 1, "🟡": 2, "🔴": 3}
        if not value:
            return None
        return mapping.get(value)

    def event_count(
        self,
        *,
        account_id: str | None,
        event_type: EventType,
        since_ts: float | None = None,
    ) -> int:
        return self._event_count(
            account_id=account_id,
            event_type=event_type.value,
            since_ts=since_ts,
        )

    @staticmethod
    def _sanitize_event_payload(payload: dict[str, object]) -> dict[str, object]:
        allowed_keys = {
            "priority",
            "confidence",
            "confidence_score",
            "decision",
            "outcome",
            "error_code",
            "delivery_mode",
            "wait_budget_seconds",
            "elapsed_to_first_send_seconds",
            "edit_applied",
            "system_mode",
            "stage",
            "stage_ms",
            "provider",
            "model",
        }
        forbidden_substrings = [
            "subject",
            "sender",
            "from",
            "to",
            "cc",
            "bcc",
            "body",
            "text",
            "html",
            "raw",
            "telegram",
            "rendered",
            "digest",
            "attachment_name",
            "filename",
            "url",
        ]
        if not isinstance(payload, Mapping):
            return {}

        def _safe_value(value: object) -> object:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return value
            if value is None:
                return None
            if isinstance(value, str):
                return value[:120]
            text = str(value)
            return text[:120]

        sanitized: dict[str, object] = {}
        for key, value in payload.items():
            key_str = str(key)
            lowered = key_str.lower()
            if any(token in lowered for token in forbidden_substrings):
                continue
            if key_str not in allowed_keys:
                continue
            sanitized[key_str] = _safe_value(value)
        if not sanitized:
            return {}
        return {key: sanitized[key] for key in sorted(sanitized.keys())}

    @staticmethod
    def _safe_payload_value(value: object, *, max_length: int = 120) -> object:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value
        if value is None:
            return None
        if isinstance(value, str):
            return value[:max_length]
        return str(value)[:max_length]

    def _sanitize_learning_payload(self, event_type: str, payload: Mapping[str, object]) -> dict[str, object]:
        allowed_by_type: dict[str, set[str]] = {
            EventType.PRIORITY_CORRECTION_RECORDED.value: {
                "old_priority",
                "new_priority",
                "engine",
                "source",
                "system_mode",
            },
            EventType.SURPRISE_DETECTED.value: {
                "old_priority",
                "new_priority",
                "delta",
                "engine",
                "source",
            },
            EventType.DELIVERY_POLICY_APPLIED.value: {
                "mode",
                "reason_codes",
                "thresholds_used",
                "attention_debt",
                "priority",
                "confidence_percent",
                "extraction_success",
                "attachment_count",
            },
            EventType.ATTENTION_DEBT_UPDATED.value: {
                "attention_debt",
                "bucket",
                "immediate_last_hour",
                "max_per_hour",
            },
            EventType.CALIBRATION_PROPOSALS_GENERATED.value: {
                "week_key",
                "proposals_count",
                "top_labels",
            },
        }
        allowed_keys = allowed_by_type.get(event_type)
        if not allowed_keys or not isinstance(payload, Mapping):
            return {}

        sanitized: dict[str, object] = {}

        def _clean_container(value: object) -> object:
            if isinstance(value, list):
                cleaned_list: list[object] = []
                for item in value:
                    if isinstance(item, (str, int, float, bool)) or item is None:
                        cleaned_list.append(self._safe_payload_value(item))
                return cleaned_list
            if isinstance(value, dict):
                cleaned_dict: dict[str, object] = {}
                for k, v in value.items():
                    if not isinstance(k, str):
                        continue
                    cleaned_dict[k] = self._safe_payload_value(v)
                return {key: cleaned_dict[key] for key in sorted(cleaned_dict.keys())}
            return self._safe_payload_value(value)

        forbidden_tokens = ["subject", "body", "raw", "sender", "from_email", "email"]
        for key in allowed_keys:
            lowered = key.lower()
            if any(token in lowered for token in forbidden_tokens):
                continue
            if key not in payload:
                continue
            sanitized[key] = _clean_container(payload.get(key))

        if not sanitized:
            return {}
        return {key: sanitized[key] for key in sorted(sanitized.keys())}

    def _event_summary(self, event_type: str, details: dict[str, object]) -> str:
        priority_marker = self._priority_emoji(details.get("priority")) if details else None
        prefix = f"{event_type}{f' {priority_marker}' if priority_marker else ''}"
        if not details:
            return prefix
        pairs: list[str] = []
        for key in sorted(details.keys()):
            if len(pairs) >= 3:
                break
            value = details.get(key)
            pairs.append(f"{key}={value}")
        return f"{prefix}: " + "; ".join(pairs)

    def bootstrap_start_ts(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
    ) -> float | None:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return None
        query = """
        SELECT MIN(ts_utc) AS start_ts
        FROM events_v1
        WHERE event_type = ?
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        try:
            rows = self._execute_select(
                query,
                ("email_received", *clause_params),
            )
        except sqlite3.OperationalError:
            return None
        if not rows:
            return None
        start_ts = rows[0].get("start_ts")
        try:
            return float(start_ts) if start_ts is not None else None
        except (TypeError, ValueError):
            return None

    def bootstrap_samples_count(
        self,
        *,
        account_email: str,
        start_ts: float,
        account_emails: Iterable[str] | None = None,
    ) -> int:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        return self._event_count_scoped(
            account_ids=account_ids,
            event_type="email_received",
            since_ts=start_ts,
        )

    def events_timeline_rows_scoped(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        limit: int,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        since_ts = self._window_start_ts(window_days)
        resolved_limit = max(0, int(limit))
        if resolved_limit <= 0:
            return []
        query = """
        SELECT event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
        FROM events_v1
        WHERE ts_utc >= ?
        """
        params: list[object] = [since_ts]
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        query += "\n        ORDER BY ts_utc DESC, event_type ASC, email_id DESC, entity_id DESC"
        query += "\n        LIMIT ?"
        params.extend(clause_params)
        params.append(resolved_limit)
        try:
            return self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

    def events_timeline(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        limit: int,
    ) -> list[dict[str, object]]:
        rows = self.events_timeline_rows_scoped(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days,
            limit=limit,
        )
        items: list[dict[str, object]] = []
        for row in rows:
            payload = self._event_payload(row)
            details = self._sanitize_event_payload(payload)
            summary = self._event_summary(str(row.get("event_type") or ""), details)
            items.append(
                {
                    "ts_utc": row.get("ts_utc"),
                    "event_type": row.get("event_type"),
                    "email_id": row.get("email_id"),
                    "entity_id": row.get("entity_id"),
                    "summary": summary,
                    "details": details,
                }
            )
        return items

    def _event_rows_scoped_multi(
        self,
        *,
        account_ids: Sequence[str],
        event_types: Sequence[str],
        since_ts: float,
        limit: int | None = None,
        order_desc: bool = True,
    ) -> list[dict[str, object]]:
        if not account_ids or not event_types:
            return []
        placeholders = ", ".join(["?"] * len(event_types))
        query = """
        SELECT id, event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
        FROM events_v1
        WHERE event_type IN ({event_types}) AND ts_utc >= ?
        """.replace("{event_types}", placeholders)
        params: list[object] = list(event_types)
        params.append(since_ts)
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        params.extend(clause_params)
        if order_desc:
            query += "\n        ORDER BY ts_utc DESC, id ASC"
        else:
            query += "\n        ORDER BY ts_utc ASC, id ASC"
        if limit is not None:
            query += "\n        LIMIT ?"
            params.append(limit)
        try:
            return self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

    def behavioral_metrics_summary(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        now_ts: float,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {}
        window_seconds = max(1, int(window_days)) * 24 * 60 * 60
        since_ts = float(now_ts) - window_seconds

        summary: dict[str, object] = {
            "window_days": window_days,
            "account_emails": account_ids,
        }

        corrections = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED.value,
            since_ts=since_ts,
        )
        surprises = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.SURPRISE_DETECTED.value,
            since_ts=since_ts,
        )
        summary["corrections"] = corrections
        summary["surprises"] = surprises
        summary["surprise_rate"] = (
            0.0 if corrections <= 0 else float(surprises) / float(corrections)
        )

        received_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.EMAIL_RECEIVED.value,
            since_ts=since_ts,
        )
        received_by_email: dict[int, float] = {}
        for row in received_rows:
            email_id = row.get("email_id")
            if email_id is None:
                continue
            try:
                key = int(email_id)
            except (TypeError, ValueError):
                continue
            ts_val = row.get("ts_utc")
            try:
                ts_float = float(ts_val)
            except (TypeError, ValueError):
                continue
            if key not in received_by_email or ts_float < received_by_email[key]:
                received_by_email[key] = ts_float
        corrections_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED.value,
            since_ts=since_ts,
        )
        tta_values: list[float] = []
        for row in corrections_rows:
            email_id = row.get("email_id")
            if email_id is None:
                continue
            try:
                key = int(email_id)
            except (TypeError, ValueError):
                continue
            received_ts = received_by_email.get(key)
            if received_ts is None:
                continue
            try:
                correction_ts = float(row.get("ts_utc") or 0.0)
            except (TypeError, ValueError):
                continue
            delta = correction_ts - received_ts
            if delta >= 0:
                tta_values.append(delta)
        summary["tta_seconds_p50"] = self._percentile(tta_values, 50) if tta_values else None
        summary["tta_seconds_p90"] = self._percentile(tta_values, 90) if tta_values else None

        delivery_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.DELIVERY_POLICY_APPLIED.value,
            since_ts=since_ts,
        )
        total_delivery = len(delivery_rows)
        non_immediate = 0
        reason_counter: Counter[str] = Counter()
        for row in delivery_rows:
            payload = self._event_payload(row)
            mode = str(payload.get("mode") or "").lower()
            if mode and mode != "immediate":
                non_immediate += 1
            reason_codes = payload.get("reason_codes")
            if isinstance(reason_codes, list):
                for reason in reason_codes:
                    reason_str = str(reason or "").strip()
                    if reason_str:
                        reason_counter[reason_str] += 1
        summary["compression_rate"] = (
            0.0 if total_delivery <= 0 else float(non_immediate) / float(total_delivery)
        )
        summary["deferral_reasons"] = [
            {"reason_code": key, "count": reason_counter[key]}
            for key in sorted(
                reason_counter.keys(),
                key=lambda item: (-reason_counter[item], item),
            )
        ]

        attention_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.ATTENTION_DEBT_UPDATED.value,
            since_ts=since_ts,
        )
        buckets: defaultdict[str, int] = defaultdict(int)
        for row in attention_rows:
            payload = self._event_payload(row)
            bucket = str(payload.get("bucket") or "").strip().lower() or "unknown"
            buckets[bucket] += 1
        summary["attention_debt_distribution"] = {
            key: buckets.get(key, 0)
            for key in ["low", "medium", "high", "unknown"]
            if buckets.get(key, 0) or key in {"low", "medium", "high"}
        }

        summary["signal_counts"] = {
            "deadlock_detected": self._event_count_scoped(
                account_ids=account_ids,
                event_type=EventType.DEADLOCK_DETECTED.value,
                since_ts=since_ts,
            ),
            "silence_signal_detected": self._event_count_scoped(
                account_ids=account_ids,
                event_type=EventType.SILENCE_SIGNAL_DETECTED.value,
                since_ts=since_ts,
            ),
        }

        generated_at = datetime.fromtimestamp(float(now_ts), tz=timezone.utc).isoformat()
        summary["generated_at_utc"] = (
            generated_at if not generated_at.endswith("+00:00") else generated_at.replace("+00:00", "Z")
        )
        return summary

    def learning_timeline(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        limit: int,
        now_ts: float,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {}
        resolved_limit = min(200, max(1, int(limit)))
        window_seconds = max(1, int(window_days)) * 24 * 60 * 60
        since_ts = float(now_ts) - window_seconds
        event_types = [
            EventType.PRIORITY_CORRECTION_RECORDED.value,
            EventType.SURPRISE_DETECTED.value,
            EventType.DELIVERY_POLICY_APPLIED.value,
            EventType.ATTENTION_DEBT_UPDATED.value,
            EventType.CALIBRATION_PROPOSALS_GENERATED.value,
        ]
        rows = self._event_rows_scoped_multi(
            account_ids=account_ids,
            event_types=event_types,
            since_ts=since_ts,
            limit=resolved_limit,
            order_desc=True,
        )
        items: list[dict[str, object]] = []
        for row in rows:
            event_type = str(row.get("event_type") or "")
            payload = self._event_payload(row)
            sanitized_payload = self._sanitize_learning_payload(event_type, payload)
            entity_raw = self._contact_key(str(row.get("entity_id") or ""))
            entity_label, entity_domain = self._contact_label(entity_raw, payload)
            ts_val = row.get("ts_utc")
            try:
                ts_float = float(ts_val)
            except (TypeError, ValueError):
                ts_float = 0.0
            iso_ts = datetime.fromtimestamp(ts_float, tz=timezone.utc).isoformat() if ts_float else None
            if iso_ts and iso_ts.endswith("+00:00"):
                iso_ts = iso_ts.replace("+00:00", "Z")
            items.append(
                {
                    "event_id": row.get("id"),
                    "event_type": event_type,
                    "ts_utc": ts_val,
                    "ts_iso": iso_ts,
                    "email_id": row.get("email_id") if row.get("email_id") is not None else None,
                    "entity": {
                        "label": entity_label,
                        "domain": entity_domain,
                    },
                    "payload": sanitized_payload,
                }
            )
        generated_at = datetime.fromtimestamp(float(now_ts), tz=timezone.utc).isoformat()
        if generated_at.endswith("+00:00"):
            generated_at = generated_at.replace("+00:00", "Z")
        return {
            "window_days": window_days,
            "account_emails": account_ids,
            "limit": resolved_limit,
            "generated_at_utc": generated_at,
            "items": items,
        }

    def relationship_graph(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        limit: int,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        since_ts = self._window_start_ts(window_days)
        resolved_limit = min(200, max(1, int(limit))) if limit else 50
        max_ts = since_ts
        nodes: list[dict[str, object]] = [
            {
                "id": "user:me",
                "label": "Вы",
                "domain": "",
                "emails_total": 0,
                "threads_total": 0,
                "last_seen_utc": None,
            }
        ]
        if not account_ids:
            return {
                "scope": {"account_emails": account_ids, "window_days": window_days},
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "nodes": nodes,
                "edges": [],
            }

        query = """
        SELECT event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
        FROM events_v1
        WHERE ts_utc >= ? AND entity_id IS NOT NULL
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        query += "\n        ORDER BY ts_utc ASC, event_type ASC, email_id ASC, entity_id ASC"
        params: list[object] = [since_ts, *clause_params]
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            rows = []

        aggregates: dict[str, dict[str, object]] = {}
        trust_events: dict[str, list[tuple[float, float]]] = {}
        for row in rows:
            entity_id_raw = str(row.get("entity_id") or "").strip()
            if not entity_id_raw:
                continue
            entity_id = entity_id_raw
            payload = self._event_payload(row)
            ts_utc = float(row.get("ts_utc") or 0.0)
            event_type = str(row.get("event_type") or "")
            entry = aggregates.setdefault(
                entity_id,
                {
                    "emails_total": 0,
                    "threads": set(),
                    "last_seen": 0.0,
                    "trust_score": None,
                    "risk_flags": set(),
                },
            )
            if row.get("email_id") is not None:
                entry["emails_total"] = int(entry["emails_total"]) + 1
                entry["threads"].add(row.get("email_id"))
            entry["last_seen"] = max(float(entry["last_seen"]), ts_utc)
            max_ts = max(max_ts, ts_utc)
            if event_type == "trust_score_updated":
                score_raw = payload.get("trust_score")
                try:
                    score = float(score_raw)
                except (TypeError, ValueError):
                    score = None
                if score is not None:
                    trust_events.setdefault(entity_id, []).append((ts_utc, score))
                    entry["trust_score"] = score
            if event_type in {"silence_signal_detected", "deadlock_detected"}:
                entry["risk_flags"].add(event_type)

        for events in trust_events.values():
            events.sort(key=lambda item: item[0])

        result_nodes: list[dict[str, object]] = []
        for entity_id, entry in aggregates.items():
            trust_delta = None
            events = trust_events.get(entity_id, [])
            if len(events) >= 2:
                first = events[0][1]
                last = events[-1][1]
                trust_delta = last - first
            label, domain = self._contact_label(entity_id)
            result_nodes.append(
                {
                    "id": f"contact:{entity_id}",
                    "label": label,
                    "domain": domain,
                    "emails_total": int(entry["emails_total"]),
                    "threads_total": len(entry["threads"]),
                    **({"trust_score": entry["trust_score"]} if entry["trust_score"] is not None else {}),
                    **({"trust_delta": trust_delta} if trust_delta is not None else {}),
                    "avg_tta_seconds": None,
                    "risk_flags": sorted(entry["risk_flags"]),
                    "last_seen_utc": datetime.fromtimestamp(entry["last_seen"], tz=timezone.utc).isoformat()
                    if entry["last_seen"]
                    else None,
                }
            )

        def _node_sort_key(node: Mapping[str, object]) -> tuple[float, int, str]:
            trust_score = node.get("trust_score")
            trust_key = -float(trust_score) if trust_score is not None else math.inf
            return (
                trust_key,
                -int(node.get("emails_total") or 0),
                str(node.get("id") or ""),
            )

        result_nodes.sort(key=_node_sort_key)
        result_nodes = result_nodes[:resolved_limit]
        edges = [
            {
                "source": "user:me",
                "target": node["id"],
                "weight": int(node.get("emails_total") or 0),
                "kind": "email_volume",
            }
            for node in result_nodes
        ]
        edges.sort(key=lambda edge: (-int(edge.get("weight") or 0), str(edge.get("target") or "")))
        generated = datetime.fromtimestamp(max_ts, tz=timezone.utc).isoformat()
        return {
            "scope": {"account_emails": account_ids, "window_days": window_days},
            "generated_at_utc": generated,
            "nodes": nodes + result_nodes,
            "edges": edges,
        }

    def relationship_contact_detail(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        contact_id: str,
        window_days: int,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {}
        entity_id = self._contact_key(contact_id)
        if not entity_id:
            return {}
        since_ts = self._window_start_ts(window_days)
        query = """
        SELECT event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
        FROM events_v1
        WHERE ts_utc >= ? AND entity_id = ?
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        query += "\n        ORDER BY ts_utc ASC, event_type ASC, email_id ASC"
        params: list[object] = [since_ts, entity_id, *clause_params]
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            rows = []

        trust_series: list[dict[str, object]] = []
        volume_by_date: dict[str, dict[str, int]] = {}
        risk_flags: set[str] = set()
        emails_total = 0
        threads: set[object] = set()
        last_seen = 0.0
        trust_score = None

        for row in rows:
            ts_utc = float(row.get("ts_utc") or 0.0)
            event_type = str(row.get("event_type") or "")
            payload = self._event_payload(row)
            date_key = datetime.fromtimestamp(ts_utc, tz=timezone.utc).date().isoformat()
            volume_entry = volume_by_date.setdefault(date_key, {"inbound": 0, "outbound": 0})
            if event_type == "email_received":
                volume_entry["inbound"] += 1
            elif event_type in {"telegram_delivered", "priority_decision_recorded", "priority_correction_recorded"}:
                volume_entry["outbound"] += 1
            if row.get("email_id") is not None:
                emails_total += 1
                threads.add(row.get("email_id"))
            last_seen = max(last_seen, ts_utc)
            if event_type == "trust_score_updated":
                score_raw = payload.get("trust_score")
                try:
                    score = float(score_raw)
                except (TypeError, ValueError):
                    score = None
                if score is not None:
                    trust_score = score
                    trust_series.append({"date": date_key, "value": score})
            if event_type in {"silence_signal_detected", "deadlock_detected"}:
                risk_flags.add(event_type)

        trust_series.sort(key=lambda item: item["date"])
        volume_series = [
            {"date": day, "inbound": counts["inbound"], "outbound": counts["outbound"]}
            for day, counts in sorted(volume_by_date.items(), key=lambda item: item[0])
        ]

        trust_delta = None
        if len(trust_series) >= 2:
            trust_delta = trust_series[-1]["value"] - trust_series[0]["value"]
        label, domain = self._contact_label(entity_id)
        contact = {
            "id": f"contact:{entity_id}",
            "label": label,
            "domain": domain,
            "emails_total": emails_total,
            "threads_total": len(threads),
            **({"trust_score": trust_score} if trust_score is not None else {}),
            **({"trust_delta": trust_delta} if trust_delta is not None else {}),
            "avg_tta_seconds": None,
            "risk_flags": sorted(risk_flags),
            "last_seen_utc": datetime.fromtimestamp(last_seen, tz=timezone.utc).isoformat() if last_seen else None,
        }
        highlights: list[dict[str, str]] = []
        for flag in sorted(risk_flags):
            if flag == "silence_signal_detected":
                highlights.append({"kind": "pattern", "text": "Обнаружен сигнал тишины за период"})
            elif flag == "deadlock_detected":
                highlights.append({"kind": "pattern", "text": "Возможный дедлок общения"})
        return {
            "contact": contact,
            "series": {"trust": trust_series, "volume": volume_series},
            "highlights": highlights,
        }

    def bootstrap_corrections_count(
        self,
        *,
        account_email: str,
        since_ts: float,
        account_emails: Iterable[str] | None = None,
    ) -> int:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        return self._event_count_scoped(
            account_ids=account_ids,
            event_type="priority_correction_recorded",
            since_ts=since_ts,
        )

    def bootstrap_surprises_count(
        self,
        *,
        account_email: str,
        since_ts: float,
        account_emails: Iterable[str] | None = None,
    ) -> int:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        return self._event_count_scoped(
            account_ids=account_ids,
            event_type="surprise_detected",
            since_ts=since_ts,
        )

    def uncertainty_queue_items(
        self,
        account_email: str,
        *,
        account_emails: Iterable[str] | None = None,
        since_ts: float,
        min_confidence: int,
        limit: int,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        resolved_limit = max(0, int(limit))
        if resolved_limit <= 0:
            return []
        query = """
        SELECT ts_utc, payload, payload_json
        FROM events_v1
        WHERE event_type = ?
          AND ts_utc >= ?
        """
        params: list[object] = [
            EventType.PRIORITY_DECISION_RECORDED.value,
            since_ts,
        ]
        clause, clause_params = self._account_scope_clause(account_ids)
        query += clause
        query += "\n        ORDER BY ts_utc DESC"
        params.extend(clause_params)
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []
        items: list[dict[str, object]] = []
        for row in rows:
            payload = self._event_payload(row)
            raw_confidence = payload.get("confidence")
            try:
                confidence = int(raw_confidence)
            except (TypeError, ValueError):
                continue
            confidence = max(0, min(100, confidence))
            if confidence >= min_confidence:
                continue
            sender = str(payload.get("sender") or "")
            subject = str(payload.get("subject") or "")
            items.append(
                {
                    "sender": sender,
                    "subject": subject,
                    "confidence": confidence,
                }
            )
            if len(items) >= resolved_limit:
                break
        return items

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

    def deferred_digest_counts(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        days: int = 1,
    ) -> dict[str, int]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {
                "total": 0,
                "attachments_only": 0,
                "informational": 0,
            }
        since_ts = self._window_start_ts(days)
        rows = self._event_rows_scoped(
            account_ids=account_ids,
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
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        limit: int = 5,
    ) -> list[dict[str, str]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids or limit <= 0:
            return []
        clause, clause_params = self._account_scope_clause(account_ids)
        if clause:
            clause = clause.replace("account_id", "account_email")
        try:
            rows = self._execute_select(
                """
                SELECT subject, from_email, received_at, body_summary
                FROM emails
                WHERE deferred_for_digest = 1
                """
                + clause
                + """
                ORDER BY datetime(received_at) DESC
                LIMIT ?
                """,
                [*clause_params, limit],
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

    def get_deadlock_insights(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int,
        limit: int,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids or window_days <= 0 or limit <= 0:
            return []
        since_ts = self._window_start_ts(window_days)
        rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="deadlock_detected",
            since_ts=since_ts,
        )
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            payload = self._event_payload(row)
            thread_key = str(payload.get("thread_key") or "").strip()
            if not thread_key:
                continue
            ts_utc = float(row.get("ts_utc") or 0)
            prev = latest.get(thread_key)
            if prev and float(prev.get("ts_utc") or 0) >= ts_utc:
                continue
            latest[thread_key] = {
                "thread_key": thread_key,
                "ts_utc": ts_utc,
            }
        if not latest:
            return []
        sorted_items = sorted(
            latest.values(),
            key=lambda item: float(item.get("ts_utc") or 0),
            reverse=True,
        )
        results: list[dict[str, object]] = []
        for item in sorted_items[:limit]:
            thread_key = str(item.get("thread_key") or "").strip()
            fields = self._thread_email_fields(
                account_email=account_email,
                account_emails=account_emails,
                thread_key=thread_key,
            )
            results.append(
                {
                    "thread_key": thread_key,
                    "subject": fields.get("subject", ""),
                    "from_email": fields.get("from_email", ""),
                    "received_at": fields.get("received_at", ""),
                }
            )
        return results

    def deadlock_insights(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int,
        limit: int,
    ) -> list[dict[str, object]]:
        return self.get_deadlock_insights(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days,
            limit=limit,
        )

    def get_silence_insights(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int,
        limit: int,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids or window_days <= 0 or limit <= 0:
            return []
        since_ts = self._window_start_ts(window_days)
        rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="silence_signal_detected",
            since_ts=since_ts,
        )
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            payload = self._event_payload(row)
            contact = str(payload.get("contact") or "").strip()
            if not contact:
                continue
            ts_utc = float(row.get("ts_utc") or 0)
            prev = latest.get(contact)
            if prev and float(prev.get("ts_utc") or 0) >= ts_utc:
                continue
            days_raw = payload.get("days_silent")
            try:
                days_silent = int(round(float(days_raw)))
            except (TypeError, ValueError):
                days_silent = 0
            latest[contact] = {
                "contact": contact,
                "ts_utc": ts_utc,
                "days_silent": days_silent,
            }
        if not latest:
            return []
        sorted_items = sorted(
            latest.values(),
            key=lambda item: float(item.get("ts_utc") or 0),
            reverse=True,
        )
        results: list[dict[str, object]] = []
        for item in sorted_items[:limit]:
            results.append(
                {
                    "contact": item.get("contact", ""),
                    "days_silent": item.get("days_silent", 0),
                }
            )
        return results

    def silence_insights(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int,
        limit: int,
    ) -> list[dict[str, object]]:
        return self.get_silence_insights(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days,
            limit=limit,
        )

    def _thread_email_fields(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        thread_key: str,
    ) -> dict[str, str]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids or not thread_key:
            return {"subject": "", "from_email": "", "received_at": ""}
        clause, clause_params = self._account_scope_clause(account_ids)
        if clause:
            clause = clause.replace("account_id", "account_email")
        try:
            rows = self._execute_select(
                """
                SELECT subject, from_email, received_at
                FROM emails
                WHERE thread_key = ?
                """
                + clause
                + """
                ORDER BY datetime(received_at) DESC
                """,
                [thread_key, *clause_params],
            )
        except sqlite3.OperationalError:
            return {"subject": "", "from_email": "", "received_at": ""}
        subject = ""
        sender = ""
        received_at = ""
        if rows:
            first = rows[0]
            sender = str(first.get("from_email") or "").strip()
            received_at = str(first.get("received_at") or "").strip()
            for row in rows:
                candidate = str(row.get("subject") or "").strip()
                if candidate:
                    subject = candidate
                    break
        return {
            "subject": subject,
            "from_email": sender,
            "received_at": received_at,
        }

    def commitment_status_counts(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
    ) -> dict[str, int]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {"pending": 0, "expired": 0}
        created_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="commitment_created",
        )
        status_rows = self._event_rows_scoped(
            account_ids=account_ids,
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

    def commitment_chain_digest_items(
        self,
        account_email: str,
        *,
        account_emails: Iterable[str] | None = None,
        since_ts: float,
        max_entities: int,
        max_items_per_entity: int,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        try:
            resolved_since_ts = float(since_ts)
        except (TypeError, ValueError):
            return []
        try:
            resolved_entities = max(0, int(max_entities))
        except (TypeError, ValueError):
            resolved_entities = 0
        try:
            resolved_items = max(0, int(max_items_per_entity))
        except (TypeError, ValueError):
            resolved_items = 0
        if resolved_entities <= 0 or resolved_items <= 0:
            return []

        try:
            clause, clause_params = self._account_scope_clause(account_ids)
            rows = self._execute_select(
                f"""
                SELECT event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json
                FROM events_v1
                WHERE event_type IN (
                    'commitment_created',
                    'commitment_status_changed',
                    'commitment_expired'
                )
                  AND ts_utc >= ?
                {clause}
                ORDER BY ts_utc DESC
                """,
                [resolved_since_ts, *clause_params],
            )
        except sqlite3.OperationalError:
            return []

        def _status_label(payload: dict[str, object]) -> str | None:
            raw = payload.get("new_status") or payload.get("status") or ""
            status = str(raw).strip().lower()
            if status == "pending":
                return "ожидает"
            if status == "expired":
                return "просрочено"
            if status == "fulfilled":
                return "выполнено"
            return None

        entries: dict[str, dict[str, object]] = {}
        for row in rows:
            payload = self._event_payload(row)
            status_label = _status_label(payload)
            if not status_label:
                continue
            text = str(payload.get("commitment_text") or "").strip()
            if not text:
                continue
            due_raw = str(payload.get("deadline_iso") or "").strip()
            due = due_raw if due_raw else None
            entity_id = str(row.get("entity_id") or "").strip()
            sender_email = str(payload.get("from_email") or "").strip()
            entity_key = entity_id or sender_email
            if not entity_key:
                continue
            if entity_id:
                try:
                    label = self.entity_label(entity_id=entity_id)
                except sqlite3.OperationalError:
                    label = None
            else:
                label = None
            if not label:
                label = entity_id or sender_email
            if not label:
                continue

            entry = entries.get(entity_key)
            if entry is None:
                entry = {
                    "label": label,
                    "items": [],
                    "has_pending": False,
                    "has_expired": False,
                    "latest_ts": 0.0,
                }
                entries[entity_key] = entry
            else:
                if entity_id and label != entry["label"]:
                    entry["label"] = label

            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc > entry["latest_ts"]:
                entry["latest_ts"] = ts_utc
            if status_label == "просрочено":
                entry["has_expired"] = True
            if status_label == "ожидает":
                entry["has_pending"] = True

            if len(entry["items"]) < resolved_items:
                entry["items"].append(
                    {
                        "text": text,
                        "status": status_label,
                        "due": due,
                    }
                )

        candidates: list[dict[str, object]] = []
        for entry in entries.values():
            if not (entry["has_pending"] or entry["has_expired"]):
                continue
            items = entry["items"]
            if not items:
                continue
            candidates.append(entry)

        candidates.sort(
            key=lambda entry: (
                0 if entry["has_expired"] else 1,
                -float(entry["latest_ts"] or 0.0),
            )
        )

        output: list[dict[str, object]] = []
        for entry in candidates[:resolved_entities]:
            output.append(
                {
                    "entity_label": str(entry["label"]),
                    "items": list(entry["items"])[:resolved_items],
                }
            )
        return output

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

    def weekly_email_volume(
        self,
        *,
        account_email: str,
        days: int = 7,
        account_emails: Iterable[str] | None = None,
    ) -> dict[str, int]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {"total": 0, "deferred": 0}
        since_ts = self._window_start_ts(days)
        return {
            "total": int(
                self._event_count_scoped(
                    account_ids=account_ids,
                    event_type="email_received",
                    since_ts=since_ts,
                )
            ),
            "deferred": int(
                self._event_count_scoped(
                    account_ids=account_ids,
                    event_type="attention_deferred_for_digest",
                    since_ts=since_ts,
                )
            ),
        }

    def weekly_accuracy_report(
        self,
        *,
        account_email: str,
        days: int = 7,
        account_emails: Iterable[str] | None = None,
    ) -> dict[str, int | float]:
        account_ids = self._normalize_account_scope(
            account_email,
            account_emails,
        )
        if not account_ids:
            return {"emails_received": 0, "priority_corrections": 0, "surprises": 0}
        since_ts = self._window_start_ts(days)
        emails_received = self._event_count_scoped(
            account_ids=account_ids,
            event_type="email_received",
            since_ts=since_ts,
        )
        corrections_total = self._event_count_scoped(
            account_ids=account_ids,
            event_type="priority_correction_recorded",
            since_ts=since_ts,
        )
        surprises_total = self._event_count_scoped(
            account_ids=account_ids,
            event_type="surprise_detected",
            since_ts=since_ts,
        )
        report: dict[str, int | float] = {
            "emails_received": int(emails_received),
            "priority_corrections": int(corrections_total),
            "surprises": int(surprises_total),
        }
        if corrections_total > 0:
            surprise_rate = surprises_total / corrections_total
            report["surprise_rate"] = surprise_rate
            report["accuracy_pct"] = round((1 - surprise_rate) * 100)
        return report

    def weekly_accuracy_progress(
        self,
        *,
        account_email: str,
        now_ts: float,
        window_days: int,
        account_emails: Iterable[str] | None = None,
    ) -> WeeklyAccuracyProgress | None:
        account_ids = self._normalize_account_scope(
            account_email,
            account_emails,
        )
        if not account_ids:
            return None
        try:
            window_days = max(1, int(window_days))
        except (TypeError, ValueError):
            window_days = 7
        current_start = now_ts - (window_days * 86400)
        prev_start = now_ts - (window_days * 2 * 86400)

        current_decisions = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.DELIVERY_POLICY_APPLIED.value,
            start_ts=current_start,
            end_ts=now_ts,
        )
        prev_decisions = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.DELIVERY_POLICY_APPLIED.value,
            start_ts=prev_start,
            end_ts=current_start,
        )
        if current_decisions < 25 or prev_decisions < 25:
            return None

        current_surprises = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.SURPRISE_DETECTED.value,
            start_ts=current_start,
            end_ts=now_ts,
        )
        prev_surprises = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.SURPRISE_DETECTED.value,
            start_ts=prev_start,
            end_ts=current_start,
        )
        current_corrections = self._event_count_scoped(
            account_ids=account_ids,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED.value,
            start_ts=current_start,
            end_ts=now_ts,
        )

        current_rate_pp = self._percent_pp(current_surprises, current_decisions)
        prev_rate_pp = self._percent_pp(prev_surprises, prev_decisions)
        delta_pp = prev_rate_pp - current_rate_pp
        return WeeklyAccuracyProgress(
            current_surprise_rate_pp=current_rate_pp,
            prev_surprise_rate_pp=prev_rate_pp,
            delta_pp=delta_pp,
            current_decisions=int(current_decisions),
            prev_decisions=int(prev_decisions),
            current_corrections=int(current_corrections),
        )

    def weekly_surprise_breakdown(
        self,
        account_email: str,
        *,
        since_ts: float,
        top_n: int,
        min_corrections: int,
        account_emails: Iterable[str] | None = None,
    ) -> dict[str, object] | None:
        account_ids = self._normalize_account_scope(
            account_email,
            account_emails,
        )
        if not account_ids:
            return None
        corrections = self._event_count_scoped(
            account_ids=account_ids,
            event_type="priority_correction_recorded",
            since_ts=since_ts,
        )
        surprises = self._event_count_scoped(
            account_ids=account_ids,
            event_type="surprise_detected",
            since_ts=since_ts,
        )
        if corrections < min_corrections:
            return None
        accuracy_pct: int | None = None
        if corrections > 0:
            accuracy_pct = round((1 - (surprises / corrections)) * 100)
        rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="surprise_detected",
            since_ts=since_ts,
        )
        totals: dict[str, int] = {}
        for row in rows:
            payload = self._event_payload(row)
            label = str(
                payload.get("sender_email")
                or payload.get("from_email")
                or payload.get("entity_id")
                or row.get("entity_id")
                or ""
            ).strip()
            if not label:
                label = "контакт"
            totals[label] = totals.get(label, 0) + 1
        ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
        top = [
            {"label": label, "count": count}
            for label, count in ordered[: max(0, top_n)]
        ]
        window_days = max(
            1,
            int(round((datetime.now(timezone.utc).timestamp() - since_ts) / 86400)),
        )
        report: dict[str, object] = {
            "window_days": window_days,
            "corrections": int(corrections),
            "surprises": int(surprises),
            "top": top,
        }
        if accuracy_pct is not None:
            report["accuracy_pct"] = accuracy_pct
        return report

    def weekly_calibration_proposals(
        self,
        account_email: str,
        *,
        since_ts: float,
        top_n: int,
        min_corrections: int,
        account_emails: Iterable[str] | None = None,
    ) -> dict[str, object] | None:
        account_ids = self._normalize_account_scope(
            account_email,
            account_emails,
        )
        if not account_ids:
            return None

        corrections = self._event_count_scoped(
            account_ids=account_ids,
            event_type="priority_correction_recorded",
            since_ts=since_ts,
        )
        surprises = self._event_count_scoped(
            account_ids=account_ids,
            event_type="surprise_detected",
            since_ts=since_ts,
        )
        if corrections < min_corrections:
            return None

        def _label_from_event(
            row: dict[str, object], payload: dict[str, object]
        ) -> str:
            label = str(
                payload.get("sender_email")
                or payload.get("from_email")
                or payload.get("entity_id")
                or row.get("entity_id")
                or ""
            ).strip()
            return label or "контакт"

        correction_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="priority_correction_recorded",
            since_ts=since_ts,
        )
        correction_totals: dict[str, int] = {}
        transition_totals: dict[str, dict[tuple[str, str], int]] = {}
        for row in correction_rows:
            payload = self._event_payload(row)
            label = _label_from_event(row, payload)
            correction_totals[label] = correction_totals.get(label, 0) + 1
            old_priority = self._priority_emoji(payload.get("old_priority"))
            new_priority = self._priority_emoji(payload.get("new_priority"))
            if not old_priority or not new_priority:
                continue
            transitions = transition_totals.setdefault(label, {})
            key = (old_priority, new_priority)
            transitions[key] = transitions.get(key, 0) + 1

        surprise_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="surprise_detected",
            since_ts=since_ts,
        )
        surprise_totals: dict[str, int] = {}
        for row in surprise_rows:
            payload = self._event_payload(row)
            label = _label_from_event(row, payload)
            surprise_totals[label] = surprise_totals.get(label, 0) + 1

        ordered_surprises = sorted(
            surprise_totals.items(), key=lambda item: (-item[1], item[0])
        )
        top = [
            {"label": label, "count": count}
            for label, count in ordered_surprises[: max(0, top_n)]
        ]

        ordered_corrections = sorted(
            correction_totals.items(), key=lambda item: (-item[1], item[0])
        )
        top_labels = [label for label, _ in ordered_corrections[: max(0, top_n)]]

        proposals: list[dict[str, object]] = []
        min_signal = 3
        for label in top_labels:
            transition_map = transition_totals.get(label, {})
            transition_entry: tuple[tuple[str, str], int] | None = None
            if transition_map:
                transition_entry = sorted(
                    transition_map.items(),
                    key=lambda item: (-item[1], f"{item[0][0]}→{item[0][1]}"),
                )[0]
            correction_count = int(correction_totals.get(label, 0))
            if transition_entry and (
                transition_entry[1] >= min_signal or correction_count >= min_signal
            ):
                (old_priority, new_priority), count = transition_entry
                old_rank = self._priority_rank(old_priority)
                new_rank = self._priority_rank(new_priority)
                if old_rank is not None and new_rank is not None:
                    if old_rank > new_rank:
                        hint = "вероятно, завышаем срочность"
                    elif old_rank < new_rank:
                        hint = "вероятно, занижаем срочность"
                    else:
                        hint = "часто корректируем срочность"
                else:
                    hint = "часто корректируем срочность"
                proposals.append(
                    {
                        "label": label,
                        "transition": f"{old_priority}→{new_priority}",
                        "count": int(count),
                        "hint": hint,
                    }
                )
                continue
            surprise_count = int(surprise_totals.get(label, 0))
            if surprise_count >= min_signal:
                proposals.append(
                    {
                        "label": label,
                        "transition": "решение→сюрприз",
                        "count": surprise_count,
                        "hint": "после решения часто возникает сюрприз",
                    }
                )

        accuracy_pct: int | None = None
        if corrections > 0:
            accuracy_pct = round((1 - (surprises / corrections)) * 100)
        window_days = max(
            1,
            int(round((datetime.now(timezone.utc).timestamp() - since_ts) / 86400)),
        )
        report: dict[str, object] = {
            "window_days": window_days,
            "corrections": int(corrections),
            "surprises": int(surprises),
            "top": top,
            "proposals": proposals,
        }
        if accuracy_pct is not None:
            report["accuracy_pct"] = accuracy_pct
        return report

    def weekly_attention_entities(
        self,
        *,
        account_email: str,
        days: int = 7,
        account_emails: Iterable[str] | None = None,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        since_ts = self._window_start_ts(days)
        rows = self._event_rows_scoped(
            account_ids=account_ids,
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
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        days: int = 7,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        since_ts = self._window_start_ts(days)
        deferred_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="attention_deferred_for_digest",
            since_ts=since_ts,
        )
        deferred_ids = {
            str(row.get("email_id"))
            for row in deferred_rows
            if row.get("email_id") is not None
        }
        rows = self._event_rows_scoped(
            account_ids=account_ids,
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

    def attention_economics_summary(
        self,
        *,
        account_emails: Sequence[str],
        window_days: int,
        limit: int,
        include_anomalies: bool,
        attention_cost_per_hour: float = 0.0,
    ) -> dict[str, object]:
        from mailbot_v26.insights.attention_economics import compute_attention_economics

        scope = self._normalize_account_scope(
            account_emails[0] if account_emails else "", account_emails
        )
        primary_account = scope[0] if scope else ""
        raw_entities = self.attention_entity_metrics(
            account_email=primary_account,
            account_emails=scope,
            days=window_days,
        )
        totals = {
            "estimated_read_minutes": float(
                sum(float(item.get("estimated_read_minutes") or 0.0) for item in raw_entities)
            ),
            "message_count": int(sum(int(item.get("message_count") or 0) for item in raw_entities)),
            "attachment_count": int(
                sum(int(item.get("attachment_count") or 0) for item in raw_entities)
            ),
            "deferred_count": int(sum(int(item.get("deferred_count") or 0) for item in raw_entities)),
        }
        if attention_cost_per_hour > 0:
            totals["estimated_cost"] = round(
                (totals["estimated_read_minutes"] / 60.0) * attention_cost_per_hour, 2
            )

        result = None
        if scope:
            result = compute_attention_economics(
                analytics=self,
                account_email=primary_account,
                account_emails=scope,
                window_days=window_days,
                include_anomalies=include_anomalies,
            )

        def _entity_entry(
            *,
            entity_id: str,
            label: str,
            message_count: int,
            attachment_count: int,
            estimated_read_minutes: float,
            deferred_count: int,
            trust_delta: float | None,
            health_delta: float | None,
            anomalies: Sequence[str] = (),
        ) -> dict[str, object]:
            entry: dict[str, object] = {
                "entity_id": entity_id,
                "entity_label": label,
                "message_count": message_count,
                "attachment_count": attachment_count,
                "estimated_read_minutes": estimated_read_minutes,
                "deferred_count": deferred_count,
            }
            if trust_delta is not None:
                entry["trust_delta"] = trust_delta
            if health_delta is not None:
                entry["health_delta"] = health_delta
            if anomalies:
                entry["anomalies"] = list(anomalies)
            if attention_cost_per_hour > 0:
                entry["estimated_cost"] = round(
                    (estimated_read_minutes / 60.0) * attention_cost_per_hour, 2
                )
            return entry

        entities: list[dict[str, object]] = []
        if result is not None:
            for entity in result.entities[:limit]:
                entities.append(
                    _entity_entry(
                        entity_id=entity.entity_id,
                        label=entity.label,
                        message_count=entity.message_count,
                        attachment_count=entity.attachment_count,
                        estimated_read_minutes=entity.estimated_read_minutes,
                        deferred_count=entity.deferred_count,
                        trust_delta=entity.trust_delta,
                        health_delta=entity.health_delta,
                        anomalies=entity.anomalies,
                    )
                )
        else:
            for item in raw_entities:
                entity_id = str(item.get("entity_id") or "").strip()
                if not entity_id:
                    continue
                label = self.entity_label(entity_id=entity_id) or entity_id
                entities.append(
                    _entity_entry(
                        entity_id=entity_id,
                        label=label,
                        message_count=int(item.get("message_count") or 0),
                        attachment_count=int(item.get("attachment_count") or 0),
                        estimated_read_minutes=float(item.get("estimated_read_minutes") or 0.0),
                        deferred_count=int(item.get("deferred_count") or 0),
                        trust_delta=None,
                        health_delta=None,
                        anomalies=(),
                    )
                )
            entities.sort(
                key=lambda item: (
                    -float(item.get("estimated_read_minutes") or 0.0),
                    str(item.get("entity_label") or item.get("entity_id") or "").lower(),
                )
            )
            entities = entities[:limit]

        generated_ts = None
        if scope:
            query = """
            SELECT MAX(ts_utc) AS max_ts
            FROM events_v1
            WHERE event_type = 'email_received'
              AND ts_utc >= ?
            """
            params: list[object] = [self._window_start_ts(window_days)]
            clause, clause_params = self._account_scope_clause(scope)
            query += clause
            params.extend(clause_params)
            try:
                rows = self._execute_select(query, params)
            except sqlite3.OperationalError:
                rows = []
            if rows and rows[0].get("max_ts") is not None:
                generated_ts = float(rows[0]["max_ts"])
        if generated_ts is None:
            generated_ts = 0.0

        generated_at = datetime.fromtimestamp(generated_ts, tz=timezone.utc).isoformat()
        if generated_at.endswith("+00:00"):
            generated_at = generated_at.replace("+00:00", "Z")
        return {
            "window_days": window_days,
            "account_emails": scope,
            "limit": limit,
            "totals": totals,
            "entities": entities,
            "generated_at_utc": generated_at,
        }

    def behavior_metrics_digest(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {}
        try:
            window_days = max(1, int(window_days))
        except (TypeError, ValueError):
            window_days = 7
        since_ts = self._window_start_ts(window_days)

        metrics: dict[str, object] = {}

        corrections_total = self._event_count_scoped(
            account_ids=account_ids,
            event_type="priority_correction_recorded",
            since_ts=since_ts,
        )
        if corrections_total > 0:
            surprise_total = self._event_count_scoped(
                account_ids=account_ids,
                event_type="surprise_detected",
                since_ts=since_ts,
            )
            metrics["surprise_rate"] = surprise_total / corrections_total

        delivery_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="delivery_policy_applied",
            since_ts=since_ts,
        )
        if delivery_rows:
            metrics["compression_rate"] = 0.0

        debt_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="attention_debt_updated",
            since_ts=since_ts,
        )
        if debt_rows:
            distribution = {"low": 0, "medium": 0, "high": 0}
            for row in debt_rows:
                payload = self._event_payload(row)
                bucket = str(payload.get("bucket") or "").strip().lower()
                if bucket in distribution:
                    distribution[bucket] += 1
            if sum(distribution.values()) > 0:
                metrics["attention_debt_distribution"] = distribution

        deadlock_count = self._event_count_scoped(
            account_ids=account_ids,
            event_type="deadlock_detected",
            since_ts=since_ts,
        )
        silence_count = self._event_count_scoped(
            account_ids=account_ids,
            event_type="silence_signal_detected",
            since_ts=since_ts,
        )
        if deadlock_count > 0 or silence_count > 0:
            metrics["signal_counts"] = {
                "deadlock_count": deadlock_count,
                "silence_count": silence_count,
            }

        return metrics

    def weekly_commitment_counts(
        self,
        *,
        account_email: str,
        days: int = 7,
        account_emails: Iterable[str] | None = None,
    ) -> dict[str, int]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {"created": 0, "fulfilled": 0, "overdue": 0}
        since_ts = self._window_start_ts(days)
        created_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="commitment_created",
            since_ts=since_ts,
        )
        status_rows = self._event_rows_scoped(
            account_ids=account_ids,
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
        self,
        *,
        account_email: str,
        days: int = 7,
        limit: int = 5,
        account_emails: Iterable[str] | None = None,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        since_ts = self._window_start_ts(days)
        rows = self._event_rows_scoped(
            account_ids=account_ids,
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

    def regret_minimization_stats(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int,
        trust_drop_window_days: int,
        min_samples: int,
        now_dt: datetime | None = None,
    ) -> dict[str, int] | None:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return None
        now_ts = (now_dt or datetime.now(timezone.utc)).timestamp()
        window_days = max(1, int(window_days))
        trust_drop_window_days = max(1, int(trust_drop_window_days))
        min_samples = max(1, int(min_samples))
        since_ts = now_ts - (window_days * 24 * 60 * 60)

        expired_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="commitment_expired",
            since_ts=since_ts,
        )
        total = len(expired_rows)
        if total < min_samples:
            return None

        trust_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="trust_score_updated",
            since_ts=since_ts,
        )
        health_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="relationship_health_updated",
            since_ts=since_ts,
        )

        trust_by_entity: dict[str, list[tuple[float, float]]] = {}
        for row in trust_rows:
            entity_id = str(row.get("entity_id") or "").strip()
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
            trust_by_entity.setdefault(entity_id, []).append((ts_utc, score))

        health_by_entity: dict[str, list[tuple[float, float]]] = {}
        for row in health_rows:
            entity_id = str(row.get("entity_id") or "").strip()
            if not entity_id:
                continue
            payload = self._event_payload(row)
            score_raw = payload.get("health_score")
            try:
                score = float(score_raw)
            except (TypeError, ValueError):
                continue
            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc <= 0:
                continue
            health_by_entity.setdefault(entity_id, []).append((ts_utc, score))

        for rows in trust_by_entity.values():
            rows.sort(key=lambda item: item[0])
        for rows in health_by_entity.values():
            rows.sort(key=lambda item: item[0])

        def _has_negative_delta(
            rows: list[tuple[float, float]],
            *,
            start_ts: float,
            end_ts: float,
        ) -> bool:
            if not rows:
                return False
            window = [entry for entry in rows if start_ts <= entry[0] <= end_ts]
            if len(window) < 2:
                return False
            first = window[0][1]
            last = window[-1][1]
            return (last - first) < 0

        drops = 0
        window_span = trust_drop_window_days * 24 * 60 * 60
        for row in expired_rows:
            entity_id = str(row.get("entity_id") or "").strip()
            if not entity_id:
                continue
            ts_utc = float(row.get("ts_utc") or 0.0)
            if ts_utc <= 0:
                continue
            end_ts = min(now_ts, ts_utc + window_span)
            trust_drop = _has_negative_delta(
                trust_by_entity.get(entity_id, []),
                start_ts=ts_utc,
                end_ts=end_ts,
            )
            health_drop = _has_negative_delta(
                health_by_entity.get(entity_id, []),
                start_ts=ts_utc,
                end_ts=end_ts,
            )
            if trust_drop or health_drop:
                drops += 1

        pct = round((drops / total) * 100) if total > 0 else 0
        return {"total": total, "drops": drops, "pct": int(pct)}

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

    def trust_and_health_deltas(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        days: int = 7,
    ) -> dict[str, dict[str, float]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {}
        since_ts = self._window_start_ts(days)
        trust_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="trust_score_updated",
            since_ts=since_ts,
        )
        health_rows = self._event_rows_scoped(
            account_ids=account_ids,
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

    def _processing_span_rows_scoped(
        self, *, account_ids: Sequence[str], since_ts: float
    ) -> list[dict[str, object]]:
        if not account_ids:
            return []
        query = """
        SELECT span_id, ts_start_utc, ts_end_utc, total_duration_ms, stage_durations_json,
               llm_latency_ms, llm_quality_score, fallback_used, outcome, error_code,
               llm_provider, llm_model
        FROM processing_spans
        WHERE ts_start_utc >= ?
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        params: list[object] = [since_ts, *clause_params]
        query += clause
        query += " ORDER BY ts_start_utc DESC"
        try:
            return self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

    @staticmethod
    def _mean(values: list[float]) -> float | None:
        if not values:
            return None
        return float(sum(values) / len(values))

    def processing_spans_metrics_digest(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        since_ts = self._window_start_ts(window_days)
        rows = self._processing_span_rows_scoped(account_ids=account_ids, since_ts=since_ts)

        span_count = len(rows)
        total_durations: list[float] = []
        llm_latencies: list[float] = []
        llm_quality_scores: list[float] = []
        fallback_count = 0
        error_count = 0
        outcome_counts: dict[str, int] = {}
        stage_durations: dict[str, list[float]] = {}

        for row in rows:
            total_value = row.get("total_duration_ms")
            if total_value is not None:
                try:
                    total_durations.append(float(total_value))
                except (TypeError, ValueError):
                    pass
            llm_value = row.get("llm_latency_ms")
            if llm_value is not None:
                try:
                    llm_latencies.append(float(llm_value))
                except (TypeError, ValueError):
                    pass
            quality_value = row.get("llm_quality_score")
            if quality_value is not None:
                try:
                    llm_quality_scores.append(float(quality_value))
                except (TypeError, ValueError):
                    pass
            if int(row.get("fallback_used") or 0):
                fallback_count += 1
            outcome = str(row.get("outcome") or "").strip()
            if outcome:
                outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
            if outcome.lower() == "error" or str(row.get("error_code") or ""):
                error_count += 1
            try:
                durations = json.loads(str(row.get("stage_durations_json") or "{}"))
                if not isinstance(durations, dict):
                    durations = {}
            except (TypeError, ValueError):
                durations = {}
            for stage, value in durations.items():
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    continue
                stage_durations.setdefault(str(stage), []).append(numeric_value)

        total_p50 = self._percentile(total_durations, 50)
        total_p90 = self._percentile(total_durations, 90)
        total_p95 = self._percentile(total_durations, 95)
        total_avg = self._mean(total_durations)
        llm_p50 = self._percentile(llm_latencies, 50)
        llm_p90 = self._percentile(llm_latencies, 90)
        llm_p95 = self._percentile(llm_latencies, 95)
        llm_avg = self._mean(llm_latencies)
        llm_quality_avg = (
            sum(llm_quality_scores) / len(llm_quality_scores)
            if llm_quality_scores
            else None
        )

        error_rate = (error_count / span_count) if span_count else 0.0
        fallback_rate = (fallback_count / span_count) if span_count else 0.0

        return {
            "span_count": span_count,
            "total_duration_ms_avg": total_avg,
            "total_duration_ms_p50": total_p50,
            "total_duration_ms_p90": total_p90,
            "total_duration_ms_p95": total_p95,
            "llm_latency_ms_avg": llm_avg,
            "llm_latency_ms_p50": llm_p50,
            "llm_latency_ms_p90": llm_p90,
            "llm_latency_ms_p95": llm_p95,
            "llm_quality_avg": llm_quality_avg,
            "error_rate": error_rate,
            "fallback_rate": fallback_rate,
            "outcome_counts": outcome_counts,
            "stage_durations": {
                stage: {
                    "avg": self._mean(values),
                    "p50": self._percentile(values, 50),
                    "p90": self._percentile(values, 90),
                    "p95": self._percentile(values, 95),
                }
                for stage, values in sorted(stage_durations.items())
            },
        }

    def processing_spans_slowest(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str],
        window_days: int,
        limit: int = 5,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        resolved_limit = max(0, int(limit))
        if resolved_limit <= 0:
            return []
        since_ts = self._window_start_ts(window_days)
        query = """
        SELECT span_id, ts_start_utc, ts_end_utc, total_duration_ms,
               account_id, email_id, llm_provider, llm_model, outcome,
               llm_latency_ms, health_snapshot_id
        FROM processing_spans
        WHERE ts_start_utc >= ?
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        params: list[object] = [since_ts, *clause_params, resolved_limit]
        query += clause
        query += """
        ORDER BY COALESCE(total_duration_ms, (ts_end_utc - ts_start_utc) * 1000.0) DESC,
                 ts_start_utc DESC,
                 span_id ASC
        LIMIT ?
        """
        rows: list[sqlite3.Row]
        with self._connect_readonly() as conn:
            conn.row_factory = sqlite3.Row
            try:
                rows = list(conn.execute(query, params).fetchall())
            except sqlite3.OperationalError:
                return []
        allowed_keys = {
            "span_id": "span_id",
            "email_uid": "email_id",
            "account_email": "account_id",
            "started_at": "ts_start_utc",
            "finished_at": "ts_end_utc",
            "total_ms": "total_duration_ms",
            "outcome": "outcome",
            "llm_provider": "llm_provider",
            "llm_model": "llm_model",
            "llm_status": None,
            "llm_latency_ms": "llm_latency_ms",
            "health_snapshot_id": "health_snapshot_id",
        }
        results: list[dict[str, object]] = []
        for row in rows:
            row_dict = dict(row)
            start_value = row_dict.get("ts_start_utc")
            end_value = row_dict.get("ts_end_utc")
            try:
                total_raw = row_dict.get("total_duration_ms")
                if total_raw is None and start_value is not None and end_value is not None:
                    total_ms = (float(end_value) - float(start_value)) * 1000.0
                else:
                    total_ms = float(total_raw) if total_raw is not None else None
            except (TypeError, ValueError):
                total_ms = None
            entry: dict[str, object] = {}
            for public_key, column in allowed_keys.items():
                if column is None:
                    continue
                if column not in row_dict.keys():
                    continue
                value = row_dict.get(column)
                if public_key == "total_ms":
                    value = total_ms
                elif public_key == "account_email":
                    value = row_dict.get(column)
                elif public_key in {"started_at", "finished_at"}:
                    try:
                        value = float(value) if value is not None else None
                    except (TypeError, ValueError):
                        value = None
                if value is None and public_key == "total_ms":
                    continue
                entry[public_key] = value
            results.append(entry)
        return results

    def processing_spans_recent_errors(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
        limit: int = 10,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        since_ts = self._window_start_ts(window_days)
        rows = self._processing_span_rows_scoped(account_ids=account_ids, since_ts=since_ts)

        errors: list[dict[str, object]] = []
        for row in rows:
            outcome = str(row.get("outcome") or "").lower()
            error_code = str(row.get("error_code") or "")
            if outcome != "error" and not error_code:
                continue
            try:
                durations = json.loads(str(row.get("stage_durations_json") or "{}"))
                if not isinstance(durations, dict):
                    durations = {}
            except (TypeError, ValueError):
                durations = {}
            errors.append(
                {
                    "span_id": row.get("span_id"),
                    "ts_start": row.get("ts_start_utc"),
                    "total_duration_ms": row.get("total_duration_ms"),
                    "llm_latency_ms": row.get("llm_latency_ms"),
                    "llm_quality_score": row.get("llm_quality_score"),
                    "fallback_used": bool(row.get("fallback_used")),
                    "outcome": row.get("outcome"),
                    "error_code": error_code,
                    "llm_provider": row.get("llm_provider"),
                    "llm_model": row.get("llm_model"),
                    "stage_durations": durations,
                }
            )

        errors.sort(key=lambda item: float(item.get("ts_start") or 0.0), reverse=True)
        return errors[: max(0, int(limit))]

    @staticmethod
    def _build_health_entry(row: Mapping[str, object]) -> dict[str, object]:
        gates_state = KnowledgeAnalytics._parse_json_dict(row.get("gates_state"))
        metrics_brief = KnowledgeAnalytics._parse_json_dict(row.get("metrics_brief"))
        ts_value = row.get("ts_end_utc") or row.get("ts_end")
        try:
            ts_end_utc = float(ts_value) if ts_value is not None else None
        except (TypeError, ValueError):
            ts_end_utc = None
        return {
            "ts_end_utc": ts_end_utc,
            "snapshot_id": row.get("snapshot_id"),
            "system_mode": row.get("system_mode") or "",
            "gates_state": gates_state,
            "metrics_brief": metrics_brief,
        }

    def processing_spans_health_current(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
    ) -> dict[str, object] | None:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return None
        since_ts = self._window_start_ts(window_days)
        query = """
        SELECT ps.ts_end_utc, ps.health_snapshot_id AS snapshot_id,
               sh.gates_state, sh.metrics_brief, sh.system_mode
        FROM processing_spans ps
        JOIN system_health_snapshots sh ON ps.health_snapshot_id = sh.snapshot_id
        WHERE ps.health_snapshot_id != '' AND ps.ts_end_utc >= ?
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        params: list[object] = [since_ts, *clause_params]
        query += clause
        query += " ORDER BY ps.ts_end_utc DESC, ps.span_id DESC LIMIT 1"
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return None
        if not rows:
            return None
        return self._build_health_entry(rows[0])

    def processing_spans_health_timeline(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        limit: int = 200,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        since_ts = self._window_start_ts(window_days)
        resolved_limit = min(500, max(1, int(limit)))
        query = """
        SELECT ps.ts_end_utc, ps.health_snapshot_id AS snapshot_id,
               sh.gates_state, sh.metrics_brief, sh.system_mode
        FROM processing_spans ps
        JOIN system_health_snapshots sh ON ps.health_snapshot_id = sh.snapshot_id
        WHERE ps.health_snapshot_id != '' AND ps.ts_end_utc >= ?
        """
        clause, clause_params = self._account_scope_clause(account_ids)
        params: list[object] = [since_ts, *clause_params, resolved_limit]
        query += clause
        query += " ORDER BY ps.ts_end_utc DESC, ps.span_id DESC LIMIT ?"
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []
        return [self._build_health_entry(row) for row in rows]
