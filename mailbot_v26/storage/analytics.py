from __future__ import annotations

import hashlib
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
    _NARRATIVE_LEARNING_TYPES = {
        EventType.PRIORITY_CORRECTION_RECORDED.value,
        EventType.SURPRISE_DETECTED.value,
        EventType.ATTENTION_DEBT_UPDATED.value,
        EventType.CALIBRATION_PROPOSALS_GENERATED.value,
    }
    _NARRATIVE_HEALTH_TYPES = {
        EventType.DEADLOCK_DETECTED.value,
        EventType.SILENCE_SIGNAL_DETECTED.value,
        EventType.RELATIONSHIP_HEALTH_UPDATED.value,
        EventType.TRUST_SCORE_UPDATED.value,
        EventType.ANOMALY_DETECTED.value,
    }
    _NARRATIVE_SYSTEM_TYPES = {
        EventType.DAILY_DIGEST_SENT.value,
        EventType.WEEKLY_DIGEST_SENT.value,
    }
    _NARRATIVE_PROCESSING_TYPES = {
        EventType.EMAIL_RECEIVED.value,
        EventType.ATTACHMENT_EXTRACTED.value,
        EventType.PRIORITY_DECISION_RECORDED.value,
        EventType.TG_RENDER_RECORDED.value,
    }
    _NARRATIVE_DELIVERY_TYPES = {
        EventType.TELEGRAM_DELIVERED.value,
        EventType.TELEGRAM_FAILED.value,
        EventType.DELIVERY_POLICY_APPLIED.value,
        EventType.ATTENTION_DEFERRED_FOR_DIGEST.value,
        EventType.DAILY_DIGEST_SENT.value,
        EventType.WEEKLY_DIGEST_SENT.value,
    }
    _COMMITMENT_EVENT_TYPES = {
        EventType.COMMITMENT_CREATED.value,
        EventType.COMMITMENT_STATUS_CHANGED.value,
        EventType.COMMITMENT_EXPIRED.value,
    }
    _LANE_KEYS = (
        "all",
        "critical",
        "commitments",
        "deferred",
        "failures",
        "learning",
    )
    _LANE_EVENT_TYPES = {
        "critical": {
            EventType.PRIORITY_DECISION_RECORDED.value,
            EventType.TELEGRAM_FAILED.value,
            EventType.DELIVERY_POLICY_APPLIED.value,
            EventType.ANOMALY_DETECTED.value,
            EventType.DEADLOCK_DETECTED.value,
        },
        "commitments": set(_COMMITMENT_EVENT_TYPES),
        "deferred": {
            EventType.ATTENTION_DEFERRED_FOR_DIGEST.value,
            EventType.DELIVERY_POLICY_APPLIED.value,
        },
        "failures": {
            EventType.TELEGRAM_FAILED.value,
            EventType.DEADLOCK_DETECTED.value,
            EventType.SILENCE_SIGNAL_DETECTED.value,
            EventType.ANOMALY_DETECTED.value,
        },
        "learning": set(_NARRATIVE_LEARNING_TYPES),
    }

    def __init__(self, path: Path | str, *, read_only: bool = False) -> None:
        self.path = Path(path)
        self._query_only = bool(read_only)

    def _connect_readonly(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        try:
            conn.execute("PRAGMA busy_timeout = 750")
        except sqlite3.Error:
            conn.close()
            raise
        if self._query_only:
            try:
                conn.execute("PRAGMA query_only = ON")
            except sqlite3.Error:
                conn.close()
                raise
        return conn

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

    def _normalize_lane(self, lane: str | None) -> str:
        candidate = str(lane or "all").strip().lower()
        if candidate not in self._LANE_KEYS:
            return "all"
        return candidate

    def _lane_event_filter_types(self, lane: str | None) -> list[str]:
        normalized = self._normalize_lane(lane)
        if normalized == "all":
            return []
        types = self._LANE_EVENT_TYPES.get(normalized, set())
        return sorted(types)

    def _lane_email_clause(self, lane: str | None, *, since_ts: float) -> tuple[str, list[object]]:
        normalized = self._normalize_lane(lane)
        if normalized == "all":
            return "", []
        conditions: list[str] = []
        params: list[object] = []
        if normalized == "critical":
            conditions.append("e.priority = ?")
            params.append("🔴")
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                "WHERE ev.email_id = e.id AND ev.event_type = ? AND ev.ts_utc >= ?)"
            )
            params.extend([EventType.TELEGRAM_FAILED.value, since_ts])
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                "WHERE ev.email_id = e.id AND ev.event_type = ? AND ev.ts_utc >= ? "
                "AND ev.payload_json IS NOT NULL AND UPPER(ev.payload_json) LIKE ?)"
            )
            params.extend([EventType.DELIVERY_POLICY_APPLIED.value, since_ts, "%IMMEDIATE%"])
        elif normalized == "commitments":
            conditions.append(
                "EXISTS (SELECT 1 FROM commitments c "
                "WHERE c.email_row_id = e.id AND c.status IN (?, ?))"
            )
            params.extend(["pending", "expired"])
            commitment_types = sorted(self._COMMITMENT_EVENT_TYPES)
            placeholders = ", ".join(["?"] * len(commitment_types))
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                f"WHERE ev.email_id = e.id AND ev.event_type IN ({placeholders}) AND ev.ts_utc >= ?)"
            )
            params.extend(commitment_types)
            params.append(since_ts)
        elif normalized == "deferred":
            conditions.append("e.deferred_for_digest = 1")
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                "WHERE ev.email_id = e.id AND ev.event_type = ? AND ev.ts_utc >= ?)"
            )
            params.extend([EventType.ATTENTION_DEFERRED_FOR_DIGEST.value, since_ts])
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                "WHERE ev.email_id = e.id AND ev.event_type = ? AND ev.ts_utc >= ? "
                "AND ev.payload_json IS NOT NULL AND ("
                "UPPER(ev.payload_json) LIKE ? OR UPPER(ev.payload_json) LIKE ? OR UPPER(ev.payload_json) LIKE ?))"
            )
            params.extend(
                [
                    EventType.DELIVERY_POLICY_APPLIED.value,
                    since_ts,
                    "%BATCH%",
                    "%DEFER%",
                    "%SILENT%",
                ]
            )
        elif normalized == "failures":
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                "WHERE ev.email_id = e.id AND ev.event_type = ? AND ev.ts_utc >= ?)"
            )
            params.extend([EventType.TELEGRAM_FAILED.value, since_ts])
            conditions.append(
                "EXISTS (SELECT 1 FROM processing_spans ps "
                "WHERE ps.email_id = e.id AND ps.ts_start_utc >= ? AND ps.outcome != ?)"
            )
            params.extend([since_ts, "ok"])
        elif normalized == "learning":
            learning_types = sorted(self._NARRATIVE_LEARNING_TYPES)
            placeholders = ", ".join(["?"] * len(learning_types))
            conditions.append(
                "EXISTS (SELECT 1 FROM events_v1 ev "
                f"WHERE ev.email_id = e.id AND ev.event_type IN ({placeholders}) AND ev.ts_utc >= ?)"
            )
            params.extend(learning_types)
            params.append(since_ts)

        if not conditions:
            return "", []
        return " AND (" + " OR ".join(conditions) + ")", params

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
    def _mask_email_address(value: object) -> str:
        if not value:
            return ""
        text = str(value).strip()
        if not text or "@" not in text:
            return ""
        local, _, domain = text.partition("@")
        if not domain:
            return ""
        first = local[0] if local else ""
        return f"{first}…@{domain}"

    def _mask_contact_label(self, *, entity_id: str, label: str) -> str:
        candidate = (label or entity_id or "").strip()
        entity_value = (entity_id or "").strip()
        if "@" in entity_value:
            masked = self._mask_email_address(entity_value)
            if masked:
                return masked
        if "@" in candidate:
            masked = self._mask_email_address(candidate)
            if masked:
                return masked
        if not candidate:
            return "contact-unknown"
        digest = hashlib.sha1(candidate.encode("utf-8")).hexdigest()[:6]
        return f"contact-{digest}"

    @staticmethod
    def _strip_emails(text: str) -> str:
        if not text:
            return ""
        return re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "", text)

    @staticmethod
    def _clamp_preview_text(text: str, *, limit: int = 140) -> str:
        cleaned = " ".join((text or "").replace("\n", " ").split())
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[: max(0, limit - 1)] + "…"

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
    def _safe_float(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _estimate_attention_minutes(self, payload: dict[str, object]) -> float:
        word_count = self._safe_float(payload.get("word_count"))
        if word_count is not None and word_count > 0:
            words = max(40.0, word_count)
        else:
            char_count = self._safe_float(payload.get("body_chars"))
            if char_count is None or char_count <= 0:
                char_count = self._safe_float(payload.get("extracted_chars"))
            if char_count is None or char_count <= 0:
                char_count = self._safe_float(payload.get("size_bytes"))
            if char_count is not None and char_count > 0:
                words = max(40.0, char_count / 5.0)
            else:
                # Fallback: 0.5 minutes per email when length is unavailable.
                return 0.5
        minutes = words / 200.0
        return max(0.25, min(12.0, float(minutes)))

    @staticmethod
    def _account_scope_clause(account_ids: Sequence[str]) -> tuple[str, list[object]]:
        if not account_ids:
            return "", []
        if len(account_ids) == 1:
            return " AND account_id = ?", [account_ids[0]]
        placeholders = ", ".join(["?"] * len(account_ids))
        return f" AND account_id IN ({placeholders})", list(account_ids)

    @staticmethod
    def _account_email_clause(account_ids: Sequence[str]) -> tuple[str, list[object]]:
        if not account_ids:
            return "", []
        if len(account_ids) == 1:
            return " AND account_email = ?", [account_ids[0]]
        placeholders = ", ".join(["?"] * len(account_ids))
        return f" AND account_email IN ({placeholders})", list(account_ids)

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

    def _narrative_group_kind(self, event_type: str) -> str:
        if event_type in self._NARRATIVE_LEARNING_TYPES:
            return "learning"
        if event_type in self._NARRATIVE_HEALTH_TYPES:
            return "health"
        if event_type in self._NARRATIVE_SYSTEM_TYPES:
            return "system"
        return "other"

    def _narrative_filter_types(self, category: str) -> list[str]:
        cleaned = str(category or "all").strip().lower()
        if cleaned == "processing":
            return sorted(self._NARRATIVE_PROCESSING_TYPES)
        if cleaned == "delivery":
            return sorted(self._NARRATIVE_DELIVERY_TYPES)
        if cleaned == "health":
            return sorted(self._NARRATIVE_HEALTH_TYPES)
        if cleaned == "learning":
            return sorted(self._NARRATIVE_LEARNING_TYPES)
        return []

    def _narrative_event_notes(self, details: dict[str, object]) -> str:
        if not details:
            return ""
        skip_keys = {"stage", "outcome"}
        pairs = []
        for key in sorted(details.keys()):
            if key in skip_keys:
                continue
            value = details.get(key)
            pairs.append(f"{key}={value}")
            if len(pairs) >= 3:
                break
        return "; ".join(pairs)

    def _events_narrative_groups(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        filter_types: Sequence[str],
        page: int,
        page_size: int,
        reveal_pii: bool,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {
                "groups": [],
                "total_groups": 0,
                "page": page,
                "page_size": page_size,
            }
        resolved_page = max(1, int(page))
        resolved_page_size = max(1, int(page_size))
        since_ts = self._window_start_ts(window_days)

        clause, clause_params = self._account_scope_clause(account_ids)
        filter_clause = ""
        filter_params: list[object] = []
        if filter_types:
            placeholders = ", ".join(["?"] * len(filter_types))
            filter_clause = f" AND event_type IN ({placeholders})"
            filter_params = list(filter_types)

        group_kind_case = """
            CASE
                WHEN event_type IN ({learning}) THEN 'learning'
                WHEN event_type IN ({health}) THEN 'health'
                WHEN event_type IN ({system}) THEN 'system'
                ELSE 'other'
            END
        """.format(
            learning=", ".join(["?"] * len(self._NARRATIVE_LEARNING_TYPES)),
            health=", ".join(["?"] * len(self._NARRATIVE_HEALTH_TYPES)),
            system=", ".join(["?"] * len(self._NARRATIVE_SYSTEM_TYPES)),
        )
        group_kind_params: list[object] = [
            *sorted(self._NARRATIVE_LEARNING_TYPES),
            *sorted(self._NARRATIVE_HEALTH_TYPES),
            *sorted(self._NARRATIVE_SYSTEM_TYPES),
        ]

        grouped_query = f"""
        WITH scoped AS (
            SELECT id, event_type, ts_utc, email_id
            FROM events_v1
            WHERE ts_utc >= ?{filter_clause}
            {clause}
        )
        SELECT
            CASE WHEN email_id IS NOT NULL THEN 'email' ELSE {group_kind_case} END AS group_kind,
            CASE WHEN email_id IS NOT NULL THEN email_id ELSE event_type END AS group_id,
            MIN(ts_utc) AS ts_first,
            MAX(ts_utc) AS ts_last,
            COUNT(*) AS event_count
        FROM scoped
        GROUP BY group_kind, group_id
        ORDER BY ts_last DESC, group_kind ASC, group_id DESC
        LIMIT ? OFFSET ?
        """
        group_params: list[object] = [
            since_ts,
            *filter_params,
            *clause_params,
            *group_kind_params,
            resolved_page_size,
            (resolved_page - 1) * resolved_page_size,
        ]

        total_query = f"""
        WITH scoped AS (
            SELECT event_type, ts_utc, email_id
            FROM events_v1
            WHERE ts_utc >= ?{filter_clause}
            {clause}
        )
        SELECT COUNT(*) AS total
        FROM (
            SELECT 1
            FROM scoped
            GROUP BY
                CASE WHEN email_id IS NOT NULL THEN 'email' ELSE {group_kind_case} END,
                CASE WHEN email_id IS NOT NULL THEN email_id ELSE event_type END
        )
        """
        total_params: list[object] = [
            since_ts,
            *filter_params,
            *clause_params,
            *group_kind_params,
        ]

        with self._connect_readonly() as conn:
            conn.row_factory = sqlite3.Row
            try:
                group_rows = conn.execute(grouped_query, tuple(group_params)).fetchall()
                total_row = conn.execute(total_query, tuple(total_params)).fetchone()
            except sqlite3.OperationalError:
                return {
                    "groups": [],
                    "total_groups": 0,
                    "page": resolved_page,
                    "page_size": resolved_page_size,
                }

        total_groups = int(total_row["total"]) if total_row and total_row["total"] is not None else 0
        group_records = [dict(row) for row in group_rows]

        email_ids: list[int] = []
        for row in group_records:
            if row.get("group_kind") == "email":
                try:
                    email_ids.append(int(row.get("group_id")))
                except (TypeError, ValueError):
                    continue

        email_map: dict[int, dict[str, object]] = {}
        if email_ids:
            placeholders = ", ".join(["?"] * len(email_ids))
            email_query = f"""
            SELECT id, account_email, from_email, action_line, body_summary
            FROM emails
            WHERE id IN ({placeholders})
            """
            with self._connect_readonly() as conn:
                conn.row_factory = sqlite3.Row
                try:
                    for row in conn.execute(email_query, tuple(email_ids)):
                        email_map[int(row["id"])] = dict(row)
                except sqlite3.OperationalError:
                    email_map = {}

        delivery_map: dict[int, dict[str, float | None]] = {}
        if email_ids:
            placeholders = ", ".join(["?"] * len(email_ids))
            delivery_query = f"""
            SELECT
                email_id,
                MIN(CASE WHEN event_type = ? THEN ts_utc END) AS received_ts,
                MAX(CASE WHEN event_type = ? THEN ts_utc END) AS delivered_ts,
                MAX(CASE WHEN event_type = ? THEN ts_utc END) AS failed_ts
            FROM events_v1
            WHERE ts_utc >= ? AND email_id IN ({placeholders})
            {clause}
            GROUP BY email_id
            """
            delivery_params: list[object] = [
                EventType.EMAIL_RECEIVED.value,
                EventType.TELEGRAM_DELIVERED.value,
                EventType.TELEGRAM_FAILED.value,
                since_ts,
                *email_ids,
                *clause_params,
            ]
            with self._connect_readonly() as conn:
                conn.row_factory = sqlite3.Row
                try:
                    rows = conn.execute(delivery_query, tuple(delivery_params)).fetchall()
                except sqlite3.OperationalError:
                    rows = []
            for row in rows:
                row_dict = dict(row)
                try:
                    email_id = int(row_dict.get("email_id"))
                except (TypeError, ValueError):
                    continue
                delivery_map[email_id] = {
                    "received_ts": row_dict.get("received_ts"),
                    "delivered_ts": row_dict.get("delivered_ts"),
                    "failed_ts": row_dict.get("failed_ts"),
                }

        def _timeline_rows(
            group_kind: str, group_id: object, *, limit: int = 25
        ) -> list[dict[str, object]]:
            params: list[object] = []
            if group_kind == "email":
                group_clause = "email_id = ?"
                try:
                    params.append(int(group_id))
                except (TypeError, ValueError):
                    return []
            else:
                group_clause = "event_type = ? AND email_id IS NULL"
                params.append(str(group_id))
            params.append(since_ts)
            params.extend(clause_params)
            timeline_query = f"""
            SELECT id, event_type, ts_utc, payload, payload_json
            FROM events_v1
            WHERE {group_clause} AND ts_utc >= ?
            {clause}
            ORDER BY ts_utc DESC, id DESC
            LIMIT ?
            """
            params.append(limit)
            with self._connect_readonly() as conn:
                conn.row_factory = sqlite3.Row
                try:
                    rows = conn.execute(timeline_query, tuple(params)).fetchall()
                except sqlite3.OperationalError:
                    return []
            items = []
            for row in rows:
                row_dict = dict(row)
                payload = self._event_payload(row_dict)
                details = self._sanitize_event_payload(payload)
                items.append(
                    {
                        "event_id": row_dict.get("id"),
                        "ts_utc": row_dict.get("ts_utc"),
                        "event_type": row_dict.get("event_type"),
                        "stage": details.get("stage") if isinstance(details, Mapping) else None,
                        "outcome": details.get("outcome") if isinstance(details, Mapping) else None,
                        "details": details,
                    }
                )
            items.sort(key=lambda item: (float(item.get("ts_utc") or 0.0), int(item.get("event_id") or 0)))
            return items

        groups: list[dict[str, object]] = []
        for row in group_records:
            group_kind = str(row.get("group_kind") or "")
            group_id = row.get("group_id")
            timeline = _timeline_rows(group_kind, group_id, limit=25)
            headline: dict[str, object] = {}
            if group_kind == "email":
                try:
                    email_id = int(group_id)
                except (TypeError, ValueError):
                    email_id = None
                email_row = email_map.get(email_id or -1, {})
                from_value = email_row.get("from_email") or ""
                to_value = email_row.get("account_email") or ""
                preview = self._build_email_preview(
                    email_row.get("action_line"),
                    email_row.get("body_summary"),
                    reveal_pii=reveal_pii,
                    limit=160,
                )
                from_label = (
                    self._mask_email_address(from_value) if not reveal_pii else str(from_value)
                )
                to_label = (
                    self._mask_email_address(to_value) if not reveal_pii else str(to_value)
                )
                delivery = delivery_map.get(email_id or -1, {})
                delivered_ts = delivery.get("delivered_ts")
                failed_ts = delivery.get("failed_ts")
                received_ts = delivery.get("received_ts")
                status = "In-flight"
                if delivered_ts is not None:
                    status = "Delivered"
                elif failed_ts is not None:
                    status = "Failed"
                e2e_latency = None
                if delivered_ts is not None and received_ts is not None:
                    try:
                        e2e_latency = max(0.0, float(delivered_ts) - float(received_ts))
                    except (TypeError, ValueError):
                        e2e_latency = None
                headline = {
                    "from_masked": from_label,
                    "to_masked": to_label,
                    "preview_masked": preview,
                    "delivery_status": status,
                    "e2e_latency_s": e2e_latency,
                }
            else:
                label = str(group_id or "")
                label = label.replace("_", " ")
                status_label = ""
                if timeline:
                    details = timeline[-1].get("details") if isinstance(timeline[-1], dict) else {}
                    if isinstance(details, Mapping):
                        for key in ("outcome", "decision", "system_mode", "delivery_mode"):
                            if details.get(key):
                                status_label = str(details.get(key))
                                break
                headline = {
                    "label": label,
                    "status_label": status_label,
                }

            timeline_view = []
            for entry in timeline:
                details = entry.get("details") if isinstance(entry, Mapping) else {}
                notes = ""
                if isinstance(details, Mapping):
                    notes = self._narrative_event_notes(dict(details))
                timeline_view.append(
                    {
                        "ts_utc": entry.get("ts_utc"),
                        "event_type": entry.get("event_type"),
                        "stage": entry.get("stage") or "",
                        "outcome": entry.get("outcome") or "",
                        "notes_safe": notes,
                    }
                )

            groups.append(
                {
                    "group_kind": group_kind,
                    "group_id": group_id,
                    "ts_first": row.get("ts_first"),
                    "ts_last": row.get("ts_last"),
                    "event_count": int(row.get("event_count") or 0),
                    "headline": headline,
                    "timeline": timeline_view,
                }
            )

        return {
            "groups": groups,
            "total_groups": total_groups,
            "page": resolved_page,
            "page_size": resolved_page_size,
        }

    def events_narrative_v1(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        event_filter: str,
        page: int,
        page_size: int,
        reveal_pii: bool,
    ) -> dict[str, object]:
        filter_types = self._narrative_filter_types(event_filter)
        return self._events_narrative_groups(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days,
            filter_types=filter_types,
            page=page,
            page_size=page_size,
            reveal_pii=reveal_pii,
        )

    def lane_event_groups(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
        lane: str | None,
        event_filter: str,
        page: int,
        page_size: int,
        reveal_pii: bool,
    ) -> dict[str, object]:
        lane_types = set(self._lane_event_filter_types(lane))
        filter_types = set(self._narrative_filter_types(event_filter))
        if lane_types and filter_types:
            combined = sorted(lane_types.intersection(filter_types))
        elif lane_types:
            combined = sorted(lane_types)
        else:
            combined = sorted(filter_types)
        if lane_types and filter_types and not combined:
            return {
                "groups": [],
                "total_groups": 0,
                "page": page,
                "page_size": page_size,
            }
        return self._events_narrative_groups(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days,
            filter_types=combined,
            page=page,
            page_size=page_size,
            reveal_pii=reveal_pii,
        )

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

    def recent_mail_activity(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
        limit: int = 30,
        reveal_pii: bool = False,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        resolved_limit = max(0, int(limit))
        if resolved_limit <= 0:
            return []
        since_ts = self._window_start_ts(window_days)
        since_iso = datetime.fromtimestamp(
            since_ts, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        clause, clause_params = self._account_email_clause(account_ids)
        query = """
        SELECT id, account_email, from_email, action_line, body_summary, received_at
        FROM emails
        WHERE received_at >= ?
        """
        params: list[object] = [since_iso, *clause_params]
        query += clause
        query += " ORDER BY received_at DESC, id DESC LIMIT ?"
        params.append(resolved_limit * 2)
        try:
            email_rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

        delivered_events = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.TELEGRAM_DELIVERED.value,
            since_ts=since_ts,
        )
        failed_events = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.TELEGRAM_FAILED.value,
            since_ts=since_ts,
        )
        policy_events = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.DELIVERY_POLICY_APPLIED.value,
            since_ts=since_ts,
        )

        entries: dict[int, dict[str, object]] = {}
        for row in email_rows:
            try:
                email_id = int(row.get("id"))
            except (TypeError, ValueError):
                continue
            received_raw = str(row.get("received_at") or "")
            received_dt = parse_sqlite_datetime(received_raw)
            received_ts = received_dt.timestamp() if received_dt else None
            entries[email_id] = {
                "email_id": email_id,
                "account_email": row.get("account_email") or "",
                "from_email": row.get("from_email") or "",
                "action_line": row.get("action_line") or "",
                "body_summary": row.get("body_summary") or "",
                "received_ts_utc": received_ts,
                "status": "In-flight",
            }

        def _event_ts(row: Mapping[str, object]) -> float | None:
            payload = self._event_payload(row)
            if isinstance(payload, Mapping):
                candidate = payload.get("occurred_at_utc")
                try:
                    return float(candidate)
                except (TypeError, ValueError):
                    pass
            try:
                return float(row.get("ts_utc"))
            except (TypeError, ValueError):
                return None

        for event in policy_events:
            try:
                email_id = int(event.get("email_id"))
            except (TypeError, ValueError):
                continue
            if email_id not in entries:
                continue
            payload = self._event_payload(event)
            mode_value = ""
            if isinstance(payload, Mapping):
                raw_mode = payload.get("mode") or payload.get("delivery_mode")
                if raw_mode:
                    mode_value = str(raw_mode)
            ts_value = _event_ts(event) or 0.0
            existing_ts = entries[email_id].get("delivery_mode_ts")
            if existing_ts is None or ts_value >= float(existing_ts):
                entries[email_id]["delivery_mode_ts"] = ts_value
                entries[email_id]["delivery_mode"] = mode_value

        for event in delivered_events:
            try:
                email_id = int(event.get("email_id"))
            except (TypeError, ValueError):
                continue
            if email_id not in entries:
                continue
            ts_value = _event_ts(event)
            if ts_value is None:
                continue
            existing_ts = entries[email_id].get("delivered_ts_utc")
            if existing_ts is None or ts_value > float(existing_ts):
                entries[email_id]["delivered_ts_utc"] = ts_value
                entries[email_id]["status"] = "Delivered"

        for event in failed_events:
            try:
                email_id = int(event.get("email_id"))
            except (TypeError, ValueError):
                continue
            if email_id not in entries:
                continue
            if entries[email_id].get("delivered_ts_utc") is not None:
                continue
            ts_value = _event_ts(event)
            if ts_value is None:
                continue
            existing_ts = entries[email_id].get("failed_ts_utc")
            if existing_ts is None or ts_value > float(existing_ts):
                entries[email_id]["failed_ts_utc"] = ts_value
                entries[email_id]["status"] = "Failed"

        results: list[dict[str, object]] = []
        for entry in entries.values():
            received_ts = entry.get("received_ts_utc")
            delivered_ts = entry.get("delivered_ts_utc")
            e2e_seconds = None
            if delivered_ts is not None and received_ts is not None:
                try:
                    e2e_seconds = max(0.0, float(delivered_ts) - float(received_ts))
                except (TypeError, ValueError):
                    e2e_seconds = None
            mask_pii = not reveal_pii
            from_label = (
                self._mask_email_address(entry.get("from_email"))
                if mask_pii
                else str(entry.get("from_email") or "")
            )
            to_label = (
                self._mask_email_address(entry.get("account_email"))
                if mask_pii
                else str(entry.get("account_email") or "")
            )
            preview_parts = [
                str(entry.get("action_line") or ""),
                str(entry.get("body_summary") or ""),
            ]
            combined_preview = " — ".join(part for part in preview_parts if part)
            if mask_pii:
                combined_preview = self._strip_emails(combined_preview)
            preview = self._clamp_preview_text(combined_preview, limit=160)

            results.append(
                {
                    "email_id": entry.get("email_id"),
                    "received_ts_utc": received_ts,
                    "delivered_ts_utc": delivered_ts,
                    "status": entry.get("status") or "In-flight",
                    "from_label": from_label,
                    "to_label": to_label,
                    "telegram_preview": preview,
                    "e2e_seconds": e2e_seconds,
                    "delivery_mode": entry.get("delivery_mode") or "",
                }
            )

        sorted_rows = sorted(
            results,
            key=lambda item: (
                item.get("delivered_ts_utc") is None,
                -(float(item.get("delivered_ts_utc") or 0.0)),
                -(float(item.get("received_ts_utc") or 0.0)),
                -(float(item.get("email_id") or 0.0)),
            ),
        )
        return sorted_rows[:resolved_limit]

    def lane_counts(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None,
        window_days: int,
    ) -> dict[str, int]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        counts: dict[str, int] = {key: 0 for key in self._LANE_KEYS}
        if not account_ids:
            return counts
        since_ts = self._window_start_ts(window_days)
        since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()
        clause, clause_params = self._account_email_clause(account_ids)
        base_query = """
        SELECT COUNT(1) AS total_count
        FROM emails e
        WHERE e.received_at >= ?
        """
        for lane in self._LANE_KEYS:
            lane_clause, lane_params = self._lane_email_clause(lane, since_ts=since_ts)
            query = base_query + clause + lane_clause
            params: list[object] = [since_iso, *clause_params, *lane_params]
            try:
                rows = self._execute_select(query, params)
            except sqlite3.OperationalError:
                rows = []
            total = int(rows[0].get("total_count") or 0) if rows else 0
            counts[lane] = total
        return counts

    def lane_activity_rows(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
        limit: int = 30,
        lane: str | None = None,
        reveal_pii: bool = False,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return []
        resolved_limit = max(0, int(limit))
        if resolved_limit <= 0:
            return []
        since_ts = self._window_start_ts(window_days)
        since_iso = datetime.fromtimestamp(
            since_ts, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        clause, clause_params = self._account_email_clause(account_ids)
        lane_clause, lane_params = self._lane_email_clause(lane, since_ts=since_ts)
        query = """
        SELECT id, account_email, from_email, action_line, body_summary, received_at
        FROM emails e
        WHERE received_at >= ?
        """
        params: list[object] = [since_iso, *clause_params, *lane_params]
        query += clause
        query += lane_clause
        query += " ORDER BY received_at DESC, id DESC LIMIT ?"
        params.append(resolved_limit * 2)
        try:
            email_rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []

        delivered_events = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.TELEGRAM_DELIVERED.value,
            since_ts=since_ts,
        )
        failed_events = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.TELEGRAM_FAILED.value,
            since_ts=since_ts,
        )
        policy_events = self._event_rows_scoped(
            account_ids=account_ids,
            event_type=EventType.DELIVERY_POLICY_APPLIED.value,
            since_ts=since_ts,
        )

        entries: dict[int, dict[str, object]] = {}
        for row in email_rows:
            try:
                email_id = int(row.get("id"))
            except (TypeError, ValueError):
                continue
            received_raw = str(row.get("received_at") or "")
            received_dt = parse_sqlite_datetime(received_raw)
            received_ts = received_dt.timestamp() if received_dt else None
            entries[email_id] = {
                "email_id": email_id,
                "account_email": row.get("account_email") or "",
                "from_email": row.get("from_email") or "",
                "action_line": row.get("action_line") or "",
                "body_summary": row.get("body_summary") or "",
                "received_ts_utc": received_ts,
                "status": "In-flight",
            }

        def _event_ts(row: Mapping[str, object]) -> float | None:
            payload = self._event_payload(row)
            if isinstance(payload, Mapping):
                candidate = payload.get("occurred_at_utc")
                try:
                    return float(candidate)
                except (TypeError, ValueError):
                    pass
            try:
                return float(row.get("ts_utc"))
            except (TypeError, ValueError):
                return None

        for event in policy_events:
            try:
                email_id = int(event.get("email_id"))
            except (TypeError, ValueError):
                continue
            if email_id not in entries:
                continue
            payload = self._event_payload(event)
            mode_value = ""
            if isinstance(payload, Mapping):
                raw_mode = payload.get("mode") or payload.get("delivery_mode")
                if raw_mode:
                    mode_value = str(raw_mode)
            ts_value = _event_ts(event) or 0.0
            existing_ts = entries[email_id].get("delivery_mode_ts")
            if existing_ts is None or ts_value >= float(existing_ts):
                entries[email_id]["delivery_mode_ts"] = ts_value
                entries[email_id]["delivery_mode"] = mode_value

        for event in delivered_events:
            try:
                email_id = int(event.get("email_id"))
            except (TypeError, ValueError):
                continue
            if email_id not in entries:
                continue
            ts_value = _event_ts(event)
            if ts_value is None:
                continue
            existing_ts = entries[email_id].get("delivered_ts_utc")
            if existing_ts is None or ts_value > float(existing_ts):
                entries[email_id]["delivered_ts_utc"] = ts_value
                entries[email_id]["status"] = "Delivered"

        for event in failed_events:
            try:
                email_id = int(event.get("email_id"))
            except (TypeError, ValueError):
                continue
            if email_id not in entries:
                continue
            if entries[email_id].get("delivered_ts_utc") is not None:
                continue
            ts_value = _event_ts(event)
            if ts_value is None:
                continue
            existing_ts = entries[email_id].get("failed_ts_utc")
            if existing_ts is None or ts_value > float(existing_ts):
                entries[email_id]["failed_ts_utc"] = ts_value
                entries[email_id]["status"] = "Failed"

        results: list[dict[str, object]] = []
        for entry in entries.values():
            received_ts = entry.get("received_ts_utc")
            delivered_ts = entry.get("delivered_ts_utc")
            e2e_seconds = None
            if delivered_ts is not None and received_ts is not None:
                try:
                    e2e_seconds = max(0.0, float(delivered_ts) - float(received_ts))
                except (TypeError, ValueError):
                    e2e_seconds = None
            mask_pii = not reveal_pii
            from_label = (
                self._mask_email_address(entry.get("from_email"))
                if mask_pii
                else str(entry.get("from_email") or "")
            )
            to_label = (
                self._mask_email_address(entry.get("account_email"))
                if mask_pii
                else str(entry.get("account_email") or "")
            )
            preview_parts = [
                str(entry.get("action_line") or ""),
                str(entry.get("body_summary") or ""),
            ]
            combined_preview = " — ".join(part for part in preview_parts if part)
            if mask_pii:
                combined_preview = self._strip_emails(combined_preview)
            preview = self._clamp_preview_text(combined_preview, limit=160)

            results.append(
                {
                    "email_id": entry.get("email_id"),
                    "received_ts_utc": received_ts,
                    "delivered_ts_utc": delivered_ts,
                    "status": entry.get("status") or "In-flight",
                    "from_label": from_label,
                    "to_label": to_label,
                    "telegram_preview": preview,
                    "e2e_seconds": e2e_seconds,
                    "delivery_mode": entry.get("delivery_mode") or "",
                }
            )

        sorted_rows = sorted(
            results,
            key=lambda item: (
                item.get("delivered_ts_utc") is None,
                -(float(item.get("delivered_ts_utc") or 0.0)),
                -(float(item.get("received_ts_utc") or 0.0)),
                -(float(item.get("email_id") or 0.0)),
            ),
        )
        return sorted_rows[:resolved_limit]

    def _build_email_preview(
        self,
        action_line: object,
        body_summary: object,
        *,
        reveal_pii: bool,
        limit: int = 160,
    ) -> str:
        preview_parts = [
            str(action_line or ""),
            str(body_summary or ""),
        ]
        combined_preview = " — ".join(part for part in preview_parts if part)
        if not reveal_pii:
            combined_preview = self._strip_emails(combined_preview)
        return self._clamp_preview_text(combined_preview, limit=limit)

    def _stage_breakdown_hint(self, raw: object, *, limit: int = 2) -> str:
        durations = self._parse_json_dict(raw)
        if not durations:
            return ""
        parsed: list[tuple[str, float]] = []
        for stage, value in durations.items():
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if numeric < 0:
                continue
            label = str(stage).replace("_", " ").strip()
            if not label:
                continue
            parsed.append((label, numeric))
        if not parsed:
            return ""
        parsed.sort(key=lambda item: (-item[1], item[0]))
        entries = []
        for label, duration in parsed[: max(0, int(limit))]:
            entries.append(f"{label} {int(round(duration))}ms")
        return " • ".join(entries)

    def _sanitize_failure_reason(self, raw: object, *, reveal_pii: bool) -> str:
        reason = str(raw or "").strip()
        if not reason:
            return ""
        if not reveal_pii:
            reason = self._strip_emails(reason)
        return self._clamp_preview_text(reason, limit=160)

    def email_archive_page(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
        page: int = 1,
        page_size: int = 50,
        status: str = "any",
        reveal_pii: bool = False,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {"rows": [], "total_count": 0}
        resolved_page = max(1, int(page))
        resolved_page_size = max(1, int(page_size))
        since_ts = self._window_start_ts(window_days)
        since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()
        status_norm = str(status or "any").strip().lower()
        if status_norm not in {"any", "ok", "warn", "fail"}:
            status_norm = "any"

        clause, clause_params = self._account_email_clause(account_ids)
        status_clause = "1=1"
        if status_norm == "ok":
            status_clause = "delivered_ts IS NOT NULL"
        elif status_norm == "fail":
            status_clause = "failed_ts IS NOT NULL AND delivered_ts IS NULL"
        elif status_norm == "warn":
            status_clause = "delivered_ts IS NULL AND failed_ts IS NULL"

        base_query = """
        WITH scoped AS (
            SELECT e.id, e.account_email, e.from_email, e.action_line, e.body_summary, e.received_at,
                (
                    SELECT MAX(ts_utc) FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                ) AS delivered_ts,
                (
                    SELECT MAX(ts_utc) FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                ) AS failed_ts,
                (
                    SELECT payload_json FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                    ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                    LIMIT 1
                ) AS failed_payload,
                (
                    SELECT payload_json FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                    ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                    LIMIT 1
                ) AS policy_payload,
                (
                    SELECT payload_json FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                    ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                    LIMIT 1
                ) AS delivered_payload,
                (
                    SELECT stage_durations_json FROM processing_spans
                    WHERE email_id = e.id AND ts_start_utc >= ?
                    ORDER BY ts_start_utc DESC, span_id ASC
                    LIMIT 1
                ) AS stage_durations_json
            FROM emails e
            WHERE e.received_at >= ?
        """
        params: list[object] = [
            EventType.TELEGRAM_DELIVERED.value,
            since_ts,
            EventType.TELEGRAM_FAILED.value,
            since_ts,
            EventType.TELEGRAM_FAILED.value,
            since_ts,
            EventType.DELIVERY_POLICY_APPLIED.value,
            since_ts,
            EventType.TELEGRAM_DELIVERED.value,
            since_ts,
            since_ts,
            since_iso,
            *clause_params,
        ]
        base_query += clause
        base_query += "\n        )\n"
        count_query = f"{base_query}SELECT COUNT(1) AS total_count FROM scoped WHERE {status_clause}"
        try:
            count_rows = self._execute_select(count_query, params)
            total_count = int(count_rows[0].get("total_count") or 0) if count_rows else 0
        except sqlite3.OperationalError:
            total_count = 0

        page_offset = (resolved_page - 1) * resolved_page_size
        page_query = (
            f"{base_query}SELECT * FROM scoped WHERE {status_clause} "
            "ORDER BY received_at DESC, id DESC LIMIT ? OFFSET ?"
        )
        page_params = [*params, resolved_page_size, page_offset]
        try:
            rows = self._execute_select(page_query, page_params)
        except sqlite3.OperationalError:
            rows = []

        results: list[dict[str, object]] = []
        for row in rows:
            received_dt = parse_sqlite_datetime(str(row.get("received_at") or ""))
            received_ts = received_dt.timestamp() if received_dt else None
            delivered_ts = self._safe_float(row.get("delivered_ts"))
            failed_ts = self._safe_float(row.get("failed_ts"))
            status_label = "In-flight"
            if delivered_ts is not None:
                status_label = "Delivered"
            elif failed_ts is not None:
                status_label = "Failed"
            e2e_seconds = None
            if delivered_ts is not None and received_ts is not None:
                e2e_seconds = max(0.0, float(delivered_ts) - float(received_ts))
            from_email = row.get("from_email") or ""
            account_email = row.get("account_email") or ""
            from_label = (
                self._mask_email_address(from_email)
                if not reveal_pii
                else str(from_email)
            )
            account_label = (
                self._mask_email_address(account_email)
                if not reveal_pii
                else str(account_email)
            )
            preview = self._build_email_preview(
                row.get("action_line"),
                row.get("body_summary"),
                reveal_pii=reveal_pii,
            )
            policy_payload = self._parse_json_dict(row.get("policy_payload"))
            delivered_payload = self._parse_json_dict(row.get("delivered_payload"))
            delivery_mode = ""
            if policy_payload:
                raw_mode = policy_payload.get("mode") or policy_payload.get("delivery_mode")
                if raw_mode:
                    delivery_mode = str(raw_mode)
            if not delivery_mode and delivered_payload:
                raw_mode = delivered_payload.get("delivery_mode")
                if raw_mode:
                    delivery_mode = str(raw_mode)
            failed_payload = self._parse_json_dict(row.get("failed_payload"))
            failure_reason = self._sanitize_failure_reason(
                failed_payload.get("error") if failed_payload else "",
                reveal_pii=reveal_pii,
            )
            stage_hint = self._stage_breakdown_hint(row.get("stage_durations_json"))

            results.append(
                {
                    "email_id": row.get("id"),
                    "received_ts_utc": received_ts,
                    "from_label": from_label,
                    "account_label": account_label,
                    "preview": preview,
                    "status": status_label,
                    "e2e_seconds": e2e_seconds,
                    "delivery_mode": delivery_mode,
                    "failure_reason": failure_reason,
                    "stage_hint": stage_hint,
                }
            )

        return {"rows": results, "total_count": total_count}

    def lane_archive_rows(
        self,
        *,
        account_email: str,
        account_emails: Iterable[str] | None = None,
        window_days: int = 7,
        page: int = 1,
        page_size: int = 50,
        status: str = "any",
        lane: str | None = None,
        reveal_pii: bool = False,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return {"rows": [], "total_count": 0}
        resolved_page = max(1, int(page))
        resolved_page_size = max(1, int(page_size))
        since_ts = self._window_start_ts(window_days)
        since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()
        status_norm = str(status or "any").strip().lower()
        if status_norm not in {"any", "ok", "warn", "fail"}:
            status_norm = "any"

        clause, clause_params = self._account_email_clause(account_ids)
        lane_clause, lane_params = self._lane_email_clause(lane, since_ts=since_ts)
        status_clause = "1=1"
        if status_norm == "ok":
            status_clause = "delivered_ts IS NOT NULL"
        elif status_norm == "fail":
            status_clause = "failed_ts IS NOT NULL AND delivered_ts IS NULL"
        elif status_norm == "warn":
            status_clause = "delivered_ts IS NULL AND failed_ts IS NULL"

        base_query = """
        WITH scoped AS (
            SELECT e.id, e.account_email, e.from_email, e.action_line, e.body_summary, e.received_at,
                (
                    SELECT MAX(ts_utc) FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                ) AS delivered_ts,
                (
                    SELECT MAX(ts_utc) FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                ) AS failed_ts,
                (
                    SELECT payload_json FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                    ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                    LIMIT 1
                ) AS failed_payload,
                (
                    SELECT payload_json FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                    ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                    LIMIT 1
                ) AS policy_payload,
                (
                    SELECT payload_json FROM events_v1
                    WHERE email_id = e.id AND event_type = ? AND ts_utc >= ?
                    ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                    LIMIT 1
                ) AS delivered_payload,
                (
                    SELECT stage_durations_json FROM processing_spans
                    WHERE email_id = e.id AND ts_start_utc >= ?
                    ORDER BY ts_start_utc DESC, span_id ASC
                    LIMIT 1
                ) AS stage_durations_json
            FROM emails e
            WHERE e.received_at >= ?
        """
        params: list[object] = [
            EventType.TELEGRAM_DELIVERED.value,
            since_ts,
            EventType.TELEGRAM_FAILED.value,
            since_ts,
            EventType.TELEGRAM_FAILED.value,
            since_ts,
            EventType.DELIVERY_POLICY_APPLIED.value,
            since_ts,
            EventType.TELEGRAM_DELIVERED.value,
            since_ts,
            since_ts,
            since_iso,
            *clause_params,
            *lane_params,
        ]
        base_query += clause
        base_query += lane_clause
        base_query += "\n        )\n"
        count_query = f"{base_query}SELECT COUNT(1) AS total_count FROM scoped WHERE {status_clause}"
        try:
            count_rows = self._execute_select(count_query, params)
            total_count = int(count_rows[0].get("total_count") or 0) if count_rows else 0
        except sqlite3.OperationalError:
            total_count = 0

        page_offset = (resolved_page - 1) * resolved_page_size
        page_query = (
            f"{base_query}SELECT * FROM scoped WHERE {status_clause} "
            "ORDER BY received_at DESC, id DESC LIMIT ? OFFSET ?"
        )
        page_params = [*params, resolved_page_size, page_offset]
        try:
            rows = self._execute_select(page_query, page_params)
        except sqlite3.OperationalError:
            rows = []

        results: list[dict[str, object]] = []
        for row in rows:
            received_dt = parse_sqlite_datetime(str(row.get("received_at") or ""))
            received_ts = received_dt.timestamp() if received_dt else None
            delivered_ts = self._safe_float(row.get("delivered_ts"))
            failed_ts = self._safe_float(row.get("failed_ts"))
            status_label = "In-flight"
            if delivered_ts is not None:
                status_label = "Delivered"
            elif failed_ts is not None:
                status_label = "Failed"
            e2e_seconds = None
            if delivered_ts is not None and received_ts is not None:
                e2e_seconds = max(0.0, float(delivered_ts) - float(received_ts))
            from_email = row.get("from_email") or ""
            account_email = row.get("account_email") or ""
            from_label = (
                self._mask_email_address(from_email)
                if not reveal_pii
                else str(from_email)
            )
            account_label = (
                self._mask_email_address(account_email)
                if not reveal_pii
                else str(account_email)
            )
            preview = self._build_email_preview(
                row.get("action_line"),
                row.get("body_summary"),
                reveal_pii=reveal_pii,
            )
            policy_payload = self._parse_json_dict(row.get("policy_payload"))
            delivered_payload = self._parse_json_dict(row.get("delivered_payload"))
            delivery_mode = ""
            if policy_payload:
                raw_mode = policy_payload.get("mode") or policy_payload.get("delivery_mode")
                if raw_mode:
                    delivery_mode = str(raw_mode)
            if not delivery_mode and delivered_payload:
                raw_mode = delivered_payload.get("delivery_mode")
                if raw_mode:
                    delivery_mode = str(raw_mode)
            failed_payload = self._parse_json_dict(row.get("failed_payload"))
            failure_reason = self._sanitize_failure_reason(
                failed_payload.get("error") if failed_payload else "",
                reveal_pii=reveal_pii,
            )
            stage_hint = self._stage_breakdown_hint(row.get("stage_durations_json"))

            results.append(
                {
                    "email_id": row.get("id"),
                    "received_ts_utc": received_ts,
                    "delivered_ts_utc": delivered_ts,
                    "failed_ts_utc": failed_ts,
                    "status": status_label,
                    "from_label": from_label,
                    "account_label": account_label,
                    "preview": preview,
                    "e2e_seconds": e2e_seconds,
                    "delivery_mode": delivery_mode,
                    "failure_reason": failure_reason,
                    "stage_hint": stage_hint,
                }
            )

        return {"rows": results, "total_count": total_count}

    def email_forensics_detail(
        self,
        *,
        email_id: int,
        reveal_pii: bool = False,
    ) -> dict[str, object] | None:
        query = """
        SELECT e.id, e.account_email, e.from_email, e.action_line, e.body_summary, e.received_at,
            (
                SELECT MAX(ts_utc) FROM events_v1
                WHERE email_id = e.id AND event_type = ?
            ) AS delivered_ts,
            (
                SELECT MAX(ts_utc) FROM events_v1
                WHERE email_id = e.id AND event_type = ?
            ) AS failed_ts,
            (
                SELECT payload_json FROM events_v1
                WHERE email_id = e.id AND event_type = ?
                ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                LIMIT 1
            ) AS failed_payload,
            (
                SELECT payload_json FROM events_v1
                WHERE email_id = e.id AND event_type = ?
                ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                LIMIT 1
            ) AS policy_payload,
            (
                SELECT payload_json FROM events_v1
                WHERE email_id = e.id AND event_type = ?
                ORDER BY ts_utc DESC, event_type ASC, email_id DESC
                LIMIT 1
            ) AS delivered_payload
        FROM emails e
        WHERE e.id = ?
        LIMIT 1
        """
        params = [
            EventType.TELEGRAM_DELIVERED.value,
            EventType.TELEGRAM_FAILED.value,
            EventType.TELEGRAM_FAILED.value,
            EventType.DELIVERY_POLICY_APPLIED.value,
            EventType.TELEGRAM_DELIVERED.value,
            int(email_id),
        ]
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return None
        if not rows:
            return None
        row = rows[0]
        received_dt = parse_sqlite_datetime(str(row.get("received_at") or ""))
        received_ts = received_dt.timestamp() if received_dt else None
        delivered_ts = self._safe_float(row.get("delivered_ts"))
        failed_ts = self._safe_float(row.get("failed_ts"))
        status_label = "In-flight"
        if delivered_ts is not None:
            status_label = "Delivered"
        elif failed_ts is not None:
            status_label = "Failed"
        e2e_seconds = None
        if delivered_ts is not None and received_ts is not None:
            e2e_seconds = max(0.0, float(delivered_ts) - float(received_ts))
        from_email = row.get("from_email") or ""
        account_email = row.get("account_email") or ""
        from_label = (
            self._mask_email_address(from_email)
            if not reveal_pii
            else str(from_email)
        )
        account_label = (
            self._mask_email_address(account_email)
            if not reveal_pii
            else str(account_email)
        )
        preview = self._build_email_preview(
            row.get("action_line"),
            row.get("body_summary"),
            reveal_pii=reveal_pii,
        )
        policy_payload = self._parse_json_dict(row.get("policy_payload"))
        delivered_payload = self._parse_json_dict(row.get("delivered_payload"))
        delivery_mode = ""
        if policy_payload:
            raw_mode = policy_payload.get("mode") or policy_payload.get("delivery_mode")
            if raw_mode:
                delivery_mode = str(raw_mode)
        if not delivery_mode and delivered_payload:
            raw_mode = delivered_payload.get("delivery_mode")
            if raw_mode:
                delivery_mode = str(raw_mode)
        failed_payload = self._parse_json_dict(row.get("failed_payload"))
        failure_reason = self._sanitize_failure_reason(
            failed_payload.get("error") if failed_payload else "",
            reveal_pii=reveal_pii,
        )

        return {
            "email_id": row.get("id"),
            "received_ts_utc": received_ts,
            "from_label": from_label,
            "account_label": account_label,
            "preview": preview,
            "status": status_label,
            "delivered_ts_utc": delivered_ts,
            "failed_ts_utc": failed_ts,
            "e2e_seconds": e2e_seconds,
            "delivery_mode": delivery_mode,
            "failure_reason": failure_reason,
        }

    def email_processing_timeline(self, *, email_id: int) -> list[dict[str, object]]:
        query = """
        SELECT span_id, ts_start_utc, total_duration_ms, stage_durations_json, outcome, error_code
        FROM processing_spans
        WHERE email_id = ?
        ORDER BY ts_start_utc ASC, span_id ASC
        """
        try:
            rows = self._execute_select(query, [int(email_id)])
        except sqlite3.OperationalError:
            return []
        timeline: list[dict[str, object]] = []
        for row in rows:
            durations = self._parse_json_dict(row.get("stage_durations_json"))
            stage_entries: list[tuple[str, float]] = []
            for stage, value in durations.items():
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    continue
                stage_name = str(stage).strip()
                if not stage_name:
                    continue
                stage_entries.append((stage_name, numeric))
            if not stage_entries:
                try:
                    total_ms = float(row.get("total_duration_ms"))
                except (TypeError, ValueError):
                    total_ms = None
                if total_ms is not None:
                    stage_entries = [("total", total_ms)]
            for stage_name, duration_ms in stage_entries:
                timeline.append(
                    {
                        "span_id": row.get("span_id"),
                        "ts_start_utc": row.get("ts_start_utc"),
                        "stage": stage_name,
                        "duration_ms": duration_ms,
                        "outcome": row.get("outcome"),
                        "error_code": row.get("error_code"),
                    }
                )
        timeline.sort(
            key=lambda item: (
                float(item.get("ts_start_utc") or 0.0),
                str(item.get("stage") or ""),
                str(item.get("span_id") or ""),
            )
        )
        return timeline

    def email_forensics_events(
        self, *, email_id: int, limit: int = 8
    ) -> list[dict[str, object]]:
        try:
            resolved_limit = max(0, int(limit))
        except (TypeError, ValueError):
            resolved_limit = 0
        if resolved_limit <= 0:
            return []
        query = """
        SELECT id, event_type, ts_utc, payload, payload_json
        FROM events_v1
        WHERE email_id = ?
        ORDER BY ts_utc DESC, id DESC
        LIMIT ?
        """
        try:
            rows = self._execute_select(query, [int(email_id), resolved_limit])
        except sqlite3.OperationalError:
            return []
        results: list[dict[str, object]] = []
        for row in rows:
            payload = self._event_payload(row)
            stage = str(payload.get("stage") or payload.get("reason") or "")
            outcome = str(payload.get("outcome") or "")
            duration_ms = payload.get("duration_ms")
            results.append(
                {
                    "event_id": row.get("id"),
                    "event_type": row.get("event_type"),
                    "ts_utc": row.get("ts_utc"),
                    "stage": stage,
                    "outcome": outcome,
                    "duration_ms": duration_ms,
                }
            )
        return results

    def _priority_digest_counts(
        self,
        *,
        account_ids: Sequence[str],
        since_ts: float,
    ) -> list[dict[str, object]]:
        if not account_ids:
            return []
        since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()
        clause, clause_params = self._account_email_clause(account_ids)
        query = """
        SELECT priority, COUNT(*) AS count
        FROM emails
        WHERE received_at >= ?
        """
        params: list[object] = [since_iso, *clause_params]
        query += clause
        query += " GROUP BY priority ORDER BY count DESC, priority ASC"
        try:
            rows = self._execute_select(query, params)
        except sqlite3.OperationalError:
            return []
        results: list[dict[str, object]] = []
        for row in rows:
            label = str(row.get("priority") or "Unclassified").strip() or "Unclassified"
            results.append({"label": label.title(), "count": int(row.get("count") or 0)})
        return results

    def _latency_distribution(
        self,
        *,
        account_ids: Sequence[str],
        window_days: int,
    ) -> list[dict[str, object]]:
        if not account_ids:
            return []
        since_ts = self._window_start_ts(window_days)
        rows = self._processing_span_rows_scoped(account_ids=account_ids, since_ts=since_ts)
        bins = [
            (0.0, 1.0, "0-1s"),
            (1.0, 2.0, "1-2s"),
            (2.0, 5.0, "2-5s"),
            (5.0, 10.0, "5-10s"),
            (10.0, 30.0, "10-30s"),
            (30.0, 60.0, "30-60s"),
        ]
        counts: list[int] = [0 for _ in bins]
        overflow = 0
        for row in rows:
            total_ms = row.get("total_duration_ms")
            if total_ms is None:
                try:
                    total_ms = (float(row.get("ts_end_utc")) - float(row.get("ts_start_utc"))) * 1000.0
                except (TypeError, ValueError):
                    total_ms = None
            if total_ms is None:
                continue
            seconds = max(0.0, float(total_ms) / 1000.0)
            placed = False
            for idx, (start, end, _) in enumerate(bins):
                if start <= seconds < end:
                    counts[idx] += 1
                    placed = True
                    break
            if not placed:
                overflow += 1
        results = [
            {"label": label, "count": counts[idx]}
            for idx, (_, _, label) in enumerate(bins)
            if counts[idx] > 0
        ]
        if overflow:
            results.append({"label": "60s+", "count": overflow})
        return results

    def cockpit_summary(
        self,
        *,
        account_emails: Iterable[str] | None,
        window_days: int,
        allow_pii: bool,
        include_engineer: bool = False,
        activity_limit: int = 15,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope("", account_emails)
        db_size_bytes = None
        try:
            db_size_bytes = self.path.stat().st_size
        except OSError:
            db_size_bytes = None

        status_strip: dict[str, object] = {
            "system_mode": "unknown",
            "gates_state": {},
            "metrics_brief": {},
            "updated_ts_utc": None,
            "db_size_bytes": db_size_bytes,
        }

        if account_ids:
            current = self.processing_spans_health_current(
                account_email=account_ids[0],
                account_emails=account_ids,
                window_days=window_days,
            )
            if current:
                status_strip = {
                    "system_mode": current.get("system_mode") or "unknown",
                    "gates_state": current.get("gates_state") or {},
                    "metrics_brief": current.get("metrics_brief") or {},
                    "updated_ts_utc": current.get("ts_end_utc"),
                    "db_size_bytes": db_size_bytes,
                }

        if not account_ids:
            return {
                "status_strip": status_strip,
                "today_digest": {"counts": [], "items": []},
                "week_digest": {"counts": [], "items": []},
                "recent_activity": [],
                "golden_signals": {},
                "engineer": {},
            }

        primary_account = account_ids[0]
        summary = self.processing_spans_metrics_digest(
            account_email=primary_account,
            account_emails=account_ids,
            window_days=window_days,
        )
        recent_activity = self.recent_mail_activity(
            account_email=primary_account,
            account_emails=account_ids,
            window_days=window_days,
            limit=min(50, max(1, int(activity_limit))),
            reveal_pii=allow_pii,
        )
        today_items = self.recent_mail_activity(
            account_email=primary_account,
            account_emails=account_ids,
            window_days=1,
            limit=3,
            reveal_pii=allow_pii,
        )
        week_items = self.recent_mail_activity(
            account_email=primary_account,
            account_emails=account_ids,
            window_days=7,
            limit=3,
            reveal_pii=allow_pii,
        )
        today_counts = self._priority_digest_counts(
            account_ids=account_ids, since_ts=self._window_start_ts(1)
        )
        week_counts = self._priority_digest_counts(
            account_ids=account_ids, since_ts=self._window_start_ts(7)
        )

        golden_signals = {
            "latency_p50_ms": summary.get("total_duration_ms_p50"),
            "latency_p95_ms": summary.get("total_duration_ms_p95"),
            "error_rate": summary.get("error_rate"),
            "fallback_rate": summary.get("fallback_rate"),
            "span_count": summary.get("span_count"),
            "db_size_bytes": db_size_bytes,
        }
        metrics_brief = status_strip.get("metrics_brief") if isinstance(status_strip, Mapping) else {}
        metrics_window: Mapping[str, object] | None = None
        if isinstance(metrics_brief, Mapping) and metrics_brief:
            for key in sorted(metrics_brief.keys(), key=lambda item: str(item)):
                window_values = metrics_brief.get(key)
                if isinstance(window_values, Mapping):
                    metrics_window = window_values
                    break
        if metrics_window:
            success_rate = metrics_window.get("telegram_delivery_success_rate")
            try:
                if success_rate is not None:
                    tg_failure_rate = max(0.0, 1.0 - float(success_rate))
                    golden_signals["tg_failure_rate"] = tg_failure_rate
            except (TypeError, ValueError):
                pass

        engineer: dict[str, object] = {}
        if include_engineer:
            engineer = {
                "slow_spans": self.processing_spans_slowest(
                    account_email=primary_account,
                    account_emails=account_ids,
                    window_days=window_days,
                    limit=20,
                ),
                "recent_errors": self.processing_spans_recent_errors(
                    account_email=primary_account,
                    account_emails=account_ids,
                    window_days=window_days,
                    limit=20,
                ),
                "latency_distribution": self._latency_distribution(
                    account_ids=account_ids, window_days=window_days
                ),
            }

        return {
            "status_strip": status_strip,
            "today_digest": {"counts": today_counts, "items": today_items},
            "week_digest": {"counts": week_counts, "items": week_items},
            "recent_activity": recent_activity,
            "golden_signals": golden_signals,
            "engineer": engineer,
        }

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

    def commitments_ledger_page(
        self,
        *,
        account_emails: Iterable[str],
        window_days: int,
        status: str,
        page: int,
        page_size: int,
        reveal_pii: bool = False,
        evidence_limit: int = 8,
    ) -> dict[str, object]:
        account_ids = self._normalize_account_scope("", account_emails)
        if not account_ids:
            return {"rows": [], "total_count": 0}
        try:
            resolved_days = max(1, int(window_days))
        except (TypeError, ValueError):
            resolved_days = 7
        try:
            resolved_page = max(1, int(page))
        except (TypeError, ValueError):
            resolved_page = 1
        try:
            resolved_page_size = max(1, int(page_size))
        except (TypeError, ValueError):
            resolved_page_size = 50
        try:
            resolved_limit = max(0, int(evidence_limit))
        except (TypeError, ValueError):
            resolved_limit = 0

        status_key = str(status or "").strip().lower()
        status_values: tuple[str, ...] | None = None
        if status_key == "open":
            status_values = ("pending", "unknown")
        elif status_key == "closed":
            status_values = ("fulfilled", "expired")

        since_ts = self._window_start_ts(resolved_days)
        since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()
        clause, clause_params = self._account_email_clause(account_ids)
        status_clause = ""
        status_params: list[object] = []
        if status_values:
            placeholders = ", ".join(["?"] * len(status_values))
            status_clause = f" AND lower(c.status) IN ({placeholders})"
            status_params.extend(status_values)
        count_query = (
            """
            SELECT COUNT(*) AS total_count
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            WHERE datetime(c.created_at) >= datetime(?)
            """
            + clause
            + status_clause
        )
        params: list[object] = [since_iso, *clause_params, *status_params]
        try:
            count_rows = self._execute_select(count_query, params)
        except sqlite3.OperationalError:
            return {"rows": [], "total_count": 0}
        total_count = int(count_rows[0].get("total_count") or 0) if count_rows else 0
        offset = max(0, (resolved_page - 1) * resolved_page_size)

        query = (
            """
            SELECT c.id, c.email_row_id, c.commitment_text, c.deadline_iso, c.status, c.source, c.created_at,
                   e.account_email, e.from_email
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            WHERE datetime(c.created_at) >= datetime(?)
            """
            + clause
            + status_clause
            + """
            ORDER BY
              CASE
                WHEN c.deadline_iso IS NULL OR c.deadline_iso = '' THEN 1
                ELSE 0
              END ASC,
              c.deadline_iso ASC,
              c.id DESC
            LIMIT ?
            OFFSET ?
            """
        )
        query_params: list[object] = [
            since_iso,
            *clause_params,
            *status_params,
            resolved_page_size,
            offset,
        ]
        try:
            rows = self._execute_select(query, query_params)
        except sqlite3.OperationalError:
            return {"rows": [], "total_count": 0}

        commitments: list[dict[str, object]] = []
        evidence_map: dict[int, list[dict[str, object]]] = {}
        text_lookup: dict[tuple[int, str], int] = {}
        for row in rows:
            commitment_id = int(row.get("id") or 0)
            if commitment_id <= 0:
                continue
            email_id = int(row.get("email_row_id") or 0)
            deadline_iso = str(row.get("deadline_iso") or "").strip() or None
            status_value = str(row.get("status") or "").strip()
            source = str(row.get("source") or "").strip()
            created_at = parse_sqlite_datetime(str(row.get("created_at") or ""))
            created_ts = created_at.timestamp() if created_at else None
            account_email = str(row.get("account_email") or "")
            from_email = str(row.get("from_email") or "")
            account_label = self._mask_email_address(account_email)
            counterparty_label = self._mask_email_address(from_email)
            commitments.append(
                {
                    "commitment_id": commitment_id,
                    "email_id": email_id,
                    "deadline_iso": deadline_iso,
                    "status": status_value,
                    "source": source,
                    "created_ts": created_ts,
                    "account_label": account_label,
                    "counterparty_label": counterparty_label,
                }
            )
            evidence_map[commitment_id] = []
            key_text = str(row.get("commitment_text") or "").strip().lower()
            if email_id and key_text:
                text_lookup[(email_id, key_text)] = commitment_id

        if not commitments:
            return {"rows": [], "total_count": total_count}

        email_ids = sorted({entry["email_id"] for entry in commitments if entry.get("email_id")})
        if email_ids and resolved_limit > 0:
            placeholders = ", ".join(["?"] * len(email_ids))
            event_placeholders = ", ".join(["?"] * len(self._COMMITMENT_EVENT_TYPES))
            evidence_query = f"""
            SELECT id, event_type, ts_utc, email_id, payload, payload_json
            FROM events_v1
            WHERE email_id IN ({placeholders})
              AND event_type IN ({event_placeholders})
              AND ts_utc >= ?
            ORDER BY ts_utc DESC, id DESC
            """
            evidence_params: list[object] = [
                *email_ids,
                *sorted(self._COMMITMENT_EVENT_TYPES),
                since_ts,
            ]
            try:
                event_rows = self._execute_select(evidence_query, evidence_params)
            except sqlite3.OperationalError:
                event_rows = []

            for row in event_rows:
                payload = self._event_payload(row)
                commitment_id_raw = payload.get("commitment_id")
                commitment_id = None
                if commitment_id_raw is not None:
                    try:
                        commitment_id = int(commitment_id_raw)
                    except (TypeError, ValueError):
                        commitment_id = None
                if not commitment_id:
                    email_id = int(row.get("email_id") or 0)
                    key_text = str(payload.get("commitment_text") or "").strip().lower()
                    commitment_id = text_lookup.get((email_id, key_text))
                if not commitment_id:
                    continue
                evidence_items = evidence_map.get(commitment_id)
                if evidence_items is None:
                    continue
                stage = str(payload.get("stage") or payload.get("reason") or "")
                outcome = str(payload.get("outcome") or "")
                duration_ms = payload.get("duration_ms")
                evidence_items.append(
                    {
                        "event_id": row.get("id"),
                        "event_type": row.get("event_type"),
                        "ts_utc": row.get("ts_utc"),
                        "stage": stage,
                        "outcome": outcome,
                        "duration_ms": duration_ms,
                    }
                )

        rows_out: list[dict[str, object]] = []
        for entry in commitments:
            commitment_id = int(entry["commitment_id"])
            evidence_items = evidence_map.get(commitment_id, [])
            evidence_items.sort(
                key=lambda item: (
                    -float(item.get("ts_utc") or 0.0),
                    -int(item.get("event_id") or 0),
                )
            )
            evidence_count = len(evidence_items)
            evidence_trimmed = evidence_items[:resolved_limit] if resolved_limit > 0 else []
            last_evidence_ts = None
            if evidence_items:
                last_evidence_ts = float(evidence_items[0].get("ts_utc") or 0.0)
            last_activity_ts = entry.get("created_ts")
            if last_evidence_ts is not None:
                if last_activity_ts is None or last_evidence_ts > float(last_activity_ts):
                    last_activity_ts = last_evidence_ts
            rows_out.append(
                {
                    **entry,
                    "last_activity_ts": last_activity_ts,
                    "evidence_count": evidence_count,
                    "last_evidence_ts": last_evidence_ts,
                    "evidence": evidence_trimmed,
                }
            )

        return {"rows": rows_out, "total_count": total_count}

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
            read_minutes = self._estimate_attention_minutes(payload)
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

    def attention_lane_breakdown(
        self,
        *,
        account_emails: Sequence[str],
        window_days: int,
    ) -> list[dict[str, object]]:
        account_ids = self._normalize_account_scope(
            account_emails[0] if account_emails else "", account_emails
        )
        if not account_ids:
            return []
        since_ts = self._window_start_ts(window_days)
        since_iso = datetime.fromtimestamp(
            since_ts, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        clause, clause_params = self._account_email_clause(account_ids)
        base_query = """
        SELECT e.id
        FROM emails e
        WHERE e.received_at >= ?
        """

        minutes_by_email: dict[int, float] = {}
        event_rows = self._event_rows_scoped(
            account_ids=account_ids,
            event_type="email_received",
            since_ts=since_ts,
        )
        for row in event_rows:
            email_id = row.get("email_id")
            if email_id is None:
                continue
            try:
                email_id_int = int(email_id)
            except (TypeError, ValueError):
                continue
            minutes_by_email[email_id_int] = self._estimate_attention_minutes(
                self._event_payload(row)
            )

        lane_order = [
            "critical",
            "commitments",
            "deferred",
            "failures",
            "learning",
            "all",
        ]
        results: list[dict[str, object]] = []
        for lane in lane_order:
            lane_clause, lane_params = self._lane_email_clause(lane, since_ts=since_ts)
            query = base_query + clause + lane_clause
            params: list[object] = [since_iso, *clause_params, *lane_params]
            try:
                rows = self._execute_select(query, params)
            except sqlite3.OperationalError:
                rows = []
            email_ids = [
                int(row.get("id") or 0)
                for row in rows
                if int(row.get("id") or 0) > 0
            ]
            count = len(email_ids)
            minutes = 0.0
            for email_id in email_ids:
                minutes += minutes_by_email.get(email_id, 0.5)
            results.append(
                {
                    "lane": lane,
                    "count": count,
                    "estimated_read_minutes": round(minutes, 2),
                }
            )

        total_minutes = next(
            (row["estimated_read_minutes"] for row in results if row["lane"] == "all"),
            0.0,
        )
        if total_minutes <= 0:
            total_minutes = 0.0
        for row in results:
            share = 0.0
            if total_minutes > 0:
                share = max(
                    0.0,
                    min(100.0, (float(row.get("estimated_read_minutes") or 0.0) / total_minutes) * 100),
                )
            row["share_percent"] = round(share, 1)
        return results

    def attention_economics_summary(
        self,
        *,
        account_emails: Sequence[str],
        window_days: int,
        limit: int,
        sort: str,
        attention_cost_per_hour: float = 0.0,
    ) -> dict[str, object]:
        sort = str(sort or "time").strip().lower()
        if sort not in {"time", "cost", "count"}:
            sort = "time"
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

        def _entity_entry(
            *,
            entity_id: str,
            label: str,
            message_count: int,
            attachment_count: int,
            estimated_read_minutes: float,
            deferred_count: int,
        ) -> dict[str, object]:
            masked_label = self._mask_contact_label(entity_id=entity_id, label=label)
            signals = "–"
            if message_count > 0 and deferred_count > 0:
                signals = f"отложено {round((deferred_count / message_count) * 100)}%"
            entry: dict[str, object] = {
                "entity_id": entity_id,
                "entity_label": masked_label,
                "message_count": message_count,
                "attachment_count": attachment_count,
                "estimated_read_minutes": estimated_read_minutes,
                "deferred_count": deferred_count,
                "signals": signals,
            }
            if attention_cost_per_hour > 0:
                entry["estimated_cost"] = round(
                    (estimated_read_minutes / 60.0) * attention_cost_per_hour, 2
                )
            return entry

        entities: list[dict[str, object]] = []
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
                )
            )

        def _sort_key(item: dict[str, object]) -> tuple[float, str]:
            entity_key = str(item.get("entity_id") or "").lower()
            if sort == "count":
                return (-float(item.get("message_count") or 0.0), entity_key)
            if sort == "cost":
                cost_value = item.get("estimated_cost")
                if cost_value is None:
                    cost_value = float(item.get("estimated_read_minutes") or 0.0)
                return (-float(cost_value or 0.0), entity_key)
            return (-float(item.get("estimated_read_minutes") or 0.0), entity_key)

        entities.sort(key=_sort_key)
        entities = entities[:limit]

        lane_breakdown = self.attention_lane_breakdown(
            account_emails=scope,
            window_days=window_days,
        )

        top_contact_label = ""
        if raw_entities:
            top_entry = sorted(
                raw_entities,
                key=lambda item: (
                    -float(item.get("estimated_read_minutes") or 0.0),
                    str(item.get("entity_id") or "").lower(),
                ),
            )[0]
            entity_id = str(top_entry.get("entity_id") or "")
            label = self.entity_label(entity_id=entity_id) or entity_id
            top_contact_label = self._mask_contact_label(entity_id=entity_id, label=label)

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
            "sort": sort,
            "totals": totals,
            "entities": entities,
            "lane_breakdown": lane_breakdown,
            "top_contact_label": top_contact_label,
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
