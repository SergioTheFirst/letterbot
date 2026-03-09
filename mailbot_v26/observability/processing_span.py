from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, MutableMapping

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


_FORBIDDEN_KEYS = {
    "subject",
    "body",
    "body_text",
    "raw_body",
    "telegram_text",
    "rendered_message",
    "digest_text",
    "html_text",
    "attachment_text",
    "payload_json",
    "email_raw",
}

_PRUNE_STATE: dict[str, str] = {}
_UTC_DATE_FORMAT = "%Y-%m-%d"

_SPANS_RETENTION_DAYS_DEFAULT = 90
_SPANS_MAX_ROWS_DEFAULT = 250_000
_HEALTH_RETENTION_DAYS_DEFAULT = 30
_HEALTH_MAX_ROWS_DEFAULT = 50_000


@dataclass
class ProcessingSpan:
    span_id: str
    account_id: str
    email_id: int | None
    ts_start_utc: float
    start_monotonic: float
    stage_durations_ms: dict[str, int] = field(default_factory=dict)

    def record_stage(self, name: str, duration_ms: int) -> None:
        if duration_ms < 0:
            return
        self.stage_durations_ms[name] = int(duration_ms)


class ProcessingSpanRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._maybe_prune()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processing_spans (
                        span_id TEXT PRIMARY KEY,
                        ts_start_utc REAL NOT NULL,
                        ts_end_utc REAL NOT NULL,
                        total_duration_ms INTEGER NOT NULL,
                        account_id TEXT NOT NULL,
                        email_id INTEGER,
                        stage_durations_json TEXT NOT NULL DEFAULT '{}',
                        llm_provider TEXT,
                        llm_model TEXT,
                        llm_latency_ms INTEGER,
                        llm_quality_score REAL,
                        fallback_used INTEGER NOT NULL DEFAULT 0,
                        outcome TEXT NOT NULL,
                        error_code TEXT NOT NULL DEFAULT '',
                        health_snapshot_id TEXT NOT NULL DEFAULT '',
                        delivery_mode TEXT DEFAULT '',
                        wait_budget_seconds INTEGER DEFAULT 0,
                        elapsed_to_first_send_ms INTEGER DEFAULT 0,
                        edit_applied INTEGER NOT NULL DEFAULT 0
                    );
                    """)
                self._ensure_column(
                    conn,
                    table="processing_spans",
                    column="delivery_mode",
                    definition="TEXT DEFAULT ''",
                )
                self._ensure_column(
                    conn,
                    table="processing_spans",
                    column="wait_budget_seconds",
                    definition="INTEGER DEFAULT 0",
                )
                self._ensure_column(
                    conn,
                    table="processing_spans",
                    column="elapsed_to_first_send_ms",
                    definition="INTEGER DEFAULT 0",
                )
                self._ensure_column(
                    conn,
                    table="processing_spans",
                    column="edit_applied",
                    definition="INTEGER NOT NULL DEFAULT 0",
                )
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_health_snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        ts_utc REAL NOT NULL,
                        payload_json TEXT NOT NULL,
                        gates_state TEXT,
                        metrics_brief TEXT,
                        system_mode TEXT
                    );
                    """)
                self._ensure_column(
                    conn,
                    table="system_health_snapshots",
                    column="gates_state",
                    definition="TEXT",
                )
                self._ensure_column(
                    conn,
                    table="system_health_snapshots",
                    column="metrics_brief",
                    definition="TEXT",
                )
                self._ensure_column(
                    conn,
                    table="system_health_snapshots",
                    column="system_mode",
                    definition="TEXT",
                )
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_processing_spans_account_ts
                        ON processing_spans(account_id, ts_start_utc);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_processing_spans_email_id
                        ON processing_spans(email_id);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_processing_spans_email_ts
                        ON processing_spans(email_id, ts_start_utc, span_id);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_processing_spans_ts_start
                        ON processing_spans(ts_start_utc);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_system_health_snapshots_ts
                        ON system_health_snapshots(ts_utc);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_system_health_snapshots_ts_snapshot
                        ON system_health_snapshots(ts_utc DESC, snapshot_id);
                    """)
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("processing_span_init_failed", error=str(exc))

    @staticmethod
    def _ensure_column(
        conn: sqlite3.Connection, *, table: str, column: str, definition: str
    ) -> None:
        try:
            existing = conn.execute("PRAGMA table_info({})".format(table)).fetchall()
            if any(row[1] == column for row in existing):
                return
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition};")
            conn.commit()
        except sqlite3.OperationalError:
            return
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("processing_span_alter_failed", error=str(exc))

    def _maybe_prune(self) -> None:
        try:
            utc_date = datetime.now(timezone.utc).strftime(_UTC_DATE_FORMAT)
            cached = _PRUNE_STATE.get("date")
            if cached == utc_date:
                return
            _PRUNE_STATE["date"] = utc_date
            with sqlite3.connect(self.path) as conn:
                _prune_observability(
                    conn,
                    spans_retention_days=_SPANS_RETENTION_DAYS_DEFAULT,
                    spans_max_rows=_SPANS_MAX_ROWS_DEFAULT,
                    health_retention_days=_HEALTH_RETENTION_DAYS_DEFAULT,
                    health_max_rows=_HEALTH_MAX_ROWS_DEFAULT,
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("observability_prune_failed", error=str(exc))

    def start(self, *, account_id: str, email_id: int | None) -> ProcessingSpan:
        span_id = uuid.uuid4().hex
        now_utc = time.time()
        return ProcessingSpan(
            span_id=span_id,
            account_id=account_id,
            email_id=email_id,
            ts_start_utc=now_utc,
            start_monotonic=time.perf_counter(),
        )

    def finalize(
        self,
        span: ProcessingSpan,
        *,
        llm_provider: str | None,
        llm_model: str | None,
        llm_latency_ms: int | None,
        llm_quality_score: float | None,
        fallback_used: bool,
        outcome: str,
        error_code: str,
        health_snapshot_payload: Mapping[str, Any] | None,
        stage_durations_override: Mapping[str, int] | None = None,
        delivery_mode: str | None = None,
        wait_budget_seconds: int | None = None,
        elapsed_to_first_send_ms: int | None = None,
        edit_applied: bool = False,
    ) -> None:
        ts_end_utc = time.time()
        total_duration_ms = max(
            1, int((time.perf_counter() - span.start_monotonic) * 1000)
        )
        scrubbed_stage_keys = 0
        stage_durations = {}
        for key, value in span.stage_durations_ms.items():
            key_lower = str(key).lower()
            if key_lower in _FORBIDDEN_KEYS:
                scrubbed_stage_keys += 1
                continue
            try:
                stage_durations[key] = int(value)
            except (TypeError, ValueError):
                continue
        if stage_durations_override:
            for k, v in stage_durations_override.items():
                key_lower = str(k).lower()
                if key_lower in _FORBIDDEN_KEYS:
                    scrubbed_stage_keys += 1
                    continue
                try:
                    stage_durations[str(k)] = int(v)
                except (TypeError, ValueError):
                    continue
        stage_durations["total"] = total_duration_ms
        sanitized_stage_durations = _sanitize_payload(
            stage_durations, base_scrubbed_count=scrubbed_stage_keys
        )
        stage_markers = {
            key: sanitized_stage_durations.get(key)
            for key in ("scrubbed", "scrubbed_keys_count")
            if key in sanitized_stage_durations
        }
        clean_stage_durations: MutableMapping[str, Any] = {}
        for key, value in sanitized_stage_durations.items():
            if key in stage_markers:
                continue
            try:
                clean_stage_durations[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
        clean_stage_durations.update(stage_markers)
        try:
            stage_durations_json = json.dumps(clean_stage_durations, ensure_ascii=False)
        except (TypeError, ValueError):
            stage_durations_json = "{}"

        health_snapshot_id = ""
        if health_snapshot_payload:
            health_snapshot_id = self._persist_health_snapshot(
                ts_utc=ts_end_utc,
                payload=health_snapshot_payload,
            )

        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO processing_spans (
                        span_id,
                        ts_start_utc,
                        ts_end_utc,
                        total_duration_ms,
                        account_id,
                        email_id,
                        stage_durations_json,
                        llm_provider,
                        llm_model,
                        llm_latency_ms,
                        llm_quality_score,
                        fallback_used,
                        outcome,
                        error_code,
                        health_snapshot_id,
                        delivery_mode,
                        wait_budget_seconds,
                        elapsed_to_first_send_ms,
                        edit_applied
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        span.span_id,
                        span.ts_start_utc,
                        ts_end_utc,
                        total_duration_ms,
                        span.account_id,
                        span.email_id,
                        stage_durations_json,
                        llm_provider,
                        llm_model,
                        llm_latency_ms,
                        llm_quality_score,
                        1 if fallback_used else 0,
                        outcome,
                        error_code,
                        health_snapshot_id,
                        delivery_mode or "",
                        int(wait_budget_seconds or 0),
                        int(elapsed_to_first_send_ms or 0),
                        1 if edit_applied else 0,
                    ),
                )
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("processing_span_persist_failed", error=str(exc))

    def _persist_health_snapshot(
        self, *, ts_utc: float, payload: Mapping[str, Any]
    ) -> str:
        sanitized_payload = _sanitize_payload(payload)
        try:
            canonical_json = json.dumps(
                sanitized_payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except (TypeError, ValueError):
            canonical_json = "{}"
        snapshot_id = hashlib.sha256(
            canonical_json.encode("utf-8", errors="ignore")
        ).hexdigest()
        try:
            with sqlite3.connect(self.path) as conn:
                existing = conn.execute(
                    "SELECT 1 FROM system_health_snapshots WHERE snapshot_id = ?",
                    (snapshot_id,),
                ).fetchone()
                if not existing:
                    conn.execute(
                        """
                        INSERT INTO system_health_snapshots (
                            snapshot_id,
                            ts_utc,
                            payload_json,
                            gates_state,
                            metrics_brief,
                            system_mode
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot_id,
                            ts_utc,
                            canonical_json,
                            _extract_gates_state(sanitized_payload),
                            _extract_metrics_brief(sanitized_payload),
                            _extract_system_mode(sanitized_payload),
                        ),
                    )
                    conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("health_snapshot_persist_failed", error=str(exc))
        return snapshot_id


def _sanitize_payload(
    payload: Mapping[str, Any], *, base_scrubbed_count: int = 0
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    sanitized, scrubbed_count = _sanitize_mapping(payload)
    scrubbed_total = scrubbed_count + max(base_scrubbed_count, 0)
    if scrubbed_total > 0 and isinstance(sanitized, MutableMapping):
        sanitized = dict(sanitized)
        sanitized["scrubbed"] = True
        sanitized["scrubbed_keys_count"] = scrubbed_total
    return sanitized if isinstance(sanitized, Mapping) else {}


def _sanitize_mapping(obj: Any) -> tuple[Any, int]:
    if isinstance(obj, Mapping):
        cleaned: dict[str, Any] = {}
        scrubbed = 0
        for k, v in obj.items():
            key_lower = str(k).lower()
            if key_lower in _FORBIDDEN_KEYS:
                scrubbed += 1
                continue
            child_cleaned, child_scrubbed = _sanitize_mapping(v)
            scrubbed += child_scrubbed
            cleaned[k] = child_cleaned
        return cleaned, scrubbed
    if isinstance(obj, list):
        cleaned_list = []
        scrubbed = 0
        for item in obj:
            child_cleaned, child_scrubbed = _sanitize_mapping(item)
            scrubbed += child_scrubbed
            cleaned_list.append(child_cleaned)
        return cleaned_list, scrubbed
    return obj, 0


def _extract_gates_state(payload: Mapping[str, Any]) -> str:
    gates = payload.get("gates") if isinstance(payload, Mapping) else None
    if not isinstance(gates, Mapping):
        return ""
    return json.dumps(gates, ensure_ascii=False, separators=(",", ":"))


def _extract_metrics_brief(payload: Mapping[str, Any]) -> str:
    metrics = payload.get("metrics") if isinstance(payload, Mapping) else None
    if not isinstance(metrics, Mapping):
        return ""
    brief = {}
    for window, values in metrics.items():
        if not isinstance(values, Mapping):
            continue
        brief[window] = {
            key: values.get(key)
            for key in (
                "shadow_accuracy",
                "llm_failure_rate",
                "telegram_delivery_success_rate",
            )
            if key in values
        }
    try:
        return json.dumps(brief, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return ""


def _extract_system_mode(payload: Mapping[str, Any]) -> str:
    if not isinstance(payload, Mapping):
        return ""
    mode_value = None
    for key in ("system_mode", "mode"):
        if key in payload:
            mode_value = payload.get(key)
            break
    if mode_value is None:
        system_section = payload.get("system")
        if isinstance(system_section, Mapping):
            mode_value = system_section.get("mode")
    if not isinstance(mode_value, str):
        return ""
    try:
        return str(mode_value)
    except Exception:
        return ""


def _prune_observability(
    conn: sqlite3.Connection,
    *,
    spans_retention_days: int,
    spans_max_rows: int,
    health_retention_days: int,
    health_max_rows: int,
) -> None:
    _prune_by_age(conn, "processing_spans", "ts_start_utc", spans_retention_days)
    _prune_by_age(conn, "system_health_snapshots", "ts_utc", health_retention_days)
    _prune_by_max_rows(
        conn,
        table="processing_spans",
        ts_column="ts_start_utc",
        pk_column="span_id",
        max_rows=spans_max_rows,
    )
    _prune_by_max_rows(
        conn,
        table="system_health_snapshots",
        ts_column="ts_utc",
        pk_column="snapshot_id",
        max_rows=health_max_rows,
    )
    conn.commit()


def _prune_by_age(
    conn: sqlite3.Connection, table: str, ts_column: str, retention_days: int
) -> None:
    if retention_days <= 0:
        return
    cutoff = time.time() - (retention_days * 86400)
    conn.execute(
        f"DELETE FROM {table} WHERE {ts_column} < ?",
        (cutoff,),
    )


def _prune_by_max_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    ts_column: str,
    pk_column: str,
    max_rows: int,
) -> None:
    if max_rows <= 0:
        return
    row = conn.execute(f"SELECT COUNT(1) FROM {table}").fetchone()
    total_rows = int(row[0]) if row else 0
    if total_rows <= max_rows:
        return
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE {pk_column} NOT IN (
            SELECT {pk_column}
            FROM {table}
            ORDER BY {ts_column} DESC, {pk_column} DESC
            LIMIT ?
        )
        """,
        (max_rows,),
    )
