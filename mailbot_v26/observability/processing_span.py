from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


_BANNED_KEYS = {
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

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute(
                    """
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
                        health_snapshot_id TEXT NOT NULL DEFAULT ''
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS system_health_snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        ts_utc REAL NOT NULL,
                        payload_json TEXT NOT NULL,
                        gates_state TEXT,
                        metrics_brief TEXT
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processing_spans_account_ts
                        ON processing_spans(account_id, ts_start_utc);
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processing_spans_email_id
                        ON processing_spans(email_id);
                    """
                )
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("processing_span_init_failed", error=str(exc))

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
    ) -> None:
        ts_end_utc = time.time()
        total_duration_ms = int((time.perf_counter() - span.start_monotonic) * 1000)
        stage_durations = {
            key: int(value)
            for key, value in span.stage_durations_ms.items()
            if str(key).lower() not in _BANNED_KEYS
        }
        if stage_durations_override:
            stage_durations.update(
                {
                    k: int(v)
                    for k, v in stage_durations_override.items()
                    if str(k).lower() not in _BANNED_KEYS
                }
            )
        try:
            stage_durations_json = json.dumps(stage_durations, ensure_ascii=False)
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
                        health_snapshot_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                sanitized_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
        except (TypeError, ValueError):
            canonical_json = "{}"
        snapshot_id = hashlib.sha256(canonical_json.encode("utf-8", errors="ignore")).hexdigest()
        try:
            with sqlite3.connect(self.path) as conn:
                existing = conn.execute(
                    "SELECT 1 FROM system_health_snapshots WHERE snapshot_id = ?", (snapshot_id,)
                ).fetchone()
                if not existing:
                    conn.execute(
                        """
                        INSERT INTO system_health_snapshots (
                            snapshot_id,
                            ts_utc,
                            payload_json,
                            gates_state,
                            metrics_brief
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot_id,
                            ts_utc,
                            canonical_json,
                            _extract_gates_state(sanitized_payload),
                            _extract_metrics_brief(sanitized_payload),
                        ),
                    )
                    conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("health_snapshot_persist_failed", error=str(exc))
        return snapshot_id


def _sanitize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    def _clean(obj: Any) -> Any:
        if isinstance(obj, Mapping):
            return {
                k: _clean(v)
                for k, v in obj.items()
                if str(k).lower() not in _BANNED_KEYS
            }
        if isinstance(obj, list):
            return [_clean(item) for item in obj]
        return obj

    if not isinstance(payload, Mapping):
        return {}
    return _clean(payload)


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
