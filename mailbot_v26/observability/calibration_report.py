from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.decision_trace_v1 import from_canonical_json

logger = get_logger("mailbot")


@dataclass(slots=True)
class _CacheEntry:
    watermark: float
    report: dict[str, object]


_CACHE: dict[tuple[str, int, int], _CacheEntry] = {}
_CACHE_FILE = Path("logs/priority_calibration_cache.json")


def _load_cache_file() -> dict[str, object] | None:
    try:
        if not _CACHE_FILE.exists():
            return None
        raw = _CACHE_FILE.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _write_cache_file(payload: dict[str, object]) -> None:
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return


def compute_priority_calibration_report(
    *,
    db_path: Path,
    days: int,
    max_rows: int = 1000,
    now_ts_utc: float | None = None,
) -> dict[str, object]:
    resolved_days = max(1, int(days))
    resolved_max_rows = max(10, int(max_rows))
    now_ts = float(now_ts_utc) if now_ts_utc is not None else time.time()
    since_ts = now_ts - resolved_days * 24 * 60 * 60
    cache_key = (str(db_path), resolved_days, resolved_max_rows)

    cached = _CACHE.get(cache_key)
    if cached is not None and cached.watermark:
        if cached.watermark >= since_ts:
            return cached.report

    cache_file = _load_cache_file()
    if cache_file:
        file_key = "|".join(str(part) for part in cache_key)
        cached_entry = cache_file.get(file_key)
        if isinstance(cached_entry, dict):
            file_watermark = float(cached_entry.get("watermark") or 0.0)
            file_report = cached_entry.get("report")
            if file_watermark and file_watermark >= since_ts and isinstance(file_report, dict):
                _CACHE[cache_key] = _CacheEntry(file_watermark, file_report)
                return file_report

    trace_rows: list[tuple[float, int | None, str]] = []
    correction_rows: list[tuple[float, int | None, str]] = []
    surprise_rows: list[tuple[float, int | None]] = []
    try:
        with sqlite3.connect(db_path) as conn:
            trace_rows = conn.execute(
                """
                SELECT ts_utc, email_id, payload_json
                FROM events_v1
                WHERE event_type = ?
                  AND ts_utc >= ?
                ORDER BY ts_utc DESC
                LIMIT ?
                """,
                (EventType.DECISION_TRACE_RECORDED.value, since_ts, resolved_max_rows),
            ).fetchall()
            correction_rows = conn.execute(
                """
                SELECT ts_utc, email_id, payload
                FROM events_v1
                WHERE event_type = ?
                  AND ts_utc >= ?
                ORDER BY ts_utc DESC
                LIMIT ?
                """,
                (EventType.PRIORITY_CORRECTION_RECORDED.value, since_ts, resolved_max_rows),
            ).fetchall()
            surprise_rows = conn.execute(
                """
                SELECT ts_utc, email_id
                FROM events_v1
                WHERE event_type = ?
                  AND ts_utc >= ?
                ORDER BY ts_utc DESC
                LIMIT ?
                """,
                (EventType.SURPRISE_DETECTED.value, since_ts, resolved_max_rows),
            ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.error("calibration_report_query_failed", error=str(exc))

    corrected_decision_keys: set[str] = set()
    corrected_email_ids: set[int] = set()
    correction_ts: list[float] = []
    for ts_utc, email_id, payload_json in correction_rows:
        correction_ts.append(float(ts_utc or 0.0))
        if email_id is not None:
            corrected_email_ids.add(int(email_id))
        payload: dict[str, Any] = {}
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        decision_key = str(payload.get("decision_key") or "")
        if decision_key:
            corrected_decision_keys.add(decision_key)

    traces: list[dict[str, object]] = []
    trace_ts: list[float] = []
    for ts_utc, email_id, payload_json in trace_rows:
        trace_ts.append(float(ts_utc or 0.0))
        trace = from_canonical_json(payload_json)
        if not trace or trace.decision_kind != "PRIORITY_HEURISTIC":
            continue
        traces.append(
            {
                "ts_utc": float(ts_utc or 0.0),
                "email_id": int(email_id) if email_id is not None else None,
                "decision_key": trace.decision_key,
                "model_fingerprint": trace.model_fingerprint or "unknown",
                "evidence": trace.evidence or {"matched": 0, "total": 0},
            }
        )

    max_ts = max(trace_ts + correction_ts + [float(ts) for ts, _ in surprise_rows] or [0.0])

    aggregates: dict[str, dict[str, object]] = {}
    total_decisions = 0
    total_corrected = 0
    for trace in traces:
        model = str(trace.get("model_fingerprint") or "unknown")
        bucket_evidence = trace.get("evidence") if isinstance(trace.get("evidence"), dict) else {}
        matched = int(bucket_evidence.get("matched") or 0)
        total = int(bucket_evidence.get("total") or 0)
        bucket = f"{matched}/{total}"
        decision_key = str(trace.get("decision_key") or "")
        email_id = trace.get("email_id")
        corrected = False
        if decision_key and decision_key in corrected_decision_keys:
            corrected = True
        elif email_id is not None and int(email_id) in corrected_email_ids:
            corrected = True
        model_entry = aggregates.setdefault(
            model,
            {
                "decisions_total": 0,
                "decisions_corrected": 0,
                "buckets": {},
            },
        )
        model_entry["decisions_total"] = int(model_entry["decisions_total"]) + 1
        if corrected:
            model_entry["decisions_corrected"] = int(model_entry["decisions_corrected"]) + 1
        bucket_entry = model_entry["buckets"].setdefault(
            bucket,
            {"total": 0, "corrected": 0},
        )
        bucket_entry["total"] = int(bucket_entry["total"]) + 1
        if corrected:
            bucket_entry["corrected"] = int(bucket_entry["corrected"]) + 1
        total_decisions += 1
        if corrected:
            total_corrected += 1

    models_payload: list[dict[str, object]] = []
    for model, entry in sorted(aggregates.items(), key=lambda item: item[0]):
        decisions_total = int(entry.get("decisions_total") or 0)
        decisions_corrected = int(entry.get("decisions_corrected") or 0)
        correction_rate = (
            decisions_corrected / decisions_total if decisions_total else None
        )
        bucket_payload = []
        buckets = entry.get("buckets", {})
        if isinstance(buckets, dict):
            for bucket_key, bucket_entry in sorted(buckets.items(), key=lambda item: item[0]):
                bucket_total = int(bucket_entry.get("total") or 0)
                bucket_corrected = int(bucket_entry.get("corrected") or 0)
                bucket_payload.append(
                    {
                        "bucket": bucket_key,
                        "total": bucket_total,
                        "corrected": bucket_corrected,
                        "correction_rate": bucket_corrected / bucket_total
                        if bucket_total
                        else None,
                    }
                )
        models_payload.append(
            {
                "model_fingerprint": model,
                "decisions_total": decisions_total,
                "decisions_corrected": decisions_corrected,
                "correction_rate": correction_rate,
                "buckets": bucket_payload,
            }
        )

    total_correction_rate = total_corrected / total_decisions if total_decisions else None

    last_7d_start = now_ts - 7 * 24 * 60 * 60
    prev_7d_start = now_ts - 14 * 24 * 60 * 60
    decisions_last_7d = sum(1 for trace in traces if trace.get("ts_utc", 0.0) >= last_7d_start)
    decisions_prev_7d = sum(
        1 for trace in traces if prev_7d_start <= trace.get("ts_utc", 0.0) < last_7d_start
    )
    corrections_last_7d = sum(1 for ts in correction_ts if ts >= last_7d_start)
    corrections_prev_7d = sum(1 for ts in correction_ts if prev_7d_start <= ts < last_7d_start)
    correction_rate_last_7d = (
        corrections_last_7d / decisions_last_7d if decisions_last_7d else None
    )
    correction_rate_prev_7d = (
        corrections_prev_7d / decisions_prev_7d if decisions_prev_7d else None
    )
    surprises_last_7d = sum(1 for ts, _ in surprise_rows if float(ts or 0.0) >= last_7d_start)
    surprise_rate_last_7d = (
        surprises_last_7d / decisions_last_7d if decisions_last_7d else None
    )
    warnings: list[str] = []
    if (
        correction_rate_last_7d is not None
        and correction_rate_prev_7d is not None
        and correction_rate_last_7d - correction_rate_prev_7d > 0.05
    ):
        warnings.append("correction_rate_spike")
    if surprise_rate_last_7d is not None and surprise_rate_last_7d > 0.10:
        warnings.append("surprise_rate_high")

    generated_at = datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat()
    if generated_at.endswith("+00:00"):
        generated_at = generated_at.replace("+00:00", "Z")
    report = {
        "window_days": resolved_days,
        "max_rows": resolved_max_rows,
        "generated_at_utc": generated_at,
        "totals": {
            "decisions_total": total_decisions,
            "decisions_corrected": total_corrected,
            "correction_rate": total_correction_rate,
        },
        "models": models_payload,
        "drift": {
            "correction_rate_last_7d": correction_rate_last_7d,
            "correction_rate_prev_7d": correction_rate_prev_7d,
            "surprise_rate_last_7d": surprise_rate_last_7d,
        },
        "warnings": warnings,
    }

    _CACHE[cache_key] = _CacheEntry(max_ts, report)
    _write_cache_file(
        {
            "|".join(str(part) for part in cache_key): {
                "watermark": max_ts,
                "report": report,
            }
        }
    )
    return report


__all__ = ["compute_priority_calibration_report"]
