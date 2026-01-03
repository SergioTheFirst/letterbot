from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from mailbot_v26.config_loader import ConfigError, load_storage_config
from mailbot_v26.insights.quality_metrics import compute_quality_metrics
from mailbot_v26.observability.notification_sla import compute_notification_sla
from mailbot_v26.storage.analytics import KnowledgeAnalytics


@dataclass(frozen=True)
class ExportResult:
    output_path: Path
    record_count: int


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_db_path(base_dir: Path) -> Path:
    config_dir = base_dir / "mailbot_v26" / "config"
    try:
        return load_storage_config(config_dir).db_path
    except ConfigError:
        return base_dir / "data" / "mailbot.sqlite"


def _parse_since(value: str, now: datetime | None = None) -> datetime:
    match = re.fullmatch(r"(\d+)([dh])?", value.strip())
    if not match:
        raise ValueError("Invalid --since value. Use formats like 30d or 12h.")
    amount = int(match.group(1))
    unit = match.group(2) or "d"
    if unit == "h":
        delta = timedelta(hours=amount)
    else:
        delta = timedelta(days=amount)
    base = now or datetime.now(timezone.utc)
    return base - delta


def _scrub_payload(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            lowered = key.lower()
            if any(token in lowered for token in ("token", "password", "secret", "key")):
                cleaned[key] = "***"
            else:
                cleaned[key] = _scrub_payload(item)
        return cleaned
    if isinstance(value, list):
        return [_scrub_payload(item) for item in value]
    return value


def _load_payload(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"_raw": raw}
    if isinstance(parsed, dict):
        return parsed
    return {"_value": parsed}


def _fetch_events(conn: sqlite3.Connection, since_ts: float) -> Iterable[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, event_type, ts_utc, ts, account_id, entity_id, email_id, payload, schema_version, fingerprint
        FROM events_v1
        WHERE ts_utc >= ?
        ORDER BY ts_utc ASC, id ASC
        """,
        (since_ts,),
    )
    for row in cursor.fetchall():
        payload = _scrub_payload(_load_payload(row[7]))
        yield {
            "record_type": "event",
            "id": row[0],
            "event_type": row[1],
            "ts_utc": row[2],
            "ts": row[3],
            "account_id": row[4],
            "entity_id": row[5],
            "email_id": row[6],
            "payload": payload,
            "schema_version": row[8],
            "fingerprint": row[9],
        }


def _fetch_commitments(conn: sqlite3.Connection, since_dt: datetime) -> Iterable[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, email_row_id, source, commitment_text, deadline_iso, status, confidence, created_at
        FROM commitments
        ORDER BY created_at ASC, id ASC
        """
    )
    for row in cursor.fetchall():
        created_at = _parse_sqlite_timestamp(row[7])
        if created_at and created_at < since_dt:
            continue
        yield {
            "record_type": "commitment",
            "id": row[0],
            "email_row_id": row[1],
            "source": row[2],
            "commitment_text": row[3],
            "deadline_iso": row[4],
            "status": row[5],
            "confidence": row[6],
            "created_at": row[7],
        }


def _fetch_relationship_snapshots(
    conn: sqlite3.Connection, since_dt: datetime
) -> Iterable[dict[str, Any]]:
    try:
        cursor = conn.execute(
            """
            SELECT id, created_at, entity_id, health_score, reason, components_breakdown, data_window_days
            FROM relationship_health_snapshots
            ORDER BY created_at ASC, id ASC
            """
        )
    except sqlite3.Error:
        return []
    rows = cursor.fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        created_at = _parse_sqlite_timestamp(row[1])
        if created_at and created_at < since_dt:
            continue
        components = _load_payload(row[5])
        results.append(
            {
                "record_type": "relationship_health_snapshot",
                "id": row[0],
                "created_at": row[1],
                "entity_id": row[2],
                "health_score": row[3],
                "reason": row[4],
                "components_breakdown": components,
                "data_window_days": row[6],
            }
        )
    return results


def _quality_metrics_record(db_path: Path, since_dt: datetime, *, now: datetime) -> dict[str, Any]:
    analytics = KnowledgeAnalytics(db_path)
    window_days = max(1, int((now - since_dt).total_seconds() // 86400))
    metrics = compute_quality_metrics(
        analytics=analytics,
        account_email=None,
        window_days=window_days,
        now=now,
    )
    if metrics is None:
        return {
            "record_type": "quality_metrics",
            "window_days": window_days,
            "corrections_total": 0,
            "correction_rate": None,
            "emails_received": 0,
            "by_new_priority": [],
            "by_engine": [],
        }
    return {
        "record_type": "quality_metrics",
        "window_days": metrics.window_days,
        "corrections_total": metrics.corrections_total,
        "correction_rate": metrics.correction_rate,
        "emails_received": metrics.emails_received,
        "by_new_priority": [
            {"key": row.key, "count": row.count}
            for row in metrics.by_new_priority
        ],
        "by_engine": [
            {"key": row.key, "count": row.count}
            for row in metrics.by_engine
        ],
    }


def _notification_sla_record(db_path: Path, *, now: datetime) -> dict[str, Any]:
    analytics = KnowledgeAnalytics(db_path)
    sla = compute_notification_sla(analytics=analytics, now=now)
    return {
        "record_type": "notification_sla",
        "delivery_rate_24h": sla.delivery_rate_24h,
        "delivery_rate_7d": sla.delivery_rate_7d,
        "salvage_rate_24h": sla.salvage_rate_24h,
        "p50_latency_24h": sla.p50_latency_24h,
        "p90_latency_24h": sla.p90_latency_24h,
        "p99_latency_24h": sla.p99_latency_24h,
        "p50_latency_7d": sla.p50_latency_7d,
        "p90_latency_7d": sla.p90_latency_7d,
        "p99_latency_7d": sla.p99_latency_7d,
        "top_error_reasons_24h": [
            {"reason": row.reason, "count": row.count, "share": row.share}
            for row in sla.top_error_reasons_24h
        ],
    }


def _parse_sqlite_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def export_data(
    *,
    db_path: Path,
    output_path: Path,
    since_dt: datetime,
) -> ExportResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    record_count = 0
    since_ts = since_dt.timestamp()
    export_now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn, output_path.open("w", encoding="utf-8") as handle:
        for record in _fetch_events(conn, since_ts):
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
            record_count += 1
        for record in _fetch_commitments(conn, since_dt):
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
            record_count += 1
        for record in _fetch_relationship_snapshots(conn, since_dt):
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
            record_count += 1
        quality_metrics = _quality_metrics_record(db_path, since_dt, now=export_now)
        handle.write(json.dumps(quality_metrics, ensure_ascii=False, sort_keys=True))
        handle.write("\n")
        record_count += 1
        notification_sla = _notification_sla_record(db_path, now=export_now)
        handle.write(json.dumps(notification_sla, ensure_ascii=False, sort_keys=True))
        handle.write("\n")
        record_count += 1

    return ExportResult(output_path=output_path, record_count=record_count)


def run_export(since: str) -> None:
    base_dir = _repo_root()
    since_dt = _parse_since(since)
    output_dir = base_dir / "exports"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"export_{timestamp}.jsonl"

    db_path = _resolve_db_path(base_dir)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    result = export_data(db_path=db_path, output_path=output_path, since_dt=since_dt)
    print(f"[OK] Export created: {result.output_path}")
    print(f"[OK] Records exported: {result.record_count}")


__all__ = ["ExportResult", "export_data", "run_export"]
