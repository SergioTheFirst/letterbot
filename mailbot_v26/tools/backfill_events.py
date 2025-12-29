from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.event_emitter import EventEmitter as ObservabilityEventEmitter

logger = get_logger("mailbot")

_UNKNOWN_ACCOUNT = "unknown"


@dataclass(frozen=True, slots=True)
class BackfillStats:
    inserted: int = 0
    attempted: int = 0


def _parse_ts(value: object | None) -> datetime:
    if value:
        parsed = parse_sqlite_datetime(str(value))
        if parsed is not None:
            return parsed
    return datetime.now(timezone.utc)


def _ensure_state_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events_backfill_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            status TEXT NOT NULL,
            completed_at TEXT
        );
        """
    )


def _is_backfill_done(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT status FROM events_backfill_state WHERE id = 1"
    ).fetchone()
    return bool(row and row[0] == "done")


def _mark_backfill_done(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO events_backfill_state (id, status, completed_at)
        VALUES (1, 'done', ?)
        ON CONFLICT(id) DO UPDATE SET
            status = excluded.status,
            completed_at = excluded.completed_at
        """,
        (datetime.now(timezone.utc).isoformat(),),
    )


def _chunked(events: Iterable[EventV1], size: int) -> Iterable[list[EventV1]]:
    batch: list[EventV1] = []
    for event in events:
        batch.append(event)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _insert_events(conn: sqlite3.Connection, events: list[EventV1]) -> BackfillStats:
    if not events:
        return BackfillStats()
    payloads: list[tuple[object, ...]] = []
    for event in events:
        ts_iso = datetime.fromtimestamp(event.ts_utc, tz=timezone.utc).isoformat()
        payload_json = json.dumps(event.payload, ensure_ascii=False)
        payloads.append(
            (
                event.event_type.value,
                event.ts_utc,
                ts_iso,
                event.account_id,
                event.entity_id,
                event.email_id,
                payload_json,
                payload_json,
                event.schema_version,
                fingerprint(event),
            )
        )
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO events_v1 (
            event_type,
            ts_utc,
            ts,
            account_id,
            entity_id,
            email_id,
            payload,
            payload_json,
            schema_version,
            fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payloads,
    )
    conn.commit()
    inserted = conn.total_changes - before
    return BackfillStats(inserted=inserted, attempted=len(events))


def _load_email_events(conn: sqlite3.Connection) -> list[EventV1]:
    try:
        attachments_rows = conn.execute(
            """
            SELECT email_id, COUNT(*) AS attachment_count
            FROM attachments
            GROUP BY email_id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        attachments_rows = []
    attachments = {
        int(row["email_id"]): int(row["attachment_count"])
        for row in attachments_rows
    }
    events: list[EventV1] = []
    try:
        rows = conn.execute(
            """
            SELECT
                id,
                account_email,
                from_email,
                subject,
                received_at,
                body_summary,
                deferred_for_digest,
                raw_body_hash
            FROM emails
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    for row in rows:
        received_at = _parse_ts(row["received_at"])
        email_id = int(row["id"])
        attachments_count = attachments.get(email_id, 0)
        events.append(
            EventV1(
                event_type=EventType.EMAIL_RECEIVED,
                ts_utc=received_at.timestamp(),
                account_id=str(row["account_email"]),
                entity_id=None,
                email_id=email_id,
                payload={
                    "from_email": row["from_email"],
                    "subject": row["subject"],
                    "body_summary": row["body_summary"],
                    "attachments_count": attachments_count,
                },
            )
        )
        if int(row["deferred_for_digest"] or 0) == 1:
            attachments_only = bool((row["raw_body_hash"] or "") == "" and attachments_count > 0)
            events.append(
                EventV1(
                    event_type=EventType.ATTENTION_DEFERRED_FOR_DIGEST,
                    ts_utc=received_at.timestamp(),
                    account_id=str(row["account_email"]),
                    entity_id=None,
                    email_id=email_id,
                    payload={
                        "reason": "backfill",
                        "attachments_only": attachments_only,
                        "attachments_count": attachments_count,
                    },
                )
            )
    return events


def _load_commitment_events(conn: sqlite3.Connection) -> list[EventV1]:
    events: list[EventV1] = []
    try:
        rows = conn.execute(
            """
            SELECT
                c.id,
                c.commitment_text,
                c.deadline_iso,
                c.status,
                c.source,
                c.confidence,
                c.created_at,
                e.account_email,
                e.from_email,
                e.id AS email_id
            FROM commitments c
            JOIN emails e ON e.id = c.email_row_id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    for row in rows:
        created_at = _parse_ts(row["created_at"])
        account_email = str(row["account_email"])
        commitment_id = int(row["id"])
        payload = {
            "commitment_id": commitment_id,
            "commitment_text": row["commitment_text"],
            "deadline_iso": row["deadline_iso"],
            "status": row["status"],
            "source": row["source"],
            "confidence": row["confidence"],
            "from_email": row["from_email"],
        }
        events.append(
            EventV1(
                event_type=EventType.COMMITMENT_CREATED,
                ts_utc=created_at.timestamp(),
                account_id=account_email,
                entity_id=None,
                email_id=int(row["email_id"]),
                payload=payload,
            )
        )
        status = str(row["status"] or "").lower()
        if status in {"fulfilled", "expired"}:
            events.append(
                EventV1(
                    event_type=EventType.COMMITMENT_STATUS_CHANGED,
                    ts_utc=created_at.timestamp(),
                    account_id=account_email,
                    entity_id=None,
                    email_id=int(row["email_id"]),
                    payload={
                        "commitment_id": commitment_id,
                        "old_status": "pending",
                        "new_status": status,
                        "reason": "backfill",
                        "deadline_iso": row["deadline_iso"],
                        "commitment_text": row["commitment_text"],
                        "from_email": row["from_email"],
                    },
                )
            )
    return events


def _load_trust_events(conn: sqlite3.Connection) -> list[EventV1]:
    events: list[EventV1] = []
    try:
        rows = conn.execute(
            """
            SELECT entity_id, trust_score, reason, sample_size, created_at
            FROM trust_snapshots
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    for row in rows:
        created_at = _parse_ts(row["created_at"])
        events.append(
            EventV1(
                event_type=EventType.TRUST_SCORE_UPDATED,
                ts_utc=created_at.timestamp(),
                account_id=_UNKNOWN_ACCOUNT,
                entity_id=row["entity_id"],
                email_id=None,
                payload={
                    "trust_score": row["trust_score"],
                    "reason": row["reason"],
                    "sample_size": row["sample_size"],
                },
            )
        )
    return events


def _load_relationship_health_events(conn: sqlite3.Connection) -> list[EventV1]:
    events: list[EventV1] = []
    try:
        rows = conn.execute(
            """
            SELECT entity_id, health_score, reason, components_breakdown, data_window_days, created_at
            FROM relationship_health_snapshots
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    for row in rows:
        created_at = _parse_ts(row["created_at"])
        components = row["components_breakdown"]
        if components:
            try:
                components = json.loads(components)
            except (TypeError, ValueError):
                components = row["components_breakdown"]
        events.append(
            EventV1(
                event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
                ts_utc=created_at.timestamp(),
                account_id=_UNKNOWN_ACCOUNT,
                entity_id=row["entity_id"],
                email_id=None,
                payload={
                    "health_score": row["health_score"],
                    "reason": row["reason"],
                    "components": components,
                    "data_window_days": row["data_window_days"],
                },
            )
        )
    return events


def _load_digest_events(conn: sqlite3.Connection) -> list[EventV1]:
    events: list[EventV1] = []
    try:
        rows = conn.execute(
            """
            SELECT account_email, last_digest_sent_at
            FROM digest_state
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        sent_at = _parse_ts(row["last_digest_sent_at"])
        events.append(
            EventV1(
                event_type=EventType.DAILY_DIGEST_SENT,
                ts_utc=sent_at.timestamp(),
                account_id=str(row["account_email"]),
                entity_id=None,
                email_id=None,
                payload={"account_email": row["account_email"]},
            )
        )
    try:
        rows = conn.execute(
            """
            SELECT account_email, last_week_key, last_sent_at
            FROM weekly_digest_state
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        sent_at = _parse_ts(row["last_sent_at"])
        events.append(
            EventV1(
                event_type=EventType.WEEKLY_DIGEST_SENT,
                ts_utc=sent_at.timestamp(),
                account_id=str(row["account_email"]),
                entity_id=None,
                email_id=None,
                payload={
                    "week_key": row["last_week_key"],
                    "account_email": row["account_email"],
                },
            )
        )
    return events


def run_backfill(db_path: Path, *, batch_size: int = 500, force: bool = False) -> BackfillStats:
    ObservabilityEventEmitter(db_path)
    ContractEventEmitter(db_path)
    total_inserted = 0
    total_attempted = 0
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            _ensure_state_table(conn)
            if not force and _is_backfill_done(conn):
                return BackfillStats()

            event_emitter = ObservabilityEventEmitter(db_path)
            event_emitter.emit(
                type="events_backfill_started",
                timestamp=datetime.now(timezone.utc),
                payload={"analytics_source": "events"},
            )

            sources = (
                _load_email_events(conn),
                _load_commitment_events(conn),
                _load_trust_events(conn),
                _load_relationship_health_events(conn),
                _load_digest_events(conn),
            )
            for source_events in sources:
                for batch in _chunked(source_events, batch_size):
                    stats = _insert_events(conn, batch)
                    total_inserted += stats.inserted
                    total_attempted += stats.attempted

            _mark_backfill_done(conn)
            event_emitter.emit(
                type="events_backfill_done",
                timestamp=datetime.now(timezone.utc),
                payload={
                    "analytics_source": "events",
                    "inserted": total_inserted,
                    "attempted": total_attempted,
                },
            )
            return BackfillStats(inserted=total_inserted, attempted=total_attempted)
    except Exception as exc:
        logger.error("events_backfill_failed", error=str(exc))
        ObservabilityEventEmitter(db_path).emit(
            type="events_backfill_failed",
            timestamp=datetime.now(timezone.utc),
            payload={"analytics_source": "events", "error": str(exc)},
        )
        raise


def maybe_backfill_events(db_path: Path, *, force: bool = False) -> BackfillStats:
    if not db_path.exists():
        return BackfillStats()
    return run_backfill(db_path, force=force)


def main() -> None:
    run_backfill(Path("database.sqlite"))


if __name__ == "__main__":
    main()
