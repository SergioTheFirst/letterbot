from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventV1, fingerprint
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(slots=True)
class EventEmitter:
    db_path: Path

    def __post_init__(self) -> None:
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS events_v1 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        ts_utc REAL NOT NULL,
                        ts TEXT,
                        account_id TEXT NOT NULL,
                        entity_id TEXT,
                        email_id INTEGER,
                        payload JSON,
                        payload_json JSON,
                        schema_version INTEGER NOT NULL,
                        fingerprint TEXT NOT NULL UNIQUE
                    );
                    """)
                self._ensure_columns(conn)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_event_type_ts
                    ON events_v1(event_type, ts_utc);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_account_ts
                    ON events_v1(account_id, ts_utc);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_entity_ts
                    ON events_v1(entity_id, ts_utc);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_email_id
                    ON events_v1(email_id);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_event_type_ts_iso
                    ON events_v1(event_type, ts);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_account_ts_iso
                    ON events_v1(account_id, ts);
                    """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_v1_entity_ts_iso
                    ON events_v1(entity_id, ts);
                    """)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("event_store_init_failed", error=str(exc))

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(events_v1)")}
        if "ts" not in columns:
            conn.execute("ALTER TABLE events_v1 ADD COLUMN ts TEXT;")
        if "payload" not in columns:
            conn.execute("ALTER TABLE events_v1 ADD COLUMN payload JSON;")
        if "payload_json" not in columns:
            conn.execute("ALTER TABLE events_v1 ADD COLUMN payload_json JSON;")

    def emit(self, event: EventV1) -> bool:
        fp = fingerprint(event)
        ts_iso = datetime.fromtimestamp(event.ts_utc, tz=timezone.utc).isoformat()
        payload = json.dumps(event.payload, ensure_ascii=False)
        payload_json = event.payload_json if event.payload_json is not None else payload
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.execute(
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
                    (
                        event.event_type.value,
                        event.ts_utc,
                        ts_iso,
                        event.account_id,
                        event.entity_id,
                        event.email_id,
                        payload,
                        payload_json,
                        event.schema_version,
                        fp,
                    ),
                )
                conn.commit()
                return conn.total_changes > 0
        except Exception as exc:
            logger.error(
                "event_emit_failed",
                event_type=event.event_type.value,
                entity_id=event.entity_id,
                email_id=event.email_id,
                error=str(exc),
            )
            return False


__all__ = ["EventEmitter"]
