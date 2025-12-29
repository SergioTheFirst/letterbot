from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
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
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events_v1 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        ts_utc REAL NOT NULL,
                        account_id TEXT NOT NULL,
                        entity_id TEXT,
                        email_id INTEGER,
                        payload JSON,
                        schema_version INTEGER NOT NULL,
                        fingerprint TEXT NOT NULL UNIQUE
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_v1_event_type_ts
                    ON events_v1(event_type, ts_utc);
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_v1_account_ts
                    ON events_v1(account_id, ts_utc);
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_v1_entity_ts
                    ON events_v1(entity_id, ts_utc);
                    """
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("event_store_init_failed", error=str(exc))

    def emit(self, event: EventV1) -> bool:
        fp = fingerprint(event)
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO events_v1 (
                        event_type,
                        ts_utc,
                        account_id,
                        entity_id,
                        email_id,
                        payload,
                        schema_version,
                        fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_type.value,
                        event.ts_utc,
                        event.account_id,
                        event.entity_id,
                        event.email_id,
                        json.dumps(event.payload, ensure_ascii=False),
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
