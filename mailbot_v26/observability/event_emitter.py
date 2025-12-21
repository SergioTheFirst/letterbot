from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class Event:
    id: str
    type: str
    timestamp: str
    entity_id: str | None
    email_id: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class EventEmitter:
    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY,
                        type TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        entity_id TEXT,
                        email_id TEXT,
                        payload JSON
                    );
                    """
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("event_store_init_failed", error=str(exc))

    def emit(
        self,
        *,
        type: str,
        timestamp: datetime | str | None = None,
        entity_id: str | None = None,
        email_id: int | str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event_time = timestamp or datetime.now(timezone.utc)
        if isinstance(event_time, datetime):
            event_timestamp = event_time.isoformat()
        else:
            event_timestamp = str(event_time)
        event = Event(
            id=uuid.uuid4().hex,
            type=type,
            timestamp=event_timestamp,
            entity_id=entity_id,
            email_id=str(email_id) if email_id is not None else None,
            payload=payload or {},
        )
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO events (
                        id,
                        type,
                        timestamp,
                        entity_id,
                        email_id,
                        payload
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.id,
                        event.type,
                        event.timestamp,
                        event.entity_id,
                        event.email_id,
                        json.dumps(event.payload, ensure_ascii=False),
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.error(
                "event_emit_failed",
                event_type=event.type,
                entity_id=event.entity_id,
                error=str(exc),
            )
            return
        logger.info(
            "event_emitted",
            event_type=event.type,
            entity_id=event.entity_id,
            email_id=event.email_id,
        )


__all__ = ["Event", "EventEmitter"]
