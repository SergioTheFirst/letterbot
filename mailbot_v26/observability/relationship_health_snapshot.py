from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.insights.relationship_health import HealthSnapshot
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(slots=True)
class RelationshipHealthSnapshotWriter:
    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS relationship_health_snapshots (
                        id TEXT PRIMARY KEY,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        entity_id TEXT NOT NULL,
                        health_score REAL,
                        reason TEXT,
                        components_breakdown JSON,
                        data_window_days INTEGER
                    );
                    """)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("relationship_health_snapshot_init_failed", error=str(exc))

    def write(self, snapshot: HealthSnapshot) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO relationship_health_snapshots (
                        id,
                        entity_id,
                        health_score,
                        reason,
                        components_breakdown,
                        data_window_days
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        snapshot.entity_id,
                        snapshot.health_score,
                        snapshot.reason,
                        json.dumps(snapshot.components_breakdown, ensure_ascii=False),
                        snapshot.data_window_days,
                    ),
                )
                conn.commit()
        except Exception:
            raise
