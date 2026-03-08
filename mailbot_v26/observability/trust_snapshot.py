from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.insights.trust_score import TrustSnapshot
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(slots=True)
class TrustSnapshotWriter:
    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trust_snapshots (
                        id TEXT PRIMARY KEY,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        entity_id TEXT NOT NULL,
                        trust_score REAL,
                        reason TEXT,
                        sample_size INTEGER,
                        data_quality TEXT,
                        model_version TEXT DEFAULT 'v1'
                    );
                    """)
                self._ensure_columns(conn)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("trust_snapshot_init_failed", error=str(exc))

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(trust_snapshots)")
        }
        if "data_quality" not in columns:
            conn.execute("ALTER TABLE trust_snapshots ADD COLUMN data_quality TEXT;")
        if "model_version" not in columns:
            conn.execute(
                "ALTER TABLE trust_snapshots ADD COLUMN model_version TEXT DEFAULT 'v1';"
            )

    def write(self, snapshot: TrustSnapshot) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO trust_snapshots (
                        id,
                        entity_id,
                        trust_score,
                        reason,
                        sample_size,
                        data_quality,
                        model_version
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        snapshot.entity_id,
                        snapshot.score,
                        snapshot.reason,
                        snapshot.sample_size,
                        snapshot.data_quality,
                        snapshot.model_version,
                    ),
                )
                conn.commit()
        except Exception:
            raise
