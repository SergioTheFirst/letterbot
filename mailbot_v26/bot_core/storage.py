from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._configure()
        self._create_schema()

    def _configure(self) -> None:
        pragmas = [
            "PRAGMA journal_mode=WAL;",
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA foreign_keys=ON;",
            "PRAGMA busy_timeout=5000;",
        ]
        for pragma in pragmas:
            self.conn.execute(pragma)

    def _create_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_email TEXT NOT NULL,
                    uid INTEGER NOT NULL,
                    message_id TEXT,
                    from_email TEXT,
                    from_name TEXT,
                    subject TEXT,
                    received_at TEXT,
                    attachments_count INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    error_last TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(account_email, uid)
                );
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_emails_status
                ON emails(status);
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_emails_received
                ON emails(received_at);
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id INTEGER NOT NULL,
                    stage TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    not_before TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(email_id, stage),
                    FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE
                );
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_stage_time
                ON queue(stage, not_before);
                """
            )

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat()

    def upsert_email(
        self,
        account_email: str,
        uid: int,
        message_id: str | None,
        from_email: str | None,
        from_name: str | None,
        subject: str | None,
        received_at: str | None,
        attachments_count: int | None,
    ) -> int:
        now = self._now()
        attach_count = attachments_count if attachments_count is not None else 0
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO emails (
                    account_email, uid, message_id, from_email, from_name, subject,
                    received_at, attachments_count, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW', ?, ?)
                ON CONFLICT(account_email, uid) DO UPDATE SET
                    message_id=excluded.message_id,
                    from_email=excluded.from_email,
                    from_name=excluded.from_name,
                    subject=excluded.subject,
                    received_at=excluded.received_at,
                    attachments_count=excluded.attachments_count,
                    updated_at=excluded.updated_at
                RETURNING id;
                """,
                (
                    account_email,
                    uid,
                    message_id,
                    from_email,
                    from_name,
                    subject,
                    received_at,
                    attach_count,
                    now,
                    now,
                ),
            )
            row = cursor.fetchone()
            return int(row[0]) if row else -1

    def enqueue_stage(self, email_id: int, stage: str, not_before: str | None = None) -> None:
        now = self._now()
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO queue (
                    email_id, stage, attempts, not_before, last_error, created_at, updated_at
                ) VALUES (?, ?, 0, ?, NULL, ?, ?);
                """,
                (email_id, stage, not_before, now, now),
            )

    def claim_next(self, stages: List[str]) -> Dict[str, Any] | None:
        if not stages:
            return None
        placeholders = ",".join("?" for _ in stages)
        now = self._now()
        with self.conn:
            row = self.conn.execute(
                f"""
                SELECT id, email_id, stage, attempts FROM queue
                WHERE stage IN ({placeholders})
                  AND (not_before IS NULL OR not_before <= ?)
                ORDER BY id
                LIMIT 1;
                """,
                (*stages, now),
            ).fetchone()
            if not row:
                return None
            queue_id, email_id, stage, attempts = row
            new_attempts = attempts + 1
            self.conn.execute(
                """
                UPDATE queue
                SET attempts = ?, updated_at = ?
                WHERE id = ?;
                """,
                (new_attempts, self._now(), queue_id),
            )
            return {
                "queue_id": int(queue_id),
                "email_id": int(email_id),
                "stage": str(stage),
                "attempts": int(new_attempts),
            }

    def mark_done(self, queue_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM queue WHERE id = ?;", (queue_id,))

    def mark_error(self, queue_id: int, error: str, backoff_seconds: int) -> None:
        now = datetime.utcnow()
        backoff_time = now + timedelta(seconds=backoff_seconds)
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET last_error = ?, not_before = ?, updated_at = ?
                WHERE id = ?;
                """,
                (
                    error,
                    backoff_time.isoformat(),
                    now.isoformat(),
                    queue_id,
                ),
            )

    def set_email_error(self, email_id: int, error: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE emails
                SET status = 'ERROR', error_last = ?, updated_at = ?
                WHERE id = ?;
                """,
                (error, self._now(), email_id),
            )


__all__ = ["Storage"]
