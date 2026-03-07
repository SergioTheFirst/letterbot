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
                    telegram_delivered_at TEXT,
                    UNIQUE(account_email, uid)
                );
                """
            )
            email_columns = {
                str(row[1])
                for row in self.conn.execute("PRAGMA table_info(emails);").fetchall()
            }
            # Migration: if uid column is missing the schema is incompatible —
            # recreate emails and all dependent tables from scratch.
            # (This only happens once on first run after schema upgrade.)
            if "uid" not in email_columns:
                self.conn.execute("DROP TABLE IF EXISTS queue;")
                self.conn.execute("DROP TABLE IF EXISTS telegram_delivery_log;")
                self.conn.execute("DROP TABLE IF EXISTS telegram_snooze;")
                self.conn.execute("DROP TABLE IF EXISTS emails;")
                self.conn.execute(
                    """
                    CREATE TABLE emails (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_email TEXT NOT NULL,
                        uid INTEGER NOT NULL,
                        message_id TEXT,
                        from_email TEXT,
                        from_name TEXT,
                        subject TEXT,
                        received_at TEXT,
                        attachments_count INTEGER DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'NEW',
                        error_last TEXT,
                        created_at TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL DEFAULT '',
                        telegram_delivered_at TEXT,
                        UNIQUE(account_email, uid)
                    );
                    """
                )
                email_columns = {
                    str(row[1])
                    for row in self.conn.execute("PRAGMA table_info(emails);").fetchall()
                }
            if "status" not in email_columns:
                self.conn.execute(
                    "ALTER TABLE emails ADD COLUMN status TEXT NOT NULL DEFAULT 'NEW';"
                )
                email_columns.add("status")
            if "telegram_delivered_at" not in email_columns:
                self.conn.execute(
                    "ALTER TABLE emails ADD COLUMN telegram_delivered_at TEXT;"
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
                CREATE TABLE IF NOT EXISTS telegram_delivery_log (
                    delivery_key TEXT PRIMARY KEY,
                    email_id INTEGER,
                    account_id TEXT,
                    chat_id TEXT,
                    kind TEXT NOT NULL DEFAULT 'email',
                    first_sent_at TEXT,
                    telegram_message_id TEXT,
                    FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE
                );
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tg_delivery_log_email_kind
                ON telegram_delivery_log(email_id, kind);
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_stage_time
                ON queue(stage, not_before);
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_snooze (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id INTEGER NOT NULL,
                    deliver_at_utc TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    reminder_text TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    delivered_at TEXT,
                    FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE,
                    UNIQUE(email_id, deliver_at_utc)
                );
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_telegram_snooze_due
                ON telegram_snooze(status, deliver_at_utc);
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

    def find_email_id(self, account_email: str, uid: int) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM emails WHERE account_email = ? AND uid = ?;",
            (account_email, uid),
        ).fetchone()
        if not row:
            return None
        return int(row[0])

    def is_telegram_delivered(self, email_id: int) -> bool:
        row = self.conn.execute(
            "SELECT telegram_delivered_at FROM emails WHERE id = ?;",
            (email_id,),
        ).fetchone()
        return bool(row and row[0])

    def mark_telegram_delivered(self, email_id: int, delivered_at: str | None = None) -> None:
        ts = delivered_at or self._now()
        with self.conn:
            self.conn.execute(
                """
                UPDATE emails
                SET telegram_delivered_at = ?, updated_at = ?
                WHERE id = ?;
                """,
                (ts, self._now(), email_id),
            )

    def reserve_telegram_delivery(
        self,
        *,
        delivery_key: str,
        email_id: int | None,
        account_id: str | None,
        chat_id: str | None,
        kind: str = "email",
    ) -> str:
        now = self._now()
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE;")
            cursor.execute(
                """
                INSERT INTO telegram_delivery_log (
                    delivery_key, email_id, account_id, chat_id, kind, first_sent_at
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (delivery_key, email_id, account_id, chat_id, kind, now),
            )
            self.conn.commit()
            return "reserved"
        except sqlite3.IntegrityError:
            self.conn.rollback()
            return "duplicate"
        except sqlite3.Error:
            self.conn.rollback()
            return "unavailable"
        finally:
            cursor.close()

    def finalize_telegram_delivery(
        self,
        *,
        delivery_key: str,
        telegram_message_id: str | None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE telegram_delivery_log
                SET telegram_message_id = COALESCE(?, telegram_message_id)
                WHERE delivery_key = ?;
                """,
                (telegram_message_id, delivery_key),
            )

    def release_telegram_delivery(self, *, delivery_key: str) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM telegram_delivery_log WHERE delivery_key = ?;",
                (delivery_key,),
            )

    def list_due_snoozes(self, *, now_iso: str, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT id, email_id, deliver_at_utc, reminder_text, attempts
            FROM telegram_snooze
            WHERE status = 'pending'
              AND deliver_at_utc <= ?
            ORDER BY deliver_at_utc ASC, id ASC
            LIMIT ?
            """,
            (now_iso, max(1, int(limit))),
        ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "id": int(row[0]),
                    "email_id": int(row[1]),
                    "deliver_at_utc": str(row[2]),
                    "reminder_text": str(row[3] or ""),
                    "attempts": int(row[4] or 0),
                }
            )
        return result

    def mark_snooze_delivered(self, *, snooze_id: int, delivered_at: str | None = None) -> None:
        ts = delivered_at or self._now()
        with self.conn:
            self.conn.execute(
                """
                UPDATE telegram_snooze
                SET status = 'delivered',
                    delivered_at = ?,
                    updated_at = ?,
                    last_error = NULL
                WHERE id = ?
                """,
                (ts, self._now(), snooze_id),
            )

    def reschedule_snooze_retry(
        self,
        *,
        snooze_id: int,
        next_deliver_at_utc: str,
        attempts: int,
        error: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE telegram_snooze
                SET status = 'pending',
                    deliver_at_utc = ?,
                    attempts = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_deliver_at_utc, attempts, error, self._now(), snooze_id),
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

    def set_email_delivery_failed(self, email_id: int, error: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE emails
                SET status = 'DELIVERY_FAILED', error_last = ?, updated_at = ?
                WHERE id = ?;
                """,
                (error, self._now(), email_id),
            )


__all__ = ["Storage"]
