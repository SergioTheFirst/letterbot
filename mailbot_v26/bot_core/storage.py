from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def normalize_message_id(value: object) -> str | None:
    """Message-ID as stored and compared: folding whitespace collapsed and
    stripped, angle brackets and case kept (the local part is case-sensitive
    and IMAP refetch compares exactly). None when empty."""
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


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
            self.conn.execute("""
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
                """)
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
                self.conn.execute("""
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
                    """)
                email_columns = {
                    str(row[1])
                    for row in self.conn.execute(
                        "PRAGMA table_info(emails);"
                    ).fetchall()
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
            # UIDVALIDITY epoch of uid; NULL for rows stored before WP2b.
            if "uidvalidity" not in email_columns:
                self.conn.execute("ALTER TABLE emails ADD COLUMN uidvalidity INTEGER;")
            # Message-ID is stored normalized since WP2c; older rows hold the
            # raw (possibly folded) header.
            self._normalize_stored_message_ids()
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_emails_account_message_id
                ON emails(account_email, message_id);
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_emails_status
                ON emails(status);
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_emails_received
                ON emails(received_at);
                """)
            self.conn.execute("""
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
                """)
            self.conn.execute("""
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
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tg_delivery_log_email_kind
                ON telegram_delivery_log(email_id, kind);
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_stage_time
                ON queue(stage, not_before);
                """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS telegram_snooze (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id INTEGER NOT NULL,
                    deliver_at_utc TEXT NOT NULL,
                    snoozed_at_utc TEXT,
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
                """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_snooze_due
                ON telegram_snooze(status, deliver_at_utc);
                """)
            snooze_columns = {
                str(row[1])
                for row in self.conn.execute(
                    "PRAGMA table_info(telegram_snooze);"
                ).fetchall()
            }
            if "snoozed_at_utc" not in snooze_columns:
                self.conn.execute(
                    "ALTER TABLE telegram_snooze ADD COLUMN snoozed_at_utc TEXT;"
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
        uidvalidity: int | None = None,
    ) -> int:
        now = self._now()
        attach_count = attachments_count if attachments_count is not None else 0
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO emails (
                    account_email, uid, message_id, from_email, from_name, subject,
                    received_at, attachments_count, status, created_at, updated_at,
                    uidvalidity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW', ?, ?, ?)
                ON CONFLICT(account_email, uid) DO UPDATE SET
                    message_id=excluded.message_id,
                    from_email=excluded.from_email,
                    from_name=excluded.from_name,
                    subject=excluded.subject,
                    received_at=excluded.received_at,
                    attachments_count=excluded.attachments_count,
                    updated_at=excluded.updated_at,
                    uidvalidity=COALESCE(excluded.uidvalidity, emails.uidvalidity)
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
                    uidvalidity,
                ),
            )
            row = cursor.fetchone()
            return int(row[0]) if row else -1

    def enqueue_stage(
        self, email_id: int, stage: str, not_before: str | None = None
    ) -> None:
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

    def has_email_history(self, account_email: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM emails
            WHERE account_email = ? COLLATE NOCASE
            LIMIT 1;
            """,
            (account_email,),
        ).fetchone()
        return bool(row)

    def is_telegram_delivered(self, email_id: int) -> bool:
        row = self.conn.execute(
            "SELECT telegram_delivered_at FROM emails WHERE id = ?;",
            (email_id,),
        ).fetchone()
        return bool(row and row[0])

    def mark_telegram_delivered(
        self, email_id: int, delivered_at: str | None = None
    ) -> None:
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

    def get_telegram_delivery_state(
        self, delivery_key: str, email_id: int | None
    ) -> str | None:
        """None (never reserved), "confirmed" or "unconfirmed" (reserved, no proof).

        Confirmed means Telegram returned the message (message_id) or the email
        is marked delivered; a reservation without either may or may not have
        reached the chat.
        """
        row = self.conn.execute(
            "SELECT telegram_message_id FROM telegram_delivery_log WHERE delivery_key = ?;",
            (delivery_key,),
        ).fetchone()
        delivered = email_id is not None and self.is_telegram_delivered(email_id)
        if delivered or (row is not None and row[0]):
            return "confirmed"
        if row is not None:
            return "unconfirmed"
        return None

    def set_email_delivery_unknown(self, email_id: int, error: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE emails
                SET status = 'DELIVERY_UNKNOWN', error_last = ?, updated_at = ?
                WHERE id = ?;
                """,
                (error, self._now(), email_id),
            )

    def get_email_account(self, email_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT account_email FROM emails WHERE id = ?;",
            (email_id,),
        ).fetchone()
        return str(row[0]) if row and row[0] else None

    def list_due_snoozes(
        self, *, now_iso: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT id, email_id, deliver_at_utc, snoozed_at_utc, reminder_text, attempts
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
                    "snoozed_at_utc": str(row[3] or ""),
                    "reminder_text": str(row[4] or ""),
                    "attempts": int(row[5] or 0),
                }
            )
        return result

    def mark_snooze_delivered(
        self, *, snooze_id: int, delivered_at: str | None = None
    ) -> None:
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

    def persist_email_and_enqueue(
        self,
        *,
        account_email: str,
        uid: int,
        message_id: str | None,
        from_email: str | None,
        from_name: str | None,
        subject: str | None,
        received_at: str | None,
        attachments_count: int | None,
        stage: str = "PARSE",
        uidvalidity: int | None = None,
        dedupe_message_id: bool = False,
    ) -> tuple[int, bool]:
        """Store a new email and its first queue row in one transaction.

        Returns (email_id, created). An email already stored for
        (account_email, uid) is left as is and nothing is enqueued; a crash
        can no longer leave an email row without its queue row (F5).
        uidvalidity is the epoch of uid, used to refetch after a restart.
        message_id is stored normalized (normalize_message_id).

        dedupe_message_id (UIDVALIDITY resync, WP2c): a settled row with the
        same Message-ID (see _message_id_rows) is returned as a duplicate,
        which also skips a second copy in the same batch; an orphan row
        retired from an old epoch (uid <= 0) is re-attached to uid and
        enqueued again (created=True). Mail without Message-ID is new.
        """
        now = self._now()
        attach_count = attachments_count if attachments_count is not None else 0
        message_id = normalize_message_id(message_id)
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE;")
            row = cursor.execute(
                "SELECT id FROM emails WHERE account_email = ? AND uid = ?;",
                (account_email, uid),
            ).fetchone()
            if row is not None:
                self.conn.commit()
                return int(row[0]), False
            if dedupe_message_id and message_id is not None:
                matches = self._message_id_rows(cursor, account_email, message_id)
                settled = next((m for m in matches if m[2]), None)
                if settled is not None:
                    self.conn.commit()
                    logger.info(
                        "duplicate_message_id_skipped account_email=%s uid=%s email_id=%s",
                        account_email,
                        uid,
                        settled[0],
                    )
                    return settled[0], False
                orphan = next((m for m in matches if m[1] <= 0), None)
                if orphan is not None:
                    cursor.execute(
                        """
                        UPDATE emails
                        SET uid = ?, uidvalidity = ?, status = 'NEW', updated_at = ?
                        WHERE id = ?;
                        """,
                        (uid, uidvalidity, now, orphan[0]),
                    )
                    self._insert_first_queue_row(cursor, orphan[0], stage, now)
                    self.conn.commit()
                    logger.warning(
                        "orphan_email_reattached account_email=%s uid=%s email_id=%s",
                        account_email,
                        uid,
                        orphan[0],
                    )
                    return orphan[0], True
            cursor.execute(
                """
                INSERT INTO emails (
                    account_email, uid, message_id, from_email, from_name, subject,
                    received_at, attachments_count, status, created_at, updated_at,
                    uidvalidity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW', ?, ?, ?);
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
                    uidvalidity,
                ),
            )
            email_id = int(cursor.lastrowid)
            self._insert_first_queue_row(cursor, email_id, stage, now)
            self.conn.commit()
            return email_id, True
        except BaseException:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def get_email_locator(self, email_id: int) -> Dict[str, Any] | None:
        """Where an email lives in IMAP: account, uid, its epoch, Message-ID.

        None when the email is not stored. uidvalidity is None for rows
        stored before the column existed; message_id is normalized.
        """
        row = self.conn.execute(
            """
            SELECT account_email, uid, uidvalidity, message_id
            FROM emails
            WHERE id = ?;
            """,
            (email_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "account_email": str(row[0]),
            "uid": int(row[1]),
            "uidvalidity": int(row[2]) if row[2] is not None else None,
            "message_id": str(row[3]) if row[3] is not None else None,
        }

    def _normalize_stored_message_ids(self) -> int:
        """Normalize Message-IDs stored raw before WP2c (folded headers), so
        lookups by normalized Message-ID find them. Returns rows changed."""
        rows = self.conn.execute(
            "SELECT id, message_id FROM emails WHERE message_id GLOB ?;",
            ("*[ \t\r\n]*",),
        ).fetchall()
        for email_id, message_id in rows:
            self.conn.execute(
                "UPDATE emails SET message_id = ? WHERE id = ?;",
                (normalize_message_id(message_id), email_id),
            )
        if rows:
            logger.info("emails_message_id_normalized rows=%d", len(rows))
        return len(rows)

    def retire_uid_epoch(self, account_email: str, uidvalidity: int) -> int:
        """Detach the account's rows of other UIDVALIDITY epochs from their UIDs.

        A new epoch reuses UIDs, so an old row must not match a new message
        by (account_email, uid) (F6). Its uid becomes -id (unique, never a
        server UID); the row, its Message-ID and queue rows stay. Rows stored
        before the uidvalidity column (NULL) are retired too. Irreversible.
        Returns the number of rows retired.
        """
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE emails SET uid = -id
                WHERE account_email = ? AND uid > 0
                  AND (uidvalidity IS NULL OR uidvalidity <> ?);
                """,
                (account_email, int(uidvalidity)),
            )
        return int(cursor.rowcount)

    @staticmethod
    def _message_id_rows(
        cursor: sqlite3.Cursor, account_email: str, message_id: str
    ) -> List[tuple[int, int, bool]]:
        """(id, uid, settled) of the account's rows with this normalized
        Message-ID, newest first. Settled: delivered or a delivery reserved
        (sent or maybe sent), DELIVERY_UNKNOWN, DELIVERY_FAILED, or still
        queued; anything else is an orphan that was never notified."""
        rows = cursor.execute(
            """
            SELECT e.id, e.uid,
                   (e.telegram_delivered_at IS NOT NULL
                    OR e.status IN ('DELIVERY_UNKNOWN', 'DELIVERY_FAILED')
                    OR EXISTS (SELECT 1 FROM queue q WHERE q.email_id = e.id)
                    OR EXISTS (
                        SELECT 1 FROM telegram_delivery_log d WHERE d.email_id = e.id
                    ))
            FROM emails e
            WHERE e.account_email = ? AND e.message_id = ?
            ORDER BY e.id DESC;
            """,
            (account_email, message_id),
        ).fetchall()
        return [(int(row[0]), int(row[1]), bool(row[2])) for row in rows]

    def is_known_message_id(self, account_email: str, message_id: str | None) -> bool:
        """True when an email with this Message-ID is settled for the account
        (see _message_id_rows), whatever its UID or epoch. An orphan, or a
        missing Message-ID, is not known (WP2c)."""
        normalized = normalize_message_id(message_id)
        if normalized is None:
            return False
        cursor = self.conn.cursor()
        try:
            rows = self._message_id_rows(cursor, account_email, normalized)
        finally:
            cursor.close()
        return any(settled for _id, _uid, settled in rows)

    @staticmethod
    def _insert_first_queue_row(
        cursor: sqlite3.Cursor, email_id: int, stage: str, now: str
    ) -> None:
        cursor.execute(
            """
            INSERT OR IGNORE INTO queue (
                email_id, stage, attempts, not_before, last_error, created_at, updated_at
            ) VALUES (?, ?, 0, NULL, NULL, ?, ?);
            """,
            (email_id, stage, now, now),
        )

    def advance_stage(
        self,
        queue_id: int,
        email_id: int,
        next_stage: str,
        not_before: str | None = None,
    ) -> None:
        """Replace the finished queue row with the next stage's row in one
        transaction (R1): a crash or a failed write between the two can no
        longer leave the email without a queue row (status NEW, never
        notified). The next stage starts with a fresh attempt budget.
        not_before is naive-UTC isoformat, as claim_next compares (F11).
        """
        now = self._now()
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE;")
            cursor.execute("DELETE FROM queue WHERE id = ?;", (queue_id,))
            cursor.execute(
                """
                INSERT OR IGNORE INTO queue (
                    email_id, stage, attempts, not_before, last_error, created_at, updated_at
                ) VALUES (?, ?, 0, ?, NULL, ?, ?);
                """,
                (email_id, next_stage, not_before, now, now),
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        finally:
            cursor.close()


__all__ = ["Storage", "normalize_message_id"]
