from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, Iterable, TypeVar

from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.insights.commitment_lifecycle import (
    CommitmentRecord,
    CommitmentStatusUpdate,
    parse_sqlite_datetime,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class AutoPriorityGateState:
    last_disabled_at_utc: float | None
    last_disabled_reason: str | None
    last_eval_at_utc: float | None


class KnowledgeDB:
    _BUSY_TIMEOUT_MS = 5000
    _WRITE_RETRIES = 3
    _WRITE_BASE_DELAY = 0.1
    _WRITE_MAX_TOTAL_WAIT = 2.0

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        schema_sql = self._read_sql_script("schema.sql")
        views_sql = self._read_sql_script("views.sql")
        try:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                if schema_sql:
                    conn.executescript(schema_sql)
                self._ensure_optional_columns(conn)
                self._ensure_priority_feedback_index(conn)
                self._ensure_auto_priority_gate_state_table(conn)
                if views_sql:
                    conn.executescript(views_sql)
                    logger.debug("[CRM-ANALYTICS] views OK")
        except Exception as exc:
            logger.error("KnowledgeDB init failed: %s", exc)

    def _read_sql_script(self, filename: str) -> str | None:
        candidate_paths = (
            self.path.parent / filename,
            Path(__file__).resolve().parent / filename,
        )
        for path in candidate_paths:
            try:
                if path.exists():
                    return path.read_text(encoding="utf-8")
            except Exception:
                continue
        return None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute(f"PRAGMA busy_timeout={self._BUSY_TIMEOUT_MS};")
        return conn

    def write_transaction(
        self,
        action: Callable[[sqlite3.Connection], T],
    ) -> T | None:
        start = time.monotonic()
        delay = self._WRITE_BASE_DELAY
        attempts = 0
        while True:
            with self._write_lock:
                try:
                    with self._connect() as conn:
                        result = action(conn)
                        conn.commit()
                        return result
                except sqlite3.OperationalError as exc:
                    if "database is locked" not in str(exc).lower():
                        logger.error("crm_write_failed: %s", str(exc))
                        return None
                    attempts += 1
                    elapsed = time.monotonic() - start
                    if attempts > self._WRITE_RETRIES or elapsed + delay > self._WRITE_MAX_TOTAL_WAIT:
                        logger.error(
                            "crm_write_failed: database is locked (attempts=%s)",
                            attempts,
                        )
                        return None
            time.sleep(delay)
            delay *= 2

    @staticmethod
    def _hash_text(text: str) -> str:
        if not text:
            return ""
        return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

    def _ensure_optional_columns(self, conn: sqlite3.Connection) -> None:
        try:
            cur = conn.execute("PRAGMA table_info(emails);")
            columns = {row[1] for row in cur.fetchall()}

            migrations: list[str] = []
            if "priority_source" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN priority_source TEXT DEFAULT 'auto';")
            if "priority_reason" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN priority_reason TEXT;")
            if "original_priority" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN original_priority TEXT;")
            if "shadow_priority" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN shadow_priority TEXT;")
            if "shadow_priority_reason" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN shadow_priority_reason TEXT;")
            if "shadow_action_line" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN shadow_action_line TEXT;")
            if "shadow_action_reason" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN shadow_action_reason TEXT;")
            if "confidence_score" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN confidence_score REAL;")
            if "confidence_decision" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN confidence_decision TEXT;")
            if "proposed_action_type" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN proposed_action_type TEXT;")
            if "proposed_action_text" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN proposed_action_text TEXT;")
            if "proposed_action_confidence" not in columns:
                migrations.append(
                    "ALTER TABLE emails ADD COLUMN proposed_action_confidence REAL;"
                )
            if "llm_provider" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN llm_provider TEXT;")
            if "deferred_for_digest" not in columns:
                migrations.append(
                    "ALTER TABLE emails ADD COLUMN deferred_for_digest INTEGER DEFAULT 0;"
                )
            if "rfc_message_id" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN rfc_message_id TEXT;")
            if "in_reply_to" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN in_reply_to TEXT;")
            if "references" not in columns:
                migrations.append('ALTER TABLE emails ADD COLUMN "references" TEXT;')
            if "thread_key" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN thread_key TEXT;")

            for statement in migrations:
                conn.execute(statement)
            if migrations:
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("KnowledgeDB migration failed: %s", exc)

    def _ensure_auto_priority_gate_state_table(
        self, conn: sqlite3.Connection
    ) -> None:
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_priority_gate_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_disabled_at_utc REAL,
                    last_disabled_reason TEXT,
                    last_eval_at_utc REAL
                );
                """
            )
            conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("KnowledgeDB auto_priority_gate_state_failed: %s", exc)


    def _ensure_priority_feedback_index(self, conn: sqlite3.Connection) -> None:
        try:
            conn.execute(
                """
                DELETE FROM priority_feedback
                WHERE rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM priority_feedback
                    GROUP BY email_id, kind, value
                );
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_priority_feedback_email_kind_value
                    ON priority_feedback(email_id, kind, value);
                """
            )
            conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("KnowledgeDB priority_feedback_index_failed: %s", exc)

    def read_auto_priority_gate_state(self) -> AutoPriorityGateState:
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT last_disabled_at_utc, last_disabled_reason, last_eval_at_utc
                    FROM auto_priority_gate_state
                    WHERE id = 1
                    """
                )
                row = cur.fetchone()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("auto_priority_gate_state_read_failed: %s", exc)
            return AutoPriorityGateState(None, None, None)
        if not row:
            return AutoPriorityGateState(None, None, None)
        return AutoPriorityGateState(
            float(row[0]) if row[0] is not None else None,
            str(row[1]) if row[1] is not None else None,
            float(row[2]) if row[2] is not None else None,
        )

    def persist_auto_priority_gate_state(
        self,
        *,
        last_disabled_at_utc: float | None,
        last_disabled_reason: str | None,
        last_eval_at_utc: float | None,
    ) -> None:
        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO auto_priority_gate_state (
                    id,
                    last_disabled_at_utc,
                    last_disabled_reason,
                    last_eval_at_utc
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    last_disabled_at_utc=excluded.last_disabled_at_utc,
                    last_disabled_reason=excluded.last_disabled_reason,
                    last_eval_at_utc=excluded.last_eval_at_utc;
                """,
                (last_disabled_at_utc, last_disabled_reason, last_eval_at_utc),
            )
        self.write_transaction(_action)


    def save_email(
        self,
        *,
        account_email: str,
        from_email: str,
        subject: str,
        received_at: str,
        priority: str,
        priority_source: str = "auto",
        original_priority: str | None = None,
        priority_reason: str | None = None,
        shadow_priority: str | None = None,
        shadow_priority_reason: str | None = None,
        shadow_action_line: str | None = None,
        shadow_action_reason: str | None = None,
        confidence_score: float | None = None,
        confidence_decision: str | None = None,
        proposed_action_type: str | None = None,
        proposed_action_text: str | None = None,
        proposed_action_confidence: float | None = None,
        llm_provider: str | None = None,
        deferred_for_digest: bool = False,
        action_line: str,
        body_summary: str,
        raw_body: str,
        rfc_message_id: str | None = None,
        in_reply_to: str | None = None,
        references: str | None = None,
        thread_key: str | None = None,
        attachment_summaries: Iterable[tuple[str, str]],
    ) -> int | None:
        try:
            def _action(conn: sqlite3.Connection) -> int | None:
                cur = conn.cursor()

                raw_body_hash = self._hash_text(raw_body)

                cur.execute(
                    """
                    INSERT INTO emails (
                        account_email,
                        from_email,
                        subject,
                        received_at,
                        priority,
                        priority_source,
                        original_priority,
                        priority_reason,
                        shadow_priority,
                        shadow_priority_reason,
                        shadow_action_line,
                        shadow_action_reason,
                        confidence_score,
                        confidence_decision,
                        proposed_action_type,
                        proposed_action_text,
                        proposed_action_confidence,
                        llm_provider,
                        deferred_for_digest,
                        action_line,
                        body_summary,
                        raw_body_hash,
                        rfc_message_id,
                        in_reply_to,
                        "references",
                        thread_key
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_email,
                        from_email,
                        subject,
                        received_at,
                        priority,
                        priority_source,
                        original_priority,
                        priority_reason,
                        shadow_priority,
                        shadow_priority_reason,
                        shadow_action_line,
                        shadow_action_reason,
                        confidence_score,
                        confidence_decision,
                        proposed_action_type,
                        proposed_action_text,
                        proposed_action_confidence,
                        llm_provider,
                        1 if deferred_for_digest else 0,
                        action_line,
                        body_summary,
                        raw_body_hash,
                        rfc_message_id,
                        in_reply_to,
                        references,
                        thread_key,
                    ),
                )

                email_id = cur.lastrowid

                for filename, summary in attachment_summaries:
                    cur.execute(
                        """
                        INSERT INTO attachments (
                            email_id,
                            filename,
                            summary
                        )
                        VALUES (?, ?, ?)
                        """,
                        (email_id, filename, summary),
                    )
                return int(email_id)

            return self.write_transaction(_action)

        except Exception as exc:
            logger.error("KnowledgeDB save failed: %s", exc)
            return None

    def save_commitments(
        self,
        *,
        email_row_id: int,
        commitments: Iterable[Commitment],
    ) -> bool:
        try:
            def _action(conn: sqlite3.Connection) -> bool:
                conn.executemany(
                    """
                    INSERT INTO commitments (
                        email_row_id,
                        source,
                        commitment_text,
                        deadline_iso,
                        status,
                        confidence
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            email_row_id,
                            commitment.source,
                            commitment.commitment_text,
                            commitment.deadline_iso,
                            commitment.status,
                            commitment.confidence,
                        )
                        for commitment in commitments
                    ],
                )
                return True

            return bool(self.write_transaction(_action))
        except Exception as exc:
            logger.error("KnowledgeDB commitments save failed: %s", exc)
            return False

    def get_last_digest_sent_at(self, *, account_email: str) -> datetime | None:
        if not account_email:
            return None
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT last_digest_sent_at
                    FROM digest_state
                    WHERE account_email = ?
                    """,
                    (account_email,),
                ).fetchone()
            if not row or not row[0]:
                return None
            return parse_sqlite_datetime(str(row[0]))
        except Exception as exc:
            logger.error("KnowledgeDB digest state read failed: %s", exc)
            return None

    def get_last_weekly_digest_key(self, *, account_email: str) -> str | None:
        if not account_email:
            return None
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT last_week_key
                    FROM weekly_digest_state
                    WHERE account_email = ?
                    """,
                    (account_email,),
                ).fetchone()
            if not row or not row[0]:
                return None
            return str(row[0])
        except Exception as exc:
            logger.error("KnowledgeDB weekly digest state read failed: %s", exc)
            return None

    def get_last_weekly_digest_sent_at(self, *, account_email: str) -> datetime | None:
        if not account_email:
            return None
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT last_sent_at
                    FROM weekly_digest_state
                    WHERE account_email = ?
                    """,
                    (account_email,),
                ).fetchone()
            if not row or not row[0]:
                return None
            return parse_sqlite_datetime(str(row[0]))
        except Exception as exc:
            logger.error("KnowledgeDB weekly digest sent read failed: %s", exc)
            return None

    def set_last_digest_sent_at(
        self,
        *,
        account_email: str,
        sent_at: datetime,
    ) -> bool:
        if not account_email:
            return False
        try:
            def _action(conn: sqlite3.Connection) -> bool:
                conn.execute(
                    """
                    INSERT INTO digest_state (account_email, last_digest_sent_at)
                    VALUES (?, ?)
                    ON CONFLICT(account_email) DO UPDATE SET last_digest_sent_at = excluded.last_digest_sent_at
                    """,
                    (account_email, sent_at.isoformat()),
                )
                return True

            return bool(self.write_transaction(_action))
        except Exception as exc:
            logger.error("KnowledgeDB digest state write failed: %s", exc)
            return False

    def set_last_weekly_digest_state(
        self,
        *,
        account_email: str,
        week_key: str,
        sent_at: datetime,
    ) -> bool:
        if not account_email:
            return False
        try:
            def _action(conn: sqlite3.Connection) -> bool:
                conn.execute(
                    """
                    INSERT INTO weekly_digest_state (account_email, last_week_key, last_sent_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(account_email) DO UPDATE SET
                        last_week_key = excluded.last_week_key,
                        last_sent_at = excluded.last_sent_at
                    """,
                    (account_email, week_key, sent_at.isoformat()),
                )
                return True

            return bool(self.write_transaction(_action))
        except Exception as exc:
            logger.error("KnowledgeDB weekly digest state write failed: %s", exc)
            return False

    def fetch_pending_commitments_by_sender(
        self,
        *,
        from_email: str,
    ) -> list[CommitmentRecord]:
        if not from_email:
            return []
        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    """
                    SELECT
                        c.id,
                        c.commitment_text,
                        c.deadline_iso,
                        c.status,
                        c.created_at
                    FROM commitments c
                    JOIN emails e ON e.id = c.email_row_id
                    WHERE lower(e.from_email) = lower(?)
                      AND c.status = 'pending'
                    ORDER BY c.created_at ASC
                    """,
                    (from_email,),
                )
                rows = cur.fetchall()
        except Exception as exc:
            logger.error("KnowledgeDB commitments fetch failed: %s", exc)
            return []

        commitments: list[CommitmentRecord] = []
        for row in rows:
            created_at = parse_sqlite_datetime(row["created_at"])
            if created_at is None:
                continue
            commitments.append(
                CommitmentRecord(
                    commitment_id=int(row["id"]),
                    commitment_text=str(row["commitment_text"]),
                    deadline_iso=row["deadline_iso"],
                    status=str(row["status"]),
                    created_at=created_at,
                )
            )
        return commitments

    def update_commitment_statuses(
        self,
        *,
        updates: Iterable[CommitmentStatusUpdate],
    ) -> bool:
        update_list = list(updates)
        if not update_list:
            return True
        try:
            def _action(conn: sqlite3.Connection) -> bool:
                conn.executemany(
                    """
                    UPDATE commitments
                    SET status = ?
                    WHERE id = ?
                    """,
                    [
                        (update.new_status, update.commitment_id)
                        for update in update_list
                    ],
                )
                return True

            return bool(self.write_transaction(_action))
        except Exception as exc:
            logger.error("KnowledgeDB commitments update failed: %s", exc)
            return False

    def upsert_entity_signal(
        self,
        *,
        entity_id: str,
        signal_type: str,
        score: int,
        label: str,
        computed_at: str,
        sample_size: int,
    ) -> str | None:
        try:
            previous_label: str | None = None

            def _action(conn: sqlite3.Connection) -> str | None:
                nonlocal previous_label
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    """
                    SELECT label
                    FROM entity_signals
                    WHERE entity_id = ? AND signal_type = ?
                    """,
                    (entity_id, signal_type),
                ).fetchone()
                previous_label = (
                    str(row["label"]) if row and row["label"] is not None else None
                )
                conn.execute(
                    """
                    INSERT INTO entity_signals (
                        entity_id,
                        signal_type,
                        score,
                        label,
                        computed_at,
                        sample_size
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(entity_id, signal_type) DO UPDATE SET
                        score = excluded.score,
                        label = excluded.label,
                        computed_at = excluded.computed_at,
                        sample_size = excluded.sample_size
                    """,
                    (
                        entity_id,
                        signal_type,
                        score,
                        label,
                        computed_at,
                        sample_size,
                    ),
                )
                return previous_label

            return self.write_transaction(_action)
        except Exception as exc:
            logger.error("KnowledgeDB entity signal upsert failed: %s", exc)
            return None

    def save_preview_action(
        self,
        *,
        email_id: int,
        proposed_action: dict | None,
        confidence: float | None,
    ) -> None:
        if proposed_action is None:
            return
        try:
            payload = json.dumps(proposed_action, ensure_ascii=False)
        except (TypeError, ValueError):
            payload = str(proposed_action)

        try:
            def _action(conn: sqlite3.Connection) -> None:
                conn.execute(
                    """
                    INSERT INTO preview_actions (
                        email_id,
                        proposed_action,
                        confidence
                    )
                    VALUES (?, ?, ?)
                    """,
                    (email_id, payload, confidence),
                )

            self.write_transaction(_action)
        except Exception as exc:
            logger.error("KnowledgeDB preview action save failed: %s", exc)

    def save_action_feedback(
        self,
        *,
        email_id: str,
        proposed_action: dict | None,
        decision: str,
        user_note: str | None = None,
    ) -> str:
        feedback_id = uuid.uuid4().hex
        payload = ""
        if proposed_action is not None:
            try:
                payload = json.dumps(proposed_action, ensure_ascii=False)
            except (TypeError, ValueError):
                payload = str(proposed_action)

        try:
            def _action(conn: sqlite3.Connection) -> None:
                conn.execute(
                    """
                    INSERT INTO action_feedback (
                        id,
                        email_id,
                        proposed_action,
                        decision,
                        user_note
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (feedback_id, email_id, payload, decision, user_note),
                )

            self.write_transaction(_action)
        except Exception as exc:
            logger.error("KnowledgeDB feedback save failed: %s", exc)
        return feedback_id

    def save_priority_feedback(
        self,
        *,
        email_id: int | str,
        kind: str,
        value: str,
        entity_id: str | None = None,
        sender_email: str | None = None,
        account_email: str | None = None,
    ) -> tuple[str, bool]:
        feedback_id = uuid.uuid4().hex
        email_value = str(email_id)

        try:
            def _action(conn: sqlite3.Connection) -> tuple[str, bool]:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO priority_feedback (
                        id,
                        email_id,
                        kind,
                        value,
                        entity_id,
                        sender_email,
                        account_email
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        feedback_id,
                        email_value,
                        kind,
                        value,
                        entity_id,
                        sender_email,
                        account_email,
                    ),
                )
                inserted = conn.total_changes > 0
                if not inserted:
                    row = conn.execute(
                        """
                        SELECT id
                        FROM priority_feedback
                        WHERE email_id = ? AND kind = ? AND value = ?
                        ORDER BY datetime(created_at) ASC, id ASC
                        LIMIT 1
                        """,
                        (email_value, kind, value),
                    ).fetchone()
                    existing_id = row[0] if row else feedback_id
                    return existing_id, False
                return feedback_id, True

            result = self.write_transaction(_action)
            if result is None:
                return feedback_id, False
            return result
        except Exception as exc:
            logger.error("KnowledgeDB priority feedback save failed: %s", exc)
            return feedback_id, False


