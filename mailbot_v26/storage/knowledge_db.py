from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterable

from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.insights.commitment_lifecycle import (
    CommitmentRecord,
    CommitmentStatusUpdate,
    parse_sqlite_datetime,
)

logger = logging.getLogger(__name__)


class KnowledgeDB:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        schema_sql = self._read_sql_script("schema.sql")
        views_sql = self._read_sql_script("views.sql")
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                if schema_sql:
                    conn.executescript(schema_sql)
                self._ensure_optional_columns(conn)
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

            for statement in migrations:
                conn.execute(statement)
            if migrations:
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("KnowledgeDB migration failed: %s", exc)

    def save_email(
        self,
        *,
        account_email: str,
        from_email: str,
        subject: str,
        received_at: str,
        priority: str,
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
        attachment_summaries: Iterable[tuple[str, str]],
    ) -> int | None:
        try:
            with sqlite3.connect(self.path) as conn:
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
                        raw_body_hash
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_email,
                        from_email,
                        subject,
                        received_at,
                        priority,
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

                conn.commit()
                return int(email_id)

        except Exception as exc:
            logger.error("KnowledgeDB save failed: %s", exc)
            return None

    def mark_deferred_for_digest(
        self,
        *,
        email_row_id: int,
        deferred: bool = True,
    ) -> bool:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    UPDATE emails
                    SET deferred_for_digest = ?
                    WHERE id = ?
                    """,
                    (1 if deferred else 0, email_row_id),
                )
                conn.commit()
            return True
        except Exception as exc:
            logger.error("KnowledgeDB deferred update failed: %s", exc)
            return False

    def save_commitments(
        self,
        *,
        email_row_id: int,
        commitments: Iterable[Commitment],
    ) -> bool:
        try:
            with sqlite3.connect(self.path) as conn:
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
                conn.commit()
            return True
        except Exception as exc:
            logger.error("KnowledgeDB commitments save failed: %s", exc)
            return False

    def get_last_digest_sent_at(self, *, account_email: str) -> datetime | None:
        if not account_email:
            return None
        try:
            with sqlite3.connect(self.path) as conn:
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

    def set_last_digest_sent_at(
        self,
        *,
        account_email: str,
        sent_at: datetime,
    ) -> bool:
        if not account_email:
            return False
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO digest_state (account_email, last_digest_sent_at)
                    VALUES (?, ?)
                    ON CONFLICT(account_email) DO UPDATE SET last_digest_sent_at = excluded.last_digest_sent_at
                    """,
                    (account_email, sent_at.isoformat()),
                )
                conn.commit()
            return True
        except Exception as exc:
            logger.error("KnowledgeDB digest state write failed: %s", exc)
            return False

    def fetch_pending_commitments_by_sender(
        self,
        *,
        from_email: str,
    ) -> list[CommitmentRecord]:
        if not from_email:
            return []
        try:
            with sqlite3.connect(self.path) as conn:
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
            with sqlite3.connect(self.path) as conn:
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
                conn.commit()
            return True
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
            with sqlite3.connect(self.path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    """
                    SELECT label
                    FROM entity_signals
                    WHERE entity_id = ? AND signal_type = ?
                    """,
                    (entity_id, signal_type),
                ).fetchone()
                previous_label = str(row["label"]) if row and row["label"] is not None else None
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
                conn.commit()
                return previous_label
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
            with sqlite3.connect(self.path) as conn:
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
                conn.commit()
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
            with sqlite3.connect(self.path) as conn:
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
                conn.commit()
        except Exception as exc:
            logger.error("KnowledgeDB feedback save failed: %s", exc)
        return feedback_id
