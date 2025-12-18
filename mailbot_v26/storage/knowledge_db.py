from __future__ import annotations

import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import Iterable

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
        action_line: str,
        body_summary: str,
        raw_body: str,
        attachment_summaries: Iterable[tuple[str, str]],
    ) -> None:
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
                        action_line,
                        body_summary,
                        raw_body_hash
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_email,
                        from_email,
                        subject,
                        received_at,
                        priority,
                        original_priority,
                        priority_reason,
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

        except Exception as exc:
            logger.error("KnowledgeDB save failed: %s", exc)
