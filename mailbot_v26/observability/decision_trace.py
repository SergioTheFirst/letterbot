from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(slots=True)
class DecisionTraceWriter:
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
                    CREATE TABLE IF NOT EXISTS decision_traces (
                        id TEXT PRIMARY KEY,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        email_id TEXT,
                        account_email TEXT,
                        signal_entropy REAL,
                        signal_printable_ratio REAL,
                        signal_quality_score REAL,
                        signal_fallback_used BOOLEAN,
                        llm_provider TEXT,
                        llm_model TEXT,
                        prompt_full TEXT,
                        response_full TEXT,
                        priority TEXT,
                        action_line TEXT,
                        confidence REAL,
                        shadow_priority TEXT,
                        compressed BOOLEAN DEFAULT FALSE
                    );
                    """
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("decision_trace_init_failed", error=str(exc))

    def write(
        self,
        *,
        email_id: str,
        account_email: str,
        signal_entropy: float,
        signal_printable_ratio: float,
        signal_quality_score: float,
        signal_fallback_used: bool,
        prompt_full: str,
        llm_provider: str,
        llm_model: str,
        response_full: str,
        confidence: float | None,
        priority: str,
        action_line: str,
        shadow_priority: str | None,
        compressed: bool = False,
    ) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO decision_traces (
                        id,
                        email_id,
                        account_email,
                        signal_entropy,
                        signal_printable_ratio,
                        signal_quality_score,
                        signal_fallback_used,
                        llm_provider,
                        llm_model,
                        prompt_full,
                        response_full,
                        priority,
                        action_line,
                        confidence,
                        shadow_priority,
                        compressed
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        email_id,
                        account_email,
                        signal_entropy,
                        signal_printable_ratio,
                        signal_quality_score,
                        signal_fallback_used,
                        llm_provider,
                        llm_model,
                        prompt_full,
                        response_full,
                        priority,
                        action_line,
                        confidence,
                        shadow_priority,
                        compressed,
                    ),
                )
                conn.commit()
        except Exception:
            raise
