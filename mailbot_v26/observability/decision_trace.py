from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


def _to_json(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(str(value), ensure_ascii=False)


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
                        created_at TEXT,
                        email_id TEXT,
                        account_email TEXT,
                        prompt_full TEXT,
                        prompt_vars TEXT,
                        crm_context TEXT,
                        llm_provider TEXT,
                        llm_model TEXT,
                        llm_request TEXT,
                        llm_response TEXT,
                        llm_latency_ms INTEGER,
                        decision_json TEXT,
                        confidence REAL,
                        reason_code TEXT,
                        reason_text TEXT,
                        shadow_decision TEXT,
                        diff_json TEXT
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
        prompt_full: str,
        prompt_vars: dict,
        crm_context: dict | None,
        llm_provider: str,
        llm_model: str,
        llm_request: str,
        llm_response: str,
        llm_latency_ms: int,
        decision: dict,
        confidence: float | None,
        reason_code: str | None,
        reason_text: str | None,
        shadow_decision: dict | None,
        diff: dict | None,
    ) -> None:
        try:
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    """
                    INSERT INTO decision_traces (
                        id,
                        created_at,
                        email_id,
                        account_email,
                        prompt_full,
                        prompt_vars,
                        crm_context,
                        llm_provider,
                        llm_model,
                        llm_request,
                        llm_response,
                        llm_latency_ms,
                        decision_json,
                        confidence,
                        reason_code,
                        reason_text,
                        shadow_decision,
                        diff_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        datetime.utcnow().isoformat(),
                        email_id,
                        account_email,
                        prompt_full,
                        _to_json(prompt_vars),
                        _to_json(crm_context),
                        llm_provider,
                        llm_model,
                        llm_request,
                        llm_response,
                        llm_latency_ms,
                        _to_json(decision),
                        confidence,
                        reason_code,
                        reason_text,
                        _to_json(shadow_decision),
                        _to_json(diff),
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.error("decision_trace_write_failed", error=str(exc))
