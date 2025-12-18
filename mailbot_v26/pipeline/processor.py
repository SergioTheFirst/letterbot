# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from intelligence.priority_engine import PriorityEngine
from priority.shadow_engine import ShadowPriorityEngine
from pipeline.stage_llm import run_llm_stage
from pipeline.stage_telegram import send_to_telegram
from storage.analytics import KnowledgeAnalytics
from storage.knowledge_db import KnowledgeDB
from tasks.shadow_actions import ShadowActionEngine

logger = logging.getLogger(__name__)

# === Инициализация write-only БД ===
DB_PATH = Path("database.sqlite")
knowledge_db = KnowledgeDB(DB_PATH)
priority_engine = PriorityEngine(DB_PATH)
analytics = KnowledgeAnalytics(DB_PATH)
shadow_priority_engine = ShadowPriorityEngine(analytics)
shadow_action_engine = ShadowActionEngine(analytics)


def process_message(
    *,
    account_email: str,
    message_id: int,
    from_email: str,
    subject: str,
    received_at: datetime,
    body_text: str,
    attachments: list[dict[str, Any]],
    telegram_chat_id: str,
) -> None:
    """
    Главный pipeline:
    PARSE → LLM → (SAVE TO DB) → TELEGRAM

    ⚠️ Поведение Telegram и LLM НЕ МЕНЯЕМ
    """

    # ---------- Stage LLM ----------
    llm_result = run_llm_stage(
        subject=subject,
        from_email=from_email,
        body_text=body_text,
        attachments=attachments,
    )

    if not llm_result:
        logger.warning("LLM returned empty result, skipping message_id=%s", message_id)
        return

    priority = llm_result.priority
    action_line = llm_result.action_line
    body_summary = llm_result.body_summary
    attachment_summaries = llm_result.attachment_summaries

    # ---------- Shadow Priority (read-only, dry run) ----------
    shadow_priority, shadow_reason = shadow_priority_engine.compute(
        llm_priority=priority,
        from_email=from_email,
    )
    if shadow_priority != priority:
        logger.info(
            "[SHADOW-PRIORITY] from=%s current=%s shadow=%s reason=%s",
            from_email or "",
            priority,
            shadow_priority,
            shadow_reason or "",
        )

    # ---------- Shadow Action (read-only, dry run) ----------
    shadow_tasks = shadow_action_engine.compute(
        account_email=account_email,
        from_email=from_email,
    )
    for task, reason in shadow_tasks:
        logger.info(
            "[SHADOW-ACTION] from=%s task=%s reason=%s",
            from_email or "",
            task or "",
            reason or "",
        )

    # ---------- Stage 1.3: PASSIVE PRIORITY ADJUSTMENT ----------
    priority, priority_reason = priority_engine.adjust_priority(
        llm_priority=priority,
        from_email=from_email,
        received_at=received_at,
    )

    # ---------- Stage 1.2: WRITE-ONLY CRM ----------
    try:
        knowledge_db.save_email(
            account_email=account_email,
            from_email=from_email,
            subject=subject,
            received_at=received_at.isoformat(),
            priority=priority,
            priority_reason=priority_reason,
            action_line=action_line,
            body_summary=body_summary,
            raw_body=body_text,
            attachment_summaries=[
                (a["filename"], a["summary"])
                for a in attachment_summaries
            ],
        )

    except Exception as exc:
        # ❗ БД — side-effect only
        logger.error("KnowledgeDB failed: %s", exc, exc_info=True)

    # ---------- Stage Telegram ----------
    send_to_telegram(
        chat_id=telegram_chat_id,
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=action_line,
        body_summary=body_summary,
        attachment_summaries=attachment_summaries,
        account_email=account_email,
    )
