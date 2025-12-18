# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from features import FeatureFlags
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
analytics = KnowledgeAnalytics(DB_PATH)
shadow_priority_engine = ShadowPriorityEngine(analytics)
shadow_action_engine = ShadowActionEngine(analytics)
feature_flags = FeatureFlags()


def _is_shadow_higher(shadow_priority: str, llm_priority: str) -> bool:
    priority_order = {"🔵": 0, "🟡": 1, "🔴": 2}
    return priority_order.get(shadow_priority, 0) > priority_order.get(llm_priority, 0)


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
    original_priority: str | None = None
    priority_reason: str | None = None
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
    shadow_action_line: str | None = None
    shadow_action_reason: str | None = None
    if shadow_tasks:
        shadow_action_line, shadow_action_reason = shadow_tasks[0]
    for task, reason in shadow_tasks:
        logger.info(
            "[SHADOW-ACTION] from=%s task=%s reason=%s",
            from_email or "",
            task or "",
            reason or "",
        )

    shadow_priority_to_persist: str | None = None
    shadow_priority_reason_to_persist: str | None = None
    shadow_action_line_to_persist: str | None = None
    shadow_action_reason_to_persist: str | None = None

    if feature_flags.ENABLE_SHADOW_PERSISTENCE:
        shadow_priority_to_persist = shadow_priority
        shadow_priority_reason_to_persist = shadow_reason
        shadow_action_line_to_persist = shadow_action_line
        shadow_action_reason_to_persist = shadow_action_reason

    # ---------- Stage 1.4: AUTO PRIORITY (feature-flagged) ----------
    if feature_flags.ENABLE_AUTO_PRIORITY and _is_shadow_higher(shadow_priority, priority):
        original_priority = priority
        priority = shadow_priority
        priority_reason = shadow_reason or "Auto-priority escalation"
        logger.info(
            "[AUTO-PRIORITY] from=%s llm=%s shadow=%s reason=%s",
            from_email or "",
            original_priority,
            shadow_priority,
            shadow_reason or "",
        )

    # ---------- Stage 1.2: WRITE-ONLY CRM ----------
    try:
        knowledge_db.save_email(
            account_email=account_email,
            from_email=from_email,
            subject=subject,
            received_at=received_at.isoformat(),
            priority=priority,
            original_priority=original_priority,
            priority_reason=priority_reason,
            shadow_priority=shadow_priority_to_persist,
            shadow_priority_reason=shadow_priority_reason_to_persist,
            shadow_action_line=shadow_action_line_to_persist,
            shadow_action_reason=shadow_action_reason_to_persist,
            action_line=action_line,
            body_summary=body_summary,
            raw_body=body_text,
            attachment_summaries=[
                (a["filename"], a["summary"])
                for a in attachment_summaries
            ],
        )
        if feature_flags.ENABLE_SHADOW_PERSISTENCE:
            logger.info(
                "[SHADOW-PERSIST] saved shadow fields for uid=%s account=%s",
                message_id,
                account_email,
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
