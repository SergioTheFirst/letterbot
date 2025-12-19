# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from features import FeatureFlags
from actions.auto_action_engine import AutoActionEngine
from priority.confidence_engine import PriorityConfidenceEngine
from priority.shadow_engine import ShadowPriorityEngine
from .stage_llm import run_llm_stage
from .stage_telegram import send_to_telegram
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
priority_confidence_engine = PriorityConfidenceEngine()
feature_flags = FeatureFlags()
auto_action_engine = AutoActionEngine(
    confidence_threshold=feature_flags.AUTO_ACTION_CONFIDENCE_THRESHOLD
)


def _is_shadow_higher(shadow_priority: str, llm_priority: str) -> bool:
    priority_order = {"🔵": 0, "🟡": 1, "🔴": 2}
    return priority_order.get(shadow_priority, 0) > priority_order.get(llm_priority, 0)


def _lookup_sender_stats(from_email: str) -> dict[str, object]:
    normalized = (from_email or "").strip().lower()
    if not normalized:
        return {}

    try:
        for row in analytics.sender_stats():
            sender = str(row.get("sender_email") or "").strip().lower()
            if sender == normalized:
                return row
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("Confidence lookup failed: %s", exc, exc_info=True)
    return {}


def _recent_history(from_email: str) -> dict[str, object]:
    normalized = (from_email or "").strip().lower()
    if not normalized:
        return {}

    try:
        records = [
            row
            for row in analytics.priority_escalations(limit=50)
            if str(row.get("from_email") or "").strip().lower() == normalized
        ]
        if not records:
            return {}

        escalations = len(records)
        is_trending_up = escalations >= 2
        return {"escalations": escalations, "is_trending_up": is_trending_up}
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("Confidence history failed: %s", exc, exc_info=True)
        return {}


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

    # ---------- Stage 1.4: AUTO PRIORITY (feature-flagged) ----------
    confidence_score: float | None = None
    confidence_decision: str | None = None
    llm_priority_for_confidence = priority
    if feature_flags.ENABLE_AUTO_PRIORITY and _is_shadow_higher(shadow_priority, priority):
        confidence_score = priority_confidence_engine.score(
            llm_priority=priority,
            shadow_priority=shadow_priority,
            sender_stats=_lookup_sender_stats(from_email),
            recent_history=_recent_history(from_email),
        )
        threshold = feature_flags.AUTO_PRIORITY_CONFIDENCE_THRESHOLD

        if confidence_score >= threshold:
            original_priority = priority
            priority = shadow_priority
            priority_reason = shadow_reason or "Auto-priority escalation"
            confidence_decision = "APPLIED"
            logger.info(
                "[AUTO-PRIORITY] from=%s llm=%s shadow=%s reason=%s",
                from_email or "",
                original_priority,
                shadow_priority,
                shadow_reason or "",
            )
        else:
            confidence_decision = "SKIPPED"

        logger.info(
            "[AUTO-PRIORITY-CONFIDENCE]\nllm=%s shadow=%s confidence=%.2f threshold=%.1f → %s",
            llm_priority_for_confidence,
            shadow_priority,
            confidence_score,
            threshold,
            confidence_decision or "SKIPPED",
        )

    shadow_priority_to_persist: str | None = None
    shadow_priority_reason_to_persist: str | None = None
    shadow_action_line_to_persist: str | None = None
    shadow_action_reason_to_persist: str | None = None
    confidence_score_to_persist: float | None = None
    confidence_decision_to_persist: str | None = None
    proposed_action_type_to_persist: str | None = None
    proposed_action_text_to_persist: str | None = None
    proposed_action_confidence_to_persist: float | None = None

    proposed_action: dict | None = None
    if feature_flags.ENABLE_AUTO_ACTIONS:
        proposed_action = auto_action_engine.propose(
            llm_action_line=action_line,
            shadow_action=shadow_action_line,
            priority=priority,
            confidence=confidence_score or 0.0,
        )

        if proposed_action:
            logger.info(
                "[AUTO-ACTION]\nproposed=%s confidence=%.2f source=%s → STORED",
                proposed_action.get("type", ""),
                proposed_action.get("confidence", 0.0),
                proposed_action.get("source", ""),
            )
        else:
            logger.info("[AUTO-ACTION]\nconditions not met → SKIPPED")

    if feature_flags.ENABLE_PREVIEW_ACTIONS and proposed_action:
        preview_reasons = [
            reason
            for reason in (shadow_action_reason, shadow_reason, priority_reason)
            if reason
        ]
        preview = {
            "email_id": message_id,
            "original_priority": original_priority or llm_result.priority,
            "final_priority": priority,
            "proposed_actions": [proposed_action],
            "confidence": proposed_action.get("confidence", 0.0),
            "reasons": preview_reasons,
        }
        logger.info("[PREVIEW] %s", preview)
        try:
            knowledge_db.save_preview_action(
                email_id=message_id,
                proposed_action=proposed_action,
                confidence=proposed_action.get("confidence"),
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Preview action persistence failed: %s", exc, exc_info=True)

    if feature_flags.ENABLE_SHADOW_PERSISTENCE:
        shadow_priority_to_persist = shadow_priority
        shadow_priority_reason_to_persist = shadow_reason
        shadow_action_line_to_persist = shadow_action_line
        shadow_action_reason_to_persist = shadow_action_reason
        confidence_score_to_persist = confidence_score
        confidence_decision_to_persist = confidence_decision
        proposed_action_type_to_persist = (
            proposed_action.get("type") if proposed_action else None
        )
        proposed_action_text_to_persist = (
            proposed_action.get("text") if proposed_action else None
        )
        proposed_action_confidence_to_persist = (
            proposed_action.get("confidence") if proposed_action else None
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
            confidence_score=confidence_score_to_persist,
            confidence_decision=confidence_decision_to_persist,
            proposed_action_type=proposed_action_type_to_persist,
            proposed_action_text=proposed_action_text_to_persist,
            proposed_action_confidence=proposed_action_confidence_to_persist,
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
