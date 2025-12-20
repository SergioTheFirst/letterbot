# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import time
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mailbot_v26.actions.auto_action_engine import AutoActionEngine
from mailbot_v26.features import FeatureFlags
from mailbot_v26.observability import get_logger
from mailbot_v26.priority.confidence_engine import PriorityConfidenceEngine
from mailbot_v26.priority.auto_gates import AutoPriorityCircuitBreaker, AutoPriorityGates
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.priority.shadow_engine import ShadowPriorityEngine
from .stage_llm import run_llm_stage
from .stage_telegram import send_preview_to_telegram, send_to_telegram
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tasks.shadow_actions import ShadowActionEngine

logger = get_logger("mailbot")

# === Инициализация write-only БД ===
DB_PATH = Path("database.sqlite")
knowledge_db = KnowledgeDB(DB_PATH)
analytics = KnowledgeAnalytics(DB_PATH)
shadow_priority_engine = ShadowPriorityEngine(analytics)
shadow_action_engine = ShadowActionEngine(analytics)
priority_confidence_engine = PriorityConfidenceEngine()
auto_priority_gates = AutoPriorityGates(analytics)
auto_priority_breaker = AutoPriorityCircuitBreaker(analytics)
feature_flags = FeatureFlags()
runtime_flag_store = RuntimeFlagStore()
auto_action_engine = AutoActionEngine(
    confidence_threshold=feature_flags.AUTO_ACTION_CONFIDENCE_THRESHOLD
)


@dataclass
class Attachment:
    filename: str
    content: bytes = b""
    content_type: str = ""
    text: str = ""


@dataclass
class AttachmentSummary:
    filename: str
    description: str
    kind: str = ""
    priority: int = 0
    text_length: int = 0


@dataclass
class InboundMessage:
    subject: str
    body: str
    sender: str = ""
    attachments: list[Attachment] = field(default_factory=list)
    received_at: datetime | None = None


class MessageProcessor:
    _ATTACHMENT_SNIPPET_LIMIT = 120
    _MAX_ATTACHMENTS = 12
    _VERB_ORDER = ["Сделать", "Ответить", "Проверить", "Уточнить"]

    def __init__(self, config: Any, state: Any) -> None:
        self.config = config
        self.state = state

    def process(self, account_login: str, message: InboundMessage) -> str:
        """Lightweight placeholder processor to keep imports stable."""
        sender = message.sender or "неизвестно"
        subject = message.subject or "(без темы)"
        summary = (message.body or "").strip()
        summary = summary.split("\n")[0] if summary else ""
        summary = self._trim_attachment_snippet(summary) if summary else ""
        lines = [
            f"🔵 от {sender}: {subject}",
            f"<b>{subject}</b>",
        ]
        if summary:
            lines.append(f"<i>{summary}</i>")
        lines.append(f"{self._VERB_ORDER[0]}: проверить письмо")
        return "\n".join(lines)

    @classmethod
    def _trim_attachment_snippet(cls, text: str) -> str:
        if len(text) <= cls._ATTACHMENT_SNIPPET_LIMIT:
            return text
        return f"{text[: cls._ATTACHMENT_SNIPPET_LIMIT - 1]}…"

    def _render_attachments(self, attachments: list[AttachmentSummary]) -> list[str]:
        rendered: list[str] = []
        for attachment in attachments[: self._MAX_ATTACHMENTS]:
            description = self._trim_attachment_snippet(attachment.description or "")
            if description:
                rendered.append(f"{attachment.filename} — {description}")
            else:
                rendered.append(attachment.filename)
        return rendered


__all__ = ["Attachment", "AttachmentSummary", "InboundMessage", "MessageProcessor"]


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
        logger.error("confidence_lookup_failed", error=str(exc))
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
        logger.error("confidence_history_failed", error=str(exc))
        return {}


def _sanitize_preview_line(text: str) -> str:
    cleaned = (
        text.replace("<", "")
        .replace(">", "")
        .replace("*", "")
        .replace("_", "")
        .replace("\n", " ")
        .replace("\r", " ")
    )
    return " ".join(cleaned.split())


def _extract_preview_actions(proposed_action: dict | list | None) -> list[str]:
    if not proposed_action:
        return []
    actions: list[str] = []
    if isinstance(proposed_action, dict):
        candidate = proposed_action.get("text")
        if candidate:
            actions.append(str(candidate))
    elif isinstance(proposed_action, list):
        for entry in proposed_action:
            if isinstance(entry, dict):
                candidate = entry.get("text")
            else:
                candidate = entry
            if candidate:
                actions.append(str(candidate))
    else:
        actions.append(str(proposed_action))
    return [_sanitize_preview_line(action) for action in actions if action]


def _build_preview_message(actions: list[str]) -> str:
    lines = ["PREVIEW ACTIONS (не применено):"]
    lines.extend(f"- {action}" for action in actions)
    lines.append("Источник: AutoActionEngine, режим preview")
    return "\n".join(lines)


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
    logger.info(
        "email_received",
        email_id=message_id,
        account=account_email,
        from_email=from_email,
        subject=subject,
        received_at=received_at.isoformat(),
    )
    llm_start = time.perf_counter()
    try:
        llm_result = run_llm_stage(
            subject=subject,
            from_email=from_email,
            body_text=body_text,
            attachments=attachments,
        )
    except Exception as exc:
        logger.error(
            "processing_error",
            stage="llm",
            email_id=message_id,
            error=str(exc),
        )
        raise
    llm_latency_ms = int((time.perf_counter() - llm_start) * 1000)

    if not llm_result:
        logger.warning("llm_empty_result", email_id=message_id)
        return

    priority = llm_result.priority
    original_priority: str | None = None
    priority_reason: str | None = None
    action_line = llm_result.action_line
    body_summary = llm_result.body_summary
    attachment_summaries = llm_result.attachment_summaries
    llm_provider: str | None = None
    if hasattr(llm_result, "llm_provider"):
        llm_provider = getattr(llm_result, "llm_provider")
    elif isinstance(llm_result, dict):
        llm_provider = llm_result.get("llm_provider")

    # ---------- Shadow Priority (read-only, dry run) ----------
    shadow_priority, shadow_reason = shadow_priority_engine.compute(
        llm_priority=priority,
        from_email=from_email,
    )
    if shadow_priority != priority:
        logger.info(
            "shadow_priority_computed",
            from_email=from_email or "",
            current_priority=priority,
            shadow_priority=shadow_priority,
            reason=shadow_reason or "",
        )

    # ---------- Stage 1.4: AUTO PRIORITY (feature-flagged + runtime) ----------
    confidence_score: float | None = None
    confidence_decision: str | None = None
    llm_priority_for_confidence = priority
    runtime_flags, _ = runtime_flag_store.get_flags()
    auto_priority_runtime_enabled = runtime_flags.enable_auto_priority
    auto_priority_enabled = feature_flags.ENABLE_AUTO_PRIORITY and auto_priority_runtime_enabled
    should_score_confidence = _is_shadow_higher(shadow_priority, priority) and (
        feature_flags.ENABLE_AUTO_PRIORITY or feature_flags.ENABLE_AUTO_ACTIONS
    )
    if should_score_confidence:
        confidence_score = priority_confidence_engine.score(
            llm_priority=priority,
            shadow_priority=shadow_priority,
            sender_stats=_lookup_sender_stats(from_email),
            recent_history=_recent_history(from_email),
        )

    if auto_priority_runtime_enabled:
        breaker_status = auto_priority_breaker.check()
        if breaker_status.tripped:
            runtime_flag_store.set_enable_auto_priority(False)
            auto_priority_runtime_enabled = False
            auto_priority_enabled = False
            logger.warning(
                "auto_priority_safety_disabled",
                reason=breaker_status.reason or "unknown",
            )

    gate_decision = None
    auto_priority_allowed = False
    if auto_priority_enabled and should_score_confidence:
        gate_decision = auto_priority_gates.evaluate(
            llm_priority=priority,
            shadow_priority=shadow_priority,
            confidence_score=confidence_score,
        )
        if gate_decision.open:
            auto_priority_allowed = True
        else:
            logger.info(
                "auto_priority_gate_closed",
                reasons=",".join(gate_decision.reasons) if gate_decision.reasons else "unknown",
            )

    if auto_priority_allowed:
        threshold = max(
            feature_flags.AUTO_PRIORITY_CONFIDENCE_THRESHOLD,
            AutoPriorityGates.MIN_CONFIDENCE,
        )
        if (confidence_score or 0.0) >= threshold:
            original_priority = priority
            priority = shadow_priority
            priority_reason = shadow_reason or "Auto-priority escalation"
            confidence_decision = "APPLIED"
            logger.info(
                "auto_priority_applied",
                from_email=from_email or "",
                llm_priority=original_priority,
                shadow_priority=shadow_priority,
                reason=shadow_reason or "",
            )
        else:
            confidence_decision = "SKIPPED"

    if auto_priority_enabled and should_score_confidence:
        logger.info(
            "auto_priority_confidence_scored",
            llm_priority=llm_priority_for_confidence,
            shadow_priority=shadow_priority,
            confidence=confidence_score or 0.0,
            threshold=max(
                feature_flags.AUTO_PRIORITY_CONFIDENCE_THRESHOLD,
                AutoPriorityGates.MIN_CONFIDENCE,
            ),
            decision=confidence_decision or "SKIPPED",
        )

    logger.info(
        "auto_priority_summary",
        enabled=auto_priority_allowed,
        applied=confidence_decision == "APPLIED",
        llm_priority=llm_priority_for_confidence,
        shadow_priority=shadow_priority,
        final_priority=priority,
        confidence=confidence_score or 0.0,
    )

    logger.info(
        "llm_decision",
        email_id=message_id,
        llm_provider=llm_provider or "unknown",
        priority_llm=llm_result.priority,
        priority_shadow=shadow_priority,
        final_priority=priority,
        confidence=confidence_score or 0.0,
        action_line=action_line,
        latency_ms=llm_latency_ms,
    )

    if auto_priority_enabled:
        logger.info(
            "auto_priority_evaluated",
            email_id=message_id,
            enabled=auto_priority_enabled,
            original_priority=original_priority or llm_result.priority,
            final_priority=priority,
            reason=priority_reason or "",
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
            "shadow_action_candidate",
            from_email=from_email or "",
            task=task or "",
            reason=reason or "",
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
                "auto_action_stored",
                action_type=proposed_action.get("type", ""),
                confidence=proposed_action.get("confidence", 0.0),
                source=proposed_action.get("source", ""),
            )
        else:
            logger.info("auto_action_skipped", reason="conditions_not_met")

    if getattr(feature_flags, "ENABLE_PREVIEW_ACTIONS", False) and proposed_action:
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
        logger.info("preview_action_generated", preview=preview)
        try:
            knowledge_db.save_preview_action(
                email_id=message_id,
                proposed_action=proposed_action,
                confidence=proposed_action.get("confidence"),
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("preview_action_persist_failed", error=str(exc))

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
            llm_provider=llm_provider,
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
                "shadow_persist_saved",
                email_id=message_id,
                account_email=account_email,
            )

    except Exception as exc:
        # ❗ БД — side-effect only
        logger.error("knowledge_db_failed", error=str(exc))
        logger.error(
            "processing_error",
            stage="crm",
            email_id=message_id,
            error=str(exc),
        )

    # ---------- Stage Telegram ----------
    try:
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
        logger.info(
            "telegram_sent",
            email_id=message_id,
            chat_id=telegram_chat_id,
            success=True,
        )
    except Exception as exc:
        logger.error(
            "processing_error",
            stage="telegram",
            email_id=message_id,
            error=str(exc),
        )
        raise

    if feature_flags.ENABLE_PREVIEW_ACTIONS:
        preview_actions = _extract_preview_actions(proposed_action)
        preview_actions = [action for action in preview_actions if action]
        if not preview_actions:
            logger.info(
                "preview_actions_skipped",
                reason="no_proposals",
                email_id=message_id,
                account_email=account_email,
            )
            return

        preview_text = _build_preview_message(preview_actions)
        send_preview_to_telegram(
            chat_id=telegram_chat_id,
            preview_text=preview_text,
            account_email=account_email,
        )
        logger.info(
            "preview_actions_sent",
            count=len(preview_actions),
            email_id=message_id,
            account_email=account_email,
        )
