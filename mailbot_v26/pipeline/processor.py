# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mailbot_v26.actions.auto_action_engine import AutoActionEngine
from mailbot_v26.features import FeatureFlags
from mailbot_v26.insights.commitment_signals import (
    CommitmentReliabilityMetrics,
    compute_commitment_reliability,
)
from mailbot_v26.insights.commitment_tracker import Commitment, detect_commitments
from mailbot_v26.insights.commitment_lifecycle import (
    CommitmentStatusUpdate,
    evaluate_commitment_updates,
)
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.observability.metrics import (
    MetricsAggregator,
    SystemGates,
    SystemHealthSnapshotter,
)
from mailbot_v26.priority.auto_engine import AutoPriorityEngine, AutoPriorityOutcome
from mailbot_v26.priority.confidence_engine import PriorityConfidenceEngine
from mailbot_v26.priority.auto_gates import AutoPriorityCircuitBreaker, AutoPriorityGates
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.priority.shadow_engine import ShadowPriorityEngine
from .stage_llm import run_llm_stage
from .stage_telegram import send_preview_to_telegram, send_system_notice, send_to_telegram
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.tasks.shadow_actions import ShadowActionEngine
from .signal_quality import evaluate_signal_quality

logger = get_logger("mailbot")

# === Инициализация write-only БД ===
DB_PATH = Path("database.sqlite")
knowledge_db = KnowledgeDB(DB_PATH)
analytics = KnowledgeAnalytics(DB_PATH)
decision_trace_writer = DecisionTraceWriter(DB_PATH)
context_store = ContextStore(DB_PATH)
shadow_priority_engine = ShadowPriorityEngine(analytics)
shadow_action_engine = ShadowActionEngine(analytics)
priority_confidence_engine = PriorityConfidenceEngine()
auto_priority_gates = AutoPriorityGates(analytics)
auto_priority_breaker = AutoPriorityCircuitBreaker(analytics)
metrics_aggregator = MetricsAggregator(DB_PATH)
system_gates = SystemGates()
system_snapshotter = SystemHealthSnapshotter(metrics_aggregator, system_gates)
feature_flags = FeatureFlags()
runtime_flag_store = RuntimeFlagStore()
auto_priority_engine = AutoPriorityEngine(
    auto_priority_gates,
    auto_priority_breaker,
    runtime_flag_store,
    system_health,
    enabled_flag=lambda: feature_flags.ENABLE_AUTO_PRIORITY,
)
auto_action_engine = AutoActionEngine(
    confidence_threshold=feature_flags.AUTO_ACTION_CONFIDENCE_THRESHOLD
)


@dataclass
class Attachment:
    filename: str
    content: bytes = b""
    content_type: str = ""
    text: str = ""
    size_bytes: int = 0


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


def _read_llm_field(llm_result: Any, key: str, default: Any = None) -> Any:
    if isinstance(llm_result, dict):
        return llm_result.get(key, default)
    return getattr(llm_result, key, default)


def _build_preview_message(
    *,
    action_text: str,
    reasons: list[str],
    confidence: float | None,
) -> str:
    action_line = _sanitize_preview_line(action_text)
    safe_reasons = [_sanitize_preview_line(reason) for reason in reasons if reason]
    if not safe_reasons:
        safe_reasons = ["нет данных"]
    confidence_value = confidence if confidence is not None else 0.0
    lines = [
        "🤖 AI Preview",
        "",
        "Предлагаемое действие:",
        f"• {action_line}",
        "Причина:",
    ]
    lines.extend(f"• {reason}" for reason in safe_reasons)
    lines.append(f"Confidence: {confidence_value:.2f}")
    lines.append("")
    lines.append("[✅ Принять] [❌ Отклонить]")
    return "\n".join(lines)


def _append_commitments_preview(
    preview_text: str, commitments: list[Commitment]
) -> str:
    if not commitments:
        return preview_text
    lines = [preview_text, "", "📝 Обязательства"]
    status_labels = {
        "pending": ("⏳", "ожидается"),
        "fulfilled": ("✅", "выполнено"),
        "expired": ("⚠️", "просрочено"),
        "unknown": ("❓", "неизвестно"),
    }
    for commitment in commitments:
        safe_text = _sanitize_preview_line(commitment.commitment_text)
        icon, label = status_labels.get(
            commitment.status, ("❓", commitment.status or "неизвестно")
        )
        line = f"• \"{safe_text}\" — {icon} {label}"
        lines.append(line)
    return "\n".join(lines)


def _append_commitment_signal_preview(
    preview_text: str,
    *,
    from_email: str,
    score: int,
    label: str,
    fulfilled_count: int,
    expired_count: int,
) -> str:
    safe_sender = _sanitize_preview_line(from_email or "неизвестно")
    lines = [
        preview_text,
        "",
        "🔎 Контекст отношений:",
        f"  Контрагент: {safe_sender}",
        f"  Надёжность обязательств: {label} {score}/100",
        f"  (выполнено: {fulfilled_count}, просрочено: {expired_count} за 30 дней)",
    ]
    return "\n".join(lines)


def _build_signal_fallback(subject: str, from_email: str) -> str:
    safe_subject = subject or "(без темы)"
    safe_sender = from_email or "неизвестно"
    return (
        "Тело письма недоступно (низкое качество извлечения).\n"
        f"Тема: {safe_subject}\n"
        f"От: {safe_sender}"
    )


def _notify_system_mode_change(
    *,
    change,
    chat_id: str,
    account_email: str,
) -> None:
    if change is None:
        return
    try:
        send_system_notice(
            chat_id=chat_id,
            notice_text=system_health.system_notice(change),
            account_email=account_email,
        )
        logger.info(
            "system_mode_notice_sent",
            chat_id=chat_id,
            account_email=account_email,
            mode=change.current.value,
        )
    except Exception as exc:  # pragma: no cover - optional notification
        logger.error(
            "system_mode_notice_failed",
            chat_id=chat_id,
            account_email=account_email,
            error=str(exc),
        )


def _check_crm_available() -> bool:
    try:
        with sqlite3.connect(knowledge_db.path) as conn:
            conn.execute("SELECT 1;")
        return True
    except Exception:
        return False


def process_message(
    *,
    account_email: str,
    message_id: int,
    from_email: str,
    from_name: str | None = None,
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

    try:
        system_snapshotter.maybe_log()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_health_snapshot_failed", error=str(exc))

    # ---------- Stage PARSE (Commitment Tracker) ----------
    logger.info(
        "email_received",
        email_id=message_id,
        account=account_email,
        from_email=from_email,
        subject=subject,
        received_at=received_at.isoformat(),
    )
    commitments: list[Commitment] = []
    enable_commitments = getattr(feature_flags, "ENABLE_COMMITMENT_TRACKER", False)
    if enable_commitments:
        try:
            commitments = detect_commitments(body_text or "")
            if commitments:
                logger.info(
                    "commitment_detected",
                    email_id=message_id,
                    account=account_email,
                    sender=from_email,
                    count=len(commitments),
                    has_deadlines=any(c.deadline_iso for c in commitments),
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "commitment_detection_failed",
                email_id=message_id,
                error=str(exc),
            )

    commitment_status_updates: list[CommitmentStatusUpdate] = []
    if enable_commitments and from_email:
        if system_health.mode == OperationalMode.EMERGENCY_READ_ONLY:
            logger.error(
                "commitment_status_update_failed",
                email_id=message_id,
                sender=from_email,
                error="crm_unavailable",
            )
        else:
            try:
                pending_commitments = knowledge_db.fetch_pending_commitments_by_sender(
                    from_email=from_email
                )
                commitment_status_updates = evaluate_commitment_updates(
                    pending_commitments,
                    message_body=body_text or "",
                    message_received_at=received_at,
                    now=datetime.now(timezone.utc),
                )
                if commitment_status_updates:
                    saved = knowledge_db.update_commitment_statuses(
                        updates=commitment_status_updates
                    )
                    if not saved:
                        logger.error(
                            "commitment_status_update_failed",
                            email_id=message_id,
                            sender=from_email,
                            error="commitment_status_save_failed",
                        )
                        commitment_status_updates = []
                    else:
                        for update in commitment_status_updates:
                            logger.info(
                                "commitment_status_changed",
                                commitment_id=update.commitment_id,
                                old_status=update.old_status,
                                new_status=update.new_status,
                                reason=update.reason,
                            )
                            if update.new_status == "fulfilled":
                                logger.info(
                                    "commitment_fulfilled_detected",
                                    commitment_id=update.commitment_id,
                                    reason=update.reason,
                                )
                            if update.new_status == "expired":
                                logger.info(
                                    "commitment_expired",
                                    commitment_id=update.commitment_id,
                                    reason=update.reason,
                                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "commitment_status_update_failed",
                    email_id=message_id,
                    sender=from_email,
                    error=str(exc),
                )

    # ---------- Stage LLM ----------
    entity_resolution = None
    try:
        entity_resolution = context_store.resolve_sender_entity(
            from_email=from_email,
            from_name=from_name,
            entity_type="person",
            event_time=received_at,
        )
        if entity_resolution:
            logger.info(
                "entity_resolved",
                entity_id=entity_resolution.entity_id,
                entity_type=entity_resolution.entity_type,
                confidence=entity_resolution.confidence,
            )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("entity_resolution_failed", error=str(exc))
    signal_quality = evaluate_signal_quality(body_text or "")
    fallback_used = False
    llm_body_text = body_text
    if not signal_quality.is_usable:
        llm_body_text = _build_signal_fallback(subject, from_email)
        fallback_used = True
        logger.info("signal_fallback_used", reason=signal_quality.reason)
    logger.info(
        "signal_evaluated",
        email_id=message_id,
        entropy=signal_quality.entropy,
        printable_ratio=signal_quality.printable_ratio,
        quality_score=signal_quality.quality_score,
        is_usable=signal_quality.is_usable,
        fallback_used=fallback_used,
    )
    llm_start = time.perf_counter()
    try:
        llm_result = run_llm_stage(
            subject=subject,
            from_email=from_email,
            body_text=llm_body_text,
            attachments=attachments,
        )
    except Exception as exc:
        change = system_health.update_component(
            "LLM",
            False,
            reason=str(exc) or "LLM stage failed",
        )
        _notify_system_mode_change(
            change=change,
            chat_id=telegram_chat_id,
            account_email=account_email,
        )
        logger.error(
            "processing_error",
            stage="llm",
            email_id=message_id,
            error=str(exc),
        )
        raise
    llm_latency_ms = int((time.perf_counter() - llm_start) * 1000)

    if not llm_result:
        change = system_health.update_component(
            "LLM",
            False,
            reason="LLM returned empty result",
        )
        _notify_system_mode_change(
            change=change,
            chat_id=telegram_chat_id,
            account_email=account_email,
        )
        logger.warning("llm_empty_result", email_id=message_id)
        return
    change = system_health.update_component("LLM", True)
    _notify_system_mode_change(
        change=change,
        chat_id=telegram_chat_id,
        account_email=account_email,
    )

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

    auto_priority_outcome = AutoPriorityOutcome(
        final_priority=priority,
        original_priority=None,
        priority_reason=None,
        confidence_score=confidence_score,
        confidence_decision=None,
        gate_decision=None,
        applied=False,
        skipped_reason=None,
    )
    if feature_flags.ENABLE_AUTO_PRIORITY:
        try:
            auto_priority_outcome = auto_priority_engine.evaluate(
                llm_priority=priority,
                shadow_priority=shadow_priority,
                shadow_reason=shadow_reason,
                confidence_score=confidence_score,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("auto_priority_error", error=str(exc))

    if auto_priority_outcome.applied:
        original_priority = auto_priority_outcome.original_priority
        priority = auto_priority_outcome.final_priority
        priority_reason = auto_priority_outcome.priority_reason
        confidence_decision = auto_priority_outcome.confidence_decision
    elif auto_priority_outcome.confidence_decision:
        confidence_decision = auto_priority_outcome.confidence_decision

    if feature_flags.ENABLE_AUTO_PRIORITY and should_score_confidence:
        logger.info(
            "auto_priority_confidence_scored",
            llm_priority=llm_priority_for_confidence,
            shadow_priority=shadow_priority,
            confidence=confidence_score or 0.0,
            threshold=AutoPriorityGates.MIN_CONFIDENCE,
            decision=confidence_decision or "SKIPPED",
        )

    logger.info(
        "auto_priority_summary",
        enabled=auto_priority_outcome.applied,
        applied=auto_priority_outcome.applied,
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

    if feature_flags.ENABLE_AUTO_PRIORITY:
        logger.info(
            "auto_priority_evaluated",
            email_id=message_id,
            enabled=auto_priority_outcome.applied,
            original_priority=original_priority or llm_result.priority,
            final_priority=priority,
            reason=priority_reason or "",
            skipped_reason=auto_priority_outcome.skipped_reason or "",
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
    email_row_id: int | None = None
    try:
        if not _check_crm_available():
            change = system_health.update_component(
                "CRM",
                False,
                reason="CRM unavailable",
            )
            _notify_system_mode_change(
                change=change,
                chat_id=telegram_chat_id,
                account_email=account_email,
            )
        email_row_id = knowledge_db.save_email(
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
        change = system_health.update_component("CRM", True)
        _notify_system_mode_change(
            change=change,
            chat_id=telegram_chat_id,
            account_email=account_email,
        )

    except Exception as exc:
        # ❗ БД — side-effect only
        change = system_health.update_component(
            "CRM",
            False,
            reason=str(exc) or "CRM write failed",
        )
        _notify_system_mode_change(
            change=change,
            chat_id=telegram_chat_id,
            account_email=account_email,
        )
        logger.error("knowledge_db_failed", error=str(exc))
        logger.error(
            "processing_error",
            stage="crm",
            email_id=message_id,
            error=str(exc),
        )

    if enable_commitments and commitments:
        if system_health.mode == OperationalMode.EMERGENCY_READ_ONLY:
            logger.error(
                "commitments_persist_failed",
                email_id=message_id,
                error="crm_unavailable",
            )
        elif email_row_id is None:
            logger.error(
                "commitments_persist_failed",
                email_id=message_id,
                error="missing_email_row_id",
            )
        else:
            try:
                saved = knowledge_db.save_commitments(
                    email_row_id=email_row_id,
                    commitments=commitments,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "commitments_persist_failed",
                    email_id=message_id,
                    error=str(exc),
                )
            else:
                if saved:
                    logger.info(
                        "commitments_persisted",
                        email_id=message_id,
                        count=len(commitments),
                    )
                else:
                    logger.error(
                        "commitments_persist_failed",
                        email_id=message_id,
                        error="commitments_save_failed",
                    )

    commitment_signal_preview: dict[str, object] | None = None
    if enable_commitments and entity_resolution and from_email:
        try:
            stats = analytics.commitment_stats_by_sender(
                from_email=from_email,
                days=30,
            )
            metrics = CommitmentReliabilityMetrics(
                total_commitments=stats["total_commitments"],
                fulfilled_count=stats["fulfilled_count"],
                expired_count=stats["expired_count"],
                unknown_count=stats["unknown_count"],
            )
            signal = compute_commitment_reliability(metrics)
            previous_label = knowledge_db.upsert_entity_signal(
                entity_id=entity_resolution.entity_id,
                signal_type="commitment_reliability",
                score=signal.score,
                label=signal.label,
                computed_at=datetime.now(timezone.utc).isoformat(),
                sample_size=signal.sample_size,
            )
            logger.info(
                "entity_signal_computed",
                entity_id=entity_resolution.entity_id,
                signal_type="commitment_reliability",
                score=signal.score,
                label=signal.label,
                sample_size=signal.sample_size,
            )
            if previous_label and previous_label != signal.label:
                logger.info(
                    "entity_signal_changed",
                    entity_id=entity_resolution.entity_id,
                    signal_type="commitment_reliability",
                    old_label=previous_label,
                    new_label=signal.label,
                    score=signal.score,
                    sample_size=signal.sample_size,
                )
            if signal.sample_size > 0:
                commitment_signal_preview = {
                    "score": signal.score,
                    "label": signal.label,
                    "fulfilled_count": metrics.fulfilled_count,
                    "expired_count": metrics.expired_count,
                }
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "entity_signal_compute_failed",
                entity_id=entity_resolution.entity_id,
                signal_type="commitment_reliability",
                error=str(exc),
            )

    if entity_resolution:
        try:
            context_store.record_interaction_event(
                entity_id=entity_resolution.entity_id,
                event_type="email_received",
                event_time=received_at,
                metadata={
                    "email_id": message_id,
                    "from_email": from_email,
                    "subject": subject,
                },
            )
            baseline_value, _ = context_store.recompute_email_frequency(
                entity_id=entity_resolution.entity_id
            )
            logger.info(
                "baseline_updated",
                entity_id=entity_resolution.entity_id,
                metric="email_frequency",
                value=baseline_value,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("context_layer_failed", error=str(exc))

    # ---------- Stage Decision Trace ----------
    try:
        prompt_full = _read_llm_field(llm_result, "prompt_full", "") or ""
        response_full = (
            _read_llm_field(llm_result, "response_full", "")
            or _read_llm_field(llm_result, "llm_response", "")
            or ""
        )
        llm_model = _read_llm_field(llm_result, "llm_model", "") or ""

        decision_trace_writer.write(
            email_id=str(message_id),
            account_email=account_email,
            signal_entropy=signal_quality.entropy,
            signal_printable_ratio=signal_quality.printable_ratio,
            signal_quality_score=signal_quality.quality_score,
            signal_fallback_used=fallback_used,
            prompt_full=prompt_full,
            llm_provider=llm_provider or "unknown",
            llm_model=llm_model,
            response_full=response_full,
            confidence=confidence_score,
            priority=priority,
            action_line=action_line,
            shadow_priority=shadow_priority,
        )
        logger.info(
            "decision_traced",
            email_id=message_id,
            provider=llm_provider or "unknown",
            entropy=signal_quality.entropy,
            fallback_used=fallback_used,
            priority=priority,
            confidence=confidence_score or 0.0,
        )
    except Exception as exc:
        logger.error(
            "TRACE_WRITE_FAILED",
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
        change = system_health.update_component("Telegram", True)
        _notify_system_mode_change(
            change=change,
            chat_id=telegram_chat_id,
            account_email=account_email,
        )
        logger.info(
            "telegram_sent",
            email_id=message_id,
            chat_id=telegram_chat_id,
            success=True,
        )
    except Exception as exc:
        change = system_health.update_component(
            "Telegram",
            False,
            reason=str(exc) or "Telegram send failed",
        )
        _notify_system_mode_change(
            change=change,
            chat_id=telegram_chat_id,
            account_email=account_email,
        )
        logger.error(
            "processing_error",
            stage="telegram",
            email_id=message_id,
            error=str(exc),
        )
        raise

    if feature_flags.ENABLE_PREVIEW_ACTIONS:
        if system_health.mode == OperationalMode.DEGRADED_NO_LLM:
            logger.info(
                "preview_actions_skipped",
                reason="system_degraded_no_llm",
                email_id=message_id,
                account_email=account_email,
                system_mode=system_health.mode.value,
            )
            return

        preview_actions = _extract_preview_actions(proposed_action)
        preview_actions = [action for action in preview_actions if action]
        if not preview_actions:
            logger.info(
                "preview_actions_skipped",
                reason="no_proposals",
                email_id=message_id,
                account_email=account_email,
                system_mode=system_health.mode.value,
            )
            return

        preview_text = _build_preview_message(
            action_text=preview_actions[0],
            reasons=[reason for reason in (shadow_action_reason, shadow_reason, priority_reason) if reason],
            confidence=proposed_action.get("confidence") if proposed_action else None,
        )
        if enable_commitments and (commitments or commitment_status_updates):
            preview_commitments = list(commitments)
            preview_commitments.extend(
                [
                    Commitment(
                        commitment_text=update.commitment_text,
                        deadline_iso=update.deadline_iso,
                        status=update.new_status,
                        source="crm",
                        confidence=1.0,
                    )
                    for update in commitment_status_updates
                ]
            )
            preview_text = _append_commitments_preview(
                preview_text, preview_commitments
            )
            logger.info(
                "commitments_preview_shown",
                email_id=message_id,
                count=len(preview_commitments),
            )
        if commitment_signal_preview:
            preview_text = _append_commitment_signal_preview(
                preview_text,
                from_email=from_email,
                score=int(commitment_signal_preview["score"]),
                label=str(commitment_signal_preview["label"]),
                fulfilled_count=int(commitment_signal_preview["fulfilled_count"]),
                expired_count=int(commitment_signal_preview["expired_count"]),
            )
        send_preview_to_telegram(
            chat_id=telegram_chat_id,
            preview_text=preview_text,
            account_email=account_email,
        )
        logger.info(
            "preview_shown",
            email_id=message_id,
            action_type=proposed_action.get("type", "") if proposed_action else "",
            confidence=proposed_action.get("confidence", 0.0) if proposed_action else 0.0,
            system_mode=system_health.mode.value,
        )
