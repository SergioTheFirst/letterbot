# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from mailbot_v26.actions.auto_action_engine import AutoActionEngine
from mailbot_v26.domain.fact_snippets import pick_attachment_fact, pick_email_body_fact
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
from mailbot_v26.insights.anomaly_engine import (
    Anomaly,
    compute_anomalies,
    max_anomaly_severity,
)
from mailbot_v26.insights.aggregator import Insight, aggregate_insights
from mailbot_v26.insights.digest import InsightDigest, build_insight_digest
from mailbot_v26.insights.relationship_anomaly import RelationshipAnomalyDetector
from mailbot_v26.insights.relationship_health import RelationshipHealthCalculator
from mailbot_v26.insights.temporal_reasoning import (
    TemporalReasoningEngine,
    TemporalState,
)
from mailbot_v26.insights.trust_score import TrustScoreCalculator
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability import get_logger
from mailbot_v26.telegram_utils import escape_tg_html
from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.observability.metrics import (
    GateEvaluation,
    MetricsAggregator,
    SystemGates,
    SystemHealthSnapshotter,
)
from mailbot_v26.observability.relationship_health_snapshot import (
    RelationshipHealthSnapshotWriter,
)
from mailbot_v26.observability.trust_snapshot import TrustSnapshotWriter
from mailbot_v26.priority.auto_engine import AutoPriorityEngine, AutoPriorityOutcome
from mailbot_v26.priority.confidence_engine import PriorityConfidenceEngine
from mailbot_v26.priority.auto_gates import AutoPriorityCircuitBreaker, AutoPriorityGates
from mailbot_v26.llm.runtime_flags import RuntimeFlags, RuntimeFlagStore
from mailbot_v26.priority.shadow_engine import ShadowPriorityEngine
from mailbot_v26.text.clean_email import clean_email_body
from .attention_gate import (
    AttentionGateInput,
    apply_attention_gate,
    max_insight_severity,
)
from .insight_arbiter import InsightArbiterInput, apply_insight_arbiter
from .stage_llm import run_llm_stage
from .stage_telegram import enqueue_tg, send_preview_to_telegram, send_system_notice
from .telegram_payload import TelegramPayload
from . import tg_renderer
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.system.orchestrator import SystemOrchestrator, SystemPolicyDecision
from mailbot_v26.tasks.shadow_actions import ShadowActionEngine
from mailbot_v26.worker.telegram_sender import DeliveryResult
from .signal_quality import evaluate_signal_quality

logger = get_logger("mailbot")

# === Инициализация write-only БД ===
DB_PATH = Path("database.sqlite")
knowledge_db = KnowledgeDB(DB_PATH)
analytics = KnowledgeAnalytics(DB_PATH)
decision_trace_writer = DecisionTraceWriter(DB_PATH)
trust_snapshot_writer = TrustSnapshotWriter(DB_PATH)
relationship_health_snapshot_writer = RelationshipHealthSnapshotWriter(DB_PATH)
context_store = ContextStore(DB_PATH)
event_emitter = EventEmitter(DB_PATH)
contract_event_emitter = ContractEventEmitter(DB_PATH)
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
trust_score_calculator = TrustScoreCalculator(analytics)
relationship_health_calculator = RelationshipHealthCalculator(
    analytics,
    trust_score_calculator,
)
relationship_anomaly_detector = RelationshipAnomalyDetector(
    analytics,
    trust_score_calculator,
)
temporal_reasoning_engine = TemporalReasoningEngine(analytics)
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
system_orchestrator = SystemOrchestrator()


@dataclass(frozen=True, slots=True)
class PolicyInputs:
    metrics: dict[str, dict[str, float]] | None
    gates: GateEvaluation | None
    runtime_flags: RuntimeFlags


@dataclass
class Attachment:
    """Attachment contract: extracted text is a string (possibly empty)."""
    filename: str
    content: bytes = b""
    content_type: str = ""
    text: str = ""
    size_bytes: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AttachmentSummary:
    filename: str
    description: str
    kind: str = ""
    priority: int = 0
    text_length: int = 0


def _emit_contract_event(
    event_type: EventType,
    *,
    ts_utc: float,
    account_id: str,
    entity_id: str | None,
    email_id: int | None,
    payload: dict[str, Any],
) -> None:
    try:
        event = EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=entity_id,
            email_id=email_id,
            payload=payload,
        )
        contract_event_emitter.emit(event)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("contract_event_emit_failed", event_type=event_type.value, error=str(exc))


def _coerce_delivery_result(result: object, *, email_id: int) -> DeliveryResult:
    if isinstance(result, DeliveryResult):
        return result
    if isinstance(result, bool):
        logger.warning(
            "telegram_delivery_result_coerced",
            email_id=email_id,
            reason="bool_returned",
        )
        return DeliveryResult(
            delivered=result,
            retryable=False,
            error=None if result else "telegram_delivery_failed",
        )
    if result is None:
        logger.warning(
            "telegram_delivery_result_coerced",
            email_id=email_id,
            reason="none_returned",
        )
        return DeliveryResult(delivered=True, retryable=False, error=None)
    logger.warning(
        "telegram_delivery_result_coerced",
        email_id=email_id,
        reason=f"unexpected_type:{type(result).__name__}",
    )
    return DeliveryResult(delivered=True, retryable=False, error=None)


def _collect_policy_inputs() -> PolicyInputs:
    metrics: dict[str, dict[str, float]] | None = None
    gates: GateEvaluation | None = None
    runtime_flags = RuntimeFlags()
    try:
        metrics = metrics_aggregator.snapshot()
        gates = system_gates.evaluate(metrics)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_policy_metrics_failed", error=str(exc))
    try:
        runtime_flags, _ = runtime_flag_store.get_flags()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_policy_runtime_flags_failed", error=str(exc))
    return PolicyInputs(metrics=metrics, gates=gates, runtime_flags=runtime_flags)


def _evaluate_policy(
    policy_inputs: PolicyInputs,
    *,
    has_daily_digest_content: bool = False,
    has_weekly_digest_content: bool = False,
) -> SystemPolicyDecision:
    system_mode = system_health.mode
    telegram_ok = system_mode != OperationalMode.DEGRADED_NO_TELEGRAM
    fallback = system_orchestrator.legacy_decision(
        system_mode=system_mode,
        runtime_flags=policy_inputs.runtime_flags,
        feature_flags=feature_flags,
        has_daily_digest_content=has_daily_digest_content,
        has_weekly_digest_content=has_weekly_digest_content,
    )
    return system_orchestrator.evaluate(
        system_mode=system_mode,
        metrics=policy_inputs.metrics,
        gates=policy_inputs.gates,
        runtime_flags=policy_inputs.runtime_flags,
        feature_flags=feature_flags,
        telegram_ok=telegram_ok,
        has_daily_digest_content=has_daily_digest_content,
        has_weekly_digest_content=has_weekly_digest_content,
        fallback_decision=fallback,
    )


@dataclass
class InboundMessage:
    subject: str
    body: str
    sender: str = ""
    attachments: list[Attachment] = field(default_factory=list)
    received_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EmailContext:
    subject: str
    from_email: str
    body_text: str
    attachments_count: int
    summary: str = ""
    action_line: str = ""


@dataclass(frozen=True, slots=True)
class TelegramRenderContext:
    extracted_text_len: int
    attachments_count: int
    llm_failed: bool
    signal_invalid: bool


@dataclass(frozen=True, slots=True)
class TelegramBuildContext:
    email_id: int
    received_at: datetime
    priority: str
    from_email: str
    subject: str
    action_line: str
    body_summary: str
    body_text: str
    attachment_summary: str
    attachment_details: list[dict[str, Any]]
    attachment_files: list[dict[str, Any]]
    attachments_count: int
    extracted_text_len: int
    llm_failed: bool
    signal_invalid: bool
    insights: list[Insight]
    insight_digest: InsightDigest | None
    commitments_present: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LlmContext:
    entity_resolution: Any | None
    signal_quality: Any
    llm_body_text: str
    fallback_used: bool


@dataclass(frozen=True, slots=True)
class AnalyticsResult:
    email_row_id: int | None
    commitment_status_updates: list[CommitmentStatusUpdate]
    commitment_signal_preview: dict[str, object] | None
    trust_result: Any | None
    health_snapshot: Any | None
    temporal_insights: list[TemporalState]
    aggregated_insights: list[Insight]
    insight_digest: InsightDigest | None


@dataclass(frozen=True, slots=True)
class RenderResult:
    payload: TelegramPayload
    render_mode: TelegramRenderMode
    payload_invalid: bool
    attachment_details: list[dict[str, Any]]
    attachment_summary: str
    extracted_text_len: int
    body_summary: str


class TelegramRenderMode(Enum):
    FULL = "full"
    SAFE_FALLBACK = "safe_fallback"
    SHORT_TEMPLATE = "short_template"


def choose_tg_render_mode(ctx: TelegramRenderContext) -> TelegramRenderMode:
    if ctx.extracted_text_len > 0 or ctx.attachments_count > 0:
        mode = TelegramRenderMode.FULL
    elif ctx.llm_failed or ctx.signal_invalid:
        mode = TelegramRenderMode.SAFE_FALLBACK
    else:
        mode = TelegramRenderMode.SHORT_TEMPLATE
    assert not (
        mode == TelegramRenderMode.SHORT_TEMPLATE
        and (ctx.extracted_text_len > 0 or ctx.attachments_count > 0)
    )
    return mode


class InvalidTelegramPayload(ValueError):
    pass


class MessageProcessor:
    _ATTACHMENT_SNIPPET_LIMIT = 120
    _MAX_ATTACHMENTS = 12
    _VERB_ORDER = ("Проверить", "Ответить", "Сделать", "Согласовать")

    def __init__(self, config: Any, state: Any) -> None:
        self.config = config
        self.state = state

    def process(self, account_login: str, message: InboundMessage) -> str:
        """Lightweight placeholder processor to keep imports stable."""
        sender = message.sender or "неизвестно"
        display_sender = sender
        if "@" in sender:
            local = sender.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
            if local:
                display_sender = local.title()
        subject = message.subject or "(без темы)"
        body_text = message.body or ""
        priority = self._choose_priority(subject, body_text, message.attachments or [])
        verb = "Проверить"
        lowered_text = f"{subject} {body_text}".lower()
        if any(token in lowered_text for token in ("счет", "оплат", "invoice")):
            verb = "Оплатить"
        action_line = self._normalize_action_subject(
            verb,
            subject,
            message.attachments or [],
            body_text,
        )
        summary = self._summarize_body(body_text, subject)
        attachments = self._build_attachment_summaries(message.attachments or [], subject)

        safe_sender = escape_tg_html(display_sender)
        safe_subject = escape_tg_html(subject)
        safe_summary = escape_tg_html(summary)
        safe_action_line = escape_tg_html(action_line)
        safe_account_login = escape_tg_html(account_login)

        lines = [
            f"{priority} от {safe_sender} — {safe_subject}",
            f"<b>{safe_subject}</b>",
            safe_action_line,
        ]
        if safe_summary:
            lines.append(f"<i>{safe_summary}</i>")
        if attachments:
            lines.extend(self._render_attachments(attachments))
        lines.append(f"<i>to: {safe_account_login}</i>")
        return "\n".join(lines)

    @classmethod
    def _trim_attachment_snippet(cls, text: str) -> str:
        if len(text) <= cls._ATTACHMENT_SNIPPET_LIMIT:
            return text
        return f"{text[: cls._ATTACHMENT_SNIPPET_LIMIT - 1]}…"

    def _render_attachments(self, attachments: list[AttachmentSummary]) -> list[str]:
        rendered: list[str] = []
        for attachment in attachments:
            filename = escape_tg_html(attachment.filename)
            description = self._trim_attachment_snippet(attachment.description or "")
            description = escape_tg_html(description)
            if description:
                rendered.append(f"{filename} — {description}")
            else:
                rendered.append(filename)
        return rendered

    def _build_attachment_summaries(
        self,
        attachments: list[Attachment],
        subject: str,
    ) -> list[AttachmentSummary]:
        summaries: list[AttachmentSummary] = []
        for attachment in attachments:
            if self._is_image_attachment(attachment):
                continue
            kind = self._attachment_kind(attachment)
            try:
                description, text_length = self._summarize_attachment(
                    attachment,
                    subject,
                    kind,
                )
            except Exception:
                description, text_length = "", len(attachment.text or "")
            summaries.append(
                AttachmentSummary(
                    filename=attachment.filename,
                    description=description,
                    kind=kind,
                    priority=0,
                    text_length=text_length,
                )
            )
        return summaries

    def _summarize_attachment(
        self,
        attachment: Attachment,
        subject: str,
        kind: str,
    ) -> tuple[str, int]:
        text = (attachment.text or "").strip()
        if not text:
            return "", 0
        lowered_subject = (subject or "").lower()
        doc_type = "OTHER"
        if kind in {"XLS", "XLSX", "XLSM", "XLSB", "TABLE"}:
            doc_type = "TABLE"
        elif kind in {"DOC", "DOCX"} and any(token in lowered_subject for token in ("договор", "contract")):
            doc_type = "CONTRACT"
        snippet = pick_attachment_fact(text, attachment.filename, doc_type) or ""
        snippet = snippet.replace('"', "").replace("«", "").replace("»", "")
        snippet = snippet.replace(";", " ").replace("|", " ")
        snippet = re.sub(r"\s+", " ", snippet).strip()
        if not snippet:
            snippet = re.sub(r"\s+", " ", text.replace(";", " ").replace("|", " ")).strip()
        if not snippet:
            return "", len(text)
        words = snippet.split()
        if len(words) < 2:
            fallback = re.sub(r"\s+", " ", text.replace(";", " ").replace("|", " ")).strip()
            fallback_words = fallback.split()
            if len(fallback_words) >= 2:
                snippet = " ".join(fallback_words[:6])
                words = snippet.split()
        if len(words) > 12:
            snippet = " ".join(words[:12])
        if len(snippet) > self._ATTACHMENT_SNIPPET_LIMIT:
            snippet = self._trim_attachment_snippet(snippet)
        return snippet, len(text)

    def _summarize_body(self, body: str, subject: str) -> str:
        cleaned = clean_email_body(body)
        if not cleaned:
            return ""
        cleaned = self._unescape_newlines(cleaned)
        base = pick_email_body_fact(cleaned) or cleaned
        base = re.sub(r"[<>]", "", base)
        base = base.replace('"', "").replace("«", "").replace("»", "")
        base = re.sub(r"\s+", " ", base).strip()
        if not base or self._is_greeting_only(base):
            return ""
        words = base.split()
        if len(words) < 8:
            words = cleaned.split()
        if len(words) < 8:
            extra_words = [word for word in subject.split() if word]
            words.extend(extra_words)
        if len(words) < 8:
            return ""
        summary = " ".join(words[:12]).strip()
        if len(summary) > 120:
            summary = summary[:119].rstrip() + "…"
        return summary

    @staticmethod
    def _unescape_newlines(text: str) -> str:
        if "\\n" in text and "\n" not in text:
            return text.replace("\\r\\n", "\n").replace("\\n", "\n")
        return text

    @staticmethod
    def _is_greeting_only(text: str) -> bool:
        greetings = ("добрый день", "добрый вечер", "здравствуйте", "привет", "hello", "hi")
        lowered = text.lower().strip()
        if any(lowered.startswith(greet) for greet in greetings) and len(lowered.split()) <= 4:
            return True
        return False

    @staticmethod
    def _choose_priority(subject: str, body: str, attachments: list[Attachment]) -> str:
        combined = " ".join([subject or "", body or ""]).lower()
        attachment_names = " ".join(att.filename.lower() for att in attachments if att.filename)
        if any(token in combined for token in ("срочно", "urgent", "оплат")):
            return "🔴"
        if any(token in combined for token in ("договор", "contract", "согласован", "approval", "счет", "invoice")):
            return "🟡"
        if any(token in attachment_names for token in ("invoice", "счет")):
            return "🟡"
        if any(token in attachment_names for token in ("contract", "догов")):
            return "🟡"
        return "🔵"

    @staticmethod
    def _is_image_attachment(attachment: Attachment) -> bool:
        content_type = (attachment.content_type or "").lower()
        filename = (attachment.filename or "").lower()
        if content_type.startswith("image/"):
            return True
        return filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp"))

    @staticmethod
    def _attachment_kind(attachment: Attachment) -> str:
        filename = (attachment.filename or "").lower()
        content_type = (attachment.content_type or "").lower()
        extension = filename.rsplit(".", 1)[-1] if "." in filename else ""
        if "excel" in content_type or extension in {"xls", "xlsx", "xlsm", "xlsb"}:
            return "XLS"
        if "word" in content_type or extension in {"doc", "docx"}:
            return "DOC"
        if "pdf" in content_type or extension == "pdf":
            return "PDF"
        if extension:
            return extension.upper()
        return "FILE"

    def _normalize_action_subject(
        self,
        verb: str,
        subject: str,
        attachments: list[Attachment],
        body: str,
    ) -> str:
        lowered = (subject or "").lower()
        if "прайс" in lowered or "цена" in lowered or "счет" in lowered:
            return f"{verb} цены"
        if "документ" in lowered or "договор" in lowered:
            return f"{verb} документы"
        if any(att.filename.lower().endswith((".xls", ".xlsx")) for att in attachments if att.filename):
            return f"{verb} таблицы"
        if body and "договор" in body.lower():
            return f"{verb} документы"
        return f"{verb} письмо"


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


_TELEGRAM_BODY_LIMIT = 800
_MIN_TELEGRAM_LEN = 40
_MIN_SUMMARY_WORDS = 2
_MIN_SUMMARY_CHARS = 12
_ALLOWED_TG_TAGS = {"<b>", "</b>", "<i>", "</i>"}
_SUMMARY_PLACEHOLDER_PATTERNS = (
    "проверить письмо",
    "проверь письмо",
    "check email",
    "check mail",
)


def _attachment_kind(attachment: dict[str, Any]) -> str:
    content_type = str(attachment.get("content_type") or attachment.get("type") or "").lower()
    filename = str(attachment.get("filename") or "").lower()
    extension = filename.rsplit(".", 1)[-1] if "." in filename else ""
    if "pdf" in content_type or extension == "pdf":
        return "PDF"
    if "word" in content_type or extension in {"doc", "docx"}:
        return "DOC"
    if "excel" in content_type or "spreadsheet" in content_type or extension in {"xls", "xlsx"}:
        return "XLS"
    if extension:
        return extension.upper()
    if content_type:
        return content_type.split("/")[-1].upper()
    return "FILE"


def _attachment_text_length(attachment: dict[str, Any]) -> int:
    if isinstance(attachment.get("chars"), int):
        return int(attachment["chars"])
    text = attachment.get("text") or ""
    if isinstance(text, bytes):
        return len(text.decode(errors="ignore"))
    return len(str(text))


def _build_attachment_details(
    attachments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for attachment in attachments:
        details.append(
            {
                "kind": _attachment_kind(attachment),
                "chars": _attachment_text_length(attachment),
            }
        )
    return details


def _build_attachment_summary(details: list[dict[str, Any]]) -> str:
    if not details:
        return ""
    total_chars = sum(detail["chars"] for detail in details)
    lines = [f"📎 Вложения: {len(details)}", f"Всего текста: {total_chars} chars"]
    lines.extend(f"- {detail['kind']}: {detail['chars']} chars" for detail in details)
    return "\n".join(lines)


def _attachment_size_bytes(attachment: dict[str, Any]) -> int:
    for key in ("size_bytes", "size"):
        value = attachment.get(key)
        if isinstance(value, int):
            return int(value)
    content = attachment.get("content")
    if isinstance(content, (bytes, bytearray)):
        return len(content)
    return 0


def _build_no_llm_summary(
    attachments: list[dict[str, Any]],
    commitments_present: bool,
) -> str:
    return ""


def _trim_telegram_body(text: str) -> str:
    cleaned = text.strip()
    if len(cleaned) <= _TELEGRAM_BODY_LIMIT:
        return cleaned
    return f"{cleaned[: _TELEGRAM_BODY_LIMIT - 1]}…"


def _looks_like_subject_only(text: str, subject: str) -> bool:
    if not text.strip():
        return True
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 2:
        return False
    normalized = " ".join(lines).lower()
    normalized_subject = (subject or "").strip().lower()
    if normalized_subject and normalized_subject in normalized:
        return len(normalized) <= len(normalized_subject) + 10
    return len(lines) == 1


def _build_telegram_text(
    *,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str,
    body_summary: str,
    body_text: str,
    attachments: list[dict[str, Any]] | None = None,
    attachment_summary: str | None = None,
) -> str:
    if attachment_summary is None:
        base_text = tg_renderer.build_telegram_text(
            priority=priority,
            from_email=from_email,
            subject=subject,
            action_line=_resolve_action_line(action_line),
            attachments=attachments or [],
        )
        summary_text = (body_summary or "").strip()
        if summary_text:
            safe_summary = escape_tg_html(summary_text)
            return f"{base_text}\n<i>{safe_summary}</i>"
        return base_text
    safe_sender = escape_tg_html(from_email or "неизвестно")
    safe_subject = escape_tg_html(subject or "(без темы)")
    safe_action = escape_tg_html(_resolve_action_line(action_line))
    safe_summary = escape_tg_html(body_summary or "")
    lines = [f"{priority} от {safe_sender} — {safe_subject}", safe_action]
    if safe_summary:
        lines.append(safe_summary)
    if attachment_summary is None and attachments:
        attachment_summary = _build_attachment_summary(
            _build_attachment_details(attachments)
        )
    if attachment_summary:
        lines.append(attachment_summary)
    return "\n".join(lines)


def _normalize_action_line(action_line: str) -> str:
    cleaned = (action_line or "").strip()
    if cleaned.lower().startswith("сделать:"):
        cleaned = cleaned.split(":", 1)[1].strip()
    return cleaned


def _resolve_action_line(action_line: str) -> str:
    cleaned = _normalize_action_line(action_line)
    if cleaned:
        return cleaned
    return "Действий не требуется"


def _normalize_summary_text(summary: str) -> str:
    cleaned = re.sub(r"[\W_]+", " ", (summary or "").lower()).strip()
    return re.sub(r"\s+", " ", cleaned)


def _is_summary_placeholder(summary: str) -> bool:
    normalized = _normalize_summary_text(summary)
    if not normalized:
        return True
    for pattern in _SUMMARY_PLACEHOLDER_PATTERNS:
        if normalized == pattern:
            return True
        if normalized.startswith(pattern) and len(normalized.split()) <= 4:
            return True
    return False


def _is_meaningful_summary(summary: str) -> bool:
    if _is_summary_placeholder(summary):
        return False
    if len((summary or "").strip()) < _MIN_SUMMARY_CHARS:
        return False
    words = _normalize_summary_text(summary).split()
    if len(words) < _MIN_SUMMARY_WORDS:
        return False
    return True


def _validate_telegram_markup(text: str) -> None:
    for tag in re.findall(r"</?[^>]+>", text):
        if tag not in _ALLOWED_TG_TAGS:
            raise InvalidTelegramPayload(f"forbidden_tag:{tag}")
    stripped = text
    for tag in _ALLOWED_TG_TAGS:
        stripped = stripped.replace(tag, "")
    if "<" in stripped or ">" in stripped:
        raise InvalidTelegramPayload("unsafe_angle_brackets")
    if re.search(r"&(?!(?:[a-zA-Z]+|#\d+);)", text):
        raise InvalidTelegramPayload("unescaped_ampersand")
    if text.count("```") % 2 != 0 or text.count("`") % 2 != 0:
        raise InvalidTelegramPayload("malformed_markdown")


def validate_tg_payload(text: str, ctx: EmailContext) -> str:
    if len(text.strip()) < _MIN_TELEGRAM_LEN:
        raise InvalidTelegramPayload("too short")
    if not ctx.subject.strip():
        raise InvalidTelegramPayload("missing_subject")
    if not ctx.from_email.strip():
        raise InvalidTelegramPayload("missing_sender")
    summary = ctx.summary if ctx.summary.strip() else ctx.body_text
    if not _is_meaningful_summary(summary):
        raise InvalidTelegramPayload("summary_invalid")
    action_line = ctx.action_line.strip() or "Действий не требуется"
    if not action_line:
        raise InvalidTelegramPayload("missing_action")
    if ctx.attachments_count > 0 and "влож" not in text.lower():
        raise InvalidTelegramPayload("attachments missing")
    if _looks_like_subject_only(text, ctx.subject) and (
        ctx.body_text.strip() or ctx.attachments_count > 0
    ):
        raise InvalidTelegramPayload("subject_only")
    _validate_telegram_markup(text)
    return text


def _build_tg_fallback(
    *,
    priority: str = "🔵",
    subject: str,
    from_email: str,
    attachments: list[dict[str, Any]] | None = None,
    attachment_summary: str | None = None,
) -> str:
    if attachment_summary is None:
        return tg_renderer.build_tg_fallback(
            priority=priority,
            subject=subject,
            from_email=from_email,
            attachments=attachments or [],
        )
    safe_subject = escape_tg_html(subject or "(без темы)")
    safe_sender = escape_tg_html(from_email or "неизвестно")
    if attachment_summary is None:
        attachment_summary = _build_attachment_summary(
            _build_attachment_details(attachments or [])
        )
    if not attachment_summary:
        attachment_summary = "📎 Вложения: 0"
    lines = [
        "Письмо получено",
        f"От: {safe_sender}",
        f"Тема: {safe_subject}",
        "Основной текст не удалось безопасно отобразить.",
        attachment_summary,
    ]
    return "\n".join(lines)


def _build_tg_short_template(*, priority: str, subject: str, from_email: str) -> str:
    return tg_renderer.build_tg_short_template(
        priority=priority,
        subject=subject,
        from_email=from_email,
    )


def _build_tg_plain_text(
    *,
    priority: str,
    subject: str,
    from_email: str,
    action_line: str,
    attachments: list[dict[str, Any]],
) -> str:
    safe_subject = escape_tg_html(subject or "(без темы)")
    safe_sender = escape_tg_html(from_email or "неизвестно")
    resolved_action = escape_tg_html(_resolve_action_line(action_line))
    lines = [f"{priority} от {safe_sender}:", safe_subject, resolved_action]
    if attachments:
        lines.append(f"Вложений: {len(attachments)}")
    return "\n".join(lines)


def _build_insights_section(
    insights: list[Insight], digest: InsightDigest | None
) -> str:
    if not insights and digest is None:
        return ""
    lines = ["", "Insights"]
    if digest is not None:
        status = _sanitize_preview_line(digest.status_label)
        headline = _sanitize_preview_line(digest.headline)
        if status:
            lines.append(status)
        if headline:
            lines.append(headline)
        for line in (digest.short_explanation or "").split("\n"):
            cleaned = _sanitize_preview_line(line)
            if cleaned:
                lines.append(cleaned)
    for insight in insights:
        title = _sanitize_preview_line(insight.type)
        severity = _sanitize_preview_line(insight.severity)
        explanation = _sanitize_preview_line(insight.explanation)
        recommendation = _sanitize_preview_line(insight.recommendation)
        lines.append(f"• {title} ({severity})")
        lines.append(f"  {explanation}")
        lines.append(f"  Рекомендация: {recommendation}")
    return "\n".join(lines)


def build_telegram_payload(
    context: TelegramBuildContext,
) -> tuple[TelegramPayload, TelegramRenderMode, bool]:
    render_context = TelegramRenderContext(
        extracted_text_len=context.extracted_text_len,
        attachments_count=context.attachments_count,
        llm_failed=context.llm_failed,
        signal_invalid=context.signal_invalid,
    )
    render_mode = choose_tg_render_mode(render_context)
    payload_invalid = False
    fallback_reasons: list[str] = []
    telegram_text_raw = ""
    summary_valid = _is_meaningful_summary(context.body_summary)
    if not summary_valid:
        fallback_reasons.append("summary_invalid")
        event_emitter.emit(
            type="telegram_empty_summary",
            timestamp=context.received_at,
            email_id=context.email_id,
            payload={"summary": context.body_summary},
        )
    if context.attachments_count > 0 and context.extracted_text_len == 0:
        fallback_reasons.append("attachments_without_text")
    if context.llm_failed:
        fallback_reasons.append("llm_failed")
    if context.signal_invalid:
        fallback_reasons.append("signal_invalid")
    if summary_valid and render_mode == TelegramRenderMode.SHORT_TEMPLATE:
        render_mode = TelegramRenderMode.FULL
    if fallback_reasons:
        render_mode = TelegramRenderMode.SAFE_FALLBACK
        logger.warning(
            "tg_payload_invalid",
            email_id=context.email_id,
            reason=",".join(fallback_reasons),
            attachments=context.attachments_count,
            body_chars=len(context.body_text or ""),
        )
        event_emitter.emit(
            type="tg_payload_invalid",
            timestamp=context.received_at,
            email_id=context.email_id,
            payload={
                "reason": ",".join(fallback_reasons),
                "attachments": context.attachments_count,
                "body_chars": len(context.body_text or ""),
            },
        )

    if render_mode == TelegramRenderMode.FULL:
        try:
            telegram_text_raw = _build_telegram_text(
                priority=context.priority,
                from_email=context.from_email,
                subject=context.subject,
                action_line=context.action_line,
                body_summary=context.body_summary,
                body_text=context.body_text,
                attachments=context.attachment_files,
                attachment_summary=None,
            )
        except Exception as exc:
            logger.error("tg_render_failed", email_id=context.email_id, error=str(exc))
            render_mode = TelegramRenderMode.SAFE_FALLBACK
            payload_invalid = True
            fallback_reasons.append("render_failed")
    if render_mode == TelegramRenderMode.SAFE_FALLBACK:
        try:
            telegram_text_raw = _build_tg_fallback(
                priority=context.priority,
                subject=context.subject,
                from_email=context.from_email,
                attachments=context.attachment_files,
                attachment_summary=None,
            )
        except Exception as exc:
            logger.error("tg_fallback_render_failed", email_id=context.email_id, error=str(exc))
            telegram_text_raw = ""
    if render_mode == TelegramRenderMode.SHORT_TEMPLATE:
        try:
            telegram_text_raw = _build_tg_short_template(
                priority=context.priority,
                subject=context.subject,
                from_email=context.from_email,
            )
        except Exception as exc:
            logger.error("tg_short_render_failed", email_id=context.email_id, error=str(exc))
            telegram_text_raw = ""
    if not telegram_text_raw:
        telegram_text_raw = _build_tg_plain_text(
            priority=context.priority,
            subject=context.subject,
            from_email=context.from_email,
            action_line=context.action_line,
            attachments=context.attachment_files,
        )

    no_llm_summary = ""
    if context.attachments_count > 0 and (not summary_valid or context.signal_invalid):
        no_llm_summary = _build_no_llm_summary(
            context.attachment_files,
            context.commitments_present,
        )
    if no_llm_summary:
        telegram_text_raw = f"{telegram_text_raw}\n\n{no_llm_summary}"

    insights_section = _build_insights_section(
        context.insights, context.insight_digest
    )
    if insights_section:
        telegram_text_raw = f"{telegram_text_raw}{escape_tg_html(insights_section)}"

    if render_mode == TelegramRenderMode.FULL:
        ctx = EmailContext(
            subject=context.subject,
            from_email=context.from_email,
            body_text=context.body_text or "",
            attachments_count=context.attachments_count,
            summary=context.body_summary,
            action_line=_resolve_action_line(context.action_line),
        )
        try:
            telegram_text_raw = validate_tg_payload(telegram_text_raw, ctx)
        except InvalidTelegramPayload as exc:
            payload_invalid = True
            render_mode = TelegramRenderMode.SAFE_FALLBACK
            logger.warning(
                "tg_payload_invalid",
                email_id=context.email_id,
                reason=str(exc),
                attachments=context.attachments_count,
                body_chars=len(context.body_text or ""),
            )
            logger.warning(
                "payload_validation_failed",
                email_id=context.email_id,
                reason=str(exc),
                attachments=context.attachments_count,
                body_chars=len(context.body_text or ""),
            )
            event_emitter.emit(
                type="tg_payload_invalid",
                timestamp=context.received_at,
                email_id=context.email_id,
                payload={
                    "reason": str(exc),
                    "attachments": context.attachments_count,
                    "body_chars": len(context.body_text or ""),
                },
            )
            event_emitter.emit(
                type="payload_validation_failed",
                timestamp=context.received_at,
                email_id=context.email_id,
                payload={
                    "reason": str(exc),
                    "attachments": context.attachments_count,
                    "body_chars": len(context.body_text or ""),
                },
            )
            telegram_text_raw = _build_tg_fallback(
                priority=context.priority,
                subject=context.subject,
                from_email=context.from_email,
                attachments=context.attachment_files,
                attachment_summary=context.attachment_summary,
            )
    telegram_text = telegram_text_raw
    if render_mode != TelegramRenderMode.FULL or payload_invalid:
        fallback_reason = ",".join(fallback_reasons) if fallback_reasons else "payload_validation_failed"
        event_emitter.emit(
            type="telegram_payload_fallback_used",
            timestamp=context.received_at,
            email_id=context.email_id,
            payload={
                "reason": fallback_reason,
                "render_mode": render_mode.name,
            },
        )
    event_emitter.emit(
        type="telegram_payload_validated",
        timestamp=context.received_at,
        email_id=context.email_id,
        payload={
            "render_mode": render_mode.name,
            "payload_invalid": payload_invalid,
            "fallback_used": render_mode != TelegramRenderMode.FULL or payload_invalid,
        },
    )
    metadata = {
        "subject": context.subject,
        "sender": context.from_email,
        "extracted_text": context.body_text,
        "attachments_summary": context.attachment_summary,
        "attachments_count": context.attachments_count,
        "insights": {
            "digest": (
                {
                    "status_label": context.insight_digest.status_label,
                    "headline": context.insight_digest.headline,
                    "short_explanation": context.insight_digest.short_explanation,
                }
                if context.insight_digest
                else None
            ),
            "items": [
                {
                    "type": insight.type,
                    "severity": insight.severity,
                    "explanation": insight.explanation,
                    "recommendation": insight.recommendation,
                }
                for insight in context.insights
            ],
        },
    }
    metadata.update(context.metadata)
    payload = TelegramPayload(
        html_text=telegram_text,
        priority=context.priority,
        metadata=metadata,
    )
    assert "Сделать:" not in payload.html_text
    return payload, render_mode, payload_invalid


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
        "AI Preview",
        "",
        "Предлагаемое действие:",
        f"• {action_line}",
        "Причина:",
    ]
    lines.extend(f"• {reason}" for reason in safe_reasons)
    lines.append(f"Confidence: {confidence_value:.2f}")
    lines.append("")
    lines.append("[Принять] [Отклонить]")
    return "\n".join(lines)


def _append_commitments_preview(
    preview_text: str, commitments: list[Commitment]
) -> str:
    if not commitments:
        return preview_text
    lines = [preview_text, "", "Обязательства"]
    status_labels = {
        "pending": ("", "ожидается"),
        "fulfilled": ("", "выполнено"),
        "expired": ("", "просрочено"),
        "unknown": ("", "неизвестно"),
    }
    for commitment in commitments:
        safe_text = _sanitize_preview_line(commitment.commitment_text)
        icon, label = status_labels.get(
            commitment.status, ("", commitment.status or "неизвестно")
        )
        line = f"• \"{safe_text}\" — {label}".replace("  ", " ").strip()
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
        "Контекст отношений:",
        f"  Контрагент: {safe_sender}",
        f"  Надёжность обязательств: {label} {score}/100",
        f"  (выполнено: {fulfilled_count}, просрочено: {expired_count} за 30 дней)",
    ]
    return "\n".join(lines)


def _append_insights_preview(
    preview_text: str,
    insights: list[Insight],
) -> str:
    if not insights:
        return preview_text
    lines = [preview_text, "", "Insights"]
    for insight in insights:
        title = _sanitize_preview_line(insight.type)
        severity = _sanitize_preview_line(insight.severity)
        explanation = _sanitize_preview_line(insight.explanation)
        recommendation = _sanitize_preview_line(insight.recommendation)
        lines.append(f"• {title} ({severity})")
        lines.append(f"  {explanation}")
        lines.append(f"  Рекомендация: {recommendation}")
    return "\n".join(lines)


def _append_insight_digest_preview(
    preview_text: str,
    digest: InsightDigest | None,
) -> str:
    if digest is None:
        return preview_text
    status = _sanitize_preview_line(digest.status_label)
    headline = _sanitize_preview_line(digest.headline)
    lines = [preview_text, "", "Insight Digest", status, headline]
    if digest.short_explanation:
        for line in digest.short_explanation.split("\n"):
            clean_line = _sanitize_preview_line(line)
            if clean_line:
                lines.append(clean_line)
    return "\n".join(lines)


def _append_anomalies_preview(
    preview_text: str,
    anomalies: list[Anomaly],
) -> str:
    if not anomalies:
        return preview_text
    lines = [preview_text, "", "Signals"]
    for anomaly in anomalies:
        title = _sanitize_preview_line(anomaly.title)
        severity = _sanitize_preview_line(anomaly.severity)
        details = _sanitize_preview_line(anomaly.details)
        lines.append(f"• {title} ({severity})")
        if details:
            lines.append(f"  {details}")
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


def _build_context(
    *,
    message_id: int,
    from_email: str,
    from_name: str | None,
    subject: str,
    received_at: datetime,
    body_text: str,
) -> LlmContext:
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
            try:
                context_store.resolve_entity_relationships(
                    entity_id=entity_resolution.entity_id,
                    from_email=from_email,
                    from_name=from_name,
                    event_time=received_at,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("entity_relationship_resolution_failed", error=str(exc))
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
    return LlmContext(
        entity_resolution=entity_resolution,
        signal_quality=signal_quality,
        llm_body_text=llm_body_text,
        fallback_used=fallback_used,
    )


def _record_analytics(
    *,
    account_email: str,
    message_id: int,
    from_email: str,
    from_name: str | None,
    subject: str,
    received_at: datetime,
    body_text: str,
    llm_result: Any,
    llm_provider: str | None,
    priority: str,
    original_priority: str | None,
    priority_reason: str | None,
    shadow_priority_to_persist: str | None,
    shadow_priority_reason_to_persist: str | None,
    shadow_action_line_to_persist: str | None,
    shadow_action_reason_to_persist: str | None,
    confidence_score_to_persist: float | None,
    confidence_decision_to_persist: str | None,
    proposed_action_type_to_persist: str | None,
    proposed_action_text_to_persist: str | None,
    proposed_action_confidence_to_persist: float | None,
    confidence_score: float | None,
    shadow_priority: str,
    action_line: str,
    body_summary: str,
    attachment_summaries: list[dict[str, Any]],
    commitments: list[Commitment],
    enable_commitments: bool,
    entity_resolution: Any | None,
    signal_quality: Any,
    fallback_used: bool,
    telegram_chat_id: str,
) -> AnalyticsResult:
    commitment_status_updates: list[CommitmentStatusUpdate] = []
    commitment_signal_preview: dict[str, object] | None = None
    email_row_id: int | None = None
    trust_result = None
    health_snapshot = None
    temporal_insights: list[TemporalState] = []
    aggregated_insights: list[Insight] = []
    insight_digest: InsightDigest | None = None

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
                            _emit_contract_event(
                                EventType.COMMITMENT_STATUS_CHANGED,
                                ts_utc=received_at.timestamp(),
                                account_id=account_email,
                                entity_id=entity_resolution.entity_id if entity_resolution else None,
                                email_id=message_id,
                                payload={
                                    "commitment_id": update.commitment_id,
                                    "old_status": update.old_status,
                                    "new_status": update.new_status,
                                    "reason": update.reason,
                                    "deadline_iso": update.deadline_iso,
                                    "commitment_text": update.commitment_text,
                                    "from_email": from_email,
                                },
                            )
                            if update.new_status == "fulfilled":
                                logger.info(
                                    "commitment_fulfilled_detected",
                                    commitment_id=update.commitment_id,
                                    reason=update.reason,
                                )
                                event_emitter.emit(
                                    type="commitment_fulfilled",
                                    timestamp=received_at,
                                    email_id=message_id,
                                    payload={
                                        "commitment_id": update.commitment_id,
                                        "old_status": update.old_status,
                                        "new_status": update.new_status,
                                        "reason": update.reason,
                                        "deadline_iso": update.deadline_iso,
                                        "commitment_text": update.commitment_text,
                                    },
                                )
                            if update.new_status == "expired":
                                logger.info(
                                    "commitment_expired",
                                    commitment_id=update.commitment_id,
                                    reason=update.reason,
                                )
                                event_emitter.emit(
                                    type="commitment_expired",
                                    timestamp=received_at,
                                    email_id=message_id,
                                    payload={
                                        "commitment_id": update.commitment_id,
                                        "old_status": update.old_status,
                                        "new_status": update.new_status,
                                        "reason": update.reason,
                                        "deadline_iso": update.deadline_iso,
                                        "commitment_text": update.commitment_text,
                                    },
                                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "commitment_status_update_failed",
                    email_id=message_id,
                    sender=from_email,
                    error=str(exc),
                )

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
                    for commitment in commitments:
                        try:
                            event_emitter.emit(
                                type="commitment_created",
                                timestamp=received_at,
                                entity_id=(
                                    entity_resolution.entity_id
                                    if entity_resolution
                                    else None
                                ),
                                email_id=message_id,
                                payload={
                                    "commitment_text": commitment.commitment_text,
                                    "deadline_iso": commitment.deadline_iso,
                                    "status": commitment.status,
                                    "source": commitment.source,
                                    "confidence": commitment.confidence,
                                },
                            )
                            _emit_contract_event(
                                EventType.COMMITMENT_CREATED,
                                ts_utc=received_at.timestamp(),
                                account_id=account_email,
                                entity_id=entity_resolution.entity_id if entity_resolution else None,
                                email_id=message_id,
                                payload={
                                    "commitment_text": commitment.commitment_text,
                                    "deadline_iso": commitment.deadline_iso,
                                    "status": commitment.status,
                                    "source": commitment.source,
                                    "confidence": commitment.confidence,
                                    "from_email": from_email,
                                },
                            )
                        except Exception as exc:  # pragma: no cover - defensive logging
                            logger.error(
                                "commitment_event_failed",
                                email_id=message_id,
                                error=str(exc),
                            )
                else:
                    logger.error(
                        "commitments_persist_failed",
                        email_id=message_id,
                        error="commitments_save_failed",
                    )

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
            last_received = context_store.latest_interaction_event_time(
                entity_id=entity_resolution.entity_id,
                event_type="email_received",
            )
            if last_received and received_at > last_received:
                response_time_hours = (
                    received_at - last_received
                ).total_seconds() / 3600.0
                context_store.record_interaction_event(
                    entity_id=entity_resolution.entity_id,
                    event_type="response_time",
                    event_time=received_at,
                    metadata={
                        "response_time_hours": round(response_time_hours, 4),
                        "previous_received_at": last_received.isoformat(),
                    },
                )
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

    if entity_resolution and from_email:
        try:
            trust_result = trust_score_calculator.compute(
                entity_id=entity_resolution.entity_id,
                from_email=from_email,
            )
            trust_snapshot_writer.write(trust_result.snapshot)
            computed_at = (
                trust_result.snapshot.computed_at.isoformat()
                if trust_result.snapshot.computed_at
                else datetime.now(timezone.utc).isoformat()
            )
            logger.info(
                "trust_v2_computed",
                entity_id=entity_resolution.entity_id,
                trust_score=trust_result.snapshot.score,
                model_version=trust_result.snapshot.model_version,
                data_quality=trust_result.snapshot.data_quality,
                components={
                    "commitment": trust_result.components.commitment_reliability,
                    "response": trust_result.components.response_consistency,
                    "trend": trust_result.components.trend,
                },
                sample_size=trust_result.snapshot.sample_size,
                data_window_days=trust_result.data_window_days,
                computed_at=computed_at,
            )
            event_emitter.emit(
                type="trust_score_updated",
                timestamp=datetime.now(timezone.utc),
                entity_id=entity_resolution.entity_id,
                email_id=message_id,
                payload={
                    "trust_score": trust_result.snapshot.score,
                    "sample_size": trust_result.snapshot.sample_size,
                    "data_window_days": trust_result.data_window_days,
                    "model_version": trust_result.snapshot.model_version,
                    "data_quality": trust_result.snapshot.data_quality,
                    "computed_at": computed_at,
                    "components": {
                        "commitment": trust_result.components.commitment_reliability,
                        "response": trust_result.components.response_consistency,
                        "trend": trust_result.components.trend,
                    },
                },
            )
            _emit_contract_event(
                EventType.TRUST_SCORE_UPDATED,
                ts_utc=datetime.now(timezone.utc).timestamp(),
                account_id=account_email,
                entity_id=entity_resolution.entity_id,
                email_id=message_id,
                payload={
                    "trust_score": trust_result.snapshot.score,
                    "sample_size": trust_result.snapshot.sample_size,
                    "data_window_days": trust_result.data_window_days,
                    "model_version": trust_result.snapshot.model_version,
                    "data_quality": trust_result.snapshot.data_quality,
                    "computed_at": computed_at,
                    "components": {
                        "commitment": trust_result.components.commitment_reliability,
                        "response": trust_result.components.response_consistency,
                        "trend": trust_result.components.trend,
                    },
                },
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "trust_score_compute_failed",
                entity_id=entity_resolution.entity_id,
                error=str(exc),
            )

    if entity_resolution and from_email and trust_result is not None:
        try:
            health_snapshot = relationship_health_calculator.compute(
                entity_id=entity_resolution.entity_id,
                from_email=from_email,
                trust_score_result=trust_result,
            )
            relationship_health_snapshot_writer.write(health_snapshot)
            logger.info(
                "relationship_health_computed",
                entity_id=entity_resolution.entity_id,
                health_score=health_snapshot.health_score,
                trust_score=health_snapshot.components_breakdown.get("trust_score"),
                commitments_expired_30d=health_snapshot.components_breakdown.get(
                    "commitments_expired_30d"
                ),
                response_time_delta=health_snapshot.components_breakdown.get(
                    "response_time_delta"
                ),
                trend=health_snapshot.components_breakdown.get("trend_delta"),
                data_window_days=health_snapshot.data_window_days,
            )
            event_emitter.emit(
                type="relationship_health_updated",
                timestamp=datetime.now(timezone.utc),
                entity_id=entity_resolution.entity_id,
                email_id=message_id,
                payload={
                    "health_score": health_snapshot.health_score,
                    "reason": health_snapshot.reason,
                    "components": health_snapshot.components_breakdown,
                    "data_window_days": health_snapshot.data_window_days,
                },
            )
            _emit_contract_event(
                EventType.RELATIONSHIP_HEALTH_UPDATED,
                ts_utc=datetime.now(timezone.utc).timestamp(),
                account_id=account_email,
                entity_id=entity_resolution.entity_id,
                email_id=message_id,
                payload={
                    "health_score": health_snapshot.health_score,
                    "reason": health_snapshot.reason,
                    "components": health_snapshot.components_breakdown,
                    "data_window_days": health_snapshot.data_window_days,
                },
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "relationship_health_compute_failed",
                entity_id=entity_resolution.entity_id,
                error=str(exc),
            )
        else:
            try:
                anomalies = relationship_anomaly_detector.detect(
                    entity_id=entity_resolution.entity_id,
                    from_email=from_email,
                    trust_score_result=trust_result,
                    health_snapshot=health_snapshot,
                )
                if anomalies:
                    for anomaly in anomalies:
                        logger.info(
                            "relationship_anomaly_detected",
                            entity_id=anomaly.entity_id,
                            anomaly_type=anomaly.anomaly_type,
                            severity=anomaly.severity,
                            rhs_current=health_snapshot.health_score,
                            trust_score=trust_result.snapshot.score,
                            evidence=anomaly.evidence,
                        )
                else:
                    logger.info(
                        "relationship_anomaly_none",
                        entity_id=entity_resolution.entity_id,
                        rhs_current=health_snapshot.health_score,
                        trust_score=trust_result.snapshot.score,
                    )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "relationship_anomaly_detection_failed",
                    entity_id=entity_resolution.entity_id,
                    error=str(exc),
                )

    if entity_resolution:
        try:
            temporal_insights = temporal_reasoning_engine.evaluate(
                entity_id=entity_resolution.entity_id,
                from_email=from_email,
                now=datetime.now(timezone.utc),
            )
            if temporal_insights:
                logger.info(
                    "temporal_insights_detected",
                    entity_id=entity_resolution.entity_id,
                    insight_types=[state.state_type for state in temporal_insights],
                    severities=[state.severity for state in temporal_insights],
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "temporal_insight_failed",
                entity_id=entity_resolution.entity_id,
                error=str(exc),
            )

    if entity_resolution:
        try:
            aggregated_insights = aggregate_insights(
                temporal_insights,
                trust_result.snapshot.score if trust_result else None,
                health_snapshot,
            )
            if aggregated_insights:
                logger.info(
                    "insights_aggregated",
                    entity_id=entity_resolution.entity_id,
                    insight_types=[insight.type for insight in aggregated_insights],
                    severities=[insight.severity for insight in aggregated_insights],
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "insight_aggregation_failed",
                entity_id=entity_resolution.entity_id,
                error=str(exc),
            )

        try:
            insight_digest = build_insight_digest(
                aggregated_insights,
                trust_result.snapshot.score if trust_result else None,
                health_snapshot,
            )
            logger.info(
                "insight_digest_built",
                entity_id=entity_resolution.entity_id,
                status_label=insight_digest.status_label,
                headline=insight_digest.headline,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "insight_digest_failed",
                entity_id=entity_resolution.entity_id,
                error=str(exc),
            )

    return AnalyticsResult(
        email_row_id=email_row_id,
        commitment_status_updates=commitment_status_updates,
        commitment_signal_preview=commitment_signal_preview,
        trust_result=trust_result,
        health_snapshot=health_snapshot,
        temporal_insights=temporal_insights,
        aggregated_insights=aggregated_insights,
        insight_digest=insight_digest,
    )


def _render_notification(
    *,
    message_id: int,
    received_at: datetime,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str,
    body_summary: str,
    body_text: str,
    attachments: list[dict[str, Any]],
    llm_result: Any,
    signal_quality: Any,
    aggregated_insights: list[Insight],
    insight_digest: InsightDigest | None,
    telegram_chat_id: str,
    account_email: str,
    attachment_summaries: list[dict[str, Any]],
    commitments: list[Commitment],
) -> RenderResult:
    attachment_details = _build_attachment_details(attachments)
    attachment_summary = _build_attachment_summary(attachment_details)
    extracted_text_len = len(body_text or "")
    try:
        arbiter_result = apply_insight_arbiter(
            InsightArbiterInput(
                llm_summary=body_summary,
                extracted_text_len=extracted_text_len,
                attachment_details=attachment_details,
                commitments=commitments,
                email_id=message_id,
            )
        )
    except Exception as exc:  # pragma: no cover - safety net
        logger.error(
            "[INSIGHT-ARBITER] failed",
            email_id=message_id,
            error=str(exc),
        )
    else:
        body_summary = arbiter_result.summary
    build_context = TelegramBuildContext(
        email_id=message_id,
        received_at=received_at,
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=action_line,
        body_summary=body_summary,
        body_text=body_text or "",
        attachment_summary=attachment_summary,
        attachment_details=attachment_details,
        attachment_files=attachments,
        attachments_count=len(attachments),
        extracted_text_len=extracted_text_len,
        llm_failed=bool(_read_llm_field(llm_result, "failed", False))
        or bool(_read_llm_field(llm_result, "error", False)),
        signal_invalid=not signal_quality.is_usable,
        insights=aggregated_insights,
        insight_digest=insight_digest,
        commitments_present=bool(commitments),
        metadata={
            "chat_id": telegram_chat_id,
            "account_email": account_email,
            "action_line": action_line,
            "body_summary": body_summary,
            "attachment_summaries": attachment_summaries,
        },
    )
    payload, render_mode, payload_invalid = build_telegram_payload(build_context)
    return RenderResult(
        payload=payload,
        render_mode=render_mode,
        payload_invalid=payload_invalid,
        attachment_details=attachment_details,
        attachment_summary=attachment_summary,
        extracted_text_len=extracted_text_len,
        body_summary=body_summary,
    )


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

    NOTE: Поведение Telegram и LLM НЕ МЕНЯЕМ
    """

    try:
        system_snapshotter.maybe_log()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_health_snapshot_failed", error=str(exc))
    policy_inputs = _collect_policy_inputs()

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

    try:
        event_emitter.emit(
            type="email_received",
            timestamp=received_at,
            email_id=message_id,
            payload={
                "account_email": account_email,
                "from_email": from_email,
                "subject": subject,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("event_emit_failed", error=str(exc))

    # ---------- Stage LLM ----------
    llm_context = _build_context(
        message_id=message_id,
        from_email=from_email,
        from_name=from_name,
        subject=subject,
        received_at=received_at,
        body_text=body_text or "",
    )
    entity_resolution = llm_context.entity_resolution
    signal_quality = llm_context.signal_quality
    _emit_contract_event(
        EventType.EMAIL_RECEIVED,
        ts_utc=received_at.timestamp(),
        account_id=account_email,
        entity_id=entity_resolution.entity_id if entity_resolution else None,
        email_id=message_id,
        payload={
            "from_email": from_email,
            "subject": subject,
            "attachments_count": len(attachments),
        },
    )
    for attachment in attachments:
        _emit_contract_event(
            EventType.ATTACHMENT_EXTRACTED,
            ts_utc=received_at.timestamp(),
            account_id=account_email,
            entity_id=entity_resolution.entity_id if entity_resolution else None,
            email_id=message_id,
            payload={
                "filename": attachment.get("filename"),
                "content_type": attachment.get("content_type") or attachment.get("type"),
                "size_bytes": _attachment_size_bytes(attachment),
                "text_length": _attachment_text_length(attachment),
            },
        )
    llm_body_text = llm_context.llm_body_text
    fallback_used = llm_context.fallback_used
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

    policy_decision = _evaluate_policy(policy_inputs)
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
    if policy_decision.allow_auto_priority:
        try:
            auto_priority_outcome = auto_priority_engine.evaluate(
                llm_priority=priority,
                shadow_priority=shadow_priority,
                shadow_reason=shadow_reason,
                confidence_score=confidence_score,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("auto_priority_error", error=str(exc))
    elif feature_flags.ENABLE_AUTO_PRIORITY:
        auto_priority_outcome = AutoPriorityOutcome(
            final_priority=priority,
            original_priority=None,
            priority_reason=None,
            confidence_score=confidence_score,
            confidence_decision=None,
            gate_decision=None,
            applied=False,
            skipped_reason="policy_denied",
        )
        logger.info(
            "auto_priority_skipped",
            reason="policy_denied",
            email_id=message_id,
            system_mode=policy_decision.mode.value,
        )

    if auto_priority_outcome.applied:
        original_priority = auto_priority_outcome.original_priority
        priority = auto_priority_outcome.final_priority
        priority_reason = auto_priority_outcome.priority_reason
        confidence_decision = auto_priority_outcome.confidence_decision
    elif auto_priority_outcome.confidence_decision:
        confidence_decision = auto_priority_outcome.confidence_decision

    if policy_decision.allow_auto_priority and should_score_confidence:
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

    if policy_decision.allow_auto_priority or feature_flags.ENABLE_AUTO_PRIORITY:
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
        if not policy_decision.allow_preview:
            preview_skip_reason = (
                "system_degraded_no_llm"
                if policy_decision.mode == OperationalMode.DEGRADED_NO_LLM
                else "policy_denied"
            )
            logger.info(
                "preview_actions_skipped",
                reason=preview_skip_reason,
                email_id=message_id,
                account_email=account_email,
                system_mode=policy_decision.mode.value,
            )
        else:
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

    analytics_result = _record_analytics(
        account_email=account_email,
        message_id=message_id,
        from_email=from_email,
        from_name=from_name,
        subject=subject,
        received_at=received_at,
        body_text=body_text,
        llm_result=llm_result,
        llm_provider=llm_provider,
        priority=priority,
        original_priority=original_priority,
        priority_reason=priority_reason,
        shadow_priority_to_persist=shadow_priority_to_persist,
        shadow_priority_reason_to_persist=shadow_priority_reason_to_persist,
        shadow_action_line_to_persist=shadow_action_line_to_persist,
        shadow_action_reason_to_persist=shadow_action_reason_to_persist,
        confidence_score_to_persist=confidence_score_to_persist,
        confidence_decision_to_persist=confidence_decision_to_persist,
        proposed_action_type_to_persist=proposed_action_type_to_persist,
        proposed_action_text_to_persist=proposed_action_text_to_persist,
        proposed_action_confidence_to_persist=proposed_action_confidence_to_persist,
        confidence_score=confidence_score,
        shadow_priority=shadow_priority,
        action_line=action_line,
        body_summary=body_summary,
        attachment_summaries=attachment_summaries,
        commitments=commitments,
        enable_commitments=enable_commitments,
        entity_resolution=entity_resolution,
        signal_quality=signal_quality,
        fallback_used=fallback_used,
        telegram_chat_id=telegram_chat_id,
    )

    policy_decision = _evaluate_policy(policy_inputs)
    anomalies: list[Anomaly] = []
    if policy_decision.allow_anomaly_alerts and entity_resolution:
        try:
            anomalies = compute_anomalies(
                entity_id=entity_resolution.entity_id,
                analytics=analytics,
                now_dt=received_at,
            )
            logger.info(
                "anomaly_computed",
                email_id=message_id,
                entity_id=entity_resolution.entity_id,
                count=len(anomalies),
                max_severity=max_anomaly_severity(anomalies),
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "anomaly_compute_failed",
                email_id=message_id,
                entity_id=entity_resolution.entity_id,
                error=str(exc),
            )
            anomalies = []

    render_result = _render_notification(
        message_id=message_id,
        received_at=received_at,
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=action_line,
        body_summary=body_summary,
        body_text=body_text or "",
        attachments=attachments,
        llm_result=llm_result,
        signal_quality=signal_quality,
        aggregated_insights=analytics_result.aggregated_insights,
        insight_digest=analytics_result.insight_digest,
        telegram_chat_id=telegram_chat_id,
        account_email=account_email,
        attachment_summaries=attachment_summaries,
        commitments=commitments,
    )
    payload = render_result.payload
    render_mode = render_result.render_mode
    payload_invalid = render_result.payload_invalid
    logger.info(
        "tg_render_mode_selected",
        email_id=message_id,
        mode=render_mode.name,
        extracted_text_len=render_result.extracted_text_len,
        attachments=len(attachments),
    )
    deadlines_count = sum(
        1 for commitment in commitments if commitment.deadline_iso
    )
    relationship_health_delta: float | None = None
    if analytics_result.health_snapshot is not None:
        value = analytics_result.health_snapshot.components_breakdown.get("trend_delta")
        try:
            relationship_health_delta = float(value)
        except (TypeError, ValueError):
            relationship_health_delta = None
    deferred_for_digest = False
    attention_reason = "default_send"
    try:
        gate_result = apply_attention_gate(
            AttentionGateInput(
                priority=priority,
                commitments=commitments,
                deadlines_count=deadlines_count,
                insight_severity=max_insight_severity(
                    analytics_result.aggregated_insights
                ),
                attachments_only=render_result.extracted_text_len <= 0
                and len(attachments) > 0,
                relationship_health_delta=relationship_health_delta,
                email_id=message_id,
            )
        )
        deferred_for_digest = gate_result.deferred
        attention_reason = gate_result.reason
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.error(
            "[ATTENTION-GATE] failed",
            email_id=message_id,
            error=str(exc),
        )
        deferred_for_digest = False
        attention_reason = "gate_failed"

    if deferred_for_digest:
        if analytics_result.email_row_id is None:
            logger.error(
                "[ATTENTION-GATE] persistence_failed",
                email_id=message_id,
                reason="missing_email_row_id",
                attention_reason=attention_reason,
            )
        else:
            persisted = knowledge_db.mark_deferred_for_digest(
                email_row_id=analytics_result.email_row_id,
                deferred=True,
            )
            if not persisted:
                logger.error(
                    "[ATTENTION-GATE] persistence_failed",
                    email_id=message_id,
                    reason="crm_update_failed",
                    attention_reason=attention_reason,
                )
            else:
                logger.info(
                    "[ATTENTION-GATE] persisted",
                    email_id=message_id,
                    attention_reason=attention_reason,
                )

    if deferred_for_digest:
        logger.info(
            "telegram_deferred_for_digest",
            email_id=message_id,
            reason=attention_reason,
        )
        _emit_contract_event(
            EventType.ATTENTION_DEFERRED_FOR_DIGEST,
            ts_utc=received_at.timestamp(),
            account_id=account_email,
            entity_id=entity_resolution.entity_id if entity_resolution else None,
            email_id=message_id,
            payload={
                "reason": attention_reason,
                "attachments_only": render_result.extracted_text_len <= 0 and len(attachments) > 0,
                "attachments_count": len(attachments),
            },
        )
    else:
        telegram_delivered = False
        try:
            result = _coerce_delivery_result(
                enqueue_tg(email_id=message_id, payload=payload),
                email_id=message_id,
            )
            if not result.delivered:
                if result.retryable:
                    raise RuntimeError(result.error or "Telegram delivery failed")
                logger.error(
                    "telegram_delivery_non_retryable",
                    email_id=message_id,
                    chat_id=telegram_chat_id,
                    error=result.error or "unknown error",
                )
                change = system_health.update_component(
                    "Telegram",
                    False,
                    reason=result.error or "Telegram send failed",
                )
                _notify_system_mode_change(
                    change=change,
                    chat_id=telegram_chat_id,
                    account_email=account_email,
                )
                fallback_metadata = dict(payload.metadata)
                fallback_metadata.setdefault("chat_id", telegram_chat_id)
                fallback_metadata.setdefault("account_email", account_email)
                fallback_payload = TelegramPayload(
                    html_text="Telegram delivery failed. Check email client.",
                    priority="🔴",
                    metadata=fallback_metadata,
                )
                enqueue_tg(email_id=message_id, payload=fallback_payload)
                event_emitter.emit(
                    type="telegram_delivery_failed",
                    timestamp=received_at,
                    email_id=message_id,
                    payload={"error": result.error or "non-retryable failure"},
                )
                _emit_contract_event(
                    EventType.TELEGRAM_FAILED,
                    ts_utc=received_at.timestamp(),
                    account_id=account_email,
                    entity_id=entity_resolution.entity_id if entity_resolution else None,
                    email_id=message_id,
                    payload={"error": result.error or "non-retryable failure"},
                )
            else:
                telegram_delivered = True
                change = system_health.update_component("Telegram", True)
                _notify_system_mode_change(
                    change=change,
                    chat_id=telegram_chat_id,
                    account_email=account_email,
                )
                if render_mode != TelegramRenderMode.FULL or payload_invalid:
                    logger.warning(
                        "telegram_fallback_sent",
                        email_id=message_id,
                        chat_id=telegram_chat_id,
                        success=True,
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
            event_emitter.emit(
                type="telegram_delivery_failed",
                timestamp=received_at,
                email_id=message_id,
                payload={"error": str(exc)},
            )
            _emit_contract_event(
                EventType.TELEGRAM_FAILED,
                ts_utc=received_at.timestamp(),
                account_id=account_email,
                entity_id=entity_resolution.entity_id if entity_resolution else None,
                email_id=message_id,
                payload={"error": str(exc)},
            )
            logger.error(
                "processing_error",
                stage="telegram",
                email_id=message_id,
                error=str(exc),
            )
            raise
        if telegram_delivered:
            event_emitter.emit(
                type="telegram_delivery_succeeded",
                timestamp=received_at,
                email_id=message_id,
                payload={"render_mode": render_mode.name},
            )
            _emit_contract_event(
                EventType.TELEGRAM_DELIVERED,
                ts_utc=received_at.timestamp(),
                account_id=account_email,
                entity_id=entity_resolution.entity_id if entity_resolution else None,
                email_id=message_id,
                payload={"render_mode": render_mode.name},
            )

    if feature_flags.ENABLE_PREVIEW_ACTIONS:
        if not policy_decision.allow_preview:
            preview_skip_reason = (
                "system_degraded_no_llm"
                if policy_decision.mode == OperationalMode.DEGRADED_NO_LLM
                else "policy_denied"
            )
            logger.info(
                "preview_actions_skipped",
                reason=preview_skip_reason,
                email_id=message_id,
                account_email=account_email,
                system_mode=policy_decision.mode.value,
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
                system_mode=policy_decision.mode.value,
            )
            return

        preview_text = _build_preview_message(
            action_text=preview_actions[0],
            reasons=[reason for reason in (shadow_action_reason, shadow_reason, priority_reason) if reason],
            confidence=proposed_action.get("confidence") if proposed_action else None,
        )
        if enable_commitments and (
            commitments or analytics_result.commitment_status_updates
        ):
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
                    for update in analytics_result.commitment_status_updates
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
        if analytics_result.commitment_signal_preview:
            preview_text = _append_commitment_signal_preview(
                preview_text,
                from_email=from_email,
                score=int(analytics_result.commitment_signal_preview["score"]),
                label=str(analytics_result.commitment_signal_preview["label"]),
                fulfilled_count=int(
                    analytics_result.commitment_signal_preview["fulfilled_count"]
                ),
                expired_count=int(
                    analytics_result.commitment_signal_preview["expired_count"]
                ),
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
            system_mode=policy_decision.mode.value,
        )
