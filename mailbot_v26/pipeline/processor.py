# mailbot_v26/pipeline/processor.py

from __future__ import annotations

import json
import re
import sqlite3
import time
from functools import lru_cache
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Optional

from mailbot_v26.actions.auto_action_engine import AutoActionEngine
from mailbot_v26.budgets.consumer import BudgetConsumer
from mailbot_v26.budgets.gate import BudgetGate
from mailbot_v26.budgets.gate import BudgetGateConfig
from mailbot_v26.budgets.importance import (
    heuristic_importance,
    is_top_percentile,
    record_importance_score,
)
from mailbot_v26.behavior.attention_engine import (
    DeliveryContext,
    DeliveryMode,
    decide_delivery,
    score_email,
)
from mailbot_v26.behavior.deadlock_detector import maybe_emit_deadlock
from mailbot_v26.behavior.threading import compute_thread_key
from mailbot_v26.config.deadlock_policy import load_deadlock_policy_config
from mailbot_v26.config.delivery_policy import load_delivery_policy_config
from mailbot_v26.config.budget_policy import (
    load_budget_gate_config,
    load_budget_usage_config,
)
from mailbot_v26.config.flow_protection import (
    FlowProtectionConfig,
    load_flow_protection_config,
)
from mailbot_v26.config.llm_queue import LLMQueueConfig, load_llm_queue_config
from mailbot_v26.config.premium_clarity import load_premium_clarity_config
from mailbot_v26.config.premium_clarity import PremiumClarityConfig
from mailbot_v26.config_loader import (
    get_account_scope,
    load_telegram_ui_config,
    resolve_account_scope,
)
from mailbot_v26.facts.fact_extractor import FactExtractor
from mailbot_v26.domain.fact_snippets import pick_attachment_fact, pick_email_body_fact
from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier
from mailbot_v26.features import FeatureFlags
from mailbot_v26.config.auto_priority_gate import (
    AutoPriorityGateConfig,
    load_auto_priority_gate_config,
)
from mailbot_v26.config.budget_policy import BudgetUsageConfig
from mailbot_v26.config.deadlock_policy import DeadlockPolicyConfig
from mailbot_v26.insights.auto_priority_quality_gate import (
    AutoPriorityGateStateStore,
    AutoPriorityQualityGate,
)
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
from mailbot_v26.insights.aggregator import (
    Insight,
    aggregate_insights,
    append_narrative_insight,
)
from mailbot_v26.insights.digest import InsightDigest, build_insight_digest
from mailbot_v26.insights.narrative_composer import NarrativeResult, compose_narrative
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
from mailbot_v26.ui.branding import append_watermark
from mailbot_v26.ui.emoji_whitelist import strip_disallowed_emojis
from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.observability.decision_trace_v1 import (
    DecisionTraceV1,
    compute_decision_key,
    compute_model_fingerprint,
    get_default_decision_trace_emitter,
    sanitize_trace,
    to_canonical_json,
)
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.observability.metrics import (
    GateEvaluation,
    MetricsAggregator,
    SystemGates,
    SystemHealthSnapshotter,
)
from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.observability.notification_sla import (
    NotificationAlertStore,
    NotificationSLAResult,
    compute_notification_sla,
)
from mailbot_v26.observability.relationship_health_snapshot import (
    RelationshipHealthSnapshotWriter,
)
from mailbot_v26.observability.trust_snapshot import TrustSnapshotWriter
from mailbot_v26.priority.auto_engine import AutoPriorityEngine, AutoPriorityOutcome
from mailbot_v26.priority.confidence_engine import PriorityConfidenceEngine, PRIORITY_ORDER
from mailbot_v26.priority.auto_gates import AutoPriorityCircuitBreaker, AutoPriorityGates
from mailbot_v26.llm.runtime_flags import RuntimeFlags, RuntimeFlagStore
from mailbot_v26.priority.shadow_engine import ShadowPriorityEngine
from mailbot_v26.priority.priority_engine_v2 import (
    PriorityBreakdownItem,
    PriorityV2Config,
    PriorityEngineV2,
    PriorityResultV2,
    VipSenderMatcher,
)
from mailbot_v26.telegram.keyboard import build_priority_keyboard
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
from .tg_renderer import (
    _escape_dynamic,
    _normalize_attachment_text,
    _truncate_attachment_text,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.system.orchestrator import SystemOrchestrator, SystemPolicyDecision
from mailbot_v26.tasks.shadow_actions import ShadowActionEngine
from mailbot_v26.worker.telegram_sender import DeliveryResult, edit_telegram_message
from mailbot_v26.llm.request_queue import BackgroundLLMWorker, LLMRequest, LLMRequestQueue
from .signal_quality import evaluate_signal_quality
from mailbot_v26.ui.i18n import (
    DEFAULT_LOCALE,
    humanize_mail_type,
    humanize_mode,
    humanize_reason_codes,
    humanize_severity,
    t,
)

logger = get_logger("mailbot")

_SUBJECT_PREFIX_RE = re.compile(r"^(?:(?:re|fw|fwd)\s*:\s*)+", re.IGNORECASE)


def _normalize_subject_for_compare(text: str) -> str:
    working = (text or "").strip()
    if not working:
        return ""
    working = re.sub(r"<[^>]+>", " ", working)
    working = working.replace("\\", "/")
    working = re.sub(r"\s+", " ", working)
    while True:
        stripped = _SUBJECT_PREFIX_RE.sub("", working).strip()
        if stripped == working:
            break
        working = stripped
    return working.casefold()


def _maybe_drop_duplicate_subject_line(
    header_subject: str,
    body_lines: list[str],
) -> list[str]:
    if not body_lines:
        return body_lines
    first_line = (body_lines[0] or "").strip()
    if not first_line:
        return body_lines
    normalized_subject = _normalize_subject_for_compare(header_subject)
    normalized_first = _normalize_subject_for_compare(first_line)
    if normalized_subject and normalized_subject == normalized_first:
        return body_lines[1:]
    return body_lines

class _LazyFeatureFlags:
    def __init__(self) -> None:
        self._flags: FeatureFlags | None = None

    def _resolve(self) -> FeatureFlags:
        if self._flags is None:
            self._flags = FeatureFlags()
        return self._flags

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resolve(), name)


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
decision_trace_emitter = get_default_decision_trace_emitter()
shadow_priority_engine = ShadowPriorityEngine(analytics)
priority_engine_v2 = PriorityEngineV2(
    analytics,
    config=PriorityV2Config(),
    vip_senders=VipSenderMatcher(),
)
shadow_action_engine = ShadowActionEngine(analytics)
priority_confidence_engine = PriorityConfidenceEngine()
_UI_LOCALE = DEFAULT_LOCALE
auto_priority_gates = AutoPriorityGates(analytics)
auto_priority_breaker = AutoPriorityCircuitBreaker(analytics)
auto_priority_gate_config: AutoPriorityGateConfig | None = None
deadlock_policy: DeadlockPolicyConfig | None = None
premium_clarity_config: PremiumClarityConfig | None = None
budget_gate_config: BudgetGateConfig | None = None
budget_usage_config: BudgetUsageConfig | None = None
llm_queue_config: LLMQueueConfig | None = None
auto_priority_gate_state_store = AutoPriorityGateStateStore(knowledge_db)
auto_priority_quality_gate = AutoPriorityQualityGate(
    analytics=analytics,
    state_store=auto_priority_gate_state_store,
)
metrics_aggregator = MetricsAggregator(DB_PATH)
system_gates = SystemGates()
system_snapshotter = SystemHealthSnapshotter(metrics_aggregator, system_gates)
processing_span_recorder = ProcessingSpanRecorder(DB_PATH)
feature_flags = _LazyFeatureFlags()
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
    confidence_threshold=0.75
)
system_orchestrator = SystemOrchestrator()
notification_alert_store = NotificationAlertStore(DB_PATH)
MAX_TELEGRAM_WAIT_SECONDS = 180
_CORRECTION_CACHE_TTL = 60.0
_CORRECTION_COUNT_CACHE: dict[str, tuple[int, float]] = {}
_MIN_CORRECTIONS_FOR_PREVIEW = 10

_PREVIEW_CORRECTIONS_TTL_SECONDS = _CORRECTION_CACHE_TTL
_preview_corrections_cache = _CORRECTION_COUNT_CACHE
budget_gate = BudgetGate(DB_PATH, BudgetGateConfig(), emitter=contract_event_emitter)
budget_consumer = BudgetConsumer(budget_gate)
llm_request_queue: LLMRequestQueue | None = None
llm_worker: Optional[BackgroundLLMWorker] = None
_ORIGINAL_RUN_LLM_STAGE = run_llm_stage
_MODULE_CONFIG_DIR: Path | None = None


def configure_processor_config_dir(config_dir: Path) -> None:
    """Configure config search path for processor module-level loaders."""
    global _MODULE_CONFIG_DIR
    global auto_priority_gate_config, deadlock_policy, premium_clarity_config
    global budget_gate_config, budget_usage_config, llm_queue_config

    _MODULE_CONFIG_DIR = config_dir
    auto_priority_gate_config = load_auto_priority_gate_config(config_dir)
    deadlock_policy = load_deadlock_policy_config(config_dir)
    premium_clarity_config = load_premium_clarity_config(config_dir)
    budget_gate_config = load_budget_gate_config(config_dir)
    budget_usage_config = load_budget_usage_config(config_dir)
    llm_queue_config = load_llm_queue_config(config_dir)

    _load_auto_priority_gate_config_cached.cache_clear()
    _load_deadlock_policy_cached.cache_clear()
    _load_premium_clarity_config_cached.cache_clear()
    _load_budget_gate_config_cached.cache_clear()
    _load_budget_usage_config_cached.cache_clear()
    _load_llm_queue_config_cached.cache_clear()


@lru_cache(maxsize=1)
def _load_auto_priority_gate_config_cached() -> AutoPriorityGateConfig:
    return load_auto_priority_gate_config(_MODULE_CONFIG_DIR)


def get_auto_priority_gate_config() -> AutoPriorityGateConfig:
    if auto_priority_gate_config is not None:
        return auto_priority_gate_config
    return _load_auto_priority_gate_config_cached()


@lru_cache(maxsize=1)
def _load_deadlock_policy_cached() -> DeadlockPolicyConfig:
    return load_deadlock_policy_config(_MODULE_CONFIG_DIR)


def get_deadlock_policy_config() -> DeadlockPolicyConfig:
    if deadlock_policy is not None:
        return deadlock_policy
    return _load_deadlock_policy_cached()


@lru_cache(maxsize=1)
def _load_premium_clarity_config_cached() -> PremiumClarityConfig:
    return load_premium_clarity_config(_MODULE_CONFIG_DIR)


def get_premium_clarity_config() -> PremiumClarityConfig:
    if premium_clarity_config is not None:
        return premium_clarity_config
    return _load_premium_clarity_config_cached()


@lru_cache(maxsize=1)
def _load_budget_gate_config_cached() -> BudgetGateConfig:
    return load_budget_gate_config(_MODULE_CONFIG_DIR)


def get_budget_gate_config() -> BudgetGateConfig:
    if budget_gate_config is not None:
        return budget_gate_config
    return _load_budget_gate_config_cached()


@lru_cache(maxsize=1)
def _load_budget_usage_config_cached() -> BudgetUsageConfig:
    return load_budget_usage_config(_MODULE_CONFIG_DIR)


def get_budget_usage_config() -> BudgetUsageConfig:
    if budget_usage_config is not None:
        return budget_usage_config
    return _load_budget_usage_config_cached()


@lru_cache(maxsize=1)
def _load_llm_queue_config_cached() -> LLMQueueConfig:
    return load_llm_queue_config(_MODULE_CONFIG_DIR)


def get_llm_queue_config() -> LLMQueueConfig:
    if llm_queue_config is not None:
        return llm_queue_config
    return _load_llm_queue_config_cached()


def get_llm_request_queue() -> LLMRequestQueue:
    global llm_request_queue
    if llm_request_queue is None:
        llm_request_queue = LLMRequestQueue(
            max_size=get_llm_queue_config().llm_request_queue_size
        )
    return llm_request_queue


@dataclass(frozen=True, slots=True)
class PolicyInputs:
    metrics: dict[str, dict[str, float]] | None
    gates: GateEvaluation | None
    runtime_flags: RuntimeFlags
    notification_sla: NotificationSLAResult | None
    notification_sla: NotificationSLAResult | None


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


@dataclass(frozen=True, slots=True)
class MailTypeAttachment:
    filename: str | None
    content_type: str


@dataclass(frozen=True, slots=True)
class DeliverySLAOutcome:
    result: DeliveryResult
    delivery_mode: str
    elapsed_to_first_send_seconds: float
    edit_applied: bool


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


def _emit_decision_trace(
    trace: DecisionTraceV1,
    *,
    account_id: str,
    entity_id: str | None,
    email_id: int | None,
    ts_utc: float,
) -> None:
    event = EventV1(
        event_type=EventType.DECISION_TRACE_RECORDED,
        ts_utc=ts_utc,
        account_id=account_id,
        entity_id=entity_id,
        email_id=email_id,
        payload={
            "decision_kind": trace.decision_kind,
            "trace_schema": trace.trace_schema,
            "trace_version": trace.trace_version,
        },
        payload_json=to_canonical_json(trace),
    )
    decision_trace_emitter.emit(contract_event_emitter, event)


def _build_delivery_context(
    *,
    now_local: datetime,
    policy_config: DeliveryPolicyConfig,
    flow_config: FlowProtectionConfig | None,
    enable_circadian: bool,
    enable_flow_protection: bool,
    immediate_sent_last_hour: int,
) -> DeliveryContext:
    return DeliveryContext(
        now_local=now_local,
        immediate_sent_last_hour=immediate_sent_last_hour,
    )


def _count_recent_immediate_deliveries(
    *, account_email: str, since_ts: float
) -> int:
    if not account_email:
        return 0
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT payload
                FROM events_v1
                WHERE account_id = ?
                  AND event_type = ?
                  AND ts_utc >= ?
                """,
                (account_email, EventType.DELIVERY_POLICY_APPLIED.value, since_ts),
            ).fetchall()
    except sqlite3.OperationalError:
        return 0
    count = 0
    for row in rows:
        try:
            payload = json.loads(row[0] or "{}")
        except (TypeError, ValueError):
            payload = {}
        if str(payload.get("mode") or "").upper() == DeliveryMode.IMMEDIATE.value:
            count += 1
    return count


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
            mode="html",
            retry_count=0,
        )
    if result is None:
        logger.warning(
            "telegram_delivery_result_coerced",
            email_id=email_id,
            reason="none_returned",
        )
        return DeliveryResult(
            delivered=True,
            retryable=False,
            error=None,
            mode="html",
            retry_count=0,
        )
    logger.warning(
        "telegram_delivery_result_coerced",
        email_id=email_id,
        reason=f"unexpected_type:{type(result).__name__}",
    )
    return DeliveryResult(
        delivered=True,
        retryable=False,
        error=None,
        mode="html",
        retry_count=0,
    )


def _apply_delivery_sla(
    *,
    processing_started_at: float,
    wait_budget_seconds: float,
    minimal_payload: TelegramPayload,
    final_payload: TelegramPayload,
    send_func: Callable[[TelegramPayload], DeliveryResult],
    edit_func: Callable[[int, TelegramPayload], bool] | None,
    on_edit_failure: Callable[[str], None] | None,
    monotonic: Callable[[], float] = time.monotonic,
) -> DeliverySLAOutcome:
    initial_elapsed = monotonic() - processing_started_at
    delivery_mode = "final_first_send"
    first_payload = final_payload
    if initial_elapsed > wait_budget_seconds:
        delivery_mode = "minimal_then_edit"
        first_payload = minimal_payload
    result = send_func(first_payload)
    elapsed_to_first_send_seconds = max(0.0, monotonic() - processing_started_at)
    edit_applied = False
    if delivery_mode == "minimal_then_edit" and result.delivered:
        message_id = result.message_id
        if message_id and edit_func:
            edit_applied = bool(edit_func(message_id, final_payload))
            if not edit_applied and on_edit_failure:
                on_edit_failure("edit_failed")
        elif on_edit_failure:
            on_edit_failure("missing_message_id")
    return DeliverySLAOutcome(
        result=result,
        delivery_mode=delivery_mode,
        elapsed_to_first_send_seconds=elapsed_to_first_send_seconds,
        edit_applied=edit_applied,
    )


def _priority_confidence_percent(
    *,
    confidence_score: float | None,
    deadlines_count: int,
    commitments_count: int,
    attachments_only: bool,
    extracted_text_len: int,
    priority: str,
) -> int:
    if confidence_score is not None:
        try:
            raw_score = float(confidence_score) * 100
        except (TypeError, ValueError):
            raw_score = 0.0
        return max(0, min(100, int(round(raw_score))))
    score = 50
    if deadlines_count > 0:
        score += 20
    if commitments_count > 0:
        score += 15
    if attachments_only or extracted_text_len > 0:
        score += 10
    if PRIORITY_ORDER.get(priority, 0) >= 1:
        score += 10
    return max(0, min(100, score))


def _collect_policy_inputs() -> PolicyInputs:
    metrics: dict[str, dict[str, float]] | None = None
    gates: GateEvaluation | None = None
    runtime_flags = RuntimeFlags()
    notification_sla: NotificationSLAResult | None = None
    try:
        metrics = metrics_aggregator.snapshot()
        gates = system_gates.evaluate(metrics)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_policy_metrics_failed", error=str(exc))
    try:
        runtime_flags, _ = runtime_flag_store.get_flags()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_policy_runtime_flags_failed", error=str(exc))
    if getattr(feature_flags, "ENABLE_NOTIFICATION_SLA", True):
        try:
            notification_sla = compute_notification_sla(analytics=analytics)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("notification_sla_compute_failed", error=str(exc))
    return PolicyInputs(
        metrics=metrics,
        gates=gates,
        runtime_flags=runtime_flags,
        notification_sla=notification_sla,
    )


def _build_health_snapshot_payload() -> dict[str, Any]:
    try:
        metrics = metrics_aggregator.snapshot()
        gates_eval = system_gates.evaluate(metrics)
        return {
            "metrics": metrics,
            "gates": {
                "passed": gates_eval.passed if gates_eval else False,
                "failed": list(gates_eval.failed_reasons) if gates_eval else [],
            },
            "system_mode": system_health.mode.value,
        }
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("health_snapshot_payload_failed", error=str(exc))
        return {}


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
    gate_result = None
    auto_priority_gate_enabled = bool(
        getattr(get_auto_priority_gate_config(), "enabled", False)
    )
    quality_metrics_enabled = bool(
        getattr(feature_flags, "ENABLE_QUALITY_METRICS", False)
    )
    if auto_priority_gate_enabled and quality_metrics_enabled:
        engine_label = (
            "priority_v2_auto"
            if getattr(feature_flags, "ENABLE_AUTO_PRIORITY", False)
            else "priority_v2_shadow"
        )
        try:
            gate_result = auto_priority_quality_gate.evaluate(
                engine=engine_label,
                window_days=get_auto_priority_gate_config().window_days,
                min_samples=get_auto_priority_gate_config().min_samples,
                max_correction_rate=get_auto_priority_gate_config().max_correction_rate,
                cooldown_hours=get_auto_priority_gate_config().cooldown_hours,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("auto_priority_quality_gate_failed", error=str(exc))
    return system_orchestrator.evaluate(
        system_mode=system_mode,
        metrics=policy_inputs.metrics,
        gates=policy_inputs.gates,
        runtime_flags=policy_inputs.runtime_flags,
        feature_flags=feature_flags,
        telegram_ok=telegram_ok,
        has_daily_digest_content=has_daily_digest_content,
        has_weekly_digest_content=has_weekly_digest_content,
        auto_priority_gate_result=gate_result,
        auto_priority_gate_enabled=auto_priority_gate_enabled,
        enable_quality_metrics=quality_metrics_enabled,
        fallback_decision=fallback,
        notification_sla=policy_inputs.notification_sla,
    )


def _notification_alert_fingerprint(reasons: list[str]) -> str:
    return "|".join(sorted(reasons))


def _maybe_alert_notification_sla(
    *,
    account_email: str,
    telegram_chat_id: str,
    sla_result: NotificationSLAResult | None,
    consecutive_failures: int,
    telegram_delivered: bool | None = None,
) -> None:
    if telegram_delivered is False:
        return
    if not getattr(feature_flags, "ENABLE_NOTIFICATION_SLA", False):
        return
    reasons: list[str] = []
    if sla_result is not None:
        reasons.extend(sla_result.degraded_reasons())
    if consecutive_failures >= 3:
        reasons.append("consecutive_failures")
    if not reasons:
        return
    fingerprint = _notification_alert_fingerprint(reasons)
    now_dt = datetime.now(timezone.utc)
    if not notification_alert_store.should_alert(
        fingerprint=fingerprint, now=now_dt
    ):
        return
    delivery_pct = (
        f"{sla_result.delivery_rate_24h * 100:.1f}%" if sla_result else "н/д"
    )
    p90_latency = (
        f"{int(sla_result.p90_latency_24h)}с" if sla_result and sla_result.p90_latency_24h is not None else "н/д"
    )
    top_error = "н/д"
    if sla_result and sla_result.top_error_reasons_24h:
        top = sla_result.top_error_reasons_24h[0]
        top_error = f"{top.reason} ({top.share * 100:.1f}%)"
    action_hint = "текст без форматирования" if consecutive_failures >= 3 else "повторяем"
    alert_prefix = t("sla.alert.title", locale=_UI_LOCALE)
    alert_text = (
        f"{alert_prefix}\n"
        f"{t('sla.alert.delivery', locale=_UI_LOCALE)}: {delivery_pct}\n"
        f"{t('sla.alert.latency', locale=_UI_LOCALE)}: {p90_latency}\n"
        f"{t('sla.alert.top_error', locale=_UI_LOCALE)}: {top_error}\n"
        f"{t('sla.alert.action', locale=_UI_LOCALE)}: {action_hint}"
    )
    alert_text = append_watermark(alert_text, html=True)
    payload = TelegramPayload(
        html_text=escape_tg_html(alert_text),
        priority="\U0001F534",
        metadata={
            "chat_id": telegram_chat_id,
            "account_email": account_email,
        },
    )
    try:
        result = enqueue_tg(email_id=0, payload=payload)
        if result is not None and not result.delivered:
            logger.warning(
                "notification_sla_alert_send_failed",
                error=result.error or "send_failed",
            )
        notification_alert_store.save_alert(fingerprint, now=now_dt)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("notification_sla_alert_failed", error=str(exc))


@dataclass
class InboundMessage:
    subject: str
    body: str
    sender: str = ""
    mail_type: str = ""
    attachments: list[Attachment] = field(default_factory=list)
    received_at: datetime | None = None
    rfc_message_id: str | None = None
    in_reply_to: str | None = None
    references: str | None = None


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
    mail_type: str
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
    preview_hint: str | None = None
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
    premium_clarity_enabled: bool


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
        get_auto_priority_gate_config()
        get_deadlock_policy_config()
        get_premium_clarity_config()
        loaded_budget_gate_config = get_budget_gate_config()
        get_budget_usage_config()
        get_llm_queue_config()
        global budget_gate, budget_consumer
        budget_gate = BudgetGate(
            DB_PATH,
            loaded_budget_gate_config,
            emitter=contract_event_emitter,
        )
        budget_consumer = BudgetConsumer(budget_gate)
        self._last_ordinary_result: dict[str, Any] = {}

    def get_last_ordinary_result(self) -> dict[str, Any]:
        return dict(self._last_ordinary_result)

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
        action_line = self._normalize_action_subject(
            message.mail_type,
            subject,
            message.attachments or [],
            body_text,
        )
        summary = self._summarize_body(body_text, subject)
        attachments = self._build_attachment_summaries(message.attachments or [], subject)

        safe_sender = escape_tg_html(display_sender)
        safe_subject = escape_tg_html(subject)
        safe_summary = escape_tg_html(summary)
        safe_account_login = escape_tg_html(account_login)

        body_lines = _maybe_drop_duplicate_subject_line(subject, [subject])
        lines = [f"{priority} от {safe_sender} — {safe_subject}"]
        if body_lines:
            lines.append(f"<b>{escape_tg_html(body_lines[0])}</b>")
        lines.append(escape_tg_html(action_line))
        if safe_summary:
            lines.append(f"<i>{safe_summary}</i>")
        if attachments:
            lines.extend(self._render_attachments(attachments))
        lines.append(f"<i>аккаунт: {safe_account_login}</i>")
        text = "\n".join(lines)
        self._last_ordinary_result = {
            "text": text,
            "priority": priority,
            "attachments": [
                {
                    "filename": attachment.filename,
                    "text": attachment.text,
                    "content_type": attachment.content_type,
                    "size_bytes": attachment.size_bytes,
                    "metadata": attachment.metadata,
                }
                for attachment in (message.attachments or [])
            ],
        }
        return text

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
        mail_type: str,
        subject: str,
        attachments: list[Attachment],
        body_text: str = "",
    ) -> str:
        normalized_mail_type = (mail_type or "").strip().upper()
        if normalized_mail_type:
            if normalized_mail_type.startswith("ACT") or "RECONCILIATION" in normalized_mail_type:
                return "Проверить акт"
            if normalized_mail_type.startswith("INVOICE") or normalized_mail_type.startswith("PAYMENT_REMINDER"):
                return "Оплатить счёт"
            if normalized_mail_type.startswith("OVERDUE_INVOICE"):
                return "Оплатить счёт"
            if normalized_mail_type.startswith("CONTRACT") or "AMENDMENT" in normalized_mail_type:
                return "Проверить договор"

        lowered = " ".join(
            [
                (subject or "").lower(),
                (body_text or "").lower(),
                " ".join((att.text or "").lower() for att in attachments if att.text),
            ]
        )
        if any(token in lowered for token in ("недоступ", "offline", "авари", "инцидент", "security", "утечк", "взлом", "phish", "promo", "скидк", "распродаж")):
            return "Проверить"
        if any(token in lowered for token in ("счет", "счёт", "invoice", "оплат")):
            return "Оплатить счёт"
        if any(token in lowered for token in ("договор", "contract", "соглашени")):
            return "Проверить договор"
        if "прайс" in lowered or "цена" in lowered or "цен" in lowered:
            return "Проверить цены"
        if any(att.filename.lower().endswith((".xls", ".xlsx")) for att in attachments if att.filename):
            return "Проверить таблицу"
        return "Проверить письмо"


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


def _render_sources(
    *,
    subject: str,
    body_text: str,
    attachments: list[dict[str, Any]],
) -> list[str]:
    sources: list[str] = []
    if subject:
        sources.append("subject")
    if body_text:
        sources.append("body")
    if attachments:
        sources.append("attachment_filename")
    if any(_attachment_text_length(attachment) > 0 for attachment in attachments):
        sources.append("attachment_text")
    return sources


def _deadline_within_days(
    commitments: list[Commitment],
    *,
    received_at: datetime,
    days: int,
) -> bool:
    if days <= 0:
        return False
    for commitment in commitments:
        if not commitment.deadline_iso:
            continue
        try:
            deadline_date = datetime.fromisoformat(commitment.deadline_iso).date()
        except ValueError:
            continue
        delta_days = (deadline_date - received_at.date()).days
        if 0 <= delta_days <= days:
            return True
    return False


_TELEGRAM_BODY_LIMIT = 800
_MIN_TELEGRAM_LEN = 40
_MIN_SUMMARY_WORDS = 2
_MIN_SUMMARY_CHARS = 12
_ALLOWED_TG_TAGS = {"<b>", "</b>", "<i>", "</i>", "<tg-spoiler>", "</tg-spoiler>"}
_SUMMARY_PLACEHOLDER_PATTERNS = (
    "проверить письмо",
    "проверь письмо",
    "check email",
    "check mail",
)

_FACT_EXTRACTOR = FactExtractor()
_SUMMARY_NUMBER_PATTERN = re.compile(r"\d+(?:[ \u00A0]?\d+)*(?:[.,]\d+)?")
_DATE_VALUE_PATTERN = re.compile(r"\b\d{1,2}\.\d{1,2}\.(?:\d{2}|\d{4})\b")


@dataclass(frozen=True, slots=True)
class _FactItem:
    label: str
    value: str
    tag: str


def _has_high_risk(insights: list[Insight]) -> bool:
    for insight in insights:
        if insight.severity.upper() != "HIGH":
            continue
        if "risk" in insight.type.lower():
            return True
    return False


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
    lines = [f"Вложения: {len(details)}", f"Всего текста: {total_chars} chars"]
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
    body_text: str,
    attachments: list[dict[str, Any]],
    commitments_present: bool,
) -> str:
    source_lines: list[str] = []
    if body_text.strip():
        source_lines.append(body_text.strip())
    for attachment in attachments:
        text = str(attachment.get("text") or "").strip()
        if text:
            source_lines.append(text)
    if not source_lines:
        return ""

    source = "\n".join(source_lines)
    cleaned = " ".join(source.split())
    if not cleaned:
        return ""

    cleaned = re.sub(r"(?i)sent from my iphone.*$", "", cleaned).strip()
    cleaned = re.sub(r"(?i)this e-mail.*confidential.*$", "", cleaned).strip()
    if not cleaned:
        return ""

    sentence_parts = re.split(r"(?<=[.!?])\s+", cleaned)
    meaningful: list[str] = []
    for part in sentence_parts:
        line = part.strip(" \t\n\r-•")
        if len(line) < 8:
            continue
        lowered = line.lower()
        if any(
            marker in lowered
            for marker in (
                "disclaimer",
                "конфиденциаль",
                "не является публичной офертой",
            )
        ):
            continue
        meaningful.append(line)
        if len(meaningful) == 2:
            break

    if meaningful:
        return "Коротко: " + " ".join(meaningful)
    fallback = cleaned[:220].rstrip()
    if fallback:
        return f"Коротко: {fallback}"
    return ""


def _normalize_fact_value(value: str) -> str:
    cleaned = " ".join((value or "").split())
    cleaned = strip_disallowed_emojis(cleaned)
    return cleaned


def _render_fact_tag(tag: str) -> str:
    if not tag:
        return ""
    return f" ({tag})"


def _fact_items_from_text(text: str, *, tag: str) -> list[_FactItem]:
    if not text:
        return []
    facts = _FACT_EXTRACTOR.extract_facts(text)
    date_matches = {
        match.group(0) for match in _DATE_VALUE_PATTERN.finditer(text)
    }
    items: list[_FactItem] = []
    for amount in facts.amounts:
        value = _normalize_fact_value(amount)
        if value:
            if date_matches and any(value in date for date in date_matches):
                continue
            items.append(_FactItem(label="Сумма", value=value, tag=tag))
    for date_value in facts.dates:
        value = _normalize_fact_value(date_value)
        if value:
            items.append(_FactItem(label="Дата", value=value, tag=tag))
    for doc_number in facts.doc_numbers:
        value = _normalize_fact_value(doc_number)
        if value:
            items.append(_FactItem(label="Номер", value=value, tag=tag))
    return items


def _summary_numbers_supported(summary: str, *, subject: str, body_text: str) -> bool:
    summary_numbers = {match.group(0) for match in _SUMMARY_NUMBER_PATTERN.finditer(summary or "")}
    if not summary_numbers:
        return True
    source_text = f"{subject}\n{body_text}".strip()
    source_numbers = {
        match.group(0) for match in _SUMMARY_NUMBER_PATTERN.finditer(source_text)
    }
    return summary_numbers.issubset(source_numbers)


def _collect_fact_items(
    *,
    subject: str,
    body_text: str,
    attachments: list[dict[str, Any]],
) -> list[_FactItem]:
    items: list[_FactItem] = []
    items.extend(_fact_items_from_text(subject, tag="тема"))
    items.extend(_fact_items_from_text(body_text, tag="письмо"))
    attachment_items: list[_FactItem] = []
    attachment_tags: dict[tuple[str, str], set[str]] = {}
    for attachment in attachments:
        raw_filename = str(attachment.get("filename") or "").strip()
        if not raw_filename:
            continue
        attachment_text = attachment.get("text") or ""
        if isinstance(attachment_text, bytes):
            attachment_text = attachment_text.decode(errors="ignore")
        attachment_text = str(attachment_text)
        if not attachment_text.strip():
            continue
        tag = _escape_dynamic(strip_disallowed_emojis(raw_filename))
        for item in _fact_items_from_text(attachment_text, tag=tag):
            attachment_items.append(item)
            key = (item.label, item.value)
            attachment_tags.setdefault(key, set()).add(tag)
    for item in attachment_items:
        tags = attachment_tags.get((item.label, item.value), set())
        if len(tags) <= 1:
            items.append(item)
        else:
            items.append(_FactItem(label=item.label, value=item.value, tag=""))
    return items


def _contains_numeric_fact(text: str) -> bool:
    return bool(_SUMMARY_NUMBER_PATTERN.search(text or ""))


def _strip_numeric_facts(text: str) -> str:
    cleaned = _SUMMARY_NUMBER_PATTERN.sub(" ", text or "")
    return re.sub(r"\s+", " ", cleaned).strip()


def _strip_attachment_numeric_summary(text: str) -> str:
    cleaned = _strip_numeric_facts(text)
    cleaned = re.sub(r"[№#]+", " ", cleaned)
    cleaned = re.sub(r"[₽$€]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.strip(" -–—:;,.")


def _should_suppress_numeric_facts(
    *,
    extraction_failed: bool,
    confidence_available: bool,
    confidence_percent: int,
    confidence_dots_threshold: int,
) -> bool:
    if extraction_failed:
        return True
    if not confidence_available:
        return True
    return confidence_percent < confidence_dots_threshold


def _select_premium_clarity_fact_items(
    *,
    subject: str,
    body_text: str,
    attachments: list[dict[str, Any]],
    suppress_numeric_facts: bool,
) -> list[_FactItem]:
    if suppress_numeric_facts:
        return []
    items = _collect_fact_items(
        subject=subject,
        body_text=body_text,
        attachments=attachments,
    )
    seen: set[tuple[str, str, str]] = set()
    selected: list[_FactItem] = []
    for item in items:
        key = (item.label, item.value, item.tag)
        if key in seen:
            continue
        seen.add(key)
        selected.append(item)
        if len(selected) >= 2:
            break
    return selected


def _build_premium_clarity_facts(
    *,
    subject: str,
    body_text: str,
    attachments: list[dict[str, Any]],
    suppress_numeric_facts: bool,
) -> str:
    selected = _select_premium_clarity_fact_items(
        subject=subject,
        body_text=body_text,
        attachments=attachments,
        suppress_numeric_facts=suppress_numeric_facts,
    )
    rendered = [
        f"{item.label}: {item.value}{_render_fact_tag(item.tag)}"
        for item in selected
    ]
    return "; ".join(rendered)


def _fact_type_label(label: str) -> str:
    return {
        "Сумма": "amount",
        "Дата": "date",
        "Номер": "doc_number",
    }.get(label, "other")


def _fact_source_tag(tag: str) -> str:
    if not tag:
        return ""
    if tag == "тема":
        return "subject"
    if tag == "письмо":
        return "body"
    return "attachment"


def _confidence_bucket(
    *,
    confidence_available: bool,
    confidence_percent: int,
) -> str:
    if not confidence_available:
        return "na"
    if confidence_percent >= 75:
        return "hi"
    if confidence_percent >= 50:
        return "med"
    return "low"




def _build_premium_clarity_attachments(
    attachments: list[dict[str, Any]],
    attachment_summaries: list[dict[str, Any]],
    *,
    suppress_numeric_facts: bool,
) -> list[str]:
    summary_by_name: dict[str, str] = {}
    for summary in attachment_summaries:
        filename = str(summary.get("filename") or "").strip()
        if not filename:
            continue
        summary_text = str(summary.get("summary") or "").strip()
        if not summary_text:
            continue
        summary_by_name[filename.lower()] = summary_text
    lines = [f"📎 Вложения ({len(attachments)}):"]
    for attachment in attachments[:3]:
        raw_filename = attachment.get("filename") or "вложение"
        filename = _escape_dynamic(strip_disallowed_emojis(raw_filename))
        summary_text = summary_by_name.get(str(raw_filename).strip().lower(), "")
        summary_text = _normalize_attachment_text(summary_text)
        if summary_text:
            summary_text = _truncate_attachment_text(summary_text)
        if summary_text and suppress_numeric_facts:
            summary_text = _strip_attachment_numeric_summary(summary_text)
        if summary_text:
            safe_text = _escape_dynamic(strip_disallowed_emojis(summary_text))
            lines.append(f"• {filename} — {safe_text}")
        else:
            lines.append(f"• {filename}")
    remaining = len(attachments) - 3
    if remaining > 0:
        lines.append(f"... и ещё {remaining}")
    return lines


def _format_confidence_dots(confidence_percent: int, scale: int) -> str:
    dots_scale = scale if scale in {5, 10} else 10
    filled = min(dots_scale, (confidence_percent * dots_scale) // 100)
    empty = dots_scale - filled
    return f"{'●' * filled}{'○' * empty}"


def _should_show_confidence_dots(
    *,
    mode: str,
    threshold: int,
    confidence_available: bool,
    confidence_percent: int,
) -> bool:
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"auto", "always", "never"}:
        normalized_mode = "auto"
    if normalized_mode == "never":
        return False
    if not confidence_available:
        return False
    if normalized_mode == "always":
        return True
    return confidence_percent < threshold


def _build_premium_clarity_spoiler_lines(
    lines: list[str],
    *,
    dots: str = "",
) -> list[str]:
    if not lines:
        return []
    sanitized = []
    for line in lines:
        cleaned = (line or "").strip()
        if cleaned:
            sanitized.append(_escape_dynamic(strip_disallowed_emojis(cleaned)))
    if not sanitized:
        return []
    limited = sanitized[:6]
    closing = "</tg-spoiler>"
    if dots:
        closing = f"{closing} {dots}"
    return ["<tg-spoiler>", "Подробнее:", *limited, closing]


def _enforce_premium_clarity_line_budget(
    *,
    base_lines: list[str],
    primary_fact_line: str,
    attachment_lines: list[str],
    action_line: str,
    optional_lines: list[str],
    spoiler_lines: list[str],
    dots_text: str,
    max_lines: int = 18,
) -> list[str]:
    def _build_lines(
        *,
        spoiler_details: list[str],
        optional_details: list[str],
    ) -> list[str]:
        lines = [*base_lines]
        if primary_fact_line:
            lines.append(primary_fact_line)
        lines.extend(attachment_lines)
        lines.append(action_line)
        lines.extend(optional_details)
        spoiler_block = _build_premium_clarity_spoiler_lines(
            spoiler_details,
            dots=dots_text,
        )
        if spoiler_block:
            lines.extend(spoiler_block)
        return lines

    current_optional = list(optional_lines)
    current_spoiler = list(spoiler_lines)
    lines = _build_lines(
        spoiler_details=current_spoiler,
        optional_details=current_optional,
    )
    if len(lines) <= max_lines:
        return lines

    if current_spoiler:
        trimmed_spoiler = list(current_spoiler)
        while trimmed_spoiler and len(
            _build_lines(
                spoiler_details=trimmed_spoiler,
                optional_details=current_optional,
            )
        ) > max_lines:
            trimmed_spoiler.pop()
        current_spoiler = trimmed_spoiler
        lines = _build_lines(
            spoiler_details=current_spoiler,
            optional_details=current_optional,
        )
        if len(lines) <= max_lines:
            return lines

    if current_optional:
        trimmed_optional = list(current_optional)
        while trimmed_optional and len(
            _build_lines(
                spoiler_details=current_spoiler,
                optional_details=trimmed_optional,
            ),
            html=True,
        ) > max_lines:
            trimmed_optional.pop()
        current_optional = trimmed_optional
        lines = _build_lines(
            spoiler_details=current_spoiler,
            optional_details=current_optional,
        )
        if len(lines) <= max_lines:
            return lines

    lines = _build_lines(
        spoiler_details=[],
        optional_details=current_optional,
    )
    return lines[:max_lines]


def _pick_action_emoji(action_text: str) -> str:
    if _is_urgent_action(action_text):
        return "⚡"
    if not action_text:
        return "⚡"
    lowered_action = action_text.lower()
    if any(token in lowered_action for token in ("ответ", "напис", "сообщ", "reply")):
        return "💬"
    if any(token in lowered_action for token in ("позже", "отлож", "pause")):
        return "⏸️"
    return "⚡"


def _is_urgent_action(action_text: str) -> bool:
    lowered_action = (action_text or "").lower()
    if not lowered_action:
        return False
    urgency_tokens = (
        "срочно",
        "немед",
        "как можно скорее",
        "сегодня",
        "оплат",
        "счет",
        "invoice",
        "до конца дня",
    )
    return any(token in lowered_action for token in urgency_tokens)


def _count_marker_hits(text: str, markers: tuple[str, ...]) -> int:
    return sum(1 for marker in markers if marker and marker in text)


def _select_premium_short_action(
    *,
    normalized_mail_type: str,
    normalized_subject: str,
    normalized_body: str,
    normalized_action: str,
    normalized_evidence: str,
) -> str:
    tech_security_markers = (
        "недоступен",
        "недоступна",
        "недоступно",
        "offline",
        "outage",
        "device unreachable",
        "не удается подключиться",
        "не удаётся подключиться",
        "security alert",
        "важное оповещение",
        "подозрительный вход",
        "авар",
        "сбой",
    )
    reconciliation_markers = ("акт сверки", "reconciliation")
    signed_markers = (
        "signed contract",
        "signed agreement",
        "подписан договор",
        "подписано",
    )
    promo_markers = (
        "fyi",
        "к сведению",
        "for your information",
        "информацион",
        "ознаком",
        "промо",
        "рассылк",
        "инвести",
        "доходност",
        "вебинар",
        "акция",
    )
    invoice_markers = (
        "invoice",
        "счет",
        "счёт",
        "счет №",
        "счёт №",
        "к оплате",
        "срок оплаты",
        "оплатить до",
        "сумма",
        "итого",
        "всего",
        "оплат",
    )
    strong_invoice_phrases = (
        "счет на оплат",
        "счёт на оплат",
        "invoice",
        "к оплате",
        "срок оплаты",
        "оплатить до",
        "счет №",
        "счёт №",
    )

    if any(marker in normalized_evidence for marker in tech_security_markers):
        return "Проверить"
    if normalized_mail_type.startswith("ACT") or "RECONCILIATION" in normalized_mail_type:
        return "Сверить"
    if any(marker in normalized_evidence for marker in reconciliation_markers):
        return "Сверить"
    if "SIGNED" in normalized_mail_type and any(
        token in normalized_mail_type for token in ("CONTRACT", "AGREEMENT")
    ):
        return "Зафиксировать"
    if any(marker in normalized_evidence for marker in signed_markers):
        return "Зафиксировать"
    if any(marker in normalized_evidence for marker in promo_markers):
        return "Ознакомиться"

    subject_invoice_hits = _count_marker_hits(normalized_subject, invoice_markers)
    body_invoice_hits = _count_marker_hits(normalized_body, invoice_markers)
    evidence_invoice_hits = _count_marker_hits(normalized_evidence, invoice_markers)
    strong_invoice_signal = any(
        phrase in normalized_subject or phrase in normalized_body
        for phrase in strong_invoice_phrases
    )
    invoice_signal = strong_invoice_signal or (
        subject_invoice_hits >= 1 and (body_invoice_hits >= 1 or evidence_invoice_hits >= 2)
    )
    if normalized_mail_type.startswith("INVOICE") or "PAYMENT_REMINDER" in normalized_mail_type:
        invoice_signal = invoice_signal or (subject_invoice_hits + body_invoice_hits >= 1)
    if not invoice_signal:
        action_invoice_hits = _count_marker_hits(normalized_action, invoice_markers)
        invoice_signal = action_invoice_hits >= 2 or any(
            phrase in normalized_action for phrase in strong_invoice_phrases
        )
    if invoice_signal:
        return "Оплатить"

    if any(token in normalized_action for token in ("ответ", "reply", "напис")):
        return "Ответить"
    return "Проверить"


def _build_premium_clarity_text(
    *,
    priority: str,
    received_at: datetime,
    from_email: str,
    from_name: str | None,
    subject: str,
    mail_type: str,
    action_line: str,
    body_summary: str,
    body_text: str,
    attachments: list[dict[str, Any]],
    attachment_summaries: list[dict[str, Any]],
    insights: list[Insight],
    insight_digest: InsightDigest | None,
    commitments: list[Commitment],
    attachments_count: int,
    extracted_text_len: int,
    confidence_percent: int,
    confidence_available: bool,
    confidence_dots_mode: str,
    confidence_dots_threshold: int,
    confidence_dots_scale: int,
    extraction_failed: bool,
) -> str:
    priority = strip_disallowed_emojis(priority or "")
    if priority not in {"🔴", "🟡", "🔵"}:
        priority = "🔵"
    sender_display = from_email or from_name or "неизвестно"
    safe_sender = _escape_dynamic(strip_disallowed_emojis(sender_display))
    safe_subject = _escape_dynamic(strip_disallowed_emojis(subject or "(без темы)"))
    action_text = strip_disallowed_emojis(_resolve_action_line(action_line))
    normalized_mail_type = (mail_type or "").strip().upper()
    normalized_action = re.sub(r"[\W_]+", " ", action_text.lower()).strip()
    normalized_subject = re.sub(r"[\W_]+", " ", (subject or "").lower()).strip()
    normalized_body = re.sub(
        r"[\W_]+",
        " ",
        " ".join(part for part in (body_summary, body_text) if part).lower(),
    ).strip()
    evidence_text = " ".join(
        part
        for part in (
            action_text,
            subject,
            body_summary,
            body_text,
            " ".join(str(attachment.get("filename") or "") for attachment in attachments),
            " ".join(str(item.get("summary") or "") for item in attachment_summaries),
        )
        if part
    )
    normalized_evidence = re.sub(r"[\W_]+", " ", evidence_text.lower()).strip()
    short_action = _select_premium_short_action(
        normalized_mail_type=normalized_mail_type,
        normalized_subject=normalized_subject,
        normalized_body=normalized_body,
        normalized_action=normalized_action,
        normalized_evidence=normalized_evidence,
    )
    safe_action = _escape_dynamic(short_action)
    excerpt_source = (body_summary or "").strip() or (body_text or "").strip()
    excerpt = tg_renderer._clean_excerpt(excerpt_source, max_lines=3)

    if attachments:
        first_name = _escape_dynamic(strip_disallowed_emojis(str(attachments[0].get("filename") or "вложение")))
        if len(attachments) == 1:
            attachment_line = f"📎 1 вложение: {first_name}"
        else:
            attachment_line = f"📎 {len(attachments)} вложения"
    else:
        attachment_line = "📎 0 вложений"

    lines = [
        f"{priority} от {safe_sender}:",
        safe_subject,
        safe_action,
        "",
        attachment_line,
    ]
    if excerpt:
        lines.extend(
            _escape_dynamic(strip_disallowed_emojis(line))
            for line in excerpt.splitlines()[:3]
            if line.strip()
        )

    deduped_lines = tg_renderer.dedup_rendered_lines(lines)
    return append_watermark("\n".join(deduped_lines), html=True)


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


def _build_minimal_telegram_payload(
    *,
    priority: str,
    from_email: str,
    subject: str,
    attachments: list[dict[str, Any]],
    metadata: dict[str, Any],
    reply_markup: dict[str, Any] | None,
) -> TelegramPayload:
    minimal_attachments: list[dict[str, Any]] = []
    for attachment in attachments:
        minimal_attachments.append(
            {
                "filename": attachment.get("filename") or "",
                "content_type": attachment.get("content_type")
                or attachment.get("type")
                or "",
                "text": "",
            }
        )
    minimal_text = tg_renderer.build_minimal_telegram_text(
        priority=priority,
        from_email=from_email,
        subject=subject,
        attachments=minimal_attachments,
    )
    trimmed_text = _trim_telegram_body(minimal_text)
    return TelegramPayload(
        html_text=trimmed_text,
        priority=priority,
        metadata=metadata,
        reply_markup=reply_markup,
    )


def _build_telegram_text(
    *,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str,
    mail_type: str = "",
    body_summary: str,
    body_text: str,
    attachments: list[dict[str, Any]] | None = None,
    attachment_summary: str | None = None,
) -> str:
    if attachment_summary is None:
        rendered = tg_renderer.render_telegram_message(
            priority=priority,
            from_email=from_email,
            subject=subject,
            action_line=_resolve_action_line(action_line),
            summary=body_summary,
            attachments=attachments or [],
            mail_type=mail_type,
        )
        return append_watermark(rendered, html=True)
    fields = tg_renderer.apply_semantic_gates(
        action_line=_resolve_action_line(action_line),
        summary=body_summary,
    )
    safe_sender = escape_tg_html(from_email or "неизвестно")
    safe_subject = escape_tg_html(subject or "(без темы)")
    safe_action = escape_tg_html(_resolve_action_line(fields.action_line))
    safe_summary = escape_tg_html(fields.summary or "")
    body_lines = tg_renderer._maybe_drop_duplicate_subject_line(
        subject,
        [fields.action_line],
    )
    lines = [f"{priority} от {safe_sender} — {safe_subject}"]
    if body_lines:
        lines.append(escape_tg_html(body_lines[0]))
    if safe_summary:
        lines.append(safe_summary)
    if attachment_summary is None and attachments:
        attachment_summary = _build_attachment_summary(
            _build_attachment_details(attachments)
        )
    if attachment_summary:
        lines.append(attachment_summary)
    return append_watermark(tg_renderer.dedup_rendered_text("\n".join(lines)), html=True)


def _normalize_action_line(action_line: str) -> str:
    cleaned = (action_line or "").strip()
    if cleaned.lower().startswith("сделать:"):
        cleaned = cleaned.split(":", 1)[1].strip()
    return cleaned


def _resolve_action_line(action_line: str) -> str:
    cleaned = _normalize_action_line(action_line)
    if not cleaned:
        return "Проверить"
    normalized = re.sub(r"[\W_]+", " ", cleaned.lower()).strip()
    generic_actions = {
        "действий не требуется",
        "действие не требуется",
        "требует рассмотрения",
        "проверьте вручную",
        "attention needed",
        "недостаточно данных для оценки",
    }
    if normalized in generic_actions:
        return "Проверить"
    return cleaned


def _build_priority_signal_text(body_text: str, attachments: list[dict[str, Any]]) -> str:
    base = (body_text or "").strip()
    if not attachments:
        return base
    signals: list[str] = []
    for attachment in attachments[:4]:
        filename = str(attachment.get("filename") or "").strip().lower()
        if not filename:
            continue
        signals.append(filename)
        if filename.endswith((".xls", ".xlsx", ".xlsm", ".xlsb")):
            signals.append("excel attachment")
        attachment_text = str(attachment.get("text") or "").strip()
        if attachment_text:
            doc_type = _detect_attachment_doc_type(filename=filename, content_type=attachment.get("content_type") or attachment.get("type"))
            fact = pick_attachment_fact(attachment_text, filename, doc_type)
            if fact:
                signals.append(fact[:180])
            compact_text = re.sub(r"\s+", " ", attachment_text)
            if len(compact_text) > 220:
                compact_text = compact_text[:219].rstrip() + "…"
            signals.append(compact_text)
    if not signals:
        return base
    signal_text = " ".join(dict.fromkeys(signals))
    return " ".join(part for part in (base, signal_text) if part).strip()


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
    has_attachments = ctx.attachments_count > 0
    if not _is_meaningful_summary(summary) and not has_attachments:
        raise InvalidTelegramPayload("summary_invalid")
    action_line = ctx.action_line.strip() or "Действий не требуется"
    if not action_line:
        raise InvalidTelegramPayload("missing_action")
    normalized_text = text.lower()
    has_attachment_marker = any(
        marker in normalized_text
        for marker in ("влож", "📎", "💰", "акт сверки", "счёт")
    )
    if ctx.attachments_count > 0 and not has_attachment_marker:
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
        return append_watermark(
            tg_renderer.build_tg_fallback(
            priority=priority,
            subject=subject,
            from_email=from_email,
            attachments=attachments or [],
            ),
            html=True,
        )
    safe_subject = escape_tg_html(subject or "(без темы)")
    safe_sender = escape_tg_html(from_email or "неизвестно")
    if attachment_summary is None:
        attachment_summary = _build_attachment_summary(
            _build_attachment_details(attachments or [])
        )
    if not attachment_summary:
        attachment_summary = "Вложения: 0"
    lines = [
        "Письмо получено",
        f"От: {safe_sender}",
        f"Тема: {safe_subject}",
        "Основной текст не удалось безопасно отобразить.",
        attachment_summary,
    ]
    return append_watermark("\n".join(lines), html=True)


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
        lines.append(f"Вложения: {len(attachments)}")
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


def _filter_insights_for_render(
    insights: list[Insight],
    *,
    action_line: str,
    summary: str,
) -> list[Insight]:
    filtered = tg_renderer.apply_semantic_gates(
        action_line=action_line,
        summary=summary,
        insights=[insight.explanation for insight in insights],
    ).insights
    if not filtered:
        return []
    normalized = {
        tg_renderer.normalize_sentence(sentence) for sentence in filtered if sentence
    }
    return [
        insight
        for insight in insights
        if tg_renderer.normalize_sentence(insight.explanation) in normalized
    ]


def _build_signal_hints(insights: list[Insight]) -> list[str]:
    hints: list[str] = []
    seen_types: set[str] = set()
    for insight in insights:
        insight_type = str(insight.type or "").strip().lower()
        explanation = _sanitize_preview_line(insight.explanation)
        normalized_expl = explanation.lower()
        if (
            "silence" in insight_type
            or "молч" in insight_type
            or "silence" in normalized_expl
            or "молчит" in normalized_expl
        ) and "silence" not in seen_types:
            if explanation:
                hints.append(f"⚠ {explanation}")
                seen_types.add("silence")
                continue
        if (
            "deadlock" in insight_type
            or "без ответа" in insight_type
            or "deadlock" in normalized_expl
            or "без ответа" in normalized_expl
        ) and "deadlock" not in seen_types:
            if explanation:
                hints.append(f"🔁 {explanation}")
                seen_types.add("deadlock")
                continue
    return hints


def _extract_narrative_insight(
    insights: list[Insight],
) -> tuple[NarrativeResult | None, list[Insight]]:
    narrative: NarrativeResult | None = None
    remaining: list[Insight] = []
    for insight in insights:
        if insight.type == "Narrative":
            fact = insight.metadata.get("fact") if insight.metadata else insight.explanation
            pattern = insight.metadata.get("pattern") if insight.metadata else None
            action = insight.metadata.get("action") if insight.metadata else insight.recommendation
            if fact:
                narrative = NarrativeResult(
                    fact=fact,
                    pattern=pattern,
                    action=action,
                    reasons=tuple(),
                )
            continue
        remaining.append(insight)
    return narrative, remaining


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
        event_emitter.emit(
            type="telegram_empty_summary",
            timestamp=context.received_at,
            email_id=context.email_id,
            payload={"summary": context.body_summary},
        )

    has_minimal_display_data = bool(
        (context.from_email or "").strip() and (context.subject or "").strip()
    )
    if context.signal_invalid and not has_minimal_display_data:
        fallback_reasons.append("signal_invalid_no_data")
    if not has_minimal_display_data:
        fallback_reasons.append("no_display_data")

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
                mail_type=context.mail_type,
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
            context.body_text or "",
            context.attachment_files,
            context.commitments_present,
        )
    if no_llm_summary:
        telegram_text_raw = f"{telegram_text_raw}\n\n{no_llm_summary}"

    narrative, remaining_insights = _extract_narrative_insight(context.insights)
    remaining_insights = _filter_insights_for_render(
        remaining_insights,
        action_line=_resolve_action_line(context.action_line),
        summary=context.body_summary,
    )

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

    if render_mode == TelegramRenderMode.FULL and not payload_invalid:
        if narrative:
            narrative_block = tg_renderer.format_narrative_block(
                fact=narrative.fact,
                context=narrative.pattern,
                action=narrative.action,
            )
            telegram_text = f"{telegram_text}\n\n{narrative_block}"
        signal_hints = _build_signal_hints(remaining_insights)
        if signal_hints:
            telegram_text = f"{telegram_text}\n" + "\n".join(signal_hints)
        insights_section = _build_insights_section(
            remaining_insights, context.insight_digest
        )
        if insights_section:
            telegram_text = f"{telegram_text}{escape_tg_html(insights_section)}"
    if context.preview_hint:
        telegram_text = f"{telegram_text}\n💡 {escape_tg_html(_sanitize_preview_line(context.preview_hint))}"
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
        reply_markup=build_priority_keyboard(context.email_id),
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


def _get_correction_count(account_email: str) -> int:
    key = str(account_email or "").strip()
    if not key:
        return 0
    now_mono = time.monotonic()
    cached = _CORRECTION_COUNT_CACHE.get(key)
    if cached is not None:
        cached_value, cached_at = cached
        if now_mono - cached_at <= _CORRECTION_CACHE_TTL:
            return int(cached_value)
    try:
        value = int(analytics.count_all_time_corrections(account_emails=[key]))
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("preview_corrections_query_failed", account_email=key, error=str(exc))
        value = 0
    _CORRECTION_COUNT_CACHE[key] = (max(0, value), now_mono)
    return max(0, value)


def _get_all_time_corrections_cached(account_email: str) -> int:
    return _get_correction_count(account_email)


def _read_llm_field(llm_result: Any, key: str, default: Any = None) -> Any:
    if isinstance(llm_result, dict):
        return llm_result.get(key, default)
    return getattr(llm_result, key, default)


def _extract_llm_tokens(llm_result: Any) -> Optional[int]:
    raw = _read_llm_field(llm_result, "tokens_used", None)
    if raw is None:
        raw = _read_llm_field(llm_result, "token_usage", None)
    if raw is None:
        raw = _read_llm_field(llm_result, "tokens", None)
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _estimate_input_chars(body_text: str, attachments: list[dict[str, Any]], subject: str) -> int:
    total = len(subject or "") + len(body_text or "")
    for attachment in attachments:
        text = attachment.get("text") or ""
        total += len(str(text))
    return total


def _count_recent_importance_history(
    *,
    account_email: str,
    received_at: datetime,
    window_days: int,
    current_email_id: int,
) -> int | None:
    since_ts = (received_at - timedelta(days=window_days)).timestamp()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                """
                SELECT COUNT(1)
                FROM email_importance_scores
                WHERE account_email = ?
                  AND ts_utc >= ?
                  AND email_id != ?
                """,
                (account_email, since_ts, int(current_email_id)),
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("importance_history_count_failed", error=str(exc))
        return None
    return int(row[0]) if row else 0


def _build_heuristic_llm_result(
    *, subject: str, body_text: str, priority: str, attachments: list[dict[str, Any]]
) -> SimpleNamespace:
    summary = _build_heuristic_summary(subject=subject, body_text=body_text)
    action_line = _build_heuristic_action_line(priority=priority)
    return SimpleNamespace(
        priority=priority,
        action_line=action_line,
        body_summary=summary,
        attachment_summaries=_build_heuristic_attachment_summaries(attachments),
        llm_provider="heuristic",
    )


def _build_heuristic_summary(*, subject: str, body_text: str) -> str:
    text = clean_email_body(body_text or "").strip()
    if not text:
        return ""

    def _contains_probable_blob(value: str) -> bool:
        for token in value.split():
            compact = token.strip()
            if len(compact) < 32:
                continue
            if re.fullmatch(r"[A-Za-z0-9+/=]+", compact):
                return True
        return False

    for paragraph in text.split("\n"):
        normalized = re.sub(r"\s+", " ", paragraph).strip()
        if len(normalized) >= _MIN_SUMMARY_CHARS and not _contains_probable_blob(normalized):
            return normalized[:200]
    normalized_text = re.sub(r"\s+", " ", text)
    if len(normalized_text) >= _MIN_SUMMARY_CHARS and not _contains_probable_blob(normalized_text):
        return normalized_text[:200]
    return ""


def _build_heuristic_action_line(*, priority: str) -> str:
    if priority == "🔴":
        return "Срочно требует внимания"
    if priority == "🟡":
        return "Требует рассмотрения"
    return "Ознакомиться"


def _build_heuristic_attachment_summaries(attachments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for attachment in attachments:
        filename = str(attachment.get("filename") or "")
        raw_text = str(attachment.get("text") or "").strip()
        summary = ""
        if raw_text:
            doc_type = _detect_attachment_doc_type(
                filename=filename,
                content_type=attachment.get("content_type") or attachment.get("type"),
            )
            fact = pick_attachment_fact(raw_text, filename, doc_type)
            source = fact or raw_text
            normalized = re.sub(r"\s+", " ", source.replace(";", " ").replace("|", " ")).strip()
            if normalized:
                words = [word for word in normalized.split() if len(word) > 1]
                if len(words) >= 4:
                    summary = " ".join(words[:16])
                else:
                    summary = " ".join(normalized.split()[:16])
                if len(summary) > 160:
                    summary = summary[:159].rstrip() + "…"
        summaries.append(
            {
                "filename": filename,
                "summary": summary,
            }
        )
    return summaries


def _detect_attachment_doc_type(*, filename: str, content_type: Any) -> str:
    lowered_filename = (filename or "").lower()
    lowered_type = str(content_type or "").lower()
    if lowered_filename.endswith((".xls", ".xlsx", ".xlsm", ".xlsb")) or "excel" in lowered_type:
        return "TABLE"
    if lowered_filename.endswith((".doc", ".docx")) or "word" in lowered_type:
        return "CONTRACT" if any(token in lowered_filename for token in ("contract", "догов", "agreement")) else "OTHER"
    if lowered_filename.endswith(".pdf") and any(token in lowered_filename for token in ("invoice", "счет", "сч", "bill")):
        return "TABLE"
    return "OTHER"


def _compute_heuristic_priority(
    *,
    subject: str,
    body_text: str,
    from_email: str,
    mail_type: str | None,
    received_at: datetime,
    commitments: list[Commitment],
    attachments: list[dict[str, Any]] | None = None,
) -> PriorityResultV2 | None:
    if not getattr(feature_flags, "ENABLE_PRIORITY_V2", False):
        return None
    try:
        return priority_engine_v2.compute(
            subject=subject,
            body_text=_build_priority_signal_text(body_text or "", attachments or []),
            from_email=from_email,
            mail_type=mail_type or "",
            received_at=received_at,
            commitments=commitments,
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("heuristic_priority_failed", error=str(exc))
        return None


def _process_llm_queue_request(request: LLMRequest) -> None:
    try:
        llm_ctx = SimpleNamespace(
            account_email=request.account_email,
            email_id=request.email_id,
            from_email=request.from_email,
            subject=request.subject,
            body_text=request.body_text,
            attachments_text=[str(item.get("text") or "") for item in request.attachments],
            llm_result=None,
        )
        llm_result = run_llm_stage(
            subject=request.subject,
            from_email=request.from_email,
            body_text=request.body_text,
            attachments=request.attachments,
            ctx=llm_ctx,
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("llm_queue_request_failed", email_id=request.email_id, error=str(exc))
        return
    if not llm_result:
        logger.warning("llm_queue_empty_result", email_id=request.email_id)
        return
    tokens_used = _extract_llm_tokens(llm_result)
    model_name = str(_read_llm_field(llm_result, "llm_provider", "gigachat") or "gigachat")
    budget_consumer.on_llm_call(
        account_email=request.account_email,
        tokens_used=tokens_used,
        input_chars=request.input_chars,
        model=model_name,
        success=True,
    )


def _ensure_llm_worker() -> BackgroundLLMWorker:
    global llm_worker
    if llm_worker is None:
        llm_worker = BackgroundLLMWorker(
            get_llm_request_queue(),
            _process_llm_queue_request,
            poll_timeout_sec=get_llm_queue_config().llm_request_queue_timeout_sec,
        )
    llm_worker.start()
    return llm_worker


_PREVIEW_DECISION_LINE = "[Принять] [Отклонить]"
_PREVIEW_PRIORITY_LINE = "[Сделать Высокий] [Сделать Средний] [Сделать Низкий]"


def _normalize_reason_code(reason: str) -> str:
    labels = humanize_reason_codes((reason,), locale=_UI_LOCALE)
    return labels[0] if labels else ""


def _format_mail_type_label(mail_type: str) -> str:
    return humanize_mail_type(mail_type, locale=_UI_LOCALE) or "unknown"


def _format_amount_value(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def _format_deadline_days(days_out: int) -> str:
    if days_out < 0:
        return "просрочено"
    if days_out == 0:
        return "сегодня"
    if days_out == 1:
        return "через 1 день"
    if 2 <= days_out <= 4:
        return f"через {days_out} дня"
    return f"через {days_out} дней"


def _parse_deadline_iso(deadline_iso: str | None) -> datetime | None:
    if not deadline_iso:
        return None
    try:
        return datetime.fromisoformat(deadline_iso)
    except ValueError:
        return None


def _select_breakdown_item(
    breakdown: tuple[PriorityBreakdownItem, ...],
    signal: str,
) -> PriorityBreakdownItem | None:
    candidates = [item for item in breakdown if item.signal == signal]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.points)


def _build_priority_explain_lines(
    *,
    mail_type: str | None,
    mail_type_reasons: list[str],
    priority_v2_result: PriorityResultV2 | None,
    commitments: list[Commitment],
    received_at: datetime,
) -> list[str]:
    lines: list[str] = []
    breakdown = priority_v2_result.breakdown if priority_v2_result else tuple()

    if mail_type:
        reason = mail_type_reasons[0] if mail_type_reasons else None
        if not reason:
            mail_type_item = _select_breakdown_item(breakdown, "mail_type")
            reason = mail_type_item.reason_code if mail_type_item else None
        reason_label = _normalize_reason_code(reason or "")
        line = f"Тип: {_format_mail_type_label(mail_type)}"
        if reason_label:
            line = f"{line} (причина: {reason_label})"
        lines.append(line)

    deadline_item = _select_breakdown_item(breakdown, "deadline")
    if deadline_item and deadline_item.detail:
        try:
            days_out = int(deadline_item.detail)
        except ValueError:
            days_out = 0
        line = f"Дедлайн: {_format_deadline_days(days_out)}"
        reason_label = _normalize_reason_code(deadline_item.reason_code)
        if reason_label:
            line = f"{line} (причина: {reason_label})"
        lines.append(line)
    elif commitments:
        parsed_deadlines = [
            _parse_deadline_iso(commitment.deadline_iso)
            for commitment in commitments
            if commitment.deadline_iso
        ]
        parsed_deadlines = [deadline for deadline in parsed_deadlines if deadline]
        if parsed_deadlines:
            nearest = min(parsed_deadlines)
            days_out = (nearest.date() - received_at.date()).days
            line = f"Дедлайн: {_format_deadline_days(days_out)}"
            lines.append(line)

    amount_item = _select_breakdown_item(breakdown, "amount")
    if amount_item and amount_item.detail:
        try:
            amount_value = int(float(amount_item.detail))
        except ValueError:
            amount_value = 0
        if amount_value > 0:
            amount_label = _format_amount_value(amount_value)
            line = f"Сумма: {amount_label}"
            reason_label = _normalize_reason_code(amount_item.reason_code)
            if reason_label:
                line = f"{line} (причина: {reason_label})"
            lines.append(line)

    if len(lines) < 3:
        urgency_item = _select_breakdown_item(breakdown, "urgency")
        if urgency_item and urgency_item.detail:
            detail = _sanitize_preview_line(str(urgency_item.detail))
            line = f"Срочность: {detail}"
            reason_label = _normalize_reason_code(urgency_item.reason_code)
            if reason_label:
                line = f"{line} (причина: {reason_label})"
            lines.append(line)

    if not lines:
        lines.append("нет данных")

    return [_sanitize_preview_line(line) for line in lines[:3]]


def _build_preview_message(
    *,
    action_text: str,
    reasons: list[str],
    confidence: float | None,
    priority_explain_lines: list[str],
) -> str:
    action_line = _sanitize_preview_line(action_text)
    localized_reasons = humanize_reason_codes(reasons, locale=_UI_LOCALE)
    safe_reasons = [
        _sanitize_preview_line(reason) for reason in (localized_reasons or reasons) if reason
    ]
    if not safe_reasons:
        safe_reasons = ["нет данных"]
    confidence_value = confidence if confidence is not None else 0.0
    lines = [
        t("preview.title", locale=_UI_LOCALE),
        "",
        t("preview.action", locale=_UI_LOCALE),
        f"• {action_line}",
        t("preview.reason", locale=_UI_LOCALE),
    ]
    lines.extend(f"• {reason}" for reason in safe_reasons)
    if priority_explain_lines:
        lines.append("")
        lines.append(t("preview.why", locale=_UI_LOCALE))
        lines.extend(f"- {line}" for line in priority_explain_lines)
    lines.append(f"{t('preview.confidence', locale=_UI_LOCALE)}: {confidence_value:.2f}")
    lines.append("")
    lines.append(_PREVIEW_DECISION_LINE)
    lines.append(_PREVIEW_PRIORITY_LINE)
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
    lines = [preview_text, "", t("preview.insights", locale=_UI_LOCALE)]
    for insight in insights:
        title = _sanitize_preview_line(insight.type)
        severity = _sanitize_preview_line(insight.severity)
        explanation = _sanitize_preview_line(insight.explanation)
        recommendation = _sanitize_preview_line(insight.recommendation)
        lines.append(f"• {title} ({severity})")
        lines.append(f"  {explanation}")
        lines.append(f"  Рекомендация: {recommendation}")
    return "\n".join(lines)


def _append_narrative_preview(
    preview_text: str,
    narrative: NarrativeResult | None,
) -> str:
    if narrative is None:
        return preview_text
    lines = preview_text.split("\n")
    insert_at = len(lines)
    for idx, line in enumerate(lines):
        cleaned = line.strip()
        if cleaned.startswith("[") and cleaned.endswith("]"):
            insert_at = idx
            break
    narrative_lines = ["", t("preview.narrative", locale=_UI_LOCALE)]
    narrative_lines.append(f"Факт: {_sanitize_preview_line(narrative.fact)}")
    if narrative.pattern:
        narrative_lines.append(f"Контекст: {_sanitize_preview_line(narrative.pattern)}")
    if narrative.action:
        narrative_lines.append(f"Действие: {_sanitize_preview_line(narrative.action)}")
    lines[insert_at:insert_at] = narrative_lines
    return "\n".join(lines)


def _append_insight_digest_preview(
    preview_text: str,
    digest: InsightDigest | None,
) -> str:
    if digest is None:
        return preview_text
    status = _sanitize_preview_line(digest.status_label)
    headline = _sanitize_preview_line(digest.headline)
    lines = [preview_text, "", t("preview.digest", locale=_UI_LOCALE), status, headline]
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
    lines = [preview_text, "", t("preview.signals", locale=_UI_LOCALE)]
    for anomaly in anomalies:
        title = _sanitize_preview_line(anomaly.title)
        severity = _sanitize_preview_line(humanize_severity(anomaly.severity, locale=_UI_LOCALE))
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
    rfc_message_id: str | None,
    in_reply_to: str | None,
    references: str | None,
    thread_key: str,
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
                                _emit_contract_event(
                                    EventType.COMMITMENT_EXPIRED,
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
        llm_model_for_span = llm_model or None

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
            rfc_message_id=rfc_message_id,
            in_reply_to=in_reply_to,
            references=references,
            thread_key=thread_key,
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
        deadlock_mode = getattr(feature_flags, "ENABLE_DEADLOCK_DETECTION", "disabled")
        if deadlock_mode in {"shadow", "enabled"}:
            try:
                maybe_emit_deadlock(
                    knowledge_db=knowledge_db,
                    event_emitter=contract_event_emitter,
                    account_email=account_email,
                    thread_key=thread_key,
                    policy=get_deadlock_policy_config(),
                    now_ts=received_at.timestamp(),
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("deadlock_detection_failed", error=str(exc))
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
    from_name: str | None,
    subject: str,
    action_line: str,
    mail_type: str,
    body_summary: str,
    body_text: str,
    attachments: list[dict[str, Any]],
    llm_result: Any,
    signal_quality: Any,
    aggregated_insights: list[Insight],
    insight_digest: InsightDigest | None,
    telegram_chat_id: str,
    telegram_bot_token: str,
    account_email: str,
    attachment_summaries: list[dict[str, Any]],
    commitments: list[Commitment],
    enable_premium_clarity: bool,
    preview_hint: str | None = None,
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
        mail_type=mail_type or "",
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
        preview_hint=preview_hint,
        metadata={
            "chat_id": telegram_chat_id,
            "bot_token": telegram_bot_token,
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
        premium_clarity_enabled=enable_premium_clarity,
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
    telegram_bot_token: str = "",
    rfc_message_id: str | None = None,
    in_reply_to: str | None = None,
    references: str | None = None,
) -> None:
    """
    Главный pipeline:
    PARSE → LLM → (SAVE TO DB) → TELEGRAM

    NOTE: Поведение Telegram и LLM НЕ МЕНЯЕМ
    """

    anchor_received_at: datetime | None = received_at
    if received_at is None:
        logger.warning("received_at_missing_fallback", email_id=message_id)
        received_at = datetime.now(timezone.utc)
        anchor_received_at = None

    span = processing_span_recorder.start(
        account_id=account_email, email_id=message_id
    )
    processing_started_at = time.monotonic()
    outcome = "ok"
    error_code = ""
    llm_provider_for_span: str | None = None
    llm_model_for_span: str | None = None
    llm_latency_ms: int | None = None
    llm_quality_score: float | None = None
    llm_used = False
    health_snapshot_payload: dict[str, Any] | None = None
    fallback_used = False
    telegram_delivered = False
    delivery_mode_for_span = ""
    elapsed_to_first_send_seconds = 0.0
    edit_applied = False
    parse_timer_start = time.perf_counter()

    try:
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
        thread_key = compute_thread_key(
            account_email=account_email,
            rfc_message_id=rfc_message_id,
            in_reply_to=in_reply_to,
            references=references,
            subject=subject,
            from_email=from_email,
        )
        mail_type: str | None = None
        mail_type_reasons: list[str] = []
        hierarchy_enabled = getattr(feature_flags, "ENABLE_HIERARCHICAL_MAIL_TYPES", False)
        mail_type_attachments = [
            MailTypeAttachment(
                filename=attachment.get("filename"),
                content_type=attachment.get("content_type") or attachment.get("type") or "",
            )
            for attachment in attachments or []
        ]
        try:
            mail_type, mail_type_reasons = MailTypeClassifier.classify_detailed(
                subject=subject,
                body=body_text or "",
                attachments=mail_type_attachments,
                enable_hierarchy=hierarchy_enabled,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "mail_type_classification_failed",
                email_id=message_id,
                error=str(exc),
            )
            mail_type = MailTypeClassifier.classify(
                subject=subject,
                body=body_text or "",
                attachments=mail_type_attachments,
            )
            mail_type_reasons = ["mt.base=fallback"]
        if mail_type:
            logger.info(
                "mail_type_classified",
                email_id=message_id,
                mail_type=mail_type,
                reason_codes=mail_type_reasons,
                hierarchy_enabled=hierarchy_enabled,
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
                    "mail_type": mail_type,
                    "mail_type_reasons": mail_type_reasons,
                },
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("event_emit_failed", error=str(exc))

        span.record_stage(
            "parse", int((time.perf_counter() - parse_timer_start) * 1000)
        )

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
        llm_quality_score = signal_quality.quality_score
        contract_event_entity_id = (
            entity_resolution.entity_id if entity_resolution else None
        )
        contract_event_payload = {
            "from_email": from_email,
            "subject": subject,
            "attachments_count": len(attachments),
            "body_chars": len(body_text or ""),
            "word_count": len(re.findall(r"\w+", body_text or "")),
            "occurred_at_utc": received_at.timestamp(),
            "thread_key": thread_key,
        }
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

        importance = heuristic_importance(
            subject=subject,
            body_text=body_text or "",
            from_email=from_email,
            attachments=attachments,
        )
        try:
            record_importance_score(
                db_path=DB_PATH,
                account_email=account_email,
                email_id=message_id,
                score=importance.score,
                occurred_at=received_at,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("importance_score_store_failed", email_id=message_id, error=str(exc))

        # Root cause: anchor budget percentile window to received_at for deterministic tests.
        use_llm_candidate = False
        prior_history_count: int | None = None
        try:
            usage_config = get_budget_usage_config()
            percentile_result = is_top_percentile(
                db_path=DB_PATH,
                account_email=account_email,
                current_score=importance.score,
                percentile_threshold=usage_config.llm_percentile_threshold,
                window_days=usage_config.window_days,
                anchor_ts_utc=anchor_received_at.timestamp() if anchor_received_at else None,
                received_at=anchor_received_at,
            )
            prior_history_count = _count_recent_importance_history(
                account_email=account_email,
                received_at=received_at,
                window_days=usage_config.window_days,
                current_email_id=message_id,
            )
            has_insufficient_history = prior_history_count is not None and prior_history_count < 3
            if not percentile_result.anchored:
                logger.warning(
                    "importance_score_anchor_missing",
                    email_id=message_id,
                )
            use_llm_candidate = percentile_result.is_top
            if has_insufficient_history and not percentile_result.is_top:
                use_llm_candidate = True
                logger.info(
                    "llm_cold_start_percentile_allow",
                    email_id=message_id,
                    account_email=account_email,
                    prior_history_count=prior_history_count,
                    anchored=percentile_result.anchored,
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("importance_score_percentile_failed", error=str(exc))
        can_use_llm = False
        if use_llm_candidate:
            can_use_llm = budget_gate.can_use_llm(account_email)
            if not can_use_llm:
                _emit_contract_event(
                    EventType.GATE_FLIPPED,
                    ts_utc=received_at.timestamp(),
                    account_id=account_email,
                    entity_id=contract_event_entity_id,
                    email_id=message_id,
                    payload={
                        "feature_name": "clarity_formatter",
                        "old_mode": "premium",
                        "new_mode": "basic",
                    },
                )
                _emit_contract_event(
                    EventType.GATE_FLIPPED,
                    ts_utc=received_at.timestamp(),
                    account_id=account_email,
                    entity_id=contract_event_entity_id,
                    email_id=message_id,
                    payload={
                        "feature_name": "trust_bootstrap",
                        "old_mode": "premium",
                        "new_mode": "basic",
                    },
                )

        anchor_ts_utc = received_at.timestamp()
        attention_signals: dict[str, bool] = {
            "TOP_PERCENTILE_CANDIDATE": use_llm_candidate,
        }
        if use_llm_candidate:
            attention_signals["BUDGET_GATE_ALLOW"] = can_use_llm
        attention_signals_evaluated = sorted(attention_signals.keys())
        attention_signals_fired = sorted(
            [key for key, fired in attention_signals.items() if fired]
        )
        attention_trace = DecisionTraceV1(
            decision_key=compute_decision_key(
                account_id=account_email,
                email_id=message_id,
                decision_kind="ATTENTION_GATE",
                anchor_ts_utc=anchor_ts_utc,
            ),
            decision_kind="ATTENTION_GATE",
            anchor_ts_utc=anchor_ts_utc,
            signals_evaluated=attention_signals_evaluated,
            signals_fired=attention_signals_fired,
            evidence={
                "matched": len(attention_signals_fired),
                "total": len(attention_signals_evaluated),
            },
            model_fingerprint=compute_model_fingerprint(
                {
                    "usage_config": get_budget_usage_config(),
                    "gate_config": get_budget_gate_config(),
                }
            ),
            explain_codes=attention_signals_fired,
        )
        attention_trace = sanitize_trace(attention_trace)
        _emit_decision_trace(
            attention_trace,
            account_id=account_email,
            entity_id=contract_event_entity_id,
            email_id=message_id,
            ts_utc=anchor_ts_utc,
        )

        priority_v2_result = _compute_heuristic_priority(
            subject=subject,
            body_text=body_text or "",
            from_email=from_email,
            mail_type=mail_type,
            received_at=received_at,
            commitments=commitments,
            attachments=attachments,
        )
        heuristic_priority = priority_v2_result.priority if priority_v2_result else "🔵"
        if priority_v2_result:
            priority_body_text = _build_priority_signal_text(body_text or "", attachments)
            priority_signals = priority_engine_v2.evaluate_signals(
                subject=subject,
                body_text=priority_body_text,
                from_email=from_email,
                mail_type=mail_type or "",
                received_at=received_at,
                commitments=commitments,
            )
            priority_signals_evaluated = sorted(priority_signals.keys())
            priority_signals_fired = sorted(
                [key for key, fired in priority_signals.items() if fired]
            )
            priority_trace = DecisionTraceV1(
                decision_key=compute_decision_key(
                    account_id=account_email,
                    email_id=message_id,
                    decision_kind="PRIORITY_HEURISTIC",
                    anchor_ts_utc=anchor_ts_utc,
                ),
                decision_kind="PRIORITY_HEURISTIC",
                anchor_ts_utc=anchor_ts_utc,
                signals_evaluated=priority_signals_evaluated,
                signals_fired=priority_signals_fired,
                evidence={
                    "matched": len(priority_signals_fired),
                    "total": len(priority_signals_evaluated),
                },
                model_fingerprint=priority_engine_v2.model_fingerprint(),
                explain_codes=priority_engine_v2.explain_codes(priority_v2_result),
            )
            priority_trace = sanitize_trace(priority_trace)
            _emit_decision_trace(
                priority_trace,
                account_id=account_email,
                entity_id=contract_event_entity_id,
                email_id=message_id,
                ts_utc=anchor_ts_utc,
            )

        llm_result = None
        input_chars = _estimate_input_chars(llm_body_text, attachments, subject)
        llm_queue_path_enabled = (
            get_llm_queue_config().llm_request_queue_enabled
            and get_llm_queue_config().max_concurrent_llm_calls == 1
            and run_llm_stage is _ORIGINAL_RUN_LLM_STAGE
        )
        llm_was_queued = False
        llm_called_direct = False
        if use_llm_candidate and can_use_llm:
            llm_start = time.perf_counter()
            try:
                llm_ctx = SimpleNamespace(
                    account_email=account_email,
                    email_id=message_id,
                    from_email=from_email,
                    subject=subject,
                    body_text=llm_body_text,
                    attachments_text=[str(item.get("text") or "") for item in attachments],
                    llm_result=None,
                )
                llm_result = run_llm_stage(
                    subject=subject,
                    from_email=from_email,
                    body_text=llm_body_text,
                    attachments=attachments,
                    ctx=llm_ctx,
                )
                if not llm_result:
                    raise RuntimeError("LLM stage returned empty result")
                llm_used = True
                llm_called_direct = True
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
                llm_result = _build_heuristic_llm_result(
                    subject=subject,
                    body_text=body_text or "",
                    priority=heuristic_priority,
                    attachments=attachments,
                )
                fallback_used = True
                if llm_queue_path_enabled:
                    _ensure_llm_worker()
                    request = LLMRequest(
                        account_email=account_email,
                        email_id=message_id,
                        subject=subject,
                        from_email=from_email,
                        body_text=llm_body_text,
                        attachments=attachments,
                        received_at=received_at,
                        input_chars=input_chars,
                    )
                    queued = get_llm_request_queue().enqueue(request, timeout_sec=0.5)
                    llm_was_queued = queued
                    if not queued:
                        logger.info("llm_queue_full", email_id=message_id)
            llm_latency_ms = int((time.perf_counter() - llm_start) * 1000)
            span.record_stage("llm", llm_latency_ms)
        else:
            llm_result = _build_heuristic_llm_result(
                subject=subject,
                body_text=body_text or "",
                priority=heuristic_priority,
                attachments=attachments,
            )
            llm_latency_ms = 0
            span.record_stage("llm", llm_latency_ms)

        llm_gate_signals = {
            "LLM_CANDIDATE": use_llm_candidate,
            "LLM_BUDGET_OK": can_use_llm,
            "LLM_QUEUE_ENABLED": llm_queue_path_enabled,
            "LLM_QUEUED": llm_was_queued,
            "LLM_CALLED_DIRECT": llm_called_direct,
        }
        llm_gate_signals_evaluated = sorted(llm_gate_signals.keys())
        llm_gate_signals_fired = sorted(
            [key for key, fired in llm_gate_signals.items() if fired]
        )
        llm_gate_trace = DecisionTraceV1(
            decision_key=compute_decision_key(
                account_id=account_email,
                email_id=message_id,
                decision_kind="LLM_GATE",
                anchor_ts_utc=anchor_ts_utc,
            ),
            decision_kind="LLM_GATE",
            anchor_ts_utc=anchor_ts_utc,
            signals_evaluated=llm_gate_signals_evaluated,
            signals_fired=llm_gate_signals_fired,
            evidence={
                "matched": len(llm_gate_signals_fired),
                "total": len(llm_gate_signals_evaluated),
            },
            model_fingerprint=compute_model_fingerprint(
                {
                    "llm_queue_config": get_llm_queue_config(),
                }
            ),
            explain_codes=llm_gate_signals_fired,
        )
        llm_gate_trace = sanitize_trace(llm_gate_trace)
        _emit_decision_trace(
            llm_gate_trace,
            account_id=account_email,
            entity_id=contract_event_entity_id,
            email_id=message_id,
            ts_utc=anchor_ts_utc,
        )

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
            outcome = "partial"
            error_code = "llm_empty"
            try:
                _emit_contract_event(
                    EventType.EMAIL_RECEIVED,
                    ts_utc=received_at.timestamp(),
                    account_id=account_email,
                    entity_id=contract_event_entity_id,
                    email_id=message_id,
                    payload={**contract_event_payload, "engine": "llm"},
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("contract_event_emit_failed", error=str(exc))
            return
        if llm_used:
            change = system_health.update_component("LLM", True)
            _notify_system_mode_change(
                change=change,
                chat_id=telegram_chat_id,
                account_email=account_email,
            )
        if fallback_used and outcome == "ok":
            outcome = "fallback"

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
        llm_provider_for_span = llm_provider

        if llm_used:
            tokens_used = _extract_llm_tokens(llm_result)
            budget_consumer.on_llm_call(
                account_email=account_email,
                tokens_used=tokens_used,
                input_chars=input_chars,
                model=llm_provider or "gigachat",
                success=True,
            )

# ---------- Shadow Priority (read-only, dry run) ----------
        priority_v2_result = None
        priority_v2_enabled = bool(getattr(feature_flags, "ENABLE_PRIORITY_V2", False))
        if priority_v2_enabled:
            try:
                priority_body_text = _build_priority_signal_text(body_text or "", attachments)
                priority_v2_result = priority_engine_v2.compute(
                    subject=subject,
                    body_text=priority_body_text,
                    from_email=from_email,
                    mail_type=mail_type or "",
                    received_at=received_at,
                    commitments=commitments,
                )
                logger.info(
                    "priority_v2_computed",
                    email_id=message_id,
                    score=priority_v2_result.score,
                    priority=priority_v2_result.priority,
                    reasons=priority_v2_result.reason_codes[:5],
                    model_version=priority_v2_result.model_version,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("priority_v2_failed", error=str(exc))
                priority_v2_result = None
    
        shadow_priority, shadow_reason = shadow_priority_engine.compute(
            llm_priority=priority,
            from_email=from_email,
        )
        if priority_v2_result and _is_shadow_higher(
            priority_v2_result.priority,
            shadow_priority,
        ):
            shadow_priority = priority_v2_result.priority
            shadow_reason = "Priority v2: " + ", ".join(
                priority_v2_result.reason_codes[:5]
            )
        if shadow_priority != priority:
            logger.info(
                "shadow_priority_computed",
                from_email=from_email or "",
                current_priority=priority,
                shadow_priority=shadow_priority,
                reason=shadow_reason or "",
            )
    
        priority_engine_label = "priority_v2_shadow" if priority_v2_result else "llm"
    
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
            priority_engine_label = "priority_v2_auto"
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
    
        try:
            _emit_contract_event(
                EventType.EMAIL_RECEIVED,
                ts_utc=received_at.timestamp(),
                account_id=account_email,
                entity_id=contract_event_entity_id,
                email_id=message_id,
                payload={**contract_event_payload, "engine": priority_engine_label},
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("contract_event_emit_failed", error=str(exc))
    
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
    
        preview_hint: str | None = None
        if (
            getattr(feature_flags, "ENABLE_PREVIEW_ACTIONS", False)
            and proposed_action
            and policy_decision.allow_preview
            and _get_correction_count(account_email) >= _MIN_CORRECTIONS_FOR_PREVIEW
        ):
            preview_actions = [
                action for action in _extract_preview_actions(proposed_action) if action
            ]
            if not preview_actions:
                logger.info(
                    "preview_actions_skipped",
                    reason="no_proposals",
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
                preview_hint = preview_actions[0]
                try:
                    knowledge_db.save_preview_action(
                        email_id=message_id,
                        proposed_action=proposed_action,
                        confidence=proposed_action.get("confidence"),
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.error("preview_action_persist_failed", error=str(exc))
        elif getattr(feature_flags, "ENABLE_PREVIEW_ACTIONS", False) and proposed_action:
            preview_skip_reason = "insufficient_corrections"
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
            rfc_message_id=rfc_message_id,
            in_reply_to=in_reply_to,
            references=references,
            thread_key=thread_key,
            commitments=commitments,
            enable_commitments=enable_commitments,
            entity_resolution=entity_resolution,
            signal_quality=signal_quality,
            fallback_used=fallback_used,
            telegram_chat_id=telegram_chat_id,
        )
    
        narrative: NarrativeResult | None = None
        if getattr(feature_flags, "ENABLE_NARRATIVE_BINDING", True):
            narrative = compose_narrative(
                email_id=message_id,
                subject=subject,
                body_text=body_text or "",
                from_email=from_email,
                mail_type=mail_type or "",
                received_at=received_at,
                attachments=attachments or [],
                entity_id=entity_resolution.entity_id if entity_resolution else None,
                analytics=analytics,
                commitments=commitments,
                enable_patterns=getattr(feature_flags, "ENABLE_NARRATIVE_PATTERNS", True),
            )
            if narrative:
                append_narrative_insight(
                    analytics_result.aggregated_insights,
                    fact=narrative.fact,
                    pattern=narrative.pattern,
                    action=narrative.action,
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
    
        enable_premium_clarity = bool(
            getattr(feature_flags, "ENABLE_PREMIUM_CLARITY_V1", False)
        )
        render_start = time.perf_counter()
        render_result = _render_notification(
            message_id=message_id,
            received_at=received_at,
            priority=priority,
            from_email=from_email,
            from_name=from_name,
            subject=subject,
            action_line=action_line,
            mail_type=mail_type or "",
            body_summary=body_summary,
            body_text=body_text or "",
            attachments=attachments,
            llm_result=llm_result,
            signal_quality=signal_quality,
            aggregated_insights=analytics_result.aggregated_insights,
            insight_digest=analytics_result.insight_digest,
            telegram_chat_id=telegram_chat_id,
            telegram_bot_token=telegram_bot_token,
            account_email=account_email,
            attachment_summaries=attachment_summaries,
            commitments=commitments,
            enable_premium_clarity=enable_premium_clarity,
            preview_hint=preview_hint,
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
        attachments_only = render_result.extracted_text_len <= 0 and len(attachments) > 0
        confidence_percent = _priority_confidence_percent(
            confidence_score=confidence_score,
            deadlines_count=deadlines_count,
            commitments_count=len(commitments),
            attachments_only=attachments_only,
            extracted_text_len=render_result.extracted_text_len,
            priority=priority,
        )
        if render_result.premium_clarity_enabled:
            confidence_available = confidence_score is not None
            extraction_failed = render_result.extracted_text_len <= 0 and len(attachments) > 0
            premium_payload = TelegramPayload(
                html_text=_build_premium_clarity_text(
                    priority=priority,
                    received_at=received_at,
                    from_email=from_email,
                    from_name=from_name,
                    subject=subject,
                    mail_type=mail_type,
                    action_line=action_line,
                    body_summary=body_summary,
                    body_text=body_text or "",
                    attachments=attachments,
                    attachment_summaries=attachment_summaries,
                    insights=analytics_result.aggregated_insights,
                    insight_digest=analytics_result.insight_digest,
                    commitments=commitments,
                    attachments_count=len(attachments),
                    extracted_text_len=render_result.extracted_text_len,
                    confidence_percent=confidence_percent,
                    confidence_available=confidence_available,
                    confidence_dots_mode=get_premium_clarity_config().confidence_dots_mode,
                    confidence_dots_threshold=(
                        get_premium_clarity_config().confidence_dots_threshold
                    ),
                    confidence_dots_scale=(
                        get_premium_clarity_config().confidence_dots_scale
                    ),
                    extraction_failed=extraction_failed,
                ),
                priority=priority,
                metadata=render_result.payload.metadata,
                reply_markup=render_result.payload.reply_markup,
            )
            render_result = RenderResult(
                payload=premium_payload,
                render_mode=render_result.render_mode,
                payload_invalid=render_result.payload_invalid,
                attachment_details=render_result.attachment_details,
                attachment_summary=render_result.attachment_summary,
                extracted_text_len=render_result.extracted_text_len,
                body_summary=render_result.body_summary,
                premium_clarity_enabled=render_result.premium_clarity_enabled,
            )
            payload = render_result.payload
        span.record_stage("render", int((time.perf_counter() - render_start) * 1000))
        priority_engine_for_event = "shadow"
        if auto_priority_outcome.applied:
            priority_engine_for_event = "rules"
        elif priority_v2_result:
            priority_engine_for_event = "priority_v2"
        _emit_contract_event(
            EventType.PRIORITY_DECISION_RECORDED,
            ts_utc=received_at.timestamp(),
            account_id=account_email,
            entity_id=contract_event_entity_id,
            email_id=message_id,
            payload={
                "priority": priority,
                "confidence": confidence_percent,
                "sender": from_email or "",
                "subject": subject or "",
                "engine": priority_engine_for_event,
            },
        )
        action_text = strip_disallowed_emojis(_resolve_action_line(action_line))
        high_impact = (
            priority == "🔴"
            or _deadline_within_days(commitments, received_at=received_at, days=3)
            or _is_urgent_action(action_text)
        )
        low_confidence = confidence_percent <= 40
        extraction_failed = render_result.extracted_text_len <= 0 and len(attachments) > 0
        suppress_numeric_facts = _should_suppress_numeric_facts(
            extraction_failed=extraction_failed,
            confidence_available=confidence_score is not None,
            confidence_percent=confidence_percent,
            confidence_dots_threshold=get_premium_clarity_config().confidence_dots_threshold,
        )
        shown_fact_items: list[_FactItem] = []
        if render_result.premium_clarity_enabled:
            shown_fact_items = _select_premium_clarity_fact_items(
                subject=subject or "",
                body_text=body_text or "",
                attachments=attachments or [],
                suppress_numeric_facts=suppress_numeric_facts,
            )
        shown_fact_types: list[str] = []
        fact_sources: list[str] = []
        for item in shown_fact_items:
            fact_type = _fact_type_label(item.label)
            if fact_type not in shown_fact_types:
                shown_fact_types.append(fact_type)
            source = _fact_source_tag(item.tag)
            if source and source not in fact_sources:
                fact_sources.append(source)
        has_attachment_fact_provenance = any(
            item.tag and item.tag not in {"тема", "письмо"}
            for item in shown_fact_items
        )
        if high_impact or low_confidence or extraction_failed:
            _emit_contract_event(
                EventType.TG_RENDER_RECORDED,
                ts_utc=received_at.timestamp(),
                account_id=account_email,
                entity_id=contract_event_entity_id,
                email_id=message_id,
                payload={
                    "shown_fact_types": shown_fact_types,
                    "fact_sources": fact_sources,
                    "extraction_failed": extraction_failed,
                    "confidence_bucket": _confidence_bucket(
                        confidence_available=confidence_score is not None,
                        confidence_percent=confidence_percent,
                    ),
                    "attachments_count": len(attachments),
                    "suppressed_numeric_facts": suppress_numeric_facts,
                    "has_attachment_fact_provenance": has_attachment_fact_provenance,
                },
            )
        relationship_health_delta: float | None = None
        if analytics_result.health_snapshot is not None:
            value = analytics_result.health_snapshot.components_breakdown.get("trend_delta")
            try:
                relationship_health_delta = float(value)
            except (TypeError, ValueError):
                relationship_health_delta = None
        attention_gate_deferred = False
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
            attention_gate_deferred = gate_result.deferred
            attention_reason = gate_result.reason
        except Exception as exc:  # pragma: no cover - defensive fallback
            logger.error(
                "[ATTENTION-GATE] failed",
                email_id=message_id,
                error=str(exc),
            )
            attention_gate_deferred = False
            attention_reason = "gate_failed"
    
        enable_circadian = bool(
            getattr(feature_flags, "ENABLE_CIRCADIAN_DELIVERY", False)
        )
        enable_flow_protection = bool(
            getattr(feature_flags, "ENABLE_FLOW_PROTECTION", False)
        )
        enable_attention_debt = bool(
            getattr(feature_flags, "ENABLE_ATTENTION_DEBT", False)
        )
        behavior_enabled = enable_circadian or enable_attention_debt or enable_flow_protection
        delivery_decision: DeliveryDecision | None = None
        delivery_context: DeliveryContext | None = None
    
        if behavior_enabled:
            try:
                policy_config = load_delivery_policy_config()
                flow_config = (
                    load_flow_protection_config() if enable_flow_protection else None
                )
                now_local = received_at.astimezone()
                immediate_sent_last_hour = 0
                if enable_attention_debt:
                    since_ts = time.time() - 3600
                    immediate_sent_last_hour = _count_recent_immediate_deliveries(
                        account_email=account_email,
                        since_ts=since_ts,
                    )
                scores = score_email(
                    priority=priority,
                    commitments_count=len(commitments),
                    deadlines_count=deadlines_count,
                    insight_severity=max_insight_severity(analytics_result.aggregated_insights),
                    relationship_health_delta=relationship_health_delta,
                )
                context = _build_delivery_context(
                    now_local=now_local,
                    policy_config=policy_config,
                    flow_config=flow_config,
                    enable_circadian=enable_circadian,
                    enable_flow_protection=enable_flow_protection,
                    immediate_sent_last_hour=immediate_sent_last_hour,
                )
                delivery_context = context
                delivery_decision = decide_delivery(
                    scores=scores,
                    context=context,
                    policy=policy_config,
                    attention_gate_deferred=attention_gate_deferred,
                )
                attention_reason = ",".join(delivery_decision.reason_codes) or attention_reason
    
                if enable_attention_debt:
                    debt_bucket = "low"
                    if delivery_decision.attention_debt >= 70:
                        debt_bucket = "high"
                    elif delivery_decision.attention_debt >= 30:
                        debt_bucket = "medium"
                    _emit_contract_event(
                        EventType.ATTENTION_DEBT_UPDATED,
                        ts_utc=received_at.timestamp(),
                        account_id=account_email,
                        entity_id=entity_resolution.entity_id if entity_resolution else None,
                        email_id=message_id,
                        payload={
                            "attention_debt": delivery_decision.attention_debt,
                            "bucket": debt_bucket,
                            "immediate_last_hour": immediate_sent_last_hour,
                            "max_per_hour": policy_config.max_immediate_per_hour,
                        },
                    )
    
            except Exception as exc:  # pragma: no cover - defensive fallback
                logger.error(
                    "[BEHAVIOR-ENGINE] failed",
                    email_id=message_id,
                    error=str(exc),
                )
                behavior_enabled = False
                delivery_decision = None
                attention_reason = "behavior_failed"
    
        sources = _render_sources(
            subject=subject or "",
            body_text=body_text or "",
            attachments=attachments,
        )
        extraction_success = not (
            render_result.extracted_text_len <= 0 and len(attachments) > 0
        )
        if behavior_enabled and delivery_decision and delivery_context:
            resolved_scope = resolve_account_scope(account_email)
            scope_chat_id = telegram_chat_id or (resolved_scope.chat_id if resolved_scope else None)
            scope_emails = list(resolved_scope.account_emails) if resolved_scope else None
            if not scope_emails and account_email:
                scope_emails = [account_email]
            scope_payload = get_account_scope(
                chat_id=scope_chat_id,
                account_emails=scope_emails,
            )
            _emit_contract_event(
                EventType.DELIVERY_POLICY_APPLIED,
                ts_utc=received_at.timestamp(),
                account_id=account_email,
                entity_id=entity_resolution.entity_id if entity_resolution else None,
                email_id=message_id,
                payload={
                    "mode": delivery_decision.mode.value,
                    "reason_codes": delivery_decision.reason_codes,
                    "thresholds_used": delivery_decision.thresholds_used,
                    "attention_debt": delivery_decision.attention_debt,
                    "priority": priority,
                    "confidence_percent": confidence_percent,
                    "extraction_success": extraction_success,
                    "attachment_count": len(attachments),
                    "sources": sources,
                    **scope_payload,
                },
            )
    
        send_start = time.perf_counter()
        telegram_delivered = False
        delivery_ts = time.time()
        consecutive_tg_failures = 0
        delivery_result: DeliveryResult | None = None
        wait_budget_seconds = MAX_TELEGRAM_WAIT_SECONDS
        delivery_mode_for_span = "final_first_send"
        elapsed_to_first_send_seconds = 0.0
        edit_applied = False
        minimal_payload = _build_minimal_telegram_payload(
            priority=priority,
            from_email=from_email,
            subject=subject,
            attachments=attachments,
            metadata=dict(payload.metadata),
            reply_markup=payload.reply_markup,
        )
        edit_errors: list[str] = []

        def _on_edit_failure(reason: str) -> None:
            edit_errors.append(reason)
            event_emitter.emit(
                type="telegram_edit_failed",
                timestamp=received_at,
                email_id=message_id,
                payload={"reason": reason, "chat_id": telegram_chat_id},
            )

        def _edit_payload(message_id: int, final_payload: TelegramPayload) -> bool:
            bot_token = str(
                payload.metadata.get("bot_token")
                or minimal_payload.metadata.get("bot_token")
                or ""
            )
            chat_id = str(
                payload.metadata.get("chat_id")
                or minimal_payload.metadata.get("chat_id")
                or ""
            )
            return edit_telegram_message(
                bot_token=bot_token,
                chat_id=chat_id,
                message_id=message_id,
                html_text=final_payload.html_text,
                reply_markup=final_payload.reply_markup,
            )

        def _send_payload(message_payload: TelegramPayload) -> DeliveryResult:
            return _coerce_delivery_result(
                enqueue_tg(email_id=message_id, payload=message_payload),
                email_id=message_id,
            )

        sla_outcome = _apply_delivery_sla(
            processing_started_at=processing_started_at,
            wait_budget_seconds=wait_budget_seconds,
            minimal_payload=minimal_payload,
            final_payload=payload,
            send_func=_send_payload,
            edit_func=_edit_payload,
            on_edit_failure=_on_edit_failure,
        )
        delivery_result = sla_outcome.result
        delivery_mode_for_span = sla_outcome.delivery_mode
        elapsed_to_first_send_seconds = sla_outcome.elapsed_to_first_send_seconds
        edit_applied = sla_outcome.edit_applied
        try:
            if delivery_result is None:
                raise RuntimeError("telegram_delivery_result_missing")
            if not delivery_result.delivered:
                if delivery_result.retryable:
                    raise RuntimeError(delivery_result.error or "Telegram delivery failed")
                logger.error(
                    "telegram_delivery_non_retryable",
                    email_id=message_id,
                    chat_id=telegram_chat_id,
                    error=delivery_result.error or "unknown error",
                )
                change = system_health.update_component(
                    "Telegram",
                    False,
                    reason=delivery_result.error or "Telegram send failed",
                )
                _notify_system_mode_change(
                    change=change,
                    chat_id=telegram_chat_id,
                    account_email=account_email,
                )
                logger.warning(
                    "telegram_delivery_non_retryable_no_fallback_message",
                    email_id=message_id,
                    chat_id=telegram_chat_id,
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
                ts_utc=delivery_ts,
                account_id=account_email,
                entity_id=entity_resolution.entity_id if entity_resolution else None,
                email_id=message_id,
                payload={
                    "error": str(exc),
                    "delivered": False,
                    "occurred_at_utc": delivery_ts,
                    "mode": "html",
                    "retry_count": 0,
                },
            )
            consecutive_tg_failures = notification_alert_store.record_failure(
                datetime.fromtimestamp(delivery_ts, tz=timezone.utc)
            )
            logger.error(
                "processing_error",
                stage="telegram",
                email_id=message_id,
                error=str(exc),
            )
            raise
        if telegram_delivered:
            notification_alert_store.reset_failures()
            event_emitter.emit(
                type="telegram_delivery_succeeded",
                timestamp=received_at,
                email_id=message_id,
                payload={
                    "render_mode": render_mode.name,
                    "delivery_mode": delivery_mode_for_span,
                    "edit_applied": edit_applied,
                    "message_id": delivery_result.message_id if delivery_result else None,
                },
            )
            _emit_contract_event(
                EventType.TELEGRAM_DELIVERED,
                ts_utc=delivery_ts,
                account_id=account_email,
                entity_id=entity_resolution.entity_id if entity_resolution else None,
                email_id=message_id,
                payload={
                    "render_mode": render_mode.name,
                    "priority": priority,
                    "mail_type": mail_type or "",
                    "from_email": from_email,
                    "delivered": True,
                    "occurred_at_utc": delivery_ts,
                    "mode": delivery_result.mode if delivery_result else "html",
                    "retry_count": delivery_result.retry_count if delivery_result else 0,
                    "message_id": delivery_result.message_id if delivery_result else None,
                    "chat_id": telegram_chat_id,
                    "delivery_mode": delivery_mode_for_span,
                },
            )

        event_emitter.emit(
            type="telegram_delivery_sla",
            timestamp=received_at,
            email_id=message_id,
            payload={
                "delivery_mode": delivery_mode_for_span,
                "wait_budget_seconds": wait_budget_seconds,
                "elapsed_to_first_send_seconds": elapsed_to_first_send_seconds,
                "edit_applied": edit_applied,
                "edit_errors": edit_errors,
            },
        )

        _maybe_alert_notification_sla(
            account_email=account_email,
            telegram_chat_id=telegram_chat_id,
            sla_result=policy_decision.notification_sla,
            consecutive_failures=consecutive_tg_failures,
            telegram_delivered=telegram_delivered,
        )
        span.record_stage("send", int((time.perf_counter() - send_start) * 1000))
    except Exception as exc:
        outcome = "error"
        error_code = exc.__class__.__name__
        raise
    finally:
        if health_snapshot_payload is None:
            health_snapshot_payload = _build_health_snapshot_payload()
        processing_span_recorder.finalize(
            span,
            llm_provider=llm_provider_for_span,
            llm_model=llm_model_for_span,
            llm_latency_ms=llm_latency_ms,
            llm_quality_score=llm_quality_score,
            fallback_used=fallback_used,
            outcome=outcome,
            error_code=error_code,
            health_snapshot_payload=health_snapshot_payload,
            delivery_mode=delivery_mode_for_span,
            wait_budget_seconds=MAX_TELEGRAM_WAIT_SECONDS,
            elapsed_to_first_send_ms=int(elapsed_to_first_send_seconds * 1000),
            edit_applied=edit_applied,
        )
