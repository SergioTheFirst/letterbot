from __future__ import annotations

import configparser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from mailbot_v26.behavior.silence_detector import run_silence_scan
from mailbot_v26.config.silence_policy import SilencePolicyConfig, load_silence_policy_config
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.llm.runtime_flags import RuntimeFlags, RuntimeFlagStore
from mailbot_v26.observability.logger import LoggerLike
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.observability.metrics import GateEvaluation, MetricsAggregator, SystemGates
from mailbot_v26.pipeline import daily_digest, weekly_digest
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.system.orchestrator import SystemOrchestrator, SystemPolicyDecision
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.worker.telegram_sender import DeliveryResult

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.ini"
_runtime_flag_store = RuntimeFlagStore()
_system_orchestrator = SystemOrchestrator()
_system_gates = SystemGates()


@dataclass(frozen=True, slots=True)
class DigestStorage:
    knowledge_db: KnowledgeDB
    analytics: KnowledgeAnalytics
    event_emitter: EventEmitter | None = None
    contract_event_emitter: ContractEventEmitter | None = None


@dataclass(frozen=True, slots=True)
class PolicyInputs:
    metrics: dict[str, dict[str, float]] | None
    gates: GateEvaluation | None
    runtime_flags: RuntimeFlags


@dataclass(frozen=True, slots=True)
class DailyDigestConfig:
    hour: int
    minute: int


@dataclass(frozen=True, slots=True)
class WeeklyDigestConfig:
    weekday: int
    hour: int
    minute: int


def _load_daily_digest_config() -> DailyDigestConfig:
    hour = 9
    minute = 0
    parser = configparser.ConfigParser()
    if _CONFIG_PATH.exists():
        parser.read(_CONFIG_PATH, encoding="utf-8")
    section = parser["daily_digest"] if "daily_digest" in parser else None
    if section is not None:
        try:
            hour = max(0, min(23, section.getint("hour", fallback=9)))
        except ValueError:
            hour = 9
        try:
            minute = max(0, min(59, section.getint("minute", fallback=0)))
        except ValueError:
            minute = 0
    return DailyDigestConfig(hour=hour, minute=minute)


def _load_weekly_digest_config() -> WeeklyDigestConfig:
    weekday = 0
    hour = 9
    minute = 0
    parser = configparser.ConfigParser()
    if _CONFIG_PATH.exists():
        parser.read(_CONFIG_PATH, encoding="utf-8")
    section = parser["weekly_digest"] if "weekly_digest" in parser else None
    if section is not None:
        weekday = weekly_digest._parse_weekday(section.get("weekday", fallback="mon"))
        try:
            hour = max(0, min(23, section.getint("hour", fallback=9)))
        except ValueError:
            hour = 9
        try:
            minute = max(0, min(59, section.getint("minute", fallback=0)))
        except ValueError:
            minute = 0
    return WeeklyDigestConfig(weekday=weekday, hour=hour, minute=minute)


def _is_daily_due(now: datetime, config: DailyDigestConfig) -> bool:
    if now.hour < config.hour:
        return False
    if now.hour == config.hour and now.minute < config.minute:
        return False
    return True


def _is_weekly_due(now: datetime, config: WeeklyDigestConfig) -> bool:
    if now.weekday() != config.weekday:
        return False
    if now.hour < config.hour:
        return False
    if now.hour == config.hour and now.minute < config.minute:
        return False
    return True


def _collect_policy_inputs(storage: DigestStorage, logger: LoggerLike) -> PolicyInputs:
    metrics: dict[str, dict[str, float]] | None = None
    gates: GateEvaluation | None = None
    runtime_flags = RuntimeFlags()
    try:
        aggregator = MetricsAggregator(storage.knowledge_db.path)
        metrics = aggregator.snapshot()
        gates = _system_gates.evaluate(metrics)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_policy_metrics_failed", error=str(exc))
    try:
        runtime_flags, _ = _runtime_flag_store.get_flags()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("system_policy_runtime_flags_failed", error=str(exc))
    return PolicyInputs(metrics=metrics, gates=gates, runtime_flags=runtime_flags)


def _evaluate_policy(
    *,
    policy_inputs: PolicyInputs,
    flags: FeatureFlags,
    has_daily_digest_content: bool,
    has_weekly_digest_content: bool,
) -> SystemPolicyDecision:
    system_mode = system_health.mode
    telegram_ok = system_mode != OperationalMode.DEGRADED_NO_TELEGRAM
    fallback = _system_orchestrator.legacy_decision(
        system_mode=system_mode,
        runtime_flags=policy_inputs.runtime_flags,
        feature_flags=flags,
        has_daily_digest_content=has_daily_digest_content,
        has_weekly_digest_content=has_weekly_digest_content,
    )
    return _system_orchestrator.evaluate(
        system_mode=system_mode,
        metrics=policy_inputs.metrics,
        gates=policy_inputs.gates,
        runtime_flags=policy_inputs.runtime_flags,
        feature_flags=flags,
        telegram_ok=telegram_ok,
        has_daily_digest_content=has_daily_digest_content,
        has_weekly_digest_content=has_weekly_digest_content,
        fallback_decision=fallback,
    )


def _has_weekly_content(data: weekly_digest.WeeklyDigestData) -> bool:
    return bool(
        data.total_emails
        or data.deferred_emails
        or data.attention_entities
        or data.commitment_counts
        or data.overdue_commitments
        or data.trust_deltas
        or data.anomaly_alerts
        or data.quality_metrics is not None
        or data.attention_economics is not None
    )


def _build_daily_payload(
    *,
    account_email: str,
    chat_id: str,
    bot_token: str,
    data: daily_digest.DigestData,
) -> TelegramPayload:
    digest_text = daily_digest._build_digest_text(data)
    return TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "bot_token": bot_token,
            "chat_id": chat_id,
            "account_email": account_email,
        },
    )


def _build_weekly_payload(
    *,
    account_email: str,
    chat_id: str,
    bot_token: str,
    week_key: str,
    data: weekly_digest.WeeklyDigestData,
) -> TelegramPayload:
    digest_text = weekly_digest._build_weekly_digest_text(data)
    return TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "bot_token": bot_token,
            "chat_id": chat_id,
            "account_email": account_email,
            "week_key": week_key,
        },
    )


def _send_payload(
    *,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    payload: TelegramPayload,
) -> DeliveryResult:
    return telegram_sender(payload)


def run_digest_tick(
    *,
    now: datetime,
    config,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
) -> None:
    try:
        flags = FeatureFlags(base_dir=_CONFIG_PATH.parent)
        overrides = RuntimeOverrideStore(storage.knowledge_db.path).get_overrides()
        daily_config = _load_daily_digest_config()
        weekly_config = _load_weekly_digest_config()
        silence_policy = load_silence_policy_config(_CONFIG_PATH.parent)
        policy_inputs = _collect_policy_inputs(storage, logger)

        for account in config.accounts:
            account_email = account.login
            chat_id = account.telegram_chat_id
            bot_token = config.keys.telegram_bot_token

            if not chat_id:
                logger.warning(
                    "digest_tick_checked",
                    digest_type="daily",
                    decision="skipped",
                    reason="missing_chat_id",
                    account_email=account_email,
                )
                logger.warning(
                    "digest_tick_checked",
                    digest_type="weekly",
                    decision="skipped",
                    reason="missing_chat_id",
                    account_email=account_email,
                )
                continue

            daily_enabled, daily_reason = _override_flag(
                flags.ENABLE_DAILY_DIGEST, overrides.digest_enabled
            )
            if daily_enabled:
                _run_daily_digest(
                    now=now,
                    config=daily_config,
                    account_email=account_email,
                    chat_id=chat_id,
                    bot_token=bot_token,
                    storage=storage,
                    telegram_sender=telegram_sender,
                    logger=logger,
                    policy_inputs=policy_inputs,
                    flags=flags,
                    silence_policy=silence_policy,
                    include_anomalies=flags.ENABLE_ANOMALY_ALERTS,
                    include_attention_economics=flags.ENABLE_ATTENTION_ECONOMICS,
                    include_quality_metrics=flags.ENABLE_QUALITY_METRICS,
                )
            else:
                logger.info(
                    "digest_tick_checked",
                    digest_type="daily",
                    decision="skipped",
                    reason=daily_reason,
                    account_email=account_email,
                )

            weekly_enabled, weekly_reason = _override_flag(
                flags.ENABLE_WEEKLY_DIGEST, overrides.digest_enabled
            )
            if weekly_enabled:
                _run_weekly_digest(
                    now=now,
                    config=weekly_config,
                    account_email=account_email,
                    chat_id=chat_id,
                    bot_token=bot_token,
                    storage=storage,
                    telegram_sender=telegram_sender,
                    logger=logger,
                    policy_inputs=policy_inputs,
                    flags=flags,
                    include_anomalies=flags.ENABLE_ANOMALY_ALERTS,
                    include_attention_economics=flags.ENABLE_ATTENTION_ECONOMICS,
                    include_quality_metrics=flags.ENABLE_QUALITY_METRICS,
                )
            else:
                logger.info(
                    "digest_tick_checked",
                    digest_type="weekly",
                    decision="skipped",
                    reason=weekly_reason,
                    account_email=account_email,
                )
    except Exception as exc:
        logger.error("digest_tick_failed", error=str(exc))


def _override_flag(default: bool, override: bool | None) -> tuple[bool, str]:
    if override is None:
        return default, "flag_disabled" if not default else "flag_enabled"
    if override:
        return True, "override_enabled"
    return False, "override_disabled"


def _run_daily_digest(
    *,
    now: datetime,
    config: DailyDigestConfig,
    account_email: str,
    chat_id: str,
    bot_token: str,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
    policy_inputs: PolicyInputs,
    flags: FeatureFlags,
    silence_policy: SilencePolicyConfig,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
) -> None:
    if not _is_daily_due(now, config):
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
        )
        return

    if storage.analytics.has_daily_digest_sent(account_email=account_email, day=now):
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
        )
        return

    if flags.ENABLE_SILENCE_AS_SIGNAL in {"shadow", "enabled"}:
        if storage.contract_event_emitter is not None:
            try:
                run_silence_scan(
                    knowledge_db=storage.knowledge_db,
                    event_emitter=storage.contract_event_emitter,
                    account_email=account_email,
                    now_ts=now.timestamp(),
                    policy=silence_policy,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("silence_detector_failed", error=str(exc))

    data = daily_digest._collect_digest_data(
        analytics=storage.analytics,
        account_email=account_email,
        include_anomalies=include_anomalies,
        include_attention_economics=include_attention_economics,
        include_quality_metrics=include_quality_metrics,
        now=now,
        contract_event_emitter=storage.contract_event_emitter,
    )
    has_content = daily_digest._has_digest_content(data)
    if not has_content:
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="no_content",
            account_email=account_email,
        )
        return

    policy_decision = _evaluate_policy(
        policy_inputs=policy_inputs,
        flags=flags,
        has_daily_digest_content=has_content,
        has_weekly_digest_content=False,
    )
    if not policy_decision.allow_daily_digest:
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="policy_denied",
            account_email=account_email,
            system_mode=policy_decision.mode.value,
        )
        return

    logger.info(
        "digest_tick_checked",
        digest_type="daily",
        decision="due",
        account_email=account_email,
    )

    payload = _build_daily_payload(
        account_email=account_email,
        chat_id=chat_id,
        bot_token=bot_token,
        data=data,
    )

    try:
        result = _send_payload(telegram_sender=telegram_sender, payload=payload)
    except Exception as exc:
        logger.error(
            "digest_failed",
            digest_type="daily",
            account_email=account_email,
            error=str(exc),
        )
        return

    if result.delivered:
        storage.knowledge_db.set_last_digest_sent_at(
            account_email=account_email,
            sent_at=now,
        )
        logger.info(
            "digest_sent",
            digest_type="daily",
            account_email=account_email,
        )
        if storage.contract_event_emitter is not None:
            try:
                storage.contract_event_emitter.emit(
                    EventV1(
                        event_type=EventType.DAILY_DIGEST_SENT,
                        ts_utc=now.timestamp(),
                        account_id=account_email,
                        entity_id=None,
                        email_id=0,
                        payload={
                            "account_email": account_email,
                        },
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "contract_event_emit_failed",
                    event_type=EventType.DAILY_DIGEST_SENT.value,
                    error=str(exc),
                )
        return

    logger.error(
        "digest_failed",
        digest_type="daily",
        account_email=account_email,
        error=result.error or "telegram delivery failed",
        retryable=result.retryable,
    )


def _run_weekly_digest(
    *,
    now: datetime,
    config: WeeklyDigestConfig,
    account_email: str,
    chat_id: str,
    bot_token: str,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
    policy_inputs: PolicyInputs,
    flags: FeatureFlags,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
) -> None:
    week_key = weekly_digest._iso_week_key(now)

    if not _is_weekly_due(now, config):
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_skipped",
                timestamp=now,
                email_id=0,
                payload={
                    "reason": "not_due",
                    "week_key": week_key,
                    "account_email": account_email,
                },
            )
        return

    if storage.analytics.has_weekly_digest_sent(
        account_email=account_email,
        week_key=week_key,
    ):
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_skipped",
                timestamp=now,
                email_id=0,
                payload={
                    "reason": "already_sent",
                    "week_key": week_key,
                    "account_email": account_email,
                },
            )
        return

    logger.info(
        "digest_tick_checked",
        digest_type="weekly",
        decision="due",
        account_email=account_email,
        week_key=week_key,
    )

    data = weekly_digest._collect_weekly_data(
        analytics=storage.analytics,
        account_email=account_email,
        week_key=week_key,
        include_anomalies=include_anomalies,
        include_attention_economics=include_attention_economics,
        include_quality_metrics=include_quality_metrics,
        event_emitter=storage.event_emitter,
        contract_event_emitter=storage.contract_event_emitter,
        now=now,
    )
    has_content = _has_weekly_content(data)
    if not has_content:
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="no_content",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_skipped",
                timestamp=now,
                email_id=0,
                payload={
                    "reason": "no_content",
                    "week_key": week_key,
                    "account_email": account_email,
                },
            )
        return

    policy_decision = _evaluate_policy(
        policy_inputs=policy_inputs,
        flags=flags,
        has_daily_digest_content=False,
        has_weekly_digest_content=has_content,
    )
    if not policy_decision.allow_weekly_digest:
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="policy_denied",
            account_email=account_email,
            week_key=week_key,
            system_mode=policy_decision.mode.value,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_skipped",
                timestamp=now,
                email_id=0,
                payload={
                    "reason": "policy_denied",
                    "week_key": week_key,
                    "account_email": account_email,
                },
            )
        return
    payload = _build_weekly_payload(
        account_email=account_email,
        chat_id=chat_id,
        bot_token=bot_token,
        week_key=week_key,
        data=data,
    )

    try:
        result = _send_payload(telegram_sender=telegram_sender, payload=payload)
    except Exception as exc:
        logger.error(
            "digest_failed",
            digest_type="weekly",
            account_email=account_email,
            week_key=week_key,
            error=str(exc),
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_failed",
                timestamp=now,
                email_id=0,
                payload={
                    "week_key": week_key,
                    "account_email": account_email,
                    "error": str(exc),
                },
            )
        return

    if result.delivered:
        storage.knowledge_db.set_last_weekly_digest_state(
            account_email=account_email,
            week_key=week_key,
            sent_at=now,
        )
        logger.info(
            "digest_sent",
            digest_type="weekly",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_sent",
                timestamp=now,
                email_id=0,
                payload={
                    "week_key": week_key,
                    "account_email": account_email,
                    "total_emails": data.total_emails,
                    "deferred_emails": data.deferred_emails,
                },
            )
        if storage.contract_event_emitter is not None:
            try:
                storage.contract_event_emitter.emit(
                    EventV1(
                        event_type=EventType.WEEKLY_DIGEST_SENT,
                        ts_utc=now.timestamp(),
                        account_id=account_email,
                        entity_id=None,
                        email_id=0,
                        payload={
                            "week_key": week_key,
                            "account_email": account_email,
                            "total_emails": data.total_emails,
                            "deferred_emails": data.deferred_emails,
                        },
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "contract_event_emit_failed",
                    event_type=EventType.WEEKLY_DIGEST_SENT.value,
                    error=str(exc),
                )
        return

    logger.error(
        "digest_failed",
        digest_type="weekly",
        account_email=account_email,
        week_key=week_key,
        error=result.error or "telegram delivery failed",
        retryable=result.retryable,
    )
    if storage.event_emitter:
        storage.event_emitter.emit(
            type="weekly_digest_failed",
            timestamp=now,
            email_id=0,
            payload={
                "week_key": week_key,
                "account_email": account_email,
                "error": result.error or "telegram delivery failed",
            },
        )


__all__ = ["DigestStorage", "DailyDigestConfig", "WeeklyDigestConfig", "run_digest_tick"]
