from __future__ import annotations

import configparser
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.behavior.silence_detector import run_silence_scan
from mailbot_v26.config_loader import resolve_account_scope
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.regret_minimization import (
    RegretMinimizationConfig,
    load_regret_minimization_config,
)
from mailbot_v26.config.commitment_chain_digest import (
    CommitmentChainDigestConfig,
    load_commitment_chain_digest_config,
)
from mailbot_v26.config.trust_bootstrap import (
    TrustBootstrapConfig,
    load_trust_bootstrap_config,
)
from mailbot_v26.config.uncertainty_queue import (
    UncertaintyQueueConfig,
    load_uncertainty_queue_config,
)
from mailbot_v26.config.silence_policy import SilencePolicyConfig, load_silence_policy_config
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.config_yaml import (
    load_config as load_yaml_config,
    resolve_support_enabled,
)
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
_LOGGER = logging.getLogger(__name__)
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


@dataclass(frozen=True, slots=True)
class WeeklyAccuracyReportConfig:
    window_days: int


@dataclass(frozen=True, slots=True)
class WeeklyCalibrationReportConfig:
    window_days: int
    top_n: int
    min_corrections: int


@dataclass(frozen=True, slots=True)
class DigestInsightsConfig:
    window_days: int
    max_items: int


@dataclass(frozen=True, slots=True)
class BehaviorMetricsDigestConfig:
    window_days: int


@dataclass(frozen=True, slots=True)
class TrustBootstrapDigestConfig:
    settings: TrustBootstrapConfig


@dataclass(frozen=True, slots=True)
class RegretMinimizationDigestConfig:
    settings: RegretMinimizationConfig


@dataclass(frozen=True, slots=True)
class SupportTelegramConfig:
    enabled: bool
    frequency_days: int
    text: str


def _load_ini_parser() -> configparser.ConfigParser:
    return read_user_ini_with_defaults(
        _CONFIG_PATH,
        logger=_LOGGER,
        scope_label="digest scheduler settings",
    )


def _load_daily_digest_config() -> DailyDigestConfig:
    hour = 9
    minute = 0
    parser = _load_ini_parser()
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
    parser = _load_ini_parser()
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


def _load_weekly_accuracy_report_config() -> WeeklyAccuracyReportConfig:
    window_days = 7
    parser = _load_ini_parser()
    section = (
        parser["weekly_accuracy_report"]
        if "weekly_accuracy_report" in parser
        else None
    )
    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=7))
        except ValueError:
            window_days = 7
    return WeeklyAccuracyReportConfig(window_days=window_days)


def _load_weekly_calibration_report_config() -> WeeklyCalibrationReportConfig:
    window_days = 7
    top_n = 3
    min_corrections = 10
    parser = _load_ini_parser()
    section = (
        parser["weekly_calibration_report"]
        if "weekly_calibration_report" in parser
        else None
    )
    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=7))
        except ValueError:
            window_days = 7
        try:
            top_n = max(0, section.getint("top_n", fallback=3))
        except ValueError:
            top_n = 3
        try:
            min_corrections = max(0, section.getint("min_corrections", fallback=10))
        except ValueError:
            min_corrections = 10
    return WeeklyCalibrationReportConfig(
        window_days=window_days,
        top_n=top_n,
        min_corrections=min_corrections,
    )


def _load_digest_insights_config() -> DigestInsightsConfig:
    window_days = 7
    max_items = 3
    parser = _load_ini_parser()
    section = parser["digest_insights"] if "digest_insights" in parser else None
    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=7))
        except ValueError:
            window_days = 7
        try:
            max_items = max(0, section.getint("max_items", fallback=3))
        except ValueError:
            max_items = 3
    return DigestInsightsConfig(window_days=window_days, max_items=max_items)


def _load_behavior_metrics_digest_config() -> BehaviorMetricsDigestConfig:
    window_days = 7
    parser = _load_ini_parser()
    section = (
        parser["behavior_metrics_digest"] if "behavior_metrics_digest" in parser else None
    )
    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=7))
        except ValueError:
            window_days = 7
    return BehaviorMetricsDigestConfig(window_days=window_days)


def _load_trust_bootstrap_config() -> TrustBootstrapDigestConfig:
    settings = load_trust_bootstrap_config(_CONFIG_PATH.parent)
    return TrustBootstrapDigestConfig(settings=settings)


def _load_regret_minimization_config() -> RegretMinimizationDigestConfig:
    settings = load_regret_minimization_config(_CONFIG_PATH.parent)
    return RegretMinimizationDigestConfig(settings=settings)


def _load_commitment_chain_digest_config() -> CommitmentChainDigestConfig:
    return load_commitment_chain_digest_config(_CONFIG_PATH.parent)


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


def _resolve_yaml_config_path() -> Path | None:
    base_dir = Path(__file__).resolve().parents[1]
    candidates = [base_dir / "config.yaml", base_dir.parent / "config.yaml"]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_support_telegram_config() -> SupportTelegramConfig:
    config_path = _resolve_yaml_config_path()
    if config_path is None:
        return SupportTelegramConfig(enabled=False, frequency_days=30, text="")
    try:
        raw = load_yaml_config(config_path)
    except Exception:
        return SupportTelegramConfig(enabled=False, frequency_days=30, text="")
    if not isinstance(raw, dict):
        return SupportTelegramConfig(enabled=False, frequency_days=30, text="")
    if not resolve_support_enabled(raw):
        return SupportTelegramConfig(enabled=False, frequency_days=30, text="")
    support = raw.get("support") if isinstance(raw, dict) else None
    telegram = support.get("telegram") if isinstance(support, dict) else None
    if not isinstance(telegram, dict):
        return SupportTelegramConfig(enabled=False, frequency_days=30, text="")
    return SupportTelegramConfig(
        enabled=bool(telegram.get("enabled", False)),
        frequency_days=max(7, min(365, int(telegram.get("frequency_days", 30) or 30))),
        text=str(telegram.get("text", "") or "").strip(),
    )


def _support_state_path(storage: DigestStorage) -> Path:
    return storage.knowledge_db.path.parent / "support_state.json"


def _support_due(*, now: datetime, state_path: Path, frequency_days: int) -> bool:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if not state_path.exists():
        return True
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return True
    last_shown_raw = payload.get("last_shown_utc") if isinstance(payload, dict) else None
    if not isinstance(last_shown_raw, str) or not last_shown_raw.strip():
        return True
    try:
        last_shown = datetime.fromisoformat(last_shown_raw)
    except ValueError:
        return True
    if last_shown.tzinfo is None:
        last_shown = last_shown.replace(tzinfo=timezone.utc)
    return now >= last_shown + timedelta(days=max(7, frequency_days))


def _store_support_shown_at(*, now: datetime, state_path: Path) -> None:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(".tmp")
    payload = {"last_shown_utc": now.astimezone(timezone.utc).isoformat()}
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(state_path)


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
        or (
            data.weekly_accuracy_report is not None
            and int(data.weekly_accuracy_report.get("priority_corrections") or 0) >= 3
        )
    )


def _build_daily_payload(
    *,
    account_email: str,
    chat_id: str,
    bot_token: str,
    data: daily_digest.DigestData,
    support_ps: str = "",
) -> TelegramPayload:
    digest_text = daily_digest._build_digest_text(data)
    if support_ps:
        digest_text = f"{digest_text}\n\n<i>P.S. {support_ps}</i>"
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


def _group_accounts_by_chat_id(accounts) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for account in accounts:
        chat_id = account.telegram_chat_id
        if not chat_id:
            continue
        groups.setdefault(chat_id, []).append(account.login)
    return groups


def _is_primary_account(
    *,
    account_email: str,
    chat_id: str,
    chat_groups: dict[str, list[str]],
) -> bool:
    group = chat_groups.get(chat_id, [])
    return bool(group) and group[0] == account_email


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
        weekly_accuracy_report_config = _load_weekly_accuracy_report_config()
        weekly_calibration_report_config = _load_weekly_calibration_report_config()
        digest_insights_config = _load_digest_insights_config()
        behavior_metrics_config = _load_behavior_metrics_digest_config()
        trust_bootstrap_config = _load_trust_bootstrap_config()
        regret_minimization_config = _load_regret_minimization_config()
        commitment_chain_digest_config = _load_commitment_chain_digest_config()
        uncertainty_queue_config = load_uncertainty_queue_config(_CONFIG_PATH.parent)
        silence_policy = load_silence_policy_config(_CONFIG_PATH.parent)
        policy_inputs = _collect_policy_inputs(storage, logger)

        chat_groups = _group_accounts_by_chat_id(config.accounts)

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
                if not _is_primary_account(
                    account_email=account_email,
                    chat_id=chat_id,
                    chat_groups=chat_groups,
                ):
                    logger.info(
                        "digest_tick_checked",
                        digest_type="daily",
                        decision="skipped",
                        reason="dedup_chat_id_already_processed",
                        account_email=account_email,
                        chat_id=chat_id,
                    )
                else:
                    _run_daily_digest(
                        now=now,
                        config=daily_config,
                        account_email=account_email,
                        account_emails=chat_groups[chat_id],
                        chat_scope=f"tg:{chat_id}",
                        chat_id=chat_id,
                        bot_token=bot_token,
                        storage=storage,
                        telegram_sender=telegram_sender,
                        logger=logger,
                        policy_inputs=policy_inputs,
                        flags=flags,
                        silence_policy=silence_policy,
                        digest_insights_config=digest_insights_config,
                        behavior_metrics_config=behavior_metrics_config,
                        commitment_chain_digest_config=commitment_chain_digest_config,
                        uncertainty_queue_config=uncertainty_queue_config,
                        trust_bootstrap_config=trust_bootstrap_config,
                        regret_minimization_config=regret_minimization_config,
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
                if not _is_primary_account(
                    account_email=account_email,
                    chat_id=chat_id,
                    chat_groups=chat_groups,
                ):
                    logger.info(
                        "digest_tick_checked",
                        digest_type="weekly",
                        decision="skipped",
                        reason="dedup_chat_id_already_processed",
                        account_email=account_email,
                        chat_id=chat_id,
                    )
                else:
                    _run_weekly_digest(
                        now=now,
                        config=weekly_config,
                        weekly_accuracy_window_days=weekly_accuracy_report_config.window_days,
                        weekly_calibration_window_days=weekly_calibration_report_config.window_days,
                        weekly_calibration_top_n=weekly_calibration_report_config.top_n,
                        weekly_calibration_min_corrections=weekly_calibration_report_config.min_corrections,
                        account_email=account_email,
                        account_emails=chat_groups[chat_id],
                        chat_scope=f"tg:{chat_id}",
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
                        include_weekly_accuracy_report=flags.ENABLE_WEEKLY_ACCURACY_REPORT,
                        include_weekly_calibration_report=flags.ENABLE_WEEKLY_CALIBRATION_REPORT,
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
    account_emails: Iterable[str],
    chat_scope: str,
    chat_id: str,
    bot_token: str,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
    policy_inputs: PolicyInputs,
    flags: FeatureFlags,
    silence_policy: SilencePolicyConfig,
    digest_insights_config: DigestInsightsConfig,
    behavior_metrics_config: BehaviorMetricsDigestConfig,
    commitment_chain_digest_config: CommitmentChainDigestConfig,
    uncertainty_queue_config: UncertaintyQueueConfig,
    trust_bootstrap_config: TrustBootstrapDigestConfig,
    regret_minimization_config: RegretMinimizationDigestConfig,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
    include_weekly_accuracy_report: bool = False,
) -> None:
    resolved_scope = resolve_account_scope(
        account_email, base_dir=_CONFIG_PATH.parent
    )
    scope_account_emails = (
        resolved_scope.account_emails if resolved_scope else None
    )
    support_cfg = _load_support_telegram_config()
    support_state_path = _support_state_path(storage)
    support_ps = ""
    if support_cfg.enabled and support_cfg.text:
        if _support_due(now=now, state_path=support_state_path, frequency_days=support_cfg.frequency_days):
            support_ps = support_cfg.text

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
                    account_emails=scope_account_emails,
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
        include_digest_insights=flags.ENABLE_DIGEST_INSIGHTS,
        digest_insights_window_days=digest_insights_config.window_days,
        digest_insights_max_items=digest_insights_config.max_items,
        include_digest_action_templates=flags.ENABLE_DIGEST_ACTION_TEMPLATES,
        include_behavior_metrics_digest=flags.ENABLE_BEHAVIOR_METRICS_DIGEST,
        behavior_metrics_window_days=behavior_metrics_config.window_days,
        include_uncertainty_queue=flags.ENABLE_UNCERTAINTY_QUEUE,
        uncertainty_queue_config=uncertainty_queue_config,
        include_commitment_chain_digest=flags.ENABLE_COMMITMENT_CHAIN_DIGEST,
        commitment_chain_digest_config=commitment_chain_digest_config,
        include_trust_bootstrap=flags.ENABLE_TRUST_BOOTSTRAP,
        trust_bootstrap_config=trust_bootstrap_config.settings,
        include_regret_minimization=flags.ENABLE_REGRET_MINIMIZATION,
        regret_minimization_config=regret_minimization_config.settings,
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
        support_ps=support_ps,
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
        for email in account_emails:
            storage.knowledge_db.set_last_digest_sent_at(
                account_email=email,
                sent_at=now,
            )
        if support_ps:
            _store_support_shown_at(now=now, state_path=support_state_path)
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
                            "account_emails": list(account_emails),
                            "chat_scope": chat_scope,
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
    weekly_accuracy_window_days: int,
    weekly_calibration_window_days: int,
    weekly_calibration_top_n: int,
    weekly_calibration_min_corrections: int,
    account_email: str,
    account_emails: Iterable[str],
    chat_scope: str,
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
    include_weekly_accuracy_report: bool = False,
    include_weekly_calibration_report: bool = False,
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
        include_weekly_accuracy_report=include_weekly_accuracy_report,
        weekly_accuracy_window_days=weekly_accuracy_window_days,
        include_weekly_calibration_report=include_weekly_calibration_report,
        weekly_calibration_window_days=weekly_calibration_window_days,
        weekly_calibration_top_n=weekly_calibration_top_n,
        weekly_calibration_min_corrections=weekly_calibration_min_corrections,
        event_emitter=storage.event_emitter,
        contract_event_emitter=storage.contract_event_emitter,
        now=now,
    )
    has_content = True

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
        for email in account_emails:
            storage.knowledge_db.set_last_weekly_digest_state(
                account_email=email,
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
                            "account_emails": list(account_emails),
                            "chat_scope": chat_scope,
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
