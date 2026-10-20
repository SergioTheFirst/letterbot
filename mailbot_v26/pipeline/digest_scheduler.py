from __future__ import annotations

import configparser
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.account_identity import normalize_login
from mailbot_v26.behavior.silence_detector import run_silence_scan
from mailbot_v26.config.paths import default_config_dir, resolve_config_paths
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
from mailbot_v26.config.silence_policy import (
    SilencePolicyConfig,
    load_silence_policy_config,
)
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.config_yaml import (
    load_config as load_yaml_config,
    resolve_support_enabled,
)
from mailbot_v26.llm.runtime_flags import RuntimeFlags, RuntimeFlagStore
from mailbot_v26.observability.logger import LoggerLike
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.observability.redact import redact_secrets
from mailbot_v26.observability.metrics import (
    GateEvaluation,
    MetricsAggregator,
    SystemGates,
)
from mailbot_v26.pipeline import daily_digest, weekly_digest
from mailbot_v26.pipeline.processor import _resolve_outbound_ui_locale
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.system.orchestrator import SystemOrchestrator, SystemPolicyDecision
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.telegram.inbound import read_chat_digest_override
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.ui.branding import append_watermark
from mailbot_v26.worker.telegram_sender import DeliveryResult

_CONFIG_PATH = default_config_dir() / "config.ini"
_LOGGER = logging.getLogger(__name__)
_runtime_flag_store = RuntimeFlagStore()
_system_orchestrator = SystemOrchestrator()
_system_gates = SystemGates()


def configure_digest_config_dir(config_dir: Path) -> None:
    global _CONFIG_PATH
    _CONFIG_PATH = config_dir / "config.ini"


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
    telegram: bool
    min_days_between_asks: int
    url: str
    message: str


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
        parser["weekly_accuracy_report"] if "weekly_accuracy_report" in parser else None
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
        parser["behavior_metrics_digest"]
        if "behavior_metrics_digest" in parser
        else None
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


def _weekly_window(now: datetime, config: WeeklyDigestConfig) -> tuple[str, bool]:
    """(week_key, due) of the weekly digest for the local time ``now`` (B6).

    A digest week starts at local midnight of the configured weekday. The
    digest is due from the configured time that day until the next such
    midnight, so a worker that was off at the slot catches up later in the
    week, whatever the weekday. The key is the ISO week of the slot day: the
    key the UTC build wrote as well, so the switch day sends no duplicate.
    """
    days_since_slot_day = (now.weekday() - config.weekday) % 7
    slot_day = now - timedelta(days=days_since_slot_day)
    past_slot_time = (now.hour, now.minute) >= (config.hour, config.minute)
    due = days_since_slot_day > 0 or past_slot_time
    return weekly_digest._iso_week_key(slot_day), due


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


def _disabled_support_config() -> SupportTelegramConfig:
    return SupportTelegramConfig(
        enabled=False,
        telegram=False,
        min_days_between_asks=30,
        url="",
        message="",
    )


def _load_support_config_from_settings(
    settings: configparser.ConfigParser,
) -> SupportTelegramConfig | None:
    if "support" not in settings:
        return None
    section = settings["support"]
    disabled = _disabled_support_config()
    try:
        enabled = section.getboolean("enabled", fallback=disabled.enabled)
    except ValueError:
        enabled = disabled.enabled
    try:
        telegram_enabled = section.getboolean("telegram", fallback=True)
    except ValueError:
        telegram_enabled = True
    try:
        min_days_between_asks = max(
            1, section.getint("min_days_between_asks", fallback=30)
        )
    except ValueError:
        min_days_between_asks = 30
    url = str(section.get("url", fallback="")).strip()
    message = str(section.get("message", fallback="")).strip()
    return SupportTelegramConfig(
        enabled=enabled,
        telegram=telegram_enabled,
        min_days_between_asks=min_days_between_asks,
        url=url,
        message=message,
    )


def _load_support_telegram_config() -> SupportTelegramConfig:
    resolved_paths = resolve_config_paths(_CONFIG_PATH.parent)
    if resolved_paths.two_file_mode:
        settings = _load_ini_parser()
        loaded = _load_support_config_from_settings(settings)
        return loaded or _disabled_support_config()

    config_path = _resolve_yaml_config_path()
    if config_path is None:
        return _disabled_support_config()
    try:
        raw = load_yaml_config(config_path)
    except Exception:
        return _disabled_support_config()
    if not isinstance(raw, dict):
        return _disabled_support_config()
    if not resolve_support_enabled(raw):
        return _disabled_support_config()
    support = raw.get("support") if isinstance(raw, dict) else None
    telegram = support.get("telegram") if isinstance(support, dict) else None
    if not isinstance(telegram, dict):
        return _disabled_support_config()
    text = str(telegram.get("text", "") or "").strip()
    return SupportTelegramConfig(
        enabled=bool(telegram.get("enabled", False)),
        telegram=True,
        min_days_between_asks=max(
            7, min(365, int(telegram.get("frequency_days", 30) or 30))
        ),
        url="",
        message=text,
    )


def _support_last_ask_key(*, chat_id: str | None, account_email: str) -> str:
    identity = str(chat_id or "").strip() or account_email.strip().lower()
    return f"support:last_ask_utc:{identity}"


def _parse_iso_utc(value: str) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _render_support_footer(config: SupportTelegramConfig) -> str:
    message = config.message.strip()
    if not message:
        return ""
    resolved_url = config.url.strip()
    if "{url}" in message and resolved_url:
        message = message.replace("{url}", resolved_url)
    return message.strip()


def _resolve_weekly_support_footer(
    *,
    now_utc: datetime,
    account_email: str,
    chat_id: str,
    override_store: RuntimeOverrideStore,
    support_config: SupportTelegramConfig,
) -> str:
    if not support_config.enabled or not support_config.telegram:
        return ""
    footer = _render_support_footer(support_config)
    if not footer:
        return ""
    key = _support_last_ask_key(chat_id=chat_id, account_email=account_email)
    last_ask_raw = override_store.get_value(key)
    if last_ask_raw:
        last_ask = _parse_iso_utc(last_ask_raw)
        if last_ask is None:
            return ""
        min_delta = timedelta(days=max(1, support_config.min_days_between_asks))
        if now_utc < last_ask + min_delta:
            return ""
    override_store.set_value(key, now_utc.astimezone(timezone.utc).isoformat())
    return footer


def _has_weekly_content(data: weekly_digest.WeeklyDigestData) -> bool:
    # Analytics return zero-valued dicts ({"created": 0, ...}, {"up": [], ...})
    # and an always-present quality snapshot, so test values, not presence.
    return bool(
        data.total_emails
        or data.deferred_emails
        or data.attention_entities
        or any(int(value or 0) > 0 for value in data.commitment_counts.values())
        or data.overdue_commitments
        or any(data.trust_deltas.values())
        or data.anomaly_alerts
        or (
            data.quality_metrics is not None
            and data.quality_metrics.corrections_total > 0
        )
        or data.attention_economics is not None
        or (
            data.silence_risk is not None
            and int(data.silence_risk.get("days_silent") or 0) > 0
        )
        or (
            data.weekly_accuracy_report is not None
            and int(data.weekly_accuracy_report.get("priority_corrections") or 0) >= 3
            and data.weekly_accuracy_report.get("accuracy_pct") is not None
            and int(data.weekly_accuracy_report.get("accuracy_pct") or 0) >= 80
        )
    )


# Per-window markers (runtime_overrides) so a due window without content is
# evaluated once instead of on every scheduler tick until the window ends.
_DAILY_NO_CONTENT_KEY = "digest:daily_no_content_day"
_WEEKLY_NO_CONTENT_KEY = "digest:weekly_no_content_week"
_WEEKLY_SKIP_STATE_KEY = "digest:weekly_skip_state"


def _digest_marker_key(prefix: str, account_email: str) -> str:
    return f"{prefix}:{account_email.strip().lower()}"


def _daily_window_key(now: datetime) -> str:
    return f"{now.year:04d}-{now.month:02d}-{now.day:02d}"


def _emit_weekly_skip_on_change(
    *,
    storage: DigestStorage,
    marker_store: RuntimeOverrideStore,
    account_email: str,
    week_key: str,
    reason: str,
    now: datetime,
) -> None:
    if not storage.event_emitter:
        return
    state_key = _digest_marker_key(_WEEKLY_SKIP_STATE_KEY, account_email)
    state = f"{week_key}:{reason}"
    if marker_store.get_value(state_key) == state:
        return
    storage.event_emitter.emit(
        type="weekly_digest_skipped",
        timestamp=now,
        email_id=0,
        payload={
            "reason": reason,
            "week_key": week_key,
            "account_email": account_email,
        },
    )
    marker_store.set_value(state_key, state)


def _build_daily_payload(
    *,
    account_email: str,
    chat_id: str,
    bot_token: str,
    data: daily_digest.DigestData,
    locale: str | None = None,
) -> TelegramPayload:
    resolved_locale = locale or _resolve_outbound_ui_locale()
    digest_text = append_watermark(
        daily_digest._build_digest_text(data, locale=resolved_locale), html=True
    )
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
    support_footer: str = "",
    locale: str | None = None,
) -> TelegramPayload:
    resolved_locale = locale or _resolve_outbound_ui_locale()
    digest_text = weekly_digest._build_weekly_digest_text(data, locale=resolved_locale)
    if support_footer:
        digest_text = f"{digest_text}\n{support_footer}"
    digest_text = append_watermark(digest_text, html=True)
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


# Sent markers (runtime_overrides, value = day or week key): reserved before
# the POST, so a digest goes out at most once per window (B17), like per-email
# Telegram delivery. Keys use the login as configured.
_DAILY_SENT_KEY = "digest:daily_sent_day"
_WEEKLY_SENT_KEY = "digest:weekly_sent_week"
_MARKER_DB_TIMEOUT_S = 5.0


def _error_text(exc: BaseException) -> str:
    return redact_secrets(f"{type(exc).__name__}: {exc}")


def _marker_connection(storage: DigestStorage) -> sqlite3.Connection:
    return sqlite3.connect(storage.knowledge_db.path, timeout=_MARKER_DB_TIMEOUT_S)


def _read_sent_marker(storage: DigestStorage, key: str) -> str | None:
    """The window a digest was last reserved for. Raises sqlite3.Error: an
    unreadable marker must not read as "not sent" (B17)."""
    with closing(_marker_connection(storage)) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_overrides WHERE key = ?", (key,)
        ).fetchone()
    return None if row is None or row[0] is None else str(row[0])


def _reserve_sent_marker(storage: DigestStorage, key: str, window: str) -> bool:
    """Reserve ``window`` before the POST; False when it is already reserved.

    One atomic upsert, so two ticks can never both win a window. Raises
    sqlite3.Error when the store is unavailable: then nothing is sent.
    """
    with closing(_marker_connection(storage)) as conn, conn:
        cursor = conn.execute(
            """
            INSERT INTO runtime_overrides (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            WHERE runtime_overrides.value IS NOT excluded.value
            """,
            (key, window, datetime.now(timezone.utc).isoformat()),
        )
        return cursor.rowcount == 1


def _release_sent_marker(
    storage: DigestStorage,
    key: str,
    window: str,
    *,
    logger: LoggerLike,
    log_fields: dict[str, object],
) -> None:
    """Give the window back after a POST that surely did not reach Telegram."""
    try:
        with closing(_marker_connection(storage)) as conn, conn:
            conn.execute(
                "DELETE FROM runtime_overrides WHERE key = ? AND value = ?",
                (key, window),
            )
    except sqlite3.Error as exc:
        # The reservation stays: no retry in this window, never a duplicate.
        logger.error(
            "digest_reservation_release_failed", **log_fields, error=_error_text(exc)
        )


@dataclass(frozen=True, slots=True)
class _SendOutcome:
    # delivered | taken | unavailable | unknown | failed | retryable
    status: str
    error: str | None = None


def _send_digest_once(
    *,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    payload: TelegramPayload,
    sent_key: str,
    window: str,
    logger: LoggerLike,
    log_fields: dict[str, object],
) -> _SendOutcome:
    """Reserve the window, POST once, then settle the reservation (B17).

    At-most-once: a POST that may have reached Telegram (the sender raised,
    or the request went out and the reply was lost) keeps the window
    reserved, and so does a definite rejection, which every later tick would
    only repeat. "Not sent, retry later" (429, 5xx, no connection) gives the
    window back for the next tick.
    """
    try:
        if not _reserve_sent_marker(storage, sent_key, window):
            return _SendOutcome("taken")
    except sqlite3.Error as exc:
        logger.error("digest_reserve_failed", **log_fields, error=_error_text(exc))
        return _SendOutcome("unavailable", _error_text(exc))
    try:
        result = _send_payload(telegram_sender=telegram_sender, payload=payload)
    except Exception as exc:
        error = _error_text(exc)
        logger.error("digest_failed", **log_fields, delivery="unknown", error=error)
        return _SendOutcome("unknown", error)
    if result.delivered:
        return _SendOutcome("delivered")
    if result.maybe_delivered:
        status = "unknown"
    elif result.retryable:
        status = "retryable"
        _release_sent_marker(
            storage, sent_key, window, logger=logger, log_fields=log_fields
        )
    else:
        status = "failed"
    error = redact_secrets(result.error or "telegram delivery failed")
    logger.error(
        "digest_failed",
        **log_fields,
        delivery=status,
        error=error,
        retryable=result.retryable,
    )
    return _SendOutcome(status, error)


def _group_accounts_by_chat_id(accounts) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for account in accounts:
        chat_id = account.telegram_chat_id
        if not chat_id:
            continue
        groups.setdefault(chat_id, []).append(account.login)
    return groups


def _chat_scope_ids(logins: Iterable[str]) -> list[str]:
    """Account ids that match a chat's accounts in stored rows (B8).

    Ingest stores rows under normalize_login(login); older rows may hold the
    login as configured, so both forms match. digest_state, the sent events
    and the marker keys keep the configured login.
    """
    scope: list[str] = []
    for login in logins:
        for candidate in (str(login or "").strip(), normalize_login(login)):
            if candidate and candidate not in scope:
                scope.append(candidate)
    return scope


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
    """Send due daily/weekly digests.

    ``now`` is the local wall-clock time (the worker passes
    ``datetime.now().astimezone()``): the due hour, weekday and day/week
    windows follow the user's clock (B6). A naive value is read as local time.
    Stored timestamps stay UTC.
    """
    if now.tzinfo is None:
        now = now.astimezone()
    try:
        flags = FeatureFlags(base_dir=_CONFIG_PATH.parent)
        override_store = RuntimeOverrideStore(storage.knowledge_db.path)
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

            # /digest on|off is per chat (B16), legacy global key as fallback.
            digest_override = read_chat_digest_override(override_store, chat_id)
            daily_enabled, daily_reason = _override_flag(
                flags.ENABLE_DAILY_DIGEST, digest_override
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
                flags.ENABLE_WEEKLY_DIGEST, digest_override
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
    # `now` is local time (due hour, day window); data and stored times use UTC.
    now_utc = now.astimezone(timezone.utc)
    # Data of the accounts routed to this chat only (B8), from the live config.
    scope_account_emails = _chat_scope_ids(account_emails)
    if not _is_daily_due(now, config):
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
        )
        return

    day_key = _daily_window_key(now)
    sent_key = _digest_marker_key(_DAILY_SENT_KEY, account_email)
    try:
        # The marker covers this build's sends; the events also cover what
        # the UTC build sent on the switch day.
        already_sent = _read_sent_marker(storage, sent_key) == day_key
        if not already_sent:
            already_sent = storage.analytics.has_daily_digest_sent(
                account_email=account_email, day=now
            )
    except sqlite3.Error as exc:
        # Fail closed (B17): an unreadable state is never "not sent yet".
        logger.error(
            "digest_sent_check_failed",
            digest_type="daily",
            account_email=account_email,
            error=_error_text(exc),
        )
        return
    if already_sent:
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
        )
        return

    marker_store = RuntimeOverrideStore(storage.knowledge_db.path)
    no_content_key = _digest_marker_key(_DAILY_NO_CONTENT_KEY, account_email)
    if marker_store.get_value(no_content_key) == day_key:
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="no_content_already_evaluated",
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
                    now_ts=now_utc.timestamp(),
                    policy=silence_policy,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("silence_detector_failed", error=str(exc))

    locale = _resolve_outbound_ui_locale()
    data = daily_digest._collect_digest_data(
        analytics=storage.analytics,
        account_email=account_email,
        account_emails=scope_account_emails,
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
        locale=locale,
        now=now_utc,
        contract_event_emitter=storage.contract_event_emitter,
    )
    has_content = daily_digest._has_digest_content(data)
    if not has_content:
        marker_store.set_value(no_content_key, day_key)
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
        locale=locale,
    )

    outcome = _send_digest_once(
        storage=storage,
        telegram_sender=telegram_sender,
        payload=payload,
        sent_key=sent_key,
        window=day_key,
        logger=logger,
        log_fields={"digest_type": "daily", "account_email": account_email},
    )
    if outcome.status == "taken":
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
        )
        return
    if outcome.status != "delivered":
        return

    for email in account_emails:
        storage.knowledge_db.set_last_digest_sent_at(
            account_email=email,
            sent_at=now_utc,
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
                    ts_utc=now_utc.timestamp(),
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
    # `now` is local time (due slot, digest week); data and stored times use UTC.
    week_key, due = _weekly_window(now, config)
    now_utc = now.astimezone(timezone.utc)
    marker_store = RuntimeOverrideStore(storage.knowledge_db.path)

    if not due:
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
            week_key=week_key,
        )
        _emit_weekly_skip_on_change(
            storage=storage,
            marker_store=marker_store,
            account_email=account_email,
            week_key=week_key,
            reason="not_due",
            now=now_utc,
        )
        return

    sent_key = _digest_marker_key(_WEEKLY_SENT_KEY, account_email)
    try:
        # The marker covers this build's sends; the events also cover what
        # the UTC build sent.
        already_sent = _read_sent_marker(storage, sent_key) == week_key
        if not already_sent:
            already_sent = storage.analytics.has_weekly_digest_sent(
                account_email=account_email, week_key=week_key
            )
    except sqlite3.Error as exc:
        # Fail closed (B17): an unreadable state is never "not sent yet".
        logger.error(
            "digest_sent_check_failed",
            digest_type="weekly",
            account_email=account_email,
            week_key=week_key,
            error=_error_text(exc),
        )
        return
    if already_sent:
        _skip_weekly_already_sent(
            storage=storage,
            marker_store=marker_store,
            logger=logger,
            account_email=account_email,
            week_key=week_key,
            now_utc=now_utc,
        )
        return

    no_content_key = _digest_marker_key(_WEEKLY_NO_CONTENT_KEY, account_email)
    if marker_store.get_value(no_content_key) == week_key:
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="no_content_already_evaluated",
            account_email=account_email,
            week_key=week_key,
        )
        return

    logger.info(
        "digest_tick_checked",
        digest_type="weekly",
        decision="due",
        account_email=account_email,
        week_key=week_key,
    )

    locale = _resolve_outbound_ui_locale()
    data = weekly_digest._collect_weekly_data(
        analytics=storage.analytics,
        account_email=account_email,
        # Data of the accounts routed to this chat only (B8).
        account_emails=_chat_scope_ids(account_emails),
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
        locale=locale,
        event_emitter=storage.event_emitter,
        contract_event_emitter=storage.contract_event_emitter,
        now=now_utc,
    )
    has_content = _has_weekly_content(data)
    if not has_content:
        marker_store.set_value(no_content_key, week_key)
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="no_content",
            account_email=account_email,
            week_key=week_key,
        )
        _emit_weekly_skip_on_change(
            storage=storage,
            marker_store=marker_store,
            account_email=account_email,
            week_key=week_key,
            reason="no_content",
            now=now_utc,
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
        _emit_weekly_skip_on_change(
            storage=storage,
            marker_store=marker_store,
            account_email=account_email,
            week_key=week_key,
            reason="policy_denied",
            now=now_utc,
        )
        return
    support_footer = ""
    support_cfg = _load_support_telegram_config()
    try:
        support_footer = _resolve_weekly_support_footer(
            now_utc=now_utc,
            account_email=account_email,
            chat_id=chat_id,
            override_store=RuntimeOverrideStore(storage.knowledge_db.path),
            support_config=support_cfg,
        )
    except Exception as exc:
        logger.error(
            "weekly_support_footer_unavailable",
            account_email=account_email,
            error=str(exc),
        )

    payload = _build_weekly_payload(
        account_email=account_email,
        chat_id=chat_id,
        bot_token=bot_token,
        week_key=week_key,
        data=data,
        support_footer=support_footer,
        locale=locale,
    )

    outcome = _send_digest_once(
        storage=storage,
        telegram_sender=telegram_sender,
        payload=payload,
        sent_key=sent_key,
        window=week_key,
        logger=logger,
        log_fields={
            "digest_type": "weekly",
            "account_email": account_email,
            "week_key": week_key,
        },
    )
    if outcome.status == "taken":
        _skip_weekly_already_sent(
            storage=storage,
            marker_store=marker_store,
            logger=logger,
            account_email=account_email,
            week_key=week_key,
            now_utc=now_utc,
        )
        return
    if outcome.status == "unavailable":
        return
    if outcome.status != "delivered":
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_failed",
                timestamp=now_utc,
                email_id=0,
                payload={
                    "week_key": week_key,
                    "account_email": account_email,
                    "error": outcome.error,
                },
            )
        return

    for email in account_emails:
        storage.knowledge_db.set_last_weekly_digest_state(
            account_email=email,
            week_key=week_key,
            sent_at=now_utc,
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
            timestamp=now_utc,
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
                    ts_utc=now_utc.timestamp(),
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


def _skip_weekly_already_sent(
    *,
    storage: DigestStorage,
    marker_store: RuntimeOverrideStore,
    logger: LoggerLike,
    account_email: str,
    week_key: str,
    now_utc: datetime,
) -> None:
    logger.info(
        "digest_tick_checked",
        digest_type="weekly",
        decision="skipped",
        reason="already_sent",
        account_email=account_email,
        week_key=week_key,
    )
    _emit_weekly_skip_on_change(
        storage=storage,
        marker_store=marker_store,
        account_email=account_email,
        week_key=week_key,
        reason="already_sent",
        now=now_utc,
    )


__all__ = [
    "DigestStorage",
    "DailyDigestConfig",
    "WeeklyDigestConfig",
    "run_digest_tick",
]
