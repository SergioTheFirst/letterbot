from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.metrics import GateEvaluation
from mailbot_v26.system_health import OperationalMode as SystemMode

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class SystemPolicyDecision:
    mode: SystemMode
    allow_llm: bool
    allow_preview: bool
    allow_auto_priority: bool
    allow_auto_priority_v2: bool
    allow_daily_digest: bool
    allow_weekly_digest: bool
    allow_anomaly_alerts: bool
    auto_priority_gate_result: GateResult | None
    reasons: list[str]


@dataclass(frozen=True, slots=True)
class SystemPolicyInputs:
    system_mode: SystemMode
    metrics: dict[str, dict[str, float]] | None
    gates: GateEvaluation | None
    runtime_flags: RuntimeFlags
    feature_flags: FeatureFlags
    telegram_ok: bool
    has_daily_digest_content: bool
    has_weekly_digest_content: bool


class SystemOrchestrator:
    def evaluate(
        self,
        *,
        system_mode: SystemMode,
        metrics: dict[str, dict[str, float]] | None,
        gates: GateEvaluation | None,
        runtime_flags: RuntimeFlags,
        feature_flags: FeatureFlags,
        telegram_ok: bool,
        has_daily_digest_content: bool,
        has_weekly_digest_content: bool,
        auto_priority_gate_result: GateResult | None = None,
        auto_priority_gate_enabled: bool = False,
        enable_quality_metrics: bool | None = None,
        fallback_decision: SystemPolicyDecision | None = None,
    ) -> SystemPolicyDecision:
        fallback = fallback_decision or self.legacy_decision(
            system_mode=system_mode,
            runtime_flags=runtime_flags,
            feature_flags=feature_flags,
            has_daily_digest_content=has_daily_digest_content,
            has_weekly_digest_content=has_weekly_digest_content,
        )
        try:
            reasons: list[str] = []
            allow_llm = system_mode != SystemMode.DEGRADED_NO_LLM
            if not allow_llm:
                _append_reason(reasons, "llm_disabled_by_mode")

            auto_priority_flag = _flag(feature_flags, "ENABLE_AUTO_PRIORITY")
            quality_metrics_flag = (
                enable_quality_metrics
                if enable_quality_metrics is not None
                else _flag(feature_flags, "ENABLE_QUALITY_METRICS")
            )
            allow_auto_priority_v2 = (
                system_mode == SystemMode.FULL
                and auto_priority_flag
                and runtime_flags.enable_auto_priority
                and quality_metrics_flag
                and auto_priority_gate_enabled
                and auto_priority_gate_result is not None
                and auto_priority_gate_result.passed
            )
            allow_auto_priority = allow_auto_priority_v2
            if system_mode != SystemMode.FULL:
                _append_reason(reasons, "mode_not_full")
            if not auto_priority_flag:
                _append_reason(reasons, "auto_priority_flag_disabled")
            if not runtime_flags.enable_auto_priority:
                _append_reason(reasons, "auto_priority_runtime_disabled")
            if not quality_metrics_flag:
                _append_reason(reasons, "quality_metrics_disabled")
            if not auto_priority_gate_enabled:
                _append_reason(reasons, "auto_priority_gate_disabled")
            if auto_priority_gate_result is None:
                _append_reason(reasons, "auto_priority_gate_unavailable")
            elif not auto_priority_gate_result.passed:
                _append_reason(
                    reasons,
                    f"auto_priority_gate:{auto_priority_gate_result.reason}",
                )

            anomaly_flag = _flag(feature_flags, "ENABLE_ANOMALY_ALERTS")
            allow_anomaly_alerts = (
                system_mode == SystemMode.FULL and anomaly_flag
            )
            if not anomaly_flag:
                _append_reason(reasons, "anomaly_flag_disabled")

            preview_flag = _flag(feature_flags, "ENABLE_PREVIEW_ACTIONS")
            allow_preview = (
                preview_flag
                and telegram_ok
                and allow_llm
                and system_mode != SystemMode.EMERGENCY_READ_ONLY
            )
            if not preview_flag:
                _append_reason(reasons, "preview_flag_disabled")
            if not telegram_ok:
                _append_reason(reasons, "telegram_unavailable")
            if system_mode == SystemMode.EMERGENCY_READ_ONLY:
                _append_reason(reasons, "crm_read_only")

            daily_digest_flag = _flag(feature_flags, "ENABLE_DAILY_DIGEST")
            allow_daily_digest = (
                daily_digest_flag
                and telegram_ok
                and has_daily_digest_content
            )
            if not daily_digest_flag:
                _append_reason(reasons, "daily_digest_flag_disabled")
            if not has_daily_digest_content:
                _append_reason(reasons, "daily_digest_no_content")

            weekly_digest_flag = _flag(feature_flags, "ENABLE_WEEKLY_DIGEST")
            allow_weekly_digest = (
                weekly_digest_flag
                and telegram_ok
                and has_weekly_digest_content
            )
            if not weekly_digest_flag:
                _append_reason(reasons, "weekly_digest_flag_disabled")
            if not has_weekly_digest_content:
                _append_reason(reasons, "weekly_digest_no_content")

            decision = SystemPolicyDecision(
                mode=system_mode,
                allow_llm=allow_llm,
                allow_preview=allow_preview,
                allow_auto_priority=allow_auto_priority,
                allow_auto_priority_v2=allow_auto_priority_v2,
                allow_daily_digest=allow_daily_digest,
                allow_weekly_digest=allow_weekly_digest,
                allow_anomaly_alerts=allow_anomaly_alerts,
                auto_priority_gate_result=auto_priority_gate_result,
                reasons=reasons,
            )
            self._log_evaluated(
                decision=decision,
                gates=gates,
                metrics=metrics,
            )
            return decision
        except Exception as exc:  # pragma: no cover - defensive fallback
            logger.error(
                "system_policy_fallback_used",
                error=str(exc),
                mode=system_mode.value,
                fallback_mode=fallback.mode.value,
                allow_llm=fallback.allow_llm,
                allow_preview=fallback.allow_preview,
                allow_auto_priority=fallback.allow_auto_priority,
                allow_auto_priority_v2=getattr(
                    fallback, "allow_auto_priority_v2", fallback.allow_auto_priority
                ),
                allow_daily_digest=fallback.allow_daily_digest,
                allow_weekly_digest=fallback.allow_weekly_digest,
                allow_anomaly_alerts=fallback.allow_anomaly_alerts,
                reasons=fallback.reasons,
            )
            return fallback

    @staticmethod
    def legacy_decision(
        *,
        system_mode: SystemMode,
        runtime_flags: RuntimeFlags,
        feature_flags: FeatureFlags,
        has_daily_digest_content: bool,
        has_weekly_digest_content: bool,
    ) -> SystemPolicyDecision:
        auto_priority_flag = _flag(feature_flags, "ENABLE_AUTO_PRIORITY")
        allow_auto_priority = (
            auto_priority_flag
            and runtime_flags.enable_auto_priority
            and system_mode == SystemMode.FULL
        )
        preview_flag = _flag(feature_flags, "ENABLE_PREVIEW_ACTIONS")
        allow_preview = (
            preview_flag
            and system_mode != SystemMode.DEGRADED_NO_LLM
        )
        allow_anomaly_alerts = _flag(feature_flags, "ENABLE_ANOMALY_ALERTS")
        allow_daily_digest = (
            _flag(feature_flags, "ENABLE_DAILY_DIGEST") and has_daily_digest_content
        )
        allow_weekly_digest = (
            _flag(feature_flags, "ENABLE_WEEKLY_DIGEST") and has_weekly_digest_content
        )
        return SystemPolicyDecision(
            mode=system_mode,
            allow_llm=True,
            allow_preview=allow_preview,
            allow_auto_priority=allow_auto_priority,
            allow_auto_priority_v2=allow_auto_priority,
            allow_daily_digest=allow_daily_digest,
            allow_weekly_digest=allow_weekly_digest,
            allow_anomaly_alerts=allow_anomaly_alerts,
            auto_priority_gate_result=None,
            reasons=["legacy_fallback"],
        )

    def _log_evaluated(
        self,
        *,
        decision: SystemPolicyDecision,
        gates: GateEvaluation | None,
        metrics: dict[str, dict[str, float]] | None,
    ) -> None:
        logger.info(
            "system_policy_evaluated",
            mode=decision.mode.value,
            allow_llm=decision.allow_llm,
            allow_preview=decision.allow_preview,
            allow_auto_priority=decision.allow_auto_priority,
            allow_auto_priority_v2=decision.allow_auto_priority_v2,
            allow_daily_digest=decision.allow_daily_digest,
            allow_weekly_digest=decision.allow_weekly_digest,
            allow_anomaly_alerts=decision.allow_anomaly_alerts,
            auto_priority_gate_reason=(
                decision.auto_priority_gate_result.reason
                if decision.auto_priority_gate_result
                else None
            ),
            reasons=decision.reasons,
            gates_passed=gates.passed if gates else None,
            gates_failed=list(gates.failed_reasons) if gates else None,
            metrics_windows=list(metrics.keys()) if metrics else None,
        )


def _append_reason(reasons: list[str], reason: str) -> None:
    if reason and reason not in reasons:
        reasons.append(reason)


def _flag(flags: FeatureFlags | None, name: str, default: bool = False) -> bool:
    if flags is None:
        return default
    return bool(getattr(flags, name, default))


__all__ = ["SystemMode", "SystemOrchestrator", "SystemPolicyDecision", "SystemPolicyInputs"]
