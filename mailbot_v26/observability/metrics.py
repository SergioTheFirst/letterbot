from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(slots=True)
class MetricsAggregator:
    path: Path

    def _connect_readonly(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)

    def snapshot(self) -> dict[str, dict[str, float]]:
        return {
            "days_7": self._window_metrics(days=7),
            "days_30": self._window_metrics(days=30),
        }

    def _window_metrics(self, *, days: int) -> dict[str, float]:
        try:
            with self._connect_readonly() as conn:
                conn.row_factory = sqlite3.Row
                window = f"-{days} days"
                shadow_stats = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN shadow_priority = priority THEN 1 ELSE 0 END) AS match_count
                    FROM decision_traces
                    WHERE TRIM(COALESCE(shadow_priority, '')) != ''
                      AND TRIM(COALESCE(priority, '')) != ''
                      AND created_at >= datetime('now', ?)
                    """,
                    (window,),
                ).fetchone()
                shadow_total = int(shadow_stats["total"] or 0)
                shadow_match = int(shadow_stats["match_count"] or 0)

                feedback_stats = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN decision = 'accepted' THEN 1 ELSE 0 END) AS accepted_count
                    FROM action_feedback
                    WHERE decision IN ('accepted', 'rejected')
                      AND created_at >= datetime('now', ?)
                    """,
                    (window,),
                ).fetchone()
                feedback_total = int(feedback_stats["total"] or 0)
                feedback_accepted = int(feedback_stats["accepted_count"] or 0)

                commitment_stats = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN status = 'fulfilled' THEN 1 ELSE 0 END) AS fulfilled_count
                    FROM commitments
                    WHERE created_at >= datetime('now', ?)
                    """,
                    (window,),
                ).fetchone()
                commitment_total = int(commitment_stats["total"] or 0)
                commitment_fulfilled = int(commitment_stats["fulfilled_count"] or 0)

                llm_stats = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN TRIM(COALESCE(response_full, '')) = '' THEN 1 ELSE 0 END) AS failed_count
                    FROM decision_traces
                    WHERE created_at >= datetime('now', ?)
                    """,
                    (window,),
                ).fetchone()
                llm_total = int(llm_stats["total"] or 0)
                llm_failed = int(llm_stats["failed_count"] or 0)

                event_stats = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN type = 'telegram_payload_validated' THEN 1 ELSE 0 END) AS payload_validated,
                        SUM(CASE WHEN type = 'telegram_payload_fallback_used' THEN 1 ELSE 0 END) AS fallback_used,
                        SUM(CASE WHEN type = 'telegram_delivery_succeeded' THEN 1 ELSE 0 END) AS delivery_ok,
                        SUM(CASE WHEN type = 'telegram_delivery_failed' THEN 1 ELSE 0 END) AS delivery_failed,
                        SUM(CASE WHEN type = 'telegram_empty_summary' THEN 1 ELSE 0 END) AS empty_summary
                    FROM events
                    WHERE timestamp >= datetime('now', ?)
                    """,
                    (window,),
                ).fetchone()
                payload_validated = int(event_stats["payload_validated"] or 0)
                fallback_used = int(event_stats["fallback_used"] or 0)
                delivery_ok = int(event_stats["delivery_ok"] or 0)
                delivery_failed = int(event_stats["delivery_failed"] or 0)
                empty_summary = int(event_stats["empty_summary"] or 0)
        except Exception as exc:
            logger.error("metrics_aggregation_failed", error=str(exc), window_days=days)
            return {
                "shadow_accuracy": 0.0,
                "preview_accept_rate": 0.0,
                "commitment_fulfillment_rate": 0.0,
                "llm_failure_rate": 0.0,
                "fallback_usage_rate": 0.0,
                "telegram_delivery_success_rate": 0.0,
                "empty_summary_rate": 0.0,
            }

        return {
            "shadow_accuracy": _safe_rate(shadow_match, shadow_total),
            "preview_accept_rate": _safe_rate(feedback_accepted, feedback_total),
            "commitment_fulfillment_rate": _safe_rate(
                commitment_fulfilled, commitment_total
            ),
            "llm_failure_rate": _safe_rate(llm_failed, llm_total),
            "fallback_usage_rate": _safe_rate(fallback_used, payload_validated),
            "telegram_delivery_success_rate": _safe_rate(
                delivery_ok, (delivery_ok + delivery_failed)
            ),
            "empty_summary_rate": _safe_rate(empty_summary, payload_validated),
        }


@dataclass(frozen=True, slots=True)
class GateEvaluation:
    passed: bool
    failed_reasons: tuple[str, ...]


class SystemGates:
    MIN_SHADOW_ACCURACY = 0.7
    MIN_PREVIEW_ACCEPT_RATE = 0.6
    MIN_COMMITMENT_FULFILLMENT_RATE = 0.7
    MAX_LLM_FAILURE_RATE = 0.15

    def evaluate(self, metrics: dict[str, dict[str, float]]) -> GateEvaluation:
        window = metrics.get("days_30") or metrics.get("days_7") or {}
        reasons: list[str] = []
        if window.get("shadow_accuracy", 0.0) < self.MIN_SHADOW_ACCURACY:
            reasons.append("shadow_accuracy")
        if window.get("preview_accept_rate", 0.0) < self.MIN_PREVIEW_ACCEPT_RATE:
            reasons.append("preview_accept_rate")
        if (
            window.get("commitment_fulfillment_rate", 0.0)
            < self.MIN_COMMITMENT_FULFILLMENT_RATE
        ):
            reasons.append("commitment_fulfillment_rate")
        if window.get("llm_failure_rate", 0.0) > self.MAX_LLM_FAILURE_RATE:
            reasons.append("llm_failure_rate")
        return GateEvaluation(passed=not reasons, failed_reasons=tuple(reasons))


@dataclass(slots=True)
class SystemHealthSnapshotter:
    aggregator: MetricsAggregator
    gates: SystemGates
    interval: int = 50
    _counter: int = 0

    def log_startup(self) -> None:
        self._log_snapshot(reason="startup")

    def maybe_log(self) -> None:
        if self.interval <= 0:
            return
        self._counter += 1
        if self._counter % self.interval == 0:
            self._log_snapshot(reason="interval")

    def _log_snapshot(self, *, reason: str) -> None:
        metrics = self.aggregator.snapshot()
        evaluation = self.gates.evaluate(metrics)
        logger.info(
            "system_health_snapshot",
            reason=reason,
            metrics=metrics,
            gates_passed=evaluation.passed,
            gates_failed=evaluation.failed_reasons,
        )


def _safe_rate(numerator: int, denominator: int) -> float:
    return (numerator / denominator) if denominator > 0 else 0.0
