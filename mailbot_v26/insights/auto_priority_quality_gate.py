from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import AutoPriorityGateState, KnowledgeDB

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class GateResult:
    passed: bool
    reason: str
    window_days: int
    samples: int
    corrections: int
    correction_rate: float
    engine: str


class AutoPriorityGateStateStore:
    def __init__(self, knowledge_db: KnowledgeDB) -> None:
        self._knowledge_db = knowledge_db

    def load(self) -> AutoPriorityGateState:
        return self._knowledge_db.read_auto_priority_gate_state()

    def set_disabled(self, *, ts_utc: float, reason: str) -> None:
        self._knowledge_db.persist_auto_priority_gate_state(
            last_disabled_at_utc=ts_utc,
            last_disabled_reason=reason,
            last_eval_at_utc=ts_utc,
        )

    def record_eval(self, *, ts_utc: float) -> None:
        state = self.load()
        self._knowledge_db.persist_auto_priority_gate_state(
            last_disabled_at_utc=state.last_disabled_at_utc,
            last_disabled_reason=state.last_disabled_reason,
            last_eval_at_utc=ts_utc,
        )


class AutoPriorityQualityGate:
    def __init__(
        self,
        *,
        analytics: KnowledgeAnalytics,
        state_store: AutoPriorityGateStateStore,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._analytics = analytics
        self._state_store = state_store
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def evaluate(
        self,
        *,
        engine: str,
        window_days: int,
        min_samples: int,
        max_correction_rate: float,
        cooldown_hours: int,
    ) -> GateResult:
        now = self._now_fn()
        now_ts = now.timestamp()
        window_start = now - timedelta(days=window_days)
        try:
            processed_rows = self._analytics._event_rows(  # noqa: SLF001
                account_id=None,
                event_type=EventType.EMAIL_RECEIVED.value,
                since_ts=window_start.timestamp(),
            )
            correction_rows = self._analytics._event_rows(  # noqa: SLF001
                account_id=None,
                event_type=EventType.PRIORITY_CORRECTION_RECORDED.value,
                since_ts=window_start.timestamp(),
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("auto_priority_gate_analytics_failed", error=str(exc))
            result = GateResult(
                passed=False,
                reason="analytics_failed",
                window_days=window_days,
                samples=0,
                corrections=0,
                correction_rate=0.0,
                engine=engine,
            )
            self._log_result(result)
            return result

        samples = _count_rows_for_engine(processed_rows, self._analytics, engine)
        corrections = _count_rows_for_engine(correction_rows, self._analytics, engine)
        correction_rate = corrections / max(1, samples)
        state = self._state_store.load()

        if state.last_disabled_at_utc is not None:
            cooldown_seconds = cooldown_hours * 3600
            if (now_ts - state.last_disabled_at_utc) < cooldown_seconds:
                result = GateResult(
                    passed=False,
                    reason="cooldown_active",
                    window_days=window_days,
                    samples=samples,
                    corrections=corrections,
                    correction_rate=correction_rate,
                    engine=engine,
                )
                self._state_store.record_eval(ts_utc=now_ts)
                self._log_result(result)
                return result

        if samples < min_samples:
            result = GateResult(
                passed=False,
                reason="insufficient_samples",
                window_days=window_days,
                samples=samples,
                corrections=corrections,
                correction_rate=correction_rate,
                engine=engine,
            )
            self._state_store.record_eval(ts_utc=now_ts)
            self._log_result(result)
            return result

        if correction_rate > max_correction_rate:
            result = GateResult(
                passed=False,
                reason="correction_rate_spike",
                window_days=window_days,
                samples=samples,
                corrections=corrections,
                correction_rate=correction_rate,
                engine=engine,
            )
            self._state_store.set_disabled(
                ts_utc=now_ts, reason="correction_rate_spike"
            )
            logger.warning(
                "auto_priority_circuit_breaker_disabled",
                reason="correction_rate_spike",
                correction_rate=correction_rate,
                samples=samples,
                engine=engine,
            )
            self._log_result(result)
            return result

        result = GateResult(
            passed=True,
            reason="ok",
            window_days=window_days,
            samples=samples,
            corrections=corrections,
            correction_rate=correction_rate,
            engine=engine,
        )
        self._state_store.record_eval(ts_utc=now_ts)
        self._log_result(result)
        return result

    def _log_result(self, result: GateResult) -> None:
        logger.info(
            "auto_priority_quality_gate_evaluated",
            passed=result.passed,
            reason=result.reason,
            window_days=result.window_days,
            samples=result.samples,
            corrections=result.corrections,
            correction_rate=result.correction_rate,
            engine=result.engine,
        )


def _count_rows_for_engine(
    rows: list[dict[str, object]],
    analytics: KnowledgeAnalytics,
    engine: str,
) -> int:
    count = 0
    for row in rows:
        payload = analytics.event_payload(row)
        row_engine = str(payload.get("engine") or "").strip() or "unknown"
        if row_engine == engine:
            count += 1
    return count


__all__ = [
    "AutoPriorityGateStateStore",
    "AutoPriorityQualityGate",
    "GateResult",
]
