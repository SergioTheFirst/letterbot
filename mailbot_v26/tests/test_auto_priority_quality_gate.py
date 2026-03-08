from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.insights.auto_priority_quality_gate import (
    AutoPriorityGateStateStore,
    AutoPriorityQualityGate,
    GateResult,
)
from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system.orchestrator import SystemMode, SystemOrchestrator


@dataclass
class _GateHarness:
    gate: AutoPriorityQualityGate
    emitter: ContractEventEmitter
    now_ref: dict[str, datetime]
    state_store: AutoPriorityGateStateStore


def _build_gate(tmp_path, now: datetime) -> _GateHarness:
    db_path = tmp_path / "gate.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now_ref: dict[str, datetime] = {"value": now}
    state_store = AutoPriorityGateStateStore(knowledge_db)
    gate = AutoPriorityQualityGate(
        analytics=analytics,
        state_store=state_store,
        now_fn=lambda: now_ref["value"],
    )
    return _GateHarness(
        gate=gate, emitter=emitter, now_ref=now_ref, state_store=state_store
    )


def _emit_event(
    emitter: ContractEventEmitter,
    *,
    event_type: EventType,
    ts: datetime,
    email_id: int,
    engine: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts.timestamp(),
            account_id="acc",
            entity_id=None,
            email_id=email_id,
            payload={"engine": engine},
        )
    )


def _emit_processed(
    emitter: ContractEventEmitter, *, count: int, ts: datetime, engine: str
) -> None:
    for idx in range(count):
        _emit_event(
            emitter,
            event_type=EventType.EMAIL_RECEIVED,
            ts=ts,
            email_id=idx,
            engine=engine,
        )


def _emit_corrections(
    emitter: ContractEventEmitter, *, count: int, ts: datetime, engine: str
) -> None:
    start = 10_000
    for idx in range(count):
        _emit_event(
            emitter,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts=ts,
            email_id=start + idx,
            engine=engine,
        )


def test_gate_insufficient_samples(tmp_path) -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    harness = _build_gate(tmp_path, now)
    _emit_processed(harness.emitter, count=10, ts=now, engine="priority_v2_auto")

    result = harness.gate.evaluate(
        engine="priority_v2_auto",
        window_days=30,
        min_samples=30,
        max_correction_rate=0.15,
        cooldown_hours=24,
    )

    assert result.passed is False
    assert result.reason == "insufficient_samples"
    assert result.samples == 10


def test_gate_passes_within_threshold(tmp_path) -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    harness = _build_gate(tmp_path, now)
    _emit_processed(harness.emitter, count=40, ts=now, engine="priority_v2_auto")
    _emit_corrections(harness.emitter, count=4, ts=now, engine="priority_v2_auto")

    result = harness.gate.evaluate(
        engine="priority_v2_auto",
        window_days=30,
        min_samples=30,
        max_correction_rate=0.15,
        cooldown_hours=24,
    )

    assert result.passed is True
    assert result.reason == "ok"
    assert result.corrections == 4


def test_circuit_breaker_blocks_during_cooldown(tmp_path) -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    harness = _build_gate(tmp_path, now)
    _emit_processed(harness.emitter, count=40, ts=now, engine="priority_v2_auto")
    _emit_corrections(harness.emitter, count=10, ts=now, engine="priority_v2_auto")

    first_result = harness.gate.evaluate(
        engine="priority_v2_auto",
        window_days=30,
        min_samples=30,
        max_correction_rate=0.15,
        cooldown_hours=24,
    )

    state = harness.state_store.load()
    assert first_result.passed is False
    assert first_result.reason == "correction_rate_spike"
    assert state.last_disabled_at_utc is not None
    assert state.last_disabled_reason == "correction_rate_spike"

    harness.now_ref["value"] = now + timedelta(hours=1)
    cooldown_result = harness.gate.evaluate(
        engine="priority_v2_auto",
        window_days=30,
        min_samples=30,
        max_correction_rate=0.15,
        cooldown_hours=24,
    )

    assert cooldown_result.passed is False
    assert cooldown_result.reason == "cooldown_active"


def test_orchestrator_blocks_without_quality_metrics() -> None:
    orchestrator = SystemOrchestrator()
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=5,
        correction_rate=0.05,
        engine="priority_v2_auto",
    )
    feature_flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=True,
        ENABLE_PREVIEW_ACTIONS=False,
        ENABLE_DAILY_DIGEST=False,
        ENABLE_WEEKLY_DIGEST=False,
        ENABLE_ANOMALY_ALERTS=False,
        ENABLE_QUALITY_METRICS=False,
    )

    decision = orchestrator.evaluate(
        system_mode=SystemMode.FULL,
        metrics=None,
        gates=None,
        runtime_flags=RuntimeFlags(enable_gigachat=False, enable_auto_priority=True),
        feature_flags=feature_flags,
        telegram_ok=True,
        has_daily_digest_content=False,
        has_weekly_digest_content=False,
        auto_priority_gate_result=gate_result,
        auto_priority_gate_enabled=True,
        enable_quality_metrics=False,
    )

    assert decision.allow_auto_priority is False
    assert decision.allow_auto_priority_v2 is False
