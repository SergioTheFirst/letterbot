from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.calibration_report import (
    compute_priority_calibration_report,
)
from mailbot_v26.observability.decision_trace_v1 import (
    DecisionTraceV1,
    to_canonical_json,
)
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_trace(
    emitter: ContractEventEmitter,
    *,
    email_id: int,
    ts_utc: float,
    model_fingerprint: str,
    decision_key: str,
) -> None:
    trace = DecisionTraceV1(
        decision_key=decision_key,
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=ts_utc,
        signals_evaluated=["SIG_A", "SIG_B"],
        signals_fired=["SIG_A"],
        evidence={"matched": 1, "total": 2},
        model_fingerprint=model_fingerprint,
        explain_codes=["CODE_A"],
    )
    emitter.emit(
        EventV1(
            event_type=EventType.DECISION_TRACE_RECORDED,
            ts_utc=ts_utc,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={},
            payload_json=to_canonical_json(trace),
        )
    )


def _emit_correction(
    emitter: ContractEventEmitter,
    *,
    email_id: int,
    ts_utc: float,
    decision_key: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=ts_utc,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={
                "decision_key": decision_key,
                "model_fingerprint": "model-a",
                "evidence": {"matched": 1, "total": 2},
            },
        )
    )


def _emit_surprise(
    emitter: ContractEventEmitter,
    *,
    email_id: int,
    ts_utc: float,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.SURPRISE_DETECTED,
            ts_utc=ts_utc,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={"delta": 1},
        )
    )


def test_priority_calibration_report_basic_and_drift(tmp_path: Path) -> None:
    db_path = tmp_path / "calibration.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2026, 1, 20, tzinfo=timezone.utc)
    prev_start = now - timedelta(days=14)
    last_start = now - timedelta(days=7)

    for idx in range(10):
        ts_prev = (prev_start + timedelta(hours=idx)).timestamp()
        _emit_trace(
            emitter,
            email_id=100 + idx,
            ts_utc=ts_prev,
            model_fingerprint="model-a",
            decision_key=f"prev-{idx}",
        )
    for idx in range(10):
        ts_last = (last_start + timedelta(hours=idx)).timestamp()
        _emit_trace(
            emitter,
            email_id=200 + idx,
            ts_utc=ts_last,
            model_fingerprint="model-a",
            decision_key=f"last-{idx}",
        )

    _emit_correction(
        emitter,
        email_id=100,
        ts_utc=(prev_start + timedelta(hours=1)).timestamp(),
        decision_key="prev-1",
    )
    _emit_correction(
        emitter,
        email_id=200,
        ts_utc=(last_start + timedelta(hours=1)).timestamp(),
        decision_key="last-1",
    )
    _emit_correction(
        emitter,
        email_id=201,
        ts_utc=(last_start + timedelta(hours=2)).timestamp(),
        decision_key="last-2",
    )
    _emit_correction(
        emitter,
        email_id=202,
        ts_utc=(last_start + timedelta(hours=3)).timestamp(),
        decision_key="last-3",
    )
    _emit_surprise(
        emitter, email_id=200, ts_utc=(last_start + timedelta(hours=1)).timestamp()
    )

    report = compute_priority_calibration_report(
        db_path=db_path,
        days=30,
        max_rows=1000,
        now_ts_utc=now.timestamp(),
    )
    warnings = report.get("warnings", [])
    assert "correction_rate_spike" in warnings
    assert "surprise_rate_high" not in warnings
