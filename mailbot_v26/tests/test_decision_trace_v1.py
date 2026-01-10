from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.observability.decision_trace_v1 import (
    DecisionTraceEmitter,
    DecisionTraceV1,
    compute_decision_key,
    sanitize_code,
    to_canonical_json,
)
from mailbot_v26.priority.priority_engine_v2 import PriorityEngineV2
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_decision_key_deterministic() -> None:
    key_one = compute_decision_key(
        account_id="acc",
        email_id=42,
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=123.456,
    )
    key_two = compute_decision_key(
        account_id="acc",
        email_id=42,
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=123.456,
    )
    key_three = compute_decision_key(
        account_id="acc",
        email_id=42,
        decision_kind="ATTENTION_GATE",
        anchor_ts_utc=123.456,
    )
    assert key_one == key_two
    assert key_one != key_three


def test_canonical_json_deterministic() -> None:
    trace = DecisionTraceV1(
        decision_key="abc123",
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=123.456,
        signals_evaluated=["A", "B"],
        signals_fired=["B"],
        evidence={"matched": 1, "total": 2},
        model_fingerprint="fp",
        explain_codes=["CODE"],
    )
    first = to_canonical_json(trace)
    second = to_canonical_json(trace)
    assert first == second


def test_emitter_circuit_breaker(tmp_path: Path) -> None:
    class FailingEmitter:
        def __init__(self) -> None:
            self.calls = 0
            self.db_path = tmp_path / "events.sqlite"

        def emit(self, event: EventV1) -> bool:
            self.calls += 1
            raise RuntimeError("boom")

    emitter = FailingEmitter()
    circuit = DecisionTraceEmitter(drop_threshold=3)
    event = EventV1(
        event_type=EventType.DECISION_TRACE_RECORDED,
        ts_utc=1.0,
        account_id="acc",
        entity_id=None,
        email_id=1,
        payload={},
        payload_json="{}",
    )

    for _ in range(3):
        circuit.emit(emitter, event)

    assert circuit.disabled is True
    assert emitter.calls == 3

    circuit.emit(emitter, event)
    assert emitter.calls == 3


def test_priority_trace_signal_hygiene(tmp_path: Path) -> None:
    db_path = tmp_path / "trace.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    engine = PriorityEngineV2(analytics)

    received_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    result = engine.compute(
        subject="Счет на 100000 рублей",
        body_text="Оплатить до завтра",
        from_email="vip@example.com",
        mail_type="INVOICE_FINAL",
        received_at=received_at,
        commitments=[],
    )
    signals = engine.evaluate_signals(
        subject="Счет на 100000 рублей",
        body_text="Оплатить до завтра",
        from_email="vip@example.com",
        mail_type="INVOICE_FINAL",
        received_at=received_at,
        commitments=[],
    )
    trace = DecisionTraceV1(
        decision_key=compute_decision_key(
            account_id="acc",
            email_id=1,
            decision_kind="PRIORITY_HEURISTIC",
            anchor_ts_utc=received_at.timestamp(),
        ),
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=received_at.timestamp(),
        signals_evaluated=sorted(signals.keys()),
        signals_fired=sorted([key for key, fired in signals.items() if fired]),
        evidence={
            "matched": sum(1 for fired in signals.values() if fired),
            "total": len(signals),
        },
        model_fingerprint="fp",
        explain_codes=engine.explain_codes(result),
    )

    allowed_pattern = re.compile(r"^[A-Z0-9_.]+$")
    heavy_digits = re.compile(r"\d{5,}")
    for entry in trace.signals_evaluated + trace.signals_fired + trace.explain_codes:
        assert allowed_pattern.match(entry)
        assert heavy_digits.search(entry) is None


def test_sanitize_code_scrubs_pii_like_strings() -> None:
    assert sanitize_code("John_Smith") == "SANITIZED_CODE"
    assert sanitize_code("alice@example.com") == "SANITIZED_CODE"


def test_failure_log_written_and_rotated(tmp_path: Path) -> None:
    class FailingEmitter:
        def __init__(self) -> None:
            self.db_path = tmp_path / "events.sqlite"

        def emit(self, event: EventV1) -> bool:
            raise RuntimeError("boom")

    log_path = tmp_path / "logs" / "decision_trace_failures.ndjson"
    emitter = FailingEmitter()
    circuit = DecisionTraceEmitter(drop_threshold=1, failure_log_path=log_path)
    event = EventV1(
        event_type=EventType.DECISION_TRACE_RECORDED,
        ts_utc=1.0,
        account_id="acc",
        entity_id=None,
        email_id=1,
        payload={"decision_kind": "ATTENTION_GATE"},
        payload_json='{"decision_key":"abc"}',
    )
    circuit.emit(emitter, event)
    assert log_path.exists()
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    log_path.write_text("x" * (256 * 1024 + 5), encoding="utf-8")
    circuit.emit(emitter, event)
    rotated = log_path.with_suffix(log_path.suffix + ".1")
    assert rotated.exists()
