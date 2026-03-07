from __future__ import annotations

from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.decision_trace_v1 import DecisionTraceV1, to_canonical_json
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


def _build_app(tmp_path: Path):
    db_path = tmp_path / "decision_trace.sqlite"
    KnowledgeDB(db_path)
    return db_path, create_app(db_path=db_path, password="pw", secret_key="secret")


def _insert_decision_trace(db_path: Path, email_id: int) -> None:
    emitter = ContractEventEmitter(db_path)
    trace = DecisionTraceV1(
        decision_key="abc",
        decision_kind="ATTENTION_GATE",
        anchor_ts_utc=1.0,
        signals_evaluated=["TOP_PERCENTILE_CANDIDATE"],
        signals_fired=["TOP_PERCENTILE_CANDIDATE"],
        evidence={"matched": 1, "total": 1},
        model_fingerprint="fp",
        explain_codes=["alice@example.com"],
    )
    event = EventV1(
        event_type=EventType.DECISION_TRACE_RECORDED,
        ts_utc=1.0,
        account_id="acc",
        entity_id=None,
        email_id=email_id,
        payload={
            "decision_kind": trace.decision_kind,
            "trace_schema": trace.trace_schema,
            "trace_version": trace.trace_version,
        },
        payload_json=to_canonical_json(trace),
    )
    emitter.emit(event)


def test_decision_trace_endpoint_scrubs_payload(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    _insert_decision_trace(db_path, email_id=1)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/v1/cockpit/decision-trace?email_id=1")
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["email_id"] == 1
        traces = payload.get("traces") or []
        assert traces
        trace = traces[0]
        assert "subject" not in trace
        assert "sender" not in trace
        assert "body" not in trace
        codes = trace.get("explain_codes") or []
        assert "alice@example.com" not in codes
        histogram = payload.get("histogram") or []
        if histogram:
            assert "alice@example.com" not in {row.get("code") for row in histogram}
