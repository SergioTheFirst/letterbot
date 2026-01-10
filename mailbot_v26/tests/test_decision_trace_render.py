from __future__ import annotations

from mailbot_v26.observability.decision_trace_eval import DecisionEvalResult
from mailbot_v26.observability.decision_trace_v1 import DecisionTraceV1, sanitize_trace
from mailbot_v26.observability import decision_trace_view
from mailbot_v26.telegram import inbound


def test_decision_trace_renderer_scrubs_pii() -> None:
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
    trace = sanitize_trace(trace)
    summary = decision_trace_view.build_decision_trace_summary(trace)
    payload = [
        {
            "decision_kind": summary.decision_kind,
            "decision_label": summary.decision_label,
            "evidence": summary.evidence,
            "explain_codes": summary.explain_codes,
            "counterfactuals": [
                {"signal": item.signal, "decision": item.decision}
                for item in summary.counterfactuals
            ],
        }
    ]
    rendered = inbound._render_decision_trace_details(payload)
    assert "@" not in rendered
    assert ".com" not in rendered


def test_counterfactual_uses_evaluator(monkeypatch) -> None:
    trace = DecisionTraceV1(
        decision_key="abc",
        decision_kind="ATTENTION_GATE",
        anchor_ts_utc=1.0,
        signals_evaluated=["TOP_PERCENTILE_CANDIDATE"],
        signals_fired=["TOP_PERCENTILE_CANDIDATE"],
        evidence={"matched": 1, "total": 1},
        model_fingerprint="fp",
        explain_codes=["TOP_PERCENTILE_CANDIDATE"],
    )
    calls: list[dict[str, bool] | None] = []

    def fake_eval(context, overrides=None):
        calls.append(overrides)
        decision = "DENY" if overrides else "ALLOW"
        return DecisionEvalResult(
            decision=decision,
            signals_evaluated=["TOP_PERCENTILE_CANDIDATE"],
            signals_fired=["TOP_PERCENTILE_CANDIDATE"],
            explain_codes=["TOP_PERCENTILE_CANDIDATE"],
            evidence={"matched": 1, "total": 1},
        )

    monkeypatch.setattr(decision_trace_view, "evaluate_attention_gate", fake_eval)
    summary = decision_trace_view.build_decision_trace_summary(trace)
    assert {"TOP_PERCENTILE_CANDIDATE": False} in calls
    assert summary.counterfactuals
    assert summary.counterfactuals[0].signal == "TOP_PERCENTILE_CANDIDATE"
    assert summary.counterfactuals[0].decision == "DENY"
