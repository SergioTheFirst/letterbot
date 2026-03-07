from __future__ import annotations

from dataclasses import dataclass

from mailbot_v26.observability.decision_trace_eval import (
    AttentionGateContext,
    DecisionEvalResult,
    LlmGateContext,
    PriorityHeuristicContext,
    evaluate_attention_gate,
    evaluate_llm_gate,
    evaluate_priority_heuristic,
)
from mailbot_v26.observability.decision_trace_v1 import DecisionTraceV1, sanitize_codes
from mailbot_v26.priority.priority_engine_v2 import load_priority_v2_config


@dataclass(frozen=True, slots=True)
class CounterfactualDelta:
    signal: str
    decision: str


@dataclass(frozen=True, slots=True)
class DecisionTraceSummary:
    decision_kind: str
    decision_label: str
    evidence: dict[str, int]
    explain_codes: list[str]
    counterfactuals: list[CounterfactualDelta]


def build_decision_trace_summary(trace: DecisionTraceV1) -> DecisionTraceSummary:
    signals, fired_signals = _signals_map(trace)
    decision_kind = trace.decision_kind
    config = load_priority_v2_config() if decision_kind == "PRIORITY_HEURISTIC" else None
    eval_result = _evaluate_trace(decision_kind, trace, signals, config)
    counterfactuals = _build_counterfactuals(
        decision_kind, trace, signals, fired_signals, eval_result, config
    )
    return DecisionTraceSummary(
        decision_kind=decision_kind,
        decision_label=eval_result.decision,
        evidence=_normalize_evidence(trace.evidence, eval_result),
        explain_codes=_sanitize_explain_codes(trace),
        counterfactuals=counterfactuals,
    )


def summaries_as_payload(traces: list[DecisionTraceV1]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for trace in traces:
        summary = build_decision_trace_summary(trace)
        payload.append(
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
        )
    return payload


def _signals_map(trace: DecisionTraceV1) -> tuple[dict[str, bool], list[str]]:
    evaluated = sanitize_codes(trace.signals_evaluated)
    fired = sanitize_codes(trace.signals_fired)
    fired_set = set(fired)
    signals = {signal: signal in fired_set for signal in evaluated}
    return signals, fired


def _evaluate_trace(
    decision_kind: str,
    trace: DecisionTraceV1,
    signals: dict[str, bool],
    config,
) -> DecisionEvalResult:
    if decision_kind == "ATTENTION_GATE":
        return evaluate_attention_gate(AttentionGateContext(signals=signals))
    if decision_kind == "LLM_GATE":
        return evaluate_llm_gate(LlmGateContext(signals=signals))
    if decision_kind == "PRIORITY_HEURISTIC":
        return evaluate_priority_heuristic(
            PriorityHeuristicContext(
                signals=signals,
                explain_codes=_sanitize_explain_codes(trace),
                config=config,
            )
        )
    return DecisionEvalResult(
        decision="UNKNOWN",
        signals_evaluated=sanitize_codes(sorted(signals.keys())),
        signals_fired=sanitize_codes(sorted([key for key, fired in signals.items() if fired])),
        explain_codes=_sanitize_explain_codes(trace),
        evidence={"matched": len(trace.signals_fired), "total": len(trace.signals_evaluated)},
    )


def _build_counterfactuals(
    decision_kind: str,
    trace: DecisionTraceV1,
    signals: dict[str, bool],
    fired_signals: list[str],
    baseline: DecisionEvalResult,
    config,
) -> list[CounterfactualDelta]:
    counterfactuals: list[CounterfactualDelta] = []
    for signal in fired_signals:
        overrides = {signal: False}
        if decision_kind == "ATTENTION_GATE":
            evaluated = evaluate_attention_gate(
                AttentionGateContext(signals=signals), overrides
            )
        elif decision_kind == "LLM_GATE":
            evaluated = evaluate_llm_gate(LlmGateContext(signals=signals), overrides)
        elif decision_kind == "PRIORITY_HEURISTIC":
            evaluated = evaluate_priority_heuristic(
                PriorityHeuristicContext(
                    signals=signals,
                    explain_codes=_sanitize_explain_codes(trace),
                    config=config,
                ),
                overrides,
            )
        else:
            evaluated = baseline
        if evaluated.decision != baseline.decision:
            counterfactuals.append(
                CounterfactualDelta(signal=signal, decision=evaluated.decision)
            )
    return counterfactuals


def _normalize_evidence(
    evidence: dict[str, int], result: DecisionEvalResult
) -> dict[str, int]:
    matched = int(evidence.get("matched") or 0)
    total = int(evidence.get("total") or 0)
    if total <= 0:
        return {"matched": len(result.signals_fired), "total": len(result.signals_evaluated)}
    return {"matched": matched, "total": total}


def _sanitize_explain_codes(trace: DecisionTraceV1) -> list[str]:
    return sanitize_codes(trace.explain_codes)


__all__ = ["DecisionTraceSummary", "CounterfactualDelta", "build_decision_trace_summary", "summaries_as_payload"]
