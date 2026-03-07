from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from mailbot_v26.observability.decision_trace_v1 import sanitize_codes
from mailbot_v26.priority.priority_engine_v2 import PriorityV2Config, load_priority_v2_config


@dataclass(frozen=True, slots=True)
class DecisionEvalResult:
    decision: str
    signals_evaluated: list[str]
    signals_fired: list[str]
    explain_codes: list[str]
    evidence: dict[str, int]


@dataclass(frozen=True, slots=True)
class AttentionGateContext:
    signals: Mapping[str, bool]


@dataclass(frozen=True, slots=True)
class LlmGateContext:
    signals: Mapping[str, bool]


@dataclass(frozen=True, slots=True)
class PriorityHeuristicContext:
    signals: Mapping[str, bool]
    explain_codes: list[str]
    config: PriorityV2Config | None = None


def _apply_overrides(
    signals: Mapping[str, bool], overrides: Mapping[str, bool] | None
) -> dict[str, bool]:
    merged = {str(key): bool(value) for key, value in signals.items()}
    if overrides:
        for key, value in overrides.items():
            if key in merged:
                merged[key] = bool(value)
    return merged


def evaluate_attention_gate(
    context: AttentionGateContext,
    overrides: Mapping[str, bool] | None = None,
) -> DecisionEvalResult:
    signals = _apply_overrides(context.signals, overrides)
    signals_evaluated = sanitize_codes(sorted(signals.keys()))
    signals_fired = sanitize_codes(sorted([key for key, fired in signals.items() if fired]))
    use_candidate = signals.get("TOP_PERCENTILE_CANDIDATE", False)
    budget_ok = signals.get("BUDGET_GATE_ALLOW", False) if use_candidate else False
    decision = "ALLOW" if use_candidate and budget_ok else "DENY"
    evidence = {"matched": len(signals_fired), "total": len(signals_evaluated)}
    return DecisionEvalResult(
        decision=decision,
        signals_evaluated=signals_evaluated,
        signals_fired=signals_fired,
        explain_codes=signals_fired,
        evidence=evidence,
    )


def evaluate_llm_gate(
    context: LlmGateContext,
    overrides: Mapping[str, bool] | None = None,
) -> DecisionEvalResult:
    signals = _apply_overrides(context.signals, overrides)
    signals_evaluated = sanitize_codes(sorted(signals.keys()))
    signals_fired = sanitize_codes(sorted([key for key, fired in signals.items() if fired]))
    llm_used = bool(signals.get("LLM_CALLED_DIRECT") or signals.get("LLM_QUEUED"))
    decision = "LLM_USED" if llm_used else "HEURISTIC"
    evidence = {"matched": len(signals_fired), "total": len(signals_evaluated)}
    return DecisionEvalResult(
        decision=decision,
        signals_evaluated=signals_evaluated,
        signals_fired=signals_fired,
        explain_codes=signals_fired,
        evidence=evidence,
    )


def evaluate_priority_heuristic(
    context: PriorityHeuristicContext,
    overrides: Mapping[str, bool] | None = None,
) -> DecisionEvalResult:
    signals = _apply_overrides(context.signals, overrides)
    config = context.config or load_priority_v2_config()
    score, reason_codes = _priority_score_from_signals(
        signals, config=config, explain_codes=context.explain_codes
    )
    if score >= config.priority_red_threshold:
        decision = "🔴"
    elif score >= config.priority_yellow_threshold:
        decision = "🟡"
    else:
        decision = "🔵"
    signals_evaluated = sanitize_codes(sorted(signals.keys()))
    signals_fired = sanitize_codes(sorted([key for key, fired in signals.items() if fired]))
    explain_codes = sanitize_codes(sorted(set(reason_codes)))
    evidence = {"matched": len(signals_fired), "total": len(signals_evaluated)}
    return DecisionEvalResult(
        decision=decision,
        signals_evaluated=signals_evaluated,
        signals_fired=signals_fired,
        explain_codes=explain_codes,
        evidence=evidence,
    )


def _priority_score_from_signals(
    signals: Mapping[str, bool],
    *,
    config: PriorityV2Config,
    explain_codes: list[str],
) -> tuple[int, list[str]]:
    score = 0
    reason_codes: list[str] = []

    if signals.get("URGENCY_KEYWORD"):
        score += config.urgency_weight_default
        reason_codes.append("PRIO_URGENT_KEYWORD")
        if signals.get("URGENCY_WEIGHTED_BY_TYPE"):
            extra = max(0, config.urgency_weight_by_type - config.urgency_weight_default)
            score += extra
            reason_codes.append("PRIO_URGENT_WEIGHTED_BY_TYPE")

    if signals.get("AMOUNT_100K"):
        score += config.amount_100k_points
        reason_codes.append("PRIO_AMOUNT_100K")
    elif signals.get("AMOUNT_50K"):
        score += config.amount_50k_points
        reason_codes.append("PRIO_AMOUNT_50K")
    elif signals.get("AMOUNT_10K"):
        score += config.amount_10k_points
        reason_codes.append("PRIO_AMOUNT_10K")
    elif signals.get("AMOUNT_PRESENT"):
        score += config.amount_base_points
        reason_codes.append("PRIO_AMOUNT_BASE")

    if signals.get("DEADLINE_WITHIN_1D"):
        score += config.deadline_1d_points
        reason_codes.append("PRIO_DEADLINE_1D")
    elif signals.get("DEADLINE_WITHIN_3D"):
        score += config.deadline_3d_points
        reason_codes.append("PRIO_DEADLINE_3D")
    elif signals.get("DEADLINE_WITHIN_7D"):
        score += config.deadline_7d_points
        reason_codes.append("PRIO_DEADLINE_7D")

    if signals.get("MAIL_TYPE_BOOST"):
        mail_type_points = _mail_type_points_from_codes(config, explain_codes)
        if mail_type_points:
            score += mail_type_points[0]
            reason_codes.append(mail_type_points[1])

    if signals.get("FREQUENCY_SPIKE"):
        score += config.freq_spike_points
        reason_codes.append("PRIO_FREQ_SPIKE_3X")

    if signals.get("REMINDER_CHAIN_3PLUS"):
        score += config.chain_three_points
        reason_codes.append("PRIO_CHAIN_3PLUS")
    elif signals.get("REMINDER_CHAIN_2PLUS"):
        score += config.chain_two_points
        reason_codes.append("PRIO_CHAIN_2PLUS")

    if signals.get("VIP_SENDER"):
        multiplier = 1.0
        if signals.get("VIP_FYI_DAMPEN"):
            multiplier *= config.vip_multiplier_fyi
            reason_codes.append("PRIO_VIP_FYI_DAMPEN")
        if signals.get("VIP_FREQ_DAMPEN"):
            multiplier *= config.vip_multiplier_freq
            reason_codes.append("PRIO_VIP_FREQ_DAMPEN")
        if signals.get("VIP_COMMITMENT_BOOST"):
            multiplier *= config.vip_multiplier_commitment
            reason_codes.append("PRIO_VIP_COMMITMENT_BOOST")
        multiplier = max(config.vip_multiplier_min, min(config.vip_multiplier_max, multiplier))
        score += int(round(config.vip_base_score * multiplier))
        reason_codes.append("PRIO_VIP_BASE")

    score = max(0, min(100, score))
    return score, reason_codes


def _mail_type_points_from_codes(
    config: PriorityV2Config, explain_codes: list[str]
) -> tuple[int, str] | None:
    mapping = {
        "PRIO_TYPE_INVOICE_FINAL": config.type_invoice_final_points,
        "PRIO_TYPE_REMINDER_ESCALATION": config.type_reminder_escalation_points,
        "PRIO_TYPE_CONTRACT_TERMINATION": config.type_contract_termination_points,
        "PRIO_TYPE_CLAIM": config.type_claim_points,
    }
    for code in explain_codes:
        if code in mapping:
            return mapping[code], code
    return None


__all__ = [
    "DecisionEvalResult",
    "AttentionGateContext",
    "LlmGateContext",
    "PriorityHeuristicContext",
    "evaluate_attention_gate",
    "evaluate_llm_gate",
    "evaluate_priority_heuristic",
]
