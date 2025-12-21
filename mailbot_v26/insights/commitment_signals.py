from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CommitmentReliabilityMetrics:
    total_commitments: int
    fulfilled_count: int
    expired_count: int
    unknown_count: int


@dataclass(frozen=True)
class CommitmentReliabilitySignal:
    score: int
    label: str
    sample_size: int


def _clamp_score(value: int) -> int:
    return max(0, min(100, value))


def _label_for_score(score: int) -> str:
    if score >= 80:
        return "🟢 Надёжен"
    if score >= 50:
        return "🟡 Нестабилен"
    return "🔴 Рискованный"


def compute_commitment_reliability(
    metrics: CommitmentReliabilityMetrics,
) -> CommitmentReliabilitySignal:
    score = 100
    score -= metrics.expired_count * 25
    score -= metrics.unknown_count * 10
    score = _clamp_score(score)
    label = _label_for_score(score)
    return CommitmentReliabilitySignal(
        score=score,
        label=label,
        sample_size=metrics.total_commitments,
    )


__all__ = [
    "CommitmentReliabilityMetrics",
    "CommitmentReliabilitySignal",
    "compute_commitment_reliability",
]
