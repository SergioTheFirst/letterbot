from mailbot_v26.insights.commitment_signals import (
    CommitmentReliabilityMetrics,
    compute_commitment_reliability,
)


def test_commitment_reliability_score() -> None:
    metrics = CommitmentReliabilityMetrics(
        total_commitments=4,
        fulfilled_count=2,
        expired_count=1,
        unknown_count=1,
    )
    signal = compute_commitment_reliability(metrics)
    assert signal.score == 65
    assert signal.sample_size == 4


def test_commitment_reliability_label_boundaries() -> None:
    high = compute_commitment_reliability(
        CommitmentReliabilityMetrics(
            total_commitments=1,
            fulfilled_count=1,
            expired_count=0,
            unknown_count=0,
        )
    )
    mid = compute_commitment_reliability(
        CommitmentReliabilityMetrics(
            total_commitments=2,
            fulfilled_count=1,
            expired_count=1,
            unknown_count=0,
        )
    )
    low = compute_commitment_reliability(
        CommitmentReliabilityMetrics(
            total_commitments=3,
            fulfilled_count=0,
            expired_count=3,
            unknown_count=0,
        )
    )
    assert high.label == "🟢 Надёжен"
    assert mid.label == "🟡 Нестабилен"
    assert low.label == "🔴 Рискованный"
