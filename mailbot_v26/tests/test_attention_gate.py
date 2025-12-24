from __future__ import annotations

from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.pipeline.attention_gate import (
    AttentionGateInput,
    apply_attention_gate,
)


def test_high_priority_bypasses_gate() -> None:
    result = apply_attention_gate(
        AttentionGateInput(
            priority="🔴",
            commitments=[],
            deadlines_count=0,
            insight_severity=None,
            attachments_only=True,
            relationship_health_delta=None,
            email_id=1,
        )
    )

    assert result.deferred is False
    assert result.reason == "priority_high"


def test_attachments_only_informational_deferred() -> None:
    result = apply_attention_gate(
        AttentionGateInput(
            priority="🔵",
            commitments=[],
            deadlines_count=0,
            insight_severity="LOW",
            attachments_only=True,
            relationship_health_delta=0.0,
            email_id=2,
        )
    )

    assert result.deferred is True
    assert result.reason == "attachments_only_low_signal"


def test_commitments_force_immediate_send() -> None:
    commitment = Commitment(
        commitment_text="Отправлю отчёт завтра.",
        deadline_iso="2024-06-03",
        status="pending",
        source="heuristic",
        confidence=0.9,
    )
    result = apply_attention_gate(
        AttentionGateInput(
            priority="🔵",
            commitments=[commitment],
            deadlines_count=1,
            insight_severity=None,
            attachments_only=False,
            relationship_health_delta=None,
            email_id=3,
        )
    )

    assert result.deferred is False
    assert result.reason == "commitments_present"
