from __future__ import annotations

from datetime import datetime, timezone

from mailbot_v26.behavior.attention_engine import (
    DeliveryContext,
    DeliveryMode,
    DeliveryScores,
    decide_delivery,
)
from mailbot_v26.config.delivery_policy import DeliveryPolicyConfig


def test_focus_hours_batches_high_value_non_critical() -> None:
    policy = DeliveryPolicyConfig()
    scores = DeliveryScores(value=90, risk=10, confidence=70)
    context = DeliveryContext(
        now_local=datetime.now(timezone.utc),
        immediate_sent_last_hour=0,
        max_immediate_per_hour=policy.max_immediate_per_hour,
    )
    decision = decide_delivery(scores=scores, context=context, policy=policy)
    assert decision.mode == DeliveryMode.IMMEDIATE
    assert "high_value" in decision.reason_codes


def test_focus_hours_does_not_override_critical() -> None:
    policy = DeliveryPolicyConfig(critical_risk_threshold=80)
    scores = DeliveryScores(value=40, risk=90, confidence=70)
    context = DeliveryContext(
        now_local=datetime.now(timezone.utc),
        immediate_sent_last_hour=0,
        max_immediate_per_hour=policy.max_immediate_per_hour,
    )
    decision = decide_delivery(scores=scores, context=context, policy=policy)
    assert decision.mode == DeliveryMode.IMMEDIATE
