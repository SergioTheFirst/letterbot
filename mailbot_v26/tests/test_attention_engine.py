from __future__ import annotations

from datetime import datetime, timezone

from mailbot_v26.behavior.attention_engine import (
    DeliveryContext,
    DeliveryMode,
    decide_delivery,
    score_email,
)
from mailbot_v26.config.delivery_policy import DeliveryPolicyConfig


def test_quiet_hours_defers_non_critical() -> None:
    policy = DeliveryPolicyConfig()
    scores = score_email(
        priority="🔵",
        commitments_count=0,
        deadlines_count=0,
        insight_severity=None,
        relationship_health_delta=None,
    )
    context = DeliveryContext(
        now_local=datetime.now(timezone.utc),
        immediate_sent_last_hour=0,
        max_immediate_per_hour=policy.max_immediate_per_hour,
    )
    decision = decide_delivery(scores=scores, context=context, policy=policy)
    assert decision.mode == DeliveryMode.BATCH_TODAY


def test_critical_risk_overrides_quiet_hours() -> None:
    policy = DeliveryPolicyConfig(critical_risk_threshold=80)
    scores = score_email(
        priority="🔵",
        commitments_count=1,
        deadlines_count=1,
        insight_severity="HIGH",
        relationship_health_delta=-0.2,
    )
    context = DeliveryContext(
        now_local=datetime.now(timezone.utc),
        immediate_sent_last_hour=0,
        max_immediate_per_hour=policy.max_immediate_per_hour,
    )
    decision = decide_delivery(scores=scores, context=context, policy=policy)
    assert decision.mode == DeliveryMode.IMMEDIATE


def test_weekend_high_value_batches_non_critical() -> None:
    policy = DeliveryPolicyConfig()
    scores = score_email(
        priority="🔴",
        commitments_count=0,
        deadlines_count=0,
        insight_severity=None,
        relationship_health_delta=None,
    )
    context = DeliveryContext(
        now_local=datetime.now(timezone.utc),
        immediate_sent_last_hour=0,
        max_immediate_per_hour=policy.max_immediate_per_hour,
    )
    decision = decide_delivery(scores=scores, context=context, policy=policy)
    assert decision.mode == DeliveryMode.IMMEDIATE
    assert "high_value" in decision.reason_codes
