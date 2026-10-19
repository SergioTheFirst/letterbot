from __future__ import annotations

from mailbot_v26.insights.attention_economics import (
    AttentionEconomicsResult,
    AttentionEntity,
)
from mailbot_v26.pipeline import daily_digest


def _entity(*, message_count: int, minutes: float) -> AttentionEntity:
    return AttentionEntity(
        entity_id="entity-1",
        label="Entity",
        message_count=message_count,
        attachment_count=0,
        estimated_read_minutes=minutes,
        deferred_count=0,
    )


def _attention(entities: tuple[AttentionEntity, ...]) -> AttentionEconomicsResult:
    return AttentionEconomicsResult(
        window_days=7,
        sample_size=5,
        entities=entities,
        top_sinks=entities,
        at_risk=(),
        best_counterparties=(),
    )


def _digest(attention: AttentionEconomicsResult | None) -> daily_digest.DigestData:
    return daily_digest.DigestData(
        deferred_total=0,
        deferred_attachments_only=0,
        deferred_informational=0,
        deferred_items=[],
        uncertainty_queue_items=[],
        commitments_pending=0,
        commitments_expired=0,
        trust_delta=None,
        health_delta=None,
        anomaly_alerts=[],
        attention_economics=attention,
        quality_metrics=None,
        notification_sla=None,
        deadlock_insights=[],
        silence_insights=[],
        digest_insights_enabled=False,
        digest_insights_max_items=0,
        digest_action_templates_enabled=False,
    )


def test_attention_without_entities_is_not_digest_content() -> None:
    assert daily_digest._has_digest_content(_digest(_attention(()))) is False


def test_attention_with_idle_entities_is_not_digest_content() -> None:
    idle = (_entity(message_count=0, minutes=0.0),)

    assert daily_digest._has_digest_content(_digest(_attention(idle))) is False


def test_attention_with_real_activity_is_digest_content() -> None:
    assert daily_digest._has_digest_content(
        _digest(_attention((_entity(message_count=3, minutes=4.5),)))
    )
    assert daily_digest._has_digest_content(
        _digest(_attention((_entity(message_count=0, minutes=1.0),)))
    )


def test_no_attention_and_no_signals_is_not_digest_content() -> None:
    assert daily_digest._has_digest_content(_digest(None)) is False
