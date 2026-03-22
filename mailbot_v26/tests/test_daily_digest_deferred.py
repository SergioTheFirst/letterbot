from __future__ import annotations

from mailbot_v26.pipeline import daily_digest


def test_daily_digest_includes_deferred_items() -> None:
    data = daily_digest.DigestData(
        deferred_total=1,
        deferred_attachments_only=0,
        deferred_informational=1,
        deferred_items=[{"sender": "user@example.com", "summary": "Кратко"}],
        uncertainty_queue_items=[],
        commitments_pending=0,
        commitments_expired=0,
        trust_delta=None,
        health_delta=None,
        anomaly_alerts=[],
        attention_economics=None,
        quality_metrics=None,
        notification_sla=None,
        deadlock_insights=[],
        silence_insights=[],
        digest_insights_enabled=False,
        digest_insights_max_items=0,
        digest_action_templates_enabled=False,
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "Отложено для снижения перегрузки" in text
    assert "user@example.com" in text
    assert "Кратко" in text
