from __future__ import annotations

from mailbot_v26.pipeline import daily_digest


def _base_digest_kwargs() -> dict[str, object]:
    return dict(
        deferred_total=0,
        deferred_attachments_only=0,
        deferred_informational=0,
        deferred_items=[],
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


def test_daily_digest_behavior_metrics_block_omitted_when_empty() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "behavior_metrics_enabled": True,
            "behavior_metrics": None,
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "ПОВЕДЕНЧЕСКИЕ МЕТРИКИ" not in text


def test_daily_digest_behavior_metrics_block_present() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "behavior_metrics_enabled": True,
            "behavior_metrics_window_days": 7,
            "behavior_metrics": {
                "surprise_rate": 0.245,
                "compression_rate": 0.5,
                "attention_debt_distribution": {
                    "low": 2,
                    "medium": 1,
                    "high": 0,
                },
                "signal_counts": {
                    "deadlock_count": 1,
                    "silence_count": 2,
                },
            },
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "ПОВЕДЕНЧЕСКИЕ МЕТРИКИ (7 дней)" in text
    assert "Ошибки приоритета: 24%" in text
    assert "Снижение шума: 50%" in text
    assert "Долг внимания" in text
    assert "Сигналы: дедлок 1, тишина 2" in text
