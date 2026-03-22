from __future__ import annotations

from mailbot_v26.pipeline import daily_digest


def _base_digest_kwargs() -> dict[str, object]:
    return dict(
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
        attention_economics=None,
        quality_metrics=None,
        notification_sla=None,
        deadlock_insights=[],
        silence_insights=[],
        digest_insights_enabled=False,
        digest_insights_max_items=0,
        digest_action_templates_enabled=False,
        trust_bootstrap_snapshot=None,
        trust_bootstrap_min_samples=0,
        trust_bootstrap_hide_action_templates=False,
        regret_minimization_stats=None,
    )


def test_daily_digest_regret_minimization_line_rendered() -> None:
    stats = daily_digest.RegretMinimizationStats(
        total=12,
        drops=5,
        pct=42,
        window_days=90,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "commitments_expired": 3,
            "regret_minimization_stats": stats,
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert (
        "• Если откладывать: в похожих случаях за 90 дней снижение доверия было "
        "в 5 из 12 (42%)."
    ) in text


def test_daily_digest_regret_minimization_hidden_during_bootstrap() -> None:
    stats = daily_digest.RegretMinimizationStats(
        total=10,
        drops=3,
        pct=30,
        window_days=90,
    )
    snapshot = daily_digest.TrustBootstrapSnapshot(
        start_ts=1.0,
        days_since_start=1.0,
        samples_count=3,
        corrections_count=0,
        surprises_count=0,
        surprise_rate=None,
        active=True,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "commitments_expired": 2,
            "regret_minimization_stats": stats,
            "trust_bootstrap_snapshot": snapshot,
            "trust_bootstrap_min_samples": 5,
        }
    )
    text = daily_digest._build_digest_text(data, locale="ru")
    assert "Если откладывать:" not in text
