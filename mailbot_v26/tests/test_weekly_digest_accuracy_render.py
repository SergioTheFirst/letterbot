from __future__ import annotations

from mailbot_v26.pipeline import weekly_digest


def _base_weekly_kwargs() -> dict[str, object]:
    return dict(
        week_key="2025-W01",
        total_emails=0,
        deferred_emails=0,
        attention_entities=[],
        commitment_counts={},
        overdue_commitments=[],
        trust_deltas={},
        anomaly_alerts=[],
        attention_economics=None,
        quality_metrics=None,
        notification_sla=None,
        previous_week_sla=None,
    )


def _text(report: dict[str, object]) -> str:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_accuracy_report": report,
        }
    )
    return weekly_digest._build_weekly_digest_text(data)


def test_weekly_accuracy_hidden_below_80() -> None:
    text = _text(
        {
            "emails_received": 12,
            "priority_corrections": 5,
            "surprises": 1,
            "accuracy_pct": 75,
        }
    )
    assert "📊 Неделя:" not in text


def test_weekly_accuracy_shown_at_or_above_80() -> None:
    text = _text(
        {
            "emails_received": 12,
            "priority_corrections": 5,
            "surprises": 1,
            "accuracy_pct": 85,
        }
    )
    assert "📊 Неделя: 12 писем · 5 коррекции · точность 85%" in text


def test_weekly_accuracy_hidden_when_accuracy_missing() -> None:
    text = _text(
        {
            "emails_received": 12,
            "priority_corrections": 5,
            "surprises": 1,
        }
    )
    assert "📊 Неделя:" not in text


def test_weekly_accuracy_hidden_when_insufficient_corrections() -> None:
    text = _text(
        {
            "emails_received": 12,
            "priority_corrections": 2,
            "surprises": 0,
            "accuracy_pct": 90,
        }
    )
    assert "📊 Неделя:" not in text
