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


def test_weekly_accuracy_report_block_hidden_when_flag_off() -> None:
    data = weekly_digest.WeeklyDigestData(**_base_weekly_kwargs())
    text = weekly_digest._build_weekly_digest_text(data)
    assert "Отчёт точности" not in text


def test_weekly_accuracy_report_block_hidden_when_no_corrections() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_accuracy_report": {
                "emails_received": 4,
                "priority_corrections": 0,
                "surprises": 0,
            },
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "Отчёт точности" not in text


def test_weekly_accuracy_report_block_renders_when_enabled() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_accuracy_report": {
                "emails_received": 12,
                "priority_corrections": 3,
                "surprises": 1,
                "accuracy_pct": 67,
            },
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "<b>Отчёт точности (7 дней)</b>" in text
    assert "• Писем обработано: 12" in text
    assert "• Коррекции приоритета: 3" in text
    assert "• Сюрпризы: 1 (точность: 67%)" in text
