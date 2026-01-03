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
        weekly_accuracy_report=None,
    )


def test_weekly_calibration_report_block_hidden_when_flag_off() -> None:
    data = weekly_digest.WeeklyDigestData(**_base_weekly_kwargs())
    text = weekly_digest._build_weekly_digest_text(data)
    assert "Где чаще всего случалось" not in text


def test_weekly_calibration_report_block_renders_when_enabled() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_calibration_report": {
                "window_days": 7,
                "corrections": 12,
                "surprises": 6,
                "accuracy_pct": 50,
                "top": [
                    {"label": "entity-a", "count": 3},
                    {"label": "entity-b", "count": 2},
                ],
            },
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "<b>Отчёт точности (7 дней)</b>" in text
    assert "• Коррекции приоритета: 12" in text
    assert "• Сюрпризы: 6 (точность: 50%)" in text
    assert "• Где чаще всего случалось:" in text
    assert "  - entity-a — 3" in text
    assert "  - entity-b — 2" in text
