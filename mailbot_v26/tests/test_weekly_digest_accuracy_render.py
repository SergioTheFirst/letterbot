from __future__ import annotations

from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import WeeklyAccuracyProgress


def _base_weekly_kwargs() -> dict[str, object]:
    return dict(
        week_key="2025-W01",
        total_emails=47,
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
        weekly_calibration_report=None,
        invoice_count=0,
        invoice_total_rub=None,
        contract_count=0,
        silence_risk=None,
    )


def test_progress_line_shown_for_sufficient_weekly_data() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_accuracy_progress": WeeklyAccuracyProgress(
                current_surprise_rate_pp=11,
                prev_surprise_rate_pp=20,
                delta_pp=9,
                current_decisions=30,
                prev_decisions=35,
                current_corrections=7,
            ),
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert (
        "Твой прогресс: точность бота выросла до 89% благодаря твоим 7 коррекциям."
        in text
    )


def test_progress_line_omitted_for_insufficient_corrections() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_accuracy_progress": WeeklyAccuracyProgress(
                current_surprise_rate_pp=11,
                prev_surprise_rate_pp=20,
                delta_pp=9,
                current_decisions=30,
                prev_decisions=35,
                current_corrections=2,
            ),
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "Твой прогресс:" not in text
