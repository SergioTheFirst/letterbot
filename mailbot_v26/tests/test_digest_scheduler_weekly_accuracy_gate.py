from __future__ import annotations

from mailbot_v26.pipeline import digest_scheduler, weekly_digest


def _base_data(report: dict[str, object]) -> weekly_digest.WeeklyDigestData:
    return weekly_digest.WeeklyDigestData(
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
        weekly_accuracy_report=report,
        notification_sla=None,
        previous_week_sla=None,
    )


def test_has_weekly_content_false_when_accuracy_below_threshold() -> None:
    data = _base_data({"priority_corrections": 5, "accuracy_pct": 75})
    assert digest_scheduler._has_weekly_content(data) is False


def test_has_weekly_content_true_when_gate_passes() -> None:
    data = _base_data({"priority_corrections": 5, "accuracy_pct": 85})
    assert digest_scheduler._has_weekly_content(data) is True


def test_has_weekly_content_false_when_accuracy_missing() -> None:
    data = _base_data({"priority_corrections": 5})
    assert digest_scheduler._has_weekly_content(data) is False
