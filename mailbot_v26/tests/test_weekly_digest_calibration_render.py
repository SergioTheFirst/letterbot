from __future__ import annotations

from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import WeeklyAccuracyProgress


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
        weekly_calibration_report=None,
        weekly_accuracy_progress=None,
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
                "proposals": [
                    {
                        "label": "entity-a",
                        "transition": "🔴→🟡",
                        "count": 5,
                        "hint": "вероятно, завышаем срочность",
                    }
                ],
            },
            "weekly_accuracy_progress": WeeklyAccuracyProgress(
                current_surprise_rate_pp=10,
                prev_surprise_rate_pp=20,
                delta_pp=10,
                current_decisions=30,
                prev_decisions=30,
                current_corrections=5,
            ),
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "<b>Отчёт точности (7 дней)</b>" in text
    assert "• Коррекции приоритета: 12" in text
    assert "• Сюрпризы: 6 (точность: 50%)" in text
    assert "• Где чаще всего случалось:" in text
    assert "  - entity-a — 3" in text
    assert "  - entity-b — 2" in text
    assert "• Предложения к калибровке (shadow):" in text
    assert "  - entity-a: 🔴→🟡 ×5 — вероятно, завышаем срочность" in text
    assert "Рост точности: +10 п.п. (сюрпризы 20% → 10%), коррекции: 5 за период" in text


def test_weekly_calibration_report_progress_hidden_without_data() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_calibration_report": {
                "window_days": 7,
                "corrections": 12,
                "surprises": 6,
                "accuracy_pct": 50,
                "top": [],
            },
            "weekly_accuracy_progress": None,
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "Рост точности:" not in text
    assert "Падение точности:" not in text


def test_weekly_calibration_proposals_limited_to_three_lines() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_calibration_report": {
                "window_days": 7,
                "corrections": 12,
                "surprises": 6,
                "accuracy_pct": 50,
                "top": [],
                "proposals": [
                    {"label": "entity-a", "transition": "🔴→🟡", "count": 5, "hint": "hint-a"},
                    {"label": "entity-b", "transition": "🔵→🔴", "count": 4, "hint": "hint-b"},
                    {"label": "entity-c", "transition": "🟡→🔴", "count": 3, "hint": "hint-c"},
                    {"label": "entity-d", "transition": "🔴→🔵", "count": 3, "hint": "hint-d"},
                ],
            },
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    lines = text.splitlines()
    header_index = lines.index("• Предложения к калибровке (shadow):")
    proposal_lines = []
    for line in lines[header_index + 1 :]:
        if not line.startswith("  - "):
            break
        proposal_lines.append(line)
    assert len(proposal_lines) == 3


def test_collect_weekly_data_accepts_account_emails() -> None:
    class DummyAnalytics:
        def __init__(self) -> None:
            self.seen: dict[str, object] = {}

        def weekly_email_volume(self, *, account_email: str, days: int = 7) -> dict[str, int]:
            return {"total": 0, "deferred": 0}

        def weekly_attention_entities(self, *, account_email: str, days: int = 7) -> list[dict[str, object]]:
            return []

        def weekly_commitment_counts(self, *, account_email: str, days: int = 7) -> dict[str, int]:
            return {}

        def weekly_overdue_commitments(
            self, *, account_email: str, days: int = 7, limit: int = 5
        ) -> list[dict[str, object]]:
            return []

        def weekly_trust_score_deltas(self, *, days: int = 7) -> dict[str, list[dict[str, object]]]:
            return {}

        def weekly_accuracy_report(
            self,
            *,
            account_email: str,
            days: int,
            account_emails: list[str] | None = None,
        ) -> dict[str, object]:
            self.seen["accuracy"] = account_emails
            return {"emails_received": 0, "priority_corrections": 0, "surprises": 0}

        def weekly_surprise_breakdown(
            self,
            account_email: str,
            *,
            since_ts: float,
            top_n: int,
            min_corrections: int,
            account_emails: list[str] | None = None,
        ) -> dict[str, object]:
            raise AssertionError("weekly_surprise_breakdown should not be called")

        def weekly_calibration_proposals(
            self,
            account_email: str,
            *,
            since_ts: float,
            top_n: int,
            min_corrections: int,
            account_emails: list[str] | None = None,
        ) -> dict[str, object]:
            self.seen["calibration"] = account_emails
            return {"corrections": 0, "surprises": 0, "top": [], "proposals": []}

        def weekly_accuracy_progress(
            self,
            *,
            account_email: str,
            now_ts: float,
            window_days: int,
            account_emails: list[str] | None = None,
        ) -> WeeklyAccuracyProgress | None:
            self.seen["progress"] = account_emails
            return None

    analytics = DummyAnalytics()
    account_emails = ["acc@example.com", "alt@example.com"]

    weekly_digest._collect_weekly_data(
        analytics=analytics,
        account_email="acc@example.com",
        account_emails=account_emails,
        week_key="2025-W01",
        include_weekly_accuracy_report=True,
        include_weekly_calibration_report=True,
        weekly_calibration_min_corrections=0,
    )

    assert analytics.seen["accuracy"] == account_emails
    assert analytics.seen["calibration"] == account_emails
    assert analytics.seen["progress"] == account_emails
