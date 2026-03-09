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
        invoice_count=0,
        invoice_total_rub=None,
        contract_count=0,
        silence_risk=None,
    )


def test_weekly_digest_empty_state_is_human_and_short() -> None:
    data = weekly_digest.WeeklyDigestData(**_base_weekly_kwargs())
    text = weekly_digest._build_weekly_digest_text(data)
    assert text.splitlines()[0] == "За неделю 0 писем. Главное:"
    assert "• Спокойная неделя: критичных сигналов не было." in text
    assert "Предложения к калибровке" not in text


def test_weekly_digest_renders_invoice_highlight_with_amount() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "invoice_count": 3,
            "invoice_total_rub": 387000,
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "• К оплате сейчас: 3 документов на 387 000 ₽" in text


def test_weekly_digest_renders_contract_highlight() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "contract_count": 2,
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "• Ждут внимания: 2 договоров" in text


def test_weekly_digest_renders_silence_risk_highlight() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "silence_risk": {"contact": "Ивановой", "days_silent": 9},
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "• От Ивановой — молчание 9 дней (риск)" in text


def test_weekly_digest_progress_does_not_render_without_signal() -> None:
    data = weekly_digest.WeeklyDigestData(
        **{
            **_base_weekly_kwargs(),
            "weekly_accuracy_progress": WeeklyAccuracyProgress(
                current_surprise_rate_pp=11,
                prev_surprise_rate_pp=12,
                delta_pp=1,
                current_decisions=30,
                prev_decisions=35,
                current_corrections=7,
            ),
        }
    )
    text = weekly_digest._build_weekly_digest_text(data)
    assert "Твой прогресс:" not in text


def test_collect_weekly_data_accepts_account_emails() -> None:
    class DummyAnalytics:
        def __init__(self) -> None:
            self.seen: dict[str, object] = {}

        def weekly_email_volume(
            self,
            *,
            account_email: str,
            days: int = 7,
            account_emails: list[str] | None = None,
        ) -> dict[str, int]:
            self.seen["volume"] = account_emails
            return {"total": 0, "deferred": 0}

        def weekly_attention_entities(
            self,
            *,
            account_email: str,
            days: int = 7,
            account_emails: list[str] | None = None,
        ) -> list[dict[str, object]]:
            self.seen["attention"] = account_emails
            return []

        def weekly_commitment_counts(
            self,
            *,
            account_email: str,
            days: int = 7,
            account_emails: list[str] | None = None,
        ) -> dict[str, int]:
            self.seen["commitments"] = account_emails
            return {}

        def weekly_overdue_commitments(
            self,
            *,
            account_email: str,
            days: int = 7,
            limit: int = 5,
            account_emails: list[str] | None = None,
        ) -> list[dict[str, object]]:
            self.seen["overdue"] = account_emails
            return []

        def weekly_trust_score_deltas(
            self, *, days: int = 7
        ) -> dict[str, list[dict[str, object]]]:
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

        def _normalize_account_scope(
            self,
            account_email: str,
            account_emails: list[str] | None,
        ) -> list[str]:
            return account_emails or [account_email]

        def _window_start_ts(self, days: int) -> float:
            return 0.0

        def _event_rows_scoped(
            self, *, account_ids: list[str], event_type: str, since_ts: float
        ):
            self.seen["event_rows"] = account_ids
            return []

        def _event_payload(self, row: object) -> dict[str, object]:
            return {}

        def get_silence_insights(
            self,
            *,
            account_email: str,
            account_emails: list[str] | None,
            window_days: int,
            limit: int,
        ) -> list[dict[str, object]]:
            self.seen["silence"] = account_emails
            return []

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
    assert analytics.seen["volume"] == account_emails
    assert analytics.seen["attention"] == account_emails
    assert analytics.seen["commitments"] == account_emails
    assert analytics.seen["overdue"] == account_emails
    assert analytics.seen["event_rows"] == account_emails
    assert analytics.seen["silence"] == account_emails
