from datetime import datetime
from pathlib import Path

from mailbot_v26.doctor import DoctorEntry, _format_report
from mailbot_v26.insights.quality_metrics import CountBreakdown, QualityMetricsSnapshot
from mailbot_v26.observability.notification_sla import (
    ErrorBreakdown,
    NotificationSLAResult,
)
from mailbot_v26.pipeline import daily_digest, processor, weekly_digest
from mailbot_v26.ui.i18n import (
    humanize_domain,
    humanize_mail_type,
    humanize_mode,
    humanize_reason_codes,
    t,
)


def test_humanize_mail_type_known_and_unknown() -> None:
    assert humanize_mail_type("INVOICE_FINAL", locale="ru") == "Счёт — финальный"
    assert humanize_mail_type("CUSTOM_UNKNOWN", locale="ru") == "Тип: CUSTOM_UNKNOWN"
    assert humanize_domain("SOME_DOMAIN", locale="ru") == "Домен: SOME_DOMAIN"
    assert humanize_mode("FULL", locale="ru").startswith("Полный")
    assert humanize_mode("CUSTOM", locale="ru") == "Режим: CUSTOM"


def test_priority_explain_lines_hide_internal_codes() -> None:
    lines = processor._build_priority_explain_lines(  # type: ignore[attr-defined]
        mail_type="INVOICE_FINAL",
        mail_type_reasons=["mt.invoice.final.keyword=финальн"],
        priority_v2_result=None,
        commitments=[],
        received_at=datetime(2024, 1, 1),
    )

    combined = " ".join(lines).lower()
    assert "invoice" not in combined
    assert any("сч" in line.lower() for line in lines)
    assert humanize_reason_codes(["mt.invoice.final.keyword=финальн"], locale="ru")[
        0
    ].startswith("финальный")


def _sample_quality() -> QualityMetricsSnapshot:
    return QualityMetricsSnapshot(
        window_days=1,
        corrections_total=2,
        by_new_priority=[CountBreakdown(key="high", count=1)],
        by_engine=[CountBreakdown(key="auto", count=2)],
        correction_rate=0.5,
        emails_received=4,
    )


def _sample_sla() -> NotificationSLAResult:
    return NotificationSLAResult(
        delivery_rate_24h=0.97,
        delivery_rate_7d=0.98,
        salvage_rate_24h=0.12,
        p50_latency_24h=10,
        p90_latency_24h=40,
        p99_latency_24h=60,
        p50_latency_7d=11,
        p90_latency_7d=41,
        p99_latency_7d=61,
        top_error_reasons_24h=[ErrorBreakdown(reason="timeout", count=1, share=0.4)],
        error_rate_24h=0.01,
        undelivered_24h=1,
        delivered_24h=100,
        total_24h=101,
    )


def test_digest_ru_has_no_english_codes() -> None:
    data = daily_digest.DigestData(
        deferred_total=2,
        deferred_attachments_only=1,
        deferred_informational=1,
        deferred_items=[],
        uncertainty_queue_items=[],
        commitments_pending=1,
        commitments_expired=0,
        trust_delta=0.123,
        health_delta=5.0,
        anomaly_alerts=["entity: тревога"],
        attention_economics=None,
        quality_metrics=_sample_quality(),
        notification_sla=_sample_sla(),
        deadlock_insights=[],
        silence_insights=[],
        digest_insights_enabled=False,
        digest_insights_max_items=0,
        digest_action_templates_enabled=False,
    )

    text = daily_digest._build_digest_text(data)  # type: ignore[attr-defined]

    assert "Trust" not in text
    assert "Delivery SLA" not in text
    assert "salvage" not in text
    assert "SLA" not in text
    assert "FULL" not in text


def test_weekly_digest_ru_labels() -> None:
    data = weekly_digest.WeeklyDigestData(
        week_key="2024-W01",
        total_emails=5,
        deferred_emails=2,
        attention_entities=[],
        commitment_counts={"created": 1, "fulfilled": 0, "overdue": 1},
        overdue_commitments=[{"entity": "acme", "count": 2}],
        trust_deltas={"acme": [{"label": "acme", "delta": 0.1}]},
        anomaly_alerts=["acme: задержка"],
        attention_economics=None,
        quality_metrics=_sample_quality(),
        notification_sla=_sample_sla(),
        previous_week_sla=_sample_sla(),
    )

    text = weekly_digest._build_weekly_digest_text(data)  # type: ignore[attr-defined]

    assert "Trust" not in text
    assert "Delivery SLA" not in text
    assert "SLA" not in text
    assert "account(s)" not in text


def test_i18n_missing_key_returns_empty_string() -> None:
    assert t("missing.key", locale="ru") == ""


def test_doctor_ru_report_has_ru_context() -> None:
    report = _format_report(
        [
            DoctorEntry("SQLite", "OK", "валидно"),
            DoctorEntry("IMAP", "FAIL", "нет настроенных аккаунтов"),
        ],
        base_dir=Path("/tmp/config"),
    )

    assert "ОТЧЁТ ДОКТОРА" in report
    assert "Version" not in report
    assert "Config dir" not in report
