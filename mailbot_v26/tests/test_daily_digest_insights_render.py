from __future__ import annotations

from mailbot_v26.pipeline import daily_digest


_TARGET_EMOJI = "\U0001F3AF"


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
        digest_insights_enabled=True,
        digest_insights_max_items=3,
    )


def test_daily_digest_insights_section_absent_when_empty() -> None:
    data = daily_digest.DigestData(**_base_digest_kwargs())
    text = daily_digest._build_digest_text(data)
    assert "ТРЕБУЕТ ВНИМАНИЯ" not in text


def test_daily_digest_insights_section_present_with_items() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "deadlock_insights": [
                {
                    "from_email": "boss@example.com",
                    "subject": "Счёт",
                }
            ],
            "silence_insights": [
                {
                    "contact": "client@example.com",
                    "days_silent": 5,
                    "count_window": 3,
                    "last_seen_ts": 100,
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "ТРЕБУЕТ ВНИМАНИЯ" in text
    assert "Deadlock" in text
    assert "Silence" in text
    assert _TARGET_EMOJI in text
