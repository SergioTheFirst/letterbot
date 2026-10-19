from __future__ import annotations

from mailbot_v26.pipeline import daily_digest
from mailbot_v26.telegram_utils import telegram_safe


def _digest_with_deferred_item(item: dict[str, str]) -> daily_digest.DigestData:
    return daily_digest.DigestData(
        deferred_total=1,
        deferred_attachments_only=0,
        deferred_informational=1,
        deferred_items=[item],
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
    )


def test_deferred_item_sender_and_subject_are_html_escaped() -> None:
    data = _digest_with_deferred_item(
        {"sender": "Evil <i>Corp</i> <x@y.ru>", "subject": "<b>Invoice & act"}
    )

    html_text = telegram_safe(daily_digest._build_digest_text(data, locale="en"))

    assert "  - Evil &lt;i&gt;Corp&lt;/i&gt; &lt;x@y.ru&gt; — &lt;b&gt;Invoice &amp; act" in (
        html_text
    )
    assert "<b>Invoice" not in html_text
    assert "<i>Corp" not in html_text
    assert html_text.count("<b>") == html_text.count("</b>")
    assert html_text.count("<i>") == html_text.count("</i>")


def test_deferred_item_summary_is_html_escaped() -> None:
    data = _digest_with_deferred_item({"sender": "a@b.ru", "summary": "1 < 2 </b>"})

    text = daily_digest._build_digest_text(data, locale="ru")

    assert "  - a@b.ru — 1 &lt; 2 &lt;/b&gt;" in text
