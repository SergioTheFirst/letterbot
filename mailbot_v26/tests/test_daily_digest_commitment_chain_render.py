from __future__ import annotations

from mailbot_v26.pipeline import daily_digest


def _base_digest_kwargs() -> dict[str, object]:
    return dict(
        deferred_total=0,
        deferred_attachments_only=0,
        deferred_informational=0,
        deferred_items=[],
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


def test_daily_digest_commitment_chain_absent_when_disabled() -> None:
    data = daily_digest.DigestData(**_base_digest_kwargs())
    text = daily_digest._build_digest_text(data)
    assert "Контекст по обязательствам" not in text


def test_daily_digest_commitment_chain_rendering() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "commitments_pending": 1,
            "commitments_expired": 0,
            "commitment_chain_digest_items": [
                {
                    "entity_label": "Клиент А",
                    "items": [
                        {
                            "text": "Отправить договор",
                            "status": "ожидает",
                            "due": "2024-07-12",
                        },
                        {
                            "text": "Подтвердить оплату",
                            "status": "выполнено",
                            "due": None,
                        },
                    ],
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "<b>Контекст по обязательствам</b>" in text
    assert "• Клиент А" in text
    assert "  - ожидает: Отправить договор (срок: 2024-07-12)" in text
    assert "  - выполнено: Подтвердить оплату" in text
    assert "None" not in text
