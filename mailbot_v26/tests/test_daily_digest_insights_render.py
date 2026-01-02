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
        digest_action_templates_enabled=False,
        trust_bootstrap_snapshot=None,
        trust_bootstrap_min_samples=0,
        trust_bootstrap_hide_action_templates=False,
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
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "\u26a0\ufe0f <b>ТРЕБУЕТ ВНИМАНИЯ</b>" in text
    assert "Застой в переписке" in text
    assert "Нет ответа" in text
    assert _TARGET_EMOJI in text
    assert "пинговать" not in text
    assert "Deadlock" not in text
    assert "Silence" not in text


def test_daily_digest_insights_order_and_limit() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "digest_insights_max_items": 3,
            "deadlock_insights": [
                {"from_email": "a@example.com", "subject": "A1"},
                {"from_email": "b@example.com", "subject": "B1"},
            ],
            "silence_insights": [
                {"contact": "c@example.com", "days_silent": 3},
                {"contact": "d@example.com", "days_silent": 4},
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    lines = text.splitlines()
    header_index = lines.index("\u26a0\ufe0f <b>ТРЕБУЕТ ВНИМАНИЯ</b>")
    insight_lines = lines[header_index + 1 : header_index + 4]
    assert insight_lines == [
        f"• Застой в переписке: a@example.com — A1 → {_TARGET_EMOJI} Предложить созвон (15 мин)",
        f"• Застой в переписке: b@example.com — B1 → {_TARGET_EMOJI} Предложить созвон (15 мин)",
        f"• Нет ответа: c@example.com — 3 дня → {_TARGET_EMOJI} Вежливо напомнить сегодня",
    ]


def test_daily_digest_insights_action_templates_present_when_enabled() -> None:
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "digest_action_templates_enabled": True,
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
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    lines = [line for line in text.splitlines() if line.strip()]
    assert any(line.startswith("  <i>Текст:") and line.endswith("</i>") for line in lines)
    assert lines.count("  <i>Текст: Предлагаю созвониться на 15 минут сегодня или завтра — так быстрее решим вопрос.</i>") == 1
    assert lines.count("  <i>Текст: Напомню про наш вопрос. Удобно вернуться к нему сегодня?</i>") == 1
    assert "пинговать" not in text
    assert "Deadlock" not in text
    assert "Silence" not in text


def test_daily_digest_insights_action_templates_absent_when_disabled() -> None:
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
                }
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "Текст:" not in text
    assert "пинговать" not in text
    assert "Deadlock" not in text
    assert "Silence" not in text


def test_daily_digest_bootstrap_block_and_templates_hidden() -> None:
    snapshot = daily_digest.TrustBootstrapSnapshot(
        start_ts=1.0,
        days_since_start=1.0,
        samples_count=12,
        corrections_count=0,
        surprises_count=0,
        surprise_rate=None,
        active=True,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "trust_bootstrap_snapshot": snapshot,
            "trust_bootstrap_min_samples": 50,
            "digest_action_templates_enabled": True,
            "deadlock_insights": [
                {"from_email": "boss@example.com", "subject": "Счёт"}
            ],
            "silence_insights": [
                {"contact": "client@example.com", "days_silent": 5}
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "\U0001F393 <b>Режим обучения</b>" in text
    assert "Прогресс: 12/50" in text
    assert "Текст:" not in text
    assert "→" not in text


def test_daily_digest_bootstrap_inactive_keeps_templates() -> None:
    snapshot = daily_digest.TrustBootstrapSnapshot(
        start_ts=1.0,
        days_since_start=20.0,
        samples_count=60,
        corrections_count=2,
        surprises_count=0,
        surprise_rate=0.0,
        active=False,
    )
    data = daily_digest.DigestData(
        **{
            **_base_digest_kwargs(),
            "trust_bootstrap_snapshot": snapshot,
            "trust_bootstrap_min_samples": 50,
            "digest_action_templates_enabled": True,
            "deadlock_insights": [
                {"from_email": "boss@example.com", "subject": "Счёт"}
            ],
            "silence_insights": [
                {"contact": "client@example.com", "days_silent": 5}
            ],
        }
    )
    text = daily_digest._build_digest_text(data)
    assert "\U0001F393 <b>Режим обучения</b>" not in text
    assert "Текст:" in text
