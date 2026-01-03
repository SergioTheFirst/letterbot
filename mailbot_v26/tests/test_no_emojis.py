from mailbot_v26.insights.aggregator import Insight
from mailbot_v26.pipeline import processor, tg_renderer
from mailbot_v26.ui.emoji_whitelist import ALLOWED_EMOJIS, find_disallowed_emojis


def _assert_whitelist(text: str) -> None:
    assert not find_disallowed_emojis(text)
    assert any(emoji in text for emoji in ALLOWED_EMOJIS)


def test_telegram_render_emoji_whitelist_premium_clarity() -> None:
    rendered = processor._build_premium_clarity_text(
        priority="🔴",
        from_email="Анна 😊 <anna@example.com>",
        from_name="Анна 😊",
        subject="Срочно: счет 😊",
        action_line="Ответить клиенту 😊",
        body_summary="Нужно оплатить счет до завтра 😊",
        attachments=[
            {"filename": "invoice😊.pdf", "text": "Счет на оплату 10 000"},
            {"filename": "notes.txt", "text": ""},
        ],
        insights=[Insight(type="Risk", severity="HIGH", explanation="", recommendation="")],
        insight_digest=None,
        commitments=[],
        attachments_count=2,
        extracted_text_len=120,
        confidence_percent=80,
        extraction_failed=False,
    )
    _assert_whitelist(rendered)


def test_telegram_render_emoji_whitelist_legacy() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🟠",
        from_email="Sender 😄 <sender@example.com>",
        subject="Тема 😄",
        action_line="Ответить 😄",
        attachments=[{"filename": "file😄.txt", "text": "ok"}],
    )
    _assert_whitelist(rendered)
