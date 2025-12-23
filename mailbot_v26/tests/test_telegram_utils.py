from mailbot_v26.telegram_utils import telegram_safe


def test_telegram_safe_escapes_html_and_removes_backslashes() -> None:
    text = "a&b<c>\"'\\path"
    assert telegram_safe(text) == "a&amp;b&lt;c&gt;&quot;&#x27;path"
