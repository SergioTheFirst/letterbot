from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe


def test_telegram_safe_escapes_html_and_removes_backslashes() -> None:
    text = "a&b<c>\"'\\path"
    assert telegram_safe(text) == "a&amp;b&lt;c&gt;\"'path"


def test_html_escaping() -> None:
    assert escape_tg_html("<user@mail.ru>") == "&lt;user@mail.ru&gt;"
