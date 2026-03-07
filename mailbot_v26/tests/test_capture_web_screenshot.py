import importlib.util

from mailbot_v26.tools import capture_web_screenshot


def test_capture_web_screenshot_skips_without_playwright(monkeypatch, capsys) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", lambda *_args, **_kwargs: None)
    result = capture_web_screenshot.main()
    output = capsys.readouterr().out.lower()
    assert result == 0
    assert "playwright not installed" in output
