from __future__ import annotations

from pathlib import Path


README_PATH = Path("README_QUICKSTART_WINDOWS.md")


def _source_mode_section(text: str) -> str:
    start = text.find("### 1) Source mode")
    assert start >= 0, "Source mode section not found"
    next_section = text.find("### 2)", start)
    return text[start:] if next_section < 0 else text[start:next_section]


def test_quickstart_does_not_reference_deprecated_bats() -> None:
    content = README_PATH.read_text(encoding="utf-8", errors="replace")
    assert "install_and_run.bat" not in content
    assert "run_mailbot.bat" not in content


def test_quickstart_source_mode_has_no_legacy_mailbot_config_path() -> None:
    content = README_PATH.read_text(encoding="utf-8", errors="replace")
    source_mode = _source_mode_section(content)
    assert "mailbot_v26\\config" not in source_mode
