from pathlib import Path


def test_deprecated_bat_wrapper_docs_only() -> None:
    wrapper = Path("mailbot_v26/run_mailbot.bat")
    assert wrapper.exists()

    content = wrapper.read_text(encoding="utf-8").lower()
    assert "deprecated" in content
    assert "run_mailbot.bat" in content
