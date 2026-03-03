from pathlib import Path


def test_deprecated_mailbot_wrapper_removed() -> None:
    assert not Path("mailbot_v26/run_mailbot.bat").exists()
