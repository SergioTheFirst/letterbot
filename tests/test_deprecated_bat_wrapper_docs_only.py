from pathlib import Path


def test_deprecated_bat_wrapper_delegates_to_root_wrapper() -> None:
    wrapper = Path("mailbot_v26/run_mailbot.bat")
    assert wrapper.exists()

    content = wrapper.read_text(encoding="utf-8")
    assert "setlocal EnableExtensions" in content
    assert 'cd /d "%REPO_ROOT%"' in content
    assert 'call "%REPO_ROOT%\\run_mailbot.bat" %*' in content
    assert "exit /b %errorlevel%" in content
