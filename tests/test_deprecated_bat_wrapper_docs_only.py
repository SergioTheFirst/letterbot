from pathlib import Path


def test_deprecated_bat_wrapper_docs_only() -> None:
    wrapper = Path("mailbot_v26/run_mailbot.bat")
    assert wrapper.exists()

    lines = [
        line.strip()
        for line in wrapper.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert lines == [
        "@echo off",
        'cd /d "%~dp0\\.."',
        "call run_mailbot.bat %*",
    ]
