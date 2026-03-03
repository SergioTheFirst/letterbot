from pathlib import Path


def test_run_mailbot_wrapper_delegates_to_letterbot() -> None:
    content = Path("run_mailbot.bat").read_text(encoding="utf-8")
    assert "[DEPRECATED]" in content
    assert 'call "%~dp0letterbot.bat" %*' in content


def test_update_and_run_propagates_start_exit_code() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert 'set "RUN_EXIT=%ERRORLEVEL%"' in content
    assert "exit /b %RUN_EXIT%" in content
    assert '--config-dir "%CONFIG_DIR%"' in content


def test_install_and_run_calls_wrapper_and_passes_args() -> None:
    content = Path("install_and_run.bat").read_text(encoding="utf-8")
    assert 'call "%~dp0letterbot.bat" %*' in content


def test_update_and_run_uses_safe_fetch_reset_and_warns_on_pip_failure() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert "Git is not available in PATH. Continuing without update." in content
    assert "git fetch origin main" in content
    assert "git reset --hard origin/main" in content
    assert "git status --porcelain" in content
    assert "Рабочее дерево не чистое" in content
    assert "Dependency installation failed. Continuing" in content


def test_update_and_run_has_python_pip_and_log_diagnostics() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert "Python 3.10+ is required" in content
    assert "pip is not available in the selected Python environment" in content
    assert "Virtual environment:" in content
    assert "Log file:" in content
    assert "[SUMMARY] OK" in content
    assert "[SUMMARY] FAIL" in content
