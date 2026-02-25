from pathlib import Path


def test_run_mailbot_bat_does_not_block_on_doctor_warning() -> None:
    content = Path("run_mailbot.bat").read_text(encoding="utf-8")
    assert "doctor checks ^(warning-first^)" in content
    assert "Doctor found issues. Startup continues" in content
    assert "exit /b 1" not in content.split("Running doctor checks", 1)[1].split("Running config validation", 1)[0]


def test_run_mailbot_bat_uses_python_readiness_check_and_recheck() -> None:
    content = Path("run_mailbot.bat").read_text(encoding="utf-8")
    assert "findstr /m \"CHANGE_ME\"" not in content
    assert content.count("-m mailbot_v26 config-ready") >= 2
    assert "start /wait notepad.exe" in content
    assert "exit /b 2" in content


def test_update_and_run_handles_config_not_ready_exit_code() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert "set \"RUN_EXIT=%ERRORLEVEL%\"" in content
    assert "setup is incomplete" in content
    assert "set \"RUN_EXIT=0\"" in content
    assert "exit /b %RUN_EXIT%" in content


def test_install_and_run_calls_migrate_config() -> None:
    content = Path("install_and_run.bat").read_text(encoding="utf-8")
    assert "-m mailbot_v26 migrate-config" in content


def test_update_and_run_is_fail_open_for_git_and_pip() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert "Git is not available in PATH. Continuing without update." in content
    assert "Git pull failed" in content
    assert "Dependency installation failed. Continuing" in content
