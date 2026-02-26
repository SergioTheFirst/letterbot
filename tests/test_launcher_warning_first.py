from pathlib import Path


def test_run_mailbot_bat_does_not_block_on_doctor_warning() -> None:
    content = Path("run_mailbot.bat").read_text(encoding="utf-8")
    assert "doctor checks ^(warning-first^)" in content
    assert "Doctor found issues. Startup continues" in content
    assert "exit /b 1" not in content.split("Running doctor checks", 1)[1].split("Running config validation", 1)[0]


def test_run_mailbot_bat_uses_python_readiness_check_loop() -> None:
    content = Path("run_mailbot.bat").read_text(encoding="utf-8")
    assert "findstr /m \"CHANGE_ME\"" not in content
    assert content.count("-m mailbot_v26 config-ready") >= 2
    assert "setlocal enableextensions enabledelayedexpansion" in content
    assert ":CONFIG_READY_LOOP" in content
    assert "if !CONFIG_READY_ATTEMPTS! GTR 20" in content
    assert "Попытка !CONFIG_READY_ATTEMPTS! из 20" in content
    assert "exit /b 2" in content


def test_update_and_run_propagates_run_mailbot_exit_code() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert "set \"RUN_EXIT=%ERRORLEVEL%\"" in content
    assert "exit /b %RUN_EXIT%" in content
    assert "set \"RUN_EXIT=0\"" not in content


def test_install_and_run_calls_migrate_config() -> None:
    content = Path("install_and_run.bat").read_text(encoding="utf-8")
    assert "-m mailbot_v26 migrate-config" in content


def test_update_and_run_uses_safe_fetch_reset_and_warns_on_pip_failure() -> None:
    content = Path("update_and_run.bat").read_text(encoding="utf-8")
    assert "Git is not available in PATH. Continuing without update." in content
    assert "git fetch origin main" in content
    assert "git reset --hard origin/main" in content
    assert "git status --porcelain" in content
    assert "Рабочее дерево не чистое" in content
    assert "Dependency installation failed. Continuing" in content
