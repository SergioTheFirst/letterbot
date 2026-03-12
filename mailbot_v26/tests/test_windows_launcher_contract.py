from __future__ import annotations

from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[2]


APPROVED_ROOT_BATS = {
    "letterbot.bat",
    "update_and_run.bat",
    "run_tests.bat",
    "run_dist.bat",
    "build_windows_onefolder.bat",
    "open_config_folder.bat",
    "backup.bat",
    "restore.bat",
    "verify_dist.bat",
}

LEGACY_BATS = {
    "install_and_run.bat",
    "run_mailbot.bat",
    "start_mailbot.bat",
    "update.bat",
    "ci_local.bat",
    "run_acceptance.bat",
    "run_maintenance_create_indexes.bat",
    "tools/smoke_check.bat",
    "mailbot_v26/run_mailbot.bat",
}


def test_root_bat_surface_is_approved_only() -> None:
    root_bats = {path.name for path in REPO_ROOT.glob("*.bat")}
    assert root_bats == APPROVED_ROOT_BATS


def test_legacy_launchers_are_absent() -> None:
    for rel in LEGACY_BATS:
        assert not (REPO_ROOT / rel).exists(), rel


def test_docs_do_not_reference_legacy_source_launchers() -> None:
    docs = [
        "README.md",
        "README_QUICKSTART_WINDOWS.md",
        "WINDOWS_QUICKSTART.md",
        "docs/ACCEPTANCE_CHECKLIST.md",
        "docs/SMOKE_TESTS_WINDOWS.md",
        "docs/WINDOWS_QUICKSTART.md",
    ]
    legacy_markers = ("install_and_run.bat", "run_mailbot.bat", "start_mailbot.bat")
    for rel in docs:
        text = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert "letterbot.bat" in text
        assert "run_dist.bat" in text or "run.bat" in text
        for marker in legacy_markers:
            assert marker not in text


def test_letterbot_bat_uses_repo_root_not_cwd() -> None:
    text = (REPO_ROOT / "letterbot.bat").read_text(encoding="utf-8")

    assert 'set "REPO_ROOT=%~dp0"' in text
    assert 'set "CONFIG_DIR=%REPO_ROOT%"' in text
    assert 'cd /d "%REPO_ROOT%"' in text
    assert 'start "" notepad "%CONFIG_DIR%\\accounts.ini"' in text

    required_commands = (
        '-m mailbot_v26 init-config --config-dir "%CONFIG_DIR%"',
        '-m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose',
        '-m mailbot_v26 doctor --config-dir "%CONFIG_DIR%"',
        '-m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"',
        '-m mailbot_v26.tools.run_stack --config-dir "%CONFIG_DIR%" --no-browser',
    )
    for marker in required_commands:
        assert marker in text


def test_letterbot_bat_error_paths_pause_before_exit() -> None:
    text = (REPO_ROOT / "letterbot.bat").read_text(encoding="utf-8")
    error_labels = (
        ":error_python_missing",
        ":error_python_version",
        ":error_venv_create",
        ":error_dependency_install",
        ":error_bootstrap_failed",
        ":error_config_not_ready",
        ":error_run_stack",
    )
    for label in error_labels:
        start_match = re.search(rf"(?m)^{re.escape(label)}\s*$", text)
        assert start_match is not None, label
        start = start_match.start()
        next_label_match = re.search(r"(?m)^:[A-Za-z0-9_]+\s*$", text[start + 1 :])
        end = start + 1 + next_label_match.start() if next_label_match else len(text)
        block = text[start:end]
        assert "pause" in block, label
        assert "exit /b" in block, label


def test_letterbot_bat_bootstrap_exits_after_opening_accounts_ini() -> None:
    text = (REPO_ROOT / "letterbot.bat").read_text(encoding="utf-8")
    start_match = re.search(r"(?m)^:bootstrap_config\s*$", text)
    end_match = re.search(r"(?m)^:error_python_missing\s*$", text)
    assert start_match is not None
    assert end_match is not None
    start = start_match.start()
    end = end_match.start()
    block = text[start:end]

    assert "call :open_accounts_ini" in block
    assert "call :pause_if_needed" in block
    assert "exit /b 2" in block


def test_letterbot_bat_is_ascii_safe() -> None:
    content = (REPO_ROOT / "letterbot.bat").read_bytes()
    assert all(byte < 128 for byte in content)


def test_letterbot_bat_uses_python_launcher_fallback_chain() -> None:
    text = (REPO_ROOT / "letterbot.bat").read_text(encoding="utf-8")
    assert "call :probe_python py -3.10" in text
    assert "call :probe_python py -3" in text
    assert "call :probe_python py" in text
    assert "call :probe_python python" in text
    assert 'set "REQ_STAMP=%VENV_DIR%\\.deps_ready"' in text
    assert 'fc /b "%REQ_FILE%" "%REQ_STAMP%" >nul' in text
    assert 'copy /Y "%REQ_FILE%" "%REQ_STAMP%" >nul' in text
    assert "Dependencies are up to date." in text
