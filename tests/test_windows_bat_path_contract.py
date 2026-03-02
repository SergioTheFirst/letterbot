from pathlib import Path

BATS = [
    "letterbot.bat",
    "update_and_run.bat",
    "run_tests.bat",
    "backup.bat",
    "open_config_folder.bat",
    "build_windows_onefolder.bat",
]


def test_windows_bat_path_contracts_are_quoted() -> None:
    for bat in BATS:
        content = Path(bat).read_text(encoding="utf-8", errors="replace")
        lowered = content.lower()

        assert 'cd /d "' in lowered, f"{bat}: missing quoted cd /d"
        assert "cd /d %" not in lowered, f"{bat}: found unquoted cd /d"
        assert "if exist %" not in lowered, f"{bat}: found unquoted if exist"
        assert "if not exist %" not in lowered, f"{bat}: found unquoted if not exist"

    letterbot = Path("letterbot.bat").read_text(encoding="utf-8", errors="replace")
    update = Path("update_and_run.bat").read_text(encoding="utf-8", errors="replace")
    assert '--config-dir "%CONFIG_DIR%"' in letterbot
    assert '--config-dir "%CONFIG_DIR%"' in update
