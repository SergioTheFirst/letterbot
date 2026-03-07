from pathlib import Path


def test_open_config_folder_bat_points_to_repo_root() -> None:
    content = Path("open_config_folder.bat").read_text(encoding="utf-8", errors="replace")
    assert "mailbot_v26\\config" not in content
    assert "REPO_ROOT" in content


def test_backup_bat_references_letterbot_not_install_and_run() -> None:
    content = Path("backup.bat").read_text(encoding="utf-8", errors="replace")
    assert "install_and_run.bat" not in content
    assert "letterbot.bat" in content or ".venv\\Scripts\\python.exe" in content


def test_quickstart_does_not_mention_old_config_path() -> None:
    content = Path("README_QUICKSTART_WINDOWS.md").read_text(encoding="utf-8", errors="replace")
    start = content.find("### 1) Source mode")
    end = content.find("### 2)", start)
    source_mode = content[start:end] if start >= 0 and end > start else content
    assert "mailbot_v26\\config" not in source_mode
    assert "install_and_run.bat" not in content
