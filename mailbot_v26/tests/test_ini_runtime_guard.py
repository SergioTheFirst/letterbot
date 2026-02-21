from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "mailbot_v26" / "config" / "config.ini"


def _run_import_processor() -> subprocess.CompletedProcess[str]:
    script = "\n".join(
        [
            "import logging",
            "logging.basicConfig(level=logging.WARNING)",
            "import mailbot_v26.pipeline.processor",
            "print('IMPORT_OK')",
        ]
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_malformed_config_ini_warns_once_and_import_survives() -> None:
    original = CONFIG_PATH.read_text(encoding="utf-8") if CONFIG_PATH.exists() else None
    try:
        CONFIG_PATH.write_text("check_interval=120\n", encoding="utf-8")
        result = _run_import_processor()
    finally:
        if original is None:
            CONFIG_PATH.unlink(missing_ok=True)
        else:
            CONFIG_PATH.write_text(original, encoding="utf-8")

    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, combined
    assert "IMPORT_OK" in combined
    assert combined.count("config.ini is invalid") == 1
    assert "Template:" in combined
    assert "Windows command: copy" in combined
    assert "MissingSectionHeaderError" not in combined


def test_missing_config_ini_warns_once_and_import_survives() -> None:
    backup_path = CONFIG_PATH.with_suffix(".ini.bak.test")
    had_config = CONFIG_PATH.exists()
    if had_config:
        CONFIG_PATH.replace(backup_path)
    try:
        result = _run_import_processor()
    finally:
        if backup_path.exists():
            backup_path.replace(CONFIG_PATH)

    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, combined
    assert "IMPORT_OK" in combined
    assert combined.count("config.ini missing") == 1
    assert "Template:" in combined
    assert "Windows command: copy" in combined
