from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows launcher smoke test")

# Letterbot.exe stand-in: config-ready reports "not filled" (exit 2) while the
# accounts.ini still holds the CHANGE_ME placeholder, everything else succeeds.
_FAKE_EXE = "\r\n".join(
    [
        "@echo off",
        'if /i "%~1"=="config-ready" goto :ready',
        'if /i "%~1"=="doctor" exit /b 0',
        "echo FAKE_START",
        "exit /b 0",
        ":ready",
        '>>"%~dp0calls.log" echo config-ready',
        'findstr /c:"CHANGE_ME" "%~dp0mailbot_v26\\config\\accounts.ini" >nul',
        "if errorlevel 1 exit /b 0",
        "exit /b 2",
        "",
    ]
)

# Notepad stand-in: the user fills the account and closes the editor.
_FAKE_NOTEPAD = "\r\n".join(
    [
        "@echo off",
        '>>"%~dp0calls.log" echo notepad',
        '>"%~1" echo [work]',
        '>>"%~1" echo password = filled',
        "exit /b 0",
        "",
    ]
)


def _launcher_copy(text: str) -> str:
    """Point run_dist.bat at .cmd fakes; `call` is needed to return from a .cmd."""
    replacements = (
        ("\\Letterbot.exe\"", "\\Letterbot.cmd\""),
        ('start /wait notepad.exe "%ACCOUNTS_FILE%"',
         'call "%REPO_ROOT%\\fake_notepad.cmd" "%ACCOUNTS_FILE%"'),
    )
    for old, new in replacements:
        assert old in text, old
        text = text.replace(old, new)
    lines = []
    for line in text.split("\r\n"):
        if line.lstrip().startswith('"%EXE_PATH%" '):
            line = line.replace('"%EXE_PATH%" ', 'call "%EXE_PATH%" ', 1)
        lines.append(line)
    return "\r\n".join(lines)


def _build_dist(root: Path) -> Path:
    config_dir = root / "mailbot_v26" / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "settings.ini.example").write_text("[general]\n", encoding="ascii")
    (config_dir / "accounts.ini.example").write_text(
        "[work]\npassword = CHANGE_ME\n", encoding="ascii"
    )
    (root / "Letterbot.cmd").write_bytes(_FAKE_EXE.encode("ascii"))
    (root / "fake_notepad.cmd").write_bytes(_FAKE_NOTEPAD.encode("ascii"))
    source = (REPO_ROOT / "run_dist.bat").read_bytes().decode("utf-8")
    run_bat = root / "run.bat"
    run_bat.write_bytes(_launcher_copy(source).encode("utf-8"))
    return run_bat


def _run(run_bat: Path) -> subprocess.CompletedProcess[str]:
    cmd = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "cmd.exe"
    return subprocess.run(
        [str(cmd), "/d", "/c", str(run_bat)],
        cwd=str(run_bat.parent.parent),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=120,
        check=False,
        encoding="utf-8",
        errors="replace",
    )


@pytest.mark.parametrize("folder", ["Letterbot", "Letter bot!x"])
def test_run_dist_bat_starts_after_user_fills_config_once(
    tmp_path: Path, folder: str
) -> None:
    run_bat = _build_dist(tmp_path / folder)

    result = _run(run_bat)

    calls = (run_bat.parent / "calls.log").read_text(encoding="ascii").split()
    assert calls == ["config-ready", "notepad", "config-ready"], result.stdout
    assert "FAKE_START" in result.stdout, result.stdout
    assert result.returncode == 0, result.stdout
