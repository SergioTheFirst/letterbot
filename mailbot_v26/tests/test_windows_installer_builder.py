from __future__ import annotations

import json
import subprocess
import zipfile
from pathlib import Path

import pytest

from mailbot_v26.integrity import compute_manifest
from mailbot_v26.tools import build_windows_installer as installer


def _write_minimal_dist(dist_dir: Path) -> None:
    (dist_dir / "mailbot_v26" / "config").mkdir(parents=True, exist_ok=True)
    (dist_dir / "_internal" / "pkg-1.0.dist-info").mkdir(parents=True, exist_ok=True)
    (dist_dir / "_internal" / "other-2.0.dist-info").mkdir(parents=True, exist_ok=True)
    (dist_dir / "Letterbot.exe").write_bytes(b"exe")
    (dist_dir / "run.bat").write_text("@echo off\r\n", encoding="utf-8")
    (dist_dir / "mailbot_v26" / "config" / "settings.ini.example").write_text(
        "[general]\n",
        encoding="utf-8",
    )
    (dist_dir / "mailbot_v26" / "config" / "accounts.ini.example").write_text(
        "[example_account]\n",
        encoding="utf-8",
    )
    # Same base name in two folders: IExpress alone cannot carry this.
    (dist_dir / "_internal" / "pkg-1.0.dist-info" / "METADATA").write_text("a", encoding="utf-8")
    (dist_dir / "_internal" / "other-2.0.dist-info" / "METADATA").write_text("b", encoding="utf-8")
    (dist_dir / "README_QUICKSTART_WINDOWS.md").write_text("quickstart", encoding="utf-8")
    (dist_dir / "UPGRADE.md").write_text("upgrade", encoding="utf-8")
    (dist_dir / "SMARTSCREEN.md").write_text("smartscreen", encoding="utf-8")
    (dist_dir / "CHANGELOG.md").write_text("changelog", encoding="utf-8")
    manifest = compute_manifest(dist_dir)
    (dist_dir / "manifest.sha256.json").write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="utf-8",
    )


def _write_runtime_files(dist_dir: Path) -> None:
    """Files a smoke run creates; verify_dist ignores them as runtime-mutable."""
    config = dist_dir / "mailbot_v26" / "config"
    (config / "accounts.ini").write_text("[work]\npassword = secret\n", encoding="utf-8")
    (config / "settings.ini").write_text("[telegram]\nbot_token = 123:abc\n", encoding="utf-8")
    (dist_dir / "database.sqlite").write_bytes(b"SQLite format 3\x00")


def test_stage_installer_layout_builds_flat_payload_and_install_cmd(
    tmp_path: Path,
) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    _write_minimal_dist(dist_dir)

    stage_dir = installer.stage_installer_layout(dist_dir, tmp_path / "build")

    assert sorted(p.name for p in stage_dir.iterdir()) == ["install.cmd", "payload.zip"]
    with zipfile.ZipFile(stage_dir / "payload.zip") as archive:
        names = set(archive.namelist())
    assert "Letterbot.exe" in names
    assert "manifest.sha256.json" in names
    assert "_internal/pkg-1.0.dist-info/METADATA" in names
    assert "_internal/other-2.0.dist-info/METADATA" in names


def test_payload_never_contains_runtime_config_or_database(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    _write_minimal_dist(dist_dir)
    _write_runtime_files(dist_dir)

    stage_dir = installer.stage_installer_layout(dist_dir, tmp_path / "build")

    with zipfile.ZipFile(stage_dir / "payload.zip") as archive:
        names = set(archive.namelist())
    assert "mailbot_v26/config/accounts.ini" not in names
    assert "mailbot_v26/config/settings.ini" not in names
    assert "database.sqlite" not in names
    _members, excluded = installer.payload_members(dist_dir)
    assert "mailbot_v26/config/accounts.ini" in excluded
    assert "database.sqlite" in excluded


def test_install_cmd_is_ascii_has_no_blocks_and_respects_quiet(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    _write_minimal_dist(dist_dir)
    stage_dir = installer.stage_installer_layout(dist_dir, tmp_path / "build")

    raw = (stage_dir / "install.cmd").read_bytes()
    raw.decode("ascii")
    script = raw.decode("ascii")
    assert "\r\n" in script
    assert "(" not in script.replace("(x86)", "")
    assert "%LOCALAPPDATA%\\Letterbot" in script
    assert '"%SYS32%\\tar.exe" -xf "%PAYLOAD%" -C "%INSTALL_ROOT%"' in script
    assert '"%SYS32%\\find.exe" /I "Letterbot.exe"' in script
    assert "Expand-Archive" in script
    assert "robocopy" not in script
    assert 'IMAGENAME eq Letterbot.exe' in script
    lines = script.splitlines()
    quiet_exit = lines.index('if "%QUIET%"=="1" exit /b 0')
    start_line = next(i for i, line in enumerate(lines) if line.startswith('start ""'))
    assert quiet_exit < start_line
    assert 'if "%QUIET%"=="0" pause' in script


@pytest.mark.parametrize(
    "bad",
    [
        'C:\\a"b',
        "C:\\a&b",
        "C:\\a|b",
        "C:\\a<b",
        "",
        "D:\\Программы\\Letterbot",
        "Letterbot",
        "C:\\Apps\\..\\Windows",
        "C:\\Apps\\100%\\Letterbot",
    ],
)
def test_install_dir_with_unsafe_characters_is_rejected(bad: str) -> None:
    with pytest.raises(installer.InstallerBuildError):
        installer.normalize_install_dir(bad)


def test_install_dir_trailing_backslash_is_trimmed() -> None:
    assert installer.normalize_install_dir("D:\\Apps\\Letterbot\\") == "D:\\Apps\\Letterbot"


def test_stage_installer_layout_rejects_invalid_dist_contract(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    dist_dir.mkdir(parents=True)

    with pytest.raises(installer.InstallerBuildError):
        installer.stage_installer_layout(dist_dir, tmp_path / "build")


def test_write_iexpress_sed_lists_only_flat_files_and_visible_window(
    tmp_path: Path,
) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    _write_minimal_dist(dist_dir)
    stage_dir = installer.stage_installer_layout(dist_dir, tmp_path / "build")

    sed_path = installer.write_iexpress_sed(
        stage_dir,
        tmp_path / "dist" / "Letterbot-Setup.exe",
    )

    sed = sed_path.read_bytes().decode(installer._sed_encoding())
    assert "InsideCompressed=1" in sed
    assert "ShowInstallProgramWindow=0" in sed
    assert "AppLaunched=cmd.exe /d /c install.cmd" in sed
    assert 'FILE0="install.cmd"' in sed
    assert 'FILE1="payload.zip"' in sed
    assert "FILE2=" not in sed
    assert "\\Letterbot.exe" not in sed
    source_line = next(line for line in sed.splitlines() if line.startswith("SourceFiles0="))
    assert source_line.endswith("\\")
    assert any(
        line.startswith("TargetName=") and line.endswith("Letterbot-Setup.exe")
        for line in sed.splitlines()
    )


def test_build_windows_installer_invokes_iexpress_and_creates_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    output_path = tmp_path / "dist" / "Letterbot-Setup.exe"
    _write_minimal_dist(dist_dir)

    monkeypatch.setattr(installer.shutil, "which", lambda _name: r"C:\Windows\System32\iexpress.exe")
    seen: dict[str, object] = {}

    def fake_run(args, capture_output, text, check, timeout):
        seen["timeout"] = timeout
        output_path.write_bytes(b"setup")

        class _Completed:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Completed()

    monkeypatch.setattr(installer.subprocess, "run", fake_run)

    built = installer.build_windows_installer(
        dist_dir=dist_dir,
        output_path=output_path,
        work_dir=tmp_path / "build",
    )

    assert built == output_path
    assert output_path.read_bytes() == b"setup"
    assert seen["timeout"] == installer.IEXPRESS_TIMEOUT_SECONDS


def test_build_windows_installer_timeout_is_a_build_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    _write_minimal_dist(dist_dir)
    monkeypatch.setattr(installer.shutil, "which", lambda _name: r"C:\Windows\System32\iexpress.exe")

    def hanging_run(args, capture_output, text, check, timeout):
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    monkeypatch.setattr(installer.subprocess, "run", hanging_run)

    with pytest.raises(installer.InstallerBuildError, match="did not finish"):
        installer.build_windows_installer(
            dist_dir=dist_dir,
            output_path=tmp_path / "dist" / "Letterbot-Setup.exe",
            work_dir=tmp_path / "build",
        )


@pytest.mark.parametrize(
    "rel", ["mailbot_v26/config/accounts.ini", "data/mailbot.sqlite", "a\\b.txt"]
)
def test_payload_rejects_runtime_or_unsafe_manifest_entries(tmp_path: Path, rel: str) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    _write_minimal_dist(dist_dir)
    target = dist_dir / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x", encoding="utf-8")
    manifest = json.loads((dist_dir / "manifest.sha256.json").read_text(encoding="utf-8"))
    manifest[rel] = "0" * 64
    (dist_dir / "manifest.sha256.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(installer.InstallerBuildError):
        installer.payload_members(dist_dir)


def test_env_rooted_install_dir_is_accepted() -> None:
    assert (
        installer.normalize_install_dir("%LOCALAPPDATA%\\Letterbot")
        == "%LOCALAPPDATA%\\Letterbot"
    )
