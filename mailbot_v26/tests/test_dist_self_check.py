from __future__ import annotations

from pathlib import Path

from mailbot_v26.dist_self_check import validate_dist_runtime


def test_validate_dist_runtime_skips_for_source_mode(tmp_path: Path) -> None:
    ok, error = validate_dist_runtime(frozen=False, executable_path=tmp_path / "python.exe")

    assert ok is True
    assert error is None


def test_validate_dist_runtime_flags_missing_files(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    (dist_dir / "mailbot_v26" / "config").mkdir(parents=True)
    (dist_dir / "Letterbot.exe").write_bytes(b"exe")

    ok, error = validate_dist_runtime(
        frozen=True,
        executable_path=dist_dir / "Letterbot.exe",
    )

    assert ok is False
    assert error is not None
    assert "mailbot_v26/config/settings.ini.example" in error
    assert "manifest.sha256.json" in error


def test_validate_dist_runtime_passes_when_required_files_exist(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "Letterbot"
    (dist_dir / "mailbot_v26" / "config").mkdir(parents=True)
    (dist_dir / "Letterbot.exe").write_bytes(b"exe")
    (dist_dir / "mailbot_v26" / "config" / "settings.ini.example").write_text("x", encoding="utf-8")
    (dist_dir / "mailbot_v26" / "config" / "accounts.ini.example").write_text("x", encoding="utf-8")
    (dist_dir / "manifest.sha256.json").write_text("{}", encoding="utf-8")

    ok, error = validate_dist_runtime(
        frozen=True,
        executable_path=dist_dir / "Letterbot.exe",
    )

    assert ok is True
    assert error is None
