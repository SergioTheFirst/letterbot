from __future__ import annotations

from pathlib import Path

from mailbot_v26.dist_self_check import validate_dist_runtime


def test_validate_dist_runtime_skips_for_source_mode(tmp_path: Path) -> None:
    ok, error = validate_dist_runtime(frozen=False, executable_path=tmp_path / "python.exe")

    assert ok is True
    assert error is None


def test_validate_dist_runtime_flags_missing_files(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "MailBot"
    dist_dir.mkdir(parents=True)
    (dist_dir / "MailBot.exe").write_bytes(b"exe")

    ok, error = validate_dist_runtime(
        frozen=True,
        executable_path=dist_dir / "MailBot.exe",
    )

    assert ok is False
    assert error is not None
    assert "config.example.yaml" in error
    assert "manifest.sha256.json" in error


def test_validate_dist_runtime_passes_when_required_files_exist(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist" / "MailBot"
    dist_dir.mkdir(parents=True)
    (dist_dir / "MailBot.exe").write_bytes(b"exe")
    (dist_dir / "config.example.yaml").write_text("x", encoding="utf-8")
    (dist_dir / "manifest.sha256.json").write_text("{}", encoding="utf-8")

    ok, error = validate_dist_runtime(
        frozen=True,
        executable_path=dist_dir / "MailBot.exe",
    )

    assert ok is True
    assert error is None
