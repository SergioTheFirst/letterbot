from __future__ import annotations

from pathlib import Path

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
        assert "run_dist.bat" in text
        for marker in legacy_markers:
            assert marker not in text
