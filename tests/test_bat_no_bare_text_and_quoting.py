from pathlib import Path


WRAPPERS = [
    "install_and_run.bat",
    "run_mailbot.bat",
    "start_mailbot.bat",
]

KEY_BATS = [
    "letterbot.bat",
    "update_and_run.bat",
    "run_tests.bat",
    "backup.bat",
    "run_dist.bat",
]


def _bat_files() -> list[Path]:
    return sorted(Path(".").rglob("*.bat"))


def test_no_bare_bracketed_text_in_bat_files() -> None:
    for bat in _bat_files():
        for raw_line in bat.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("::"):
                continue
            lowered = line.lower()
            if line.startswith("["):
                assert lowered.startswith("echo [") or lowered.startswith("rem ["), (
                    f"{bat}: bare bracket text -> {line}"
                )


def test_key_bats_have_quoted_cd_and_config_dir() -> None:
    for bat_path in KEY_BATS:
        content = Path(bat_path).read_text(encoding="utf-8", errors="replace")
        lowered = content.lower()
        assert 'cd /d "' in lowered, f"{bat_path}: missing quoted cd /d"

    for bat_path in ["letterbot.bat", "update_and_run.bat", "run_dist.bat"]:
        content = Path(bat_path).read_text(encoding="utf-8", errors="replace")
        assert '--config-dir "' in content, f"{bat_path}: missing quoted --config-dir"


def test_wrappers_delegate_to_letterbot_with_args() -> None:
    for bat_path in WRAPPERS:
        content = Path(bat_path).read_text(encoding="utf-8", errors="replace")
        assert 'call "%~dp0letterbot.bat" %*' in content, f"{bat_path}: no canonical delegation"
