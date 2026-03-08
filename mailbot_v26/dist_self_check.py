"""Runtime checks for frozen one-folder distribution."""

from __future__ import annotations

from pathlib import Path

_REQUIRED_DIST_FILES: tuple[str, ...] = (
    "Letterbot.exe",
    "mailbot_v26/config/settings.ini.example",
    "mailbot_v26/config/accounts.ini.example",
    "manifest.sha256.json",
)


def is_dist_mode(*, frozen: bool) -> bool:
    return frozen


def find_missing_dist_files(dist_root: Path) -> list[str]:
    missing: list[str] = []
    for rel_path in _REQUIRED_DIST_FILES:
        if not (dist_root / rel_path).exists():
            missing.append(rel_path)
    return missing


def validate_dist_runtime(
    *, frozen: bool, executable_path: Path
) -> tuple[bool, str | None]:
    if not is_dist_mode(frozen=frozen):
        return True, None

    dist_root = Path(executable_path).resolve().parent
    missing = find_missing_dist_files(dist_root)
    if not missing:
        return True, None

    missing_list = ", ".join(missing)
    return (
        False,
        "[ERROR] Dist package is incomplete. Missing: "
        f"{missing_list}. Re-extract full Letterbot ZIP and run run.bat.",
    )
