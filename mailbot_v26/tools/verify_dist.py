from __future__ import annotations

import argparse
from pathlib import Path

from mailbot_v26.integrity import verify_manifest

_REQUIRED_FILES: tuple[str, ...] = (
    "Letterbot.exe",
    "run.bat",
    "mailbot_v26/config/settings.ini.example",
    "mailbot_v26/config/accounts.ini.example",
    "README_QUICKSTART_WINDOWS.md",
    "manifest.sha256.json",
    "UPGRADE.md",
    "SMARTSCREEN.md",
    "CHANGELOG.md",
)


def verify_dist_contract(dist_dir: Path) -> tuple[bool, str]:
    root = Path(dist_dir)
    if not root.exists():
        return False, "dist\\Letterbot not found. Run build_windows_onefolder.bat."

    missing = [name for name in _REQUIRED_FILES if not (root / name).exists()]
    if missing:
        return False, f"missing required files: {', '.join(missing)}"

    ok, changed = verify_manifest(root, root / "manifest.sha256.json")
    if not ok:
        preview = ", ".join(changed[:5])
        suffix = " ..." if len(changed) > 5 else ""
        return False, f"manifest mismatch: {preview}{suffix}"

    return True, "dist\\Letterbot contract is valid and manifest status OK."


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify dist/Letterbot release contract"
    )
    parser.add_argument("dist_dir", nargs="?", default="dist/Letterbot")
    args = parser.parse_args()

    ok, details = verify_dist_contract(Path(args.dist_dir))
    if ok:
        print(f"VERIFY_DIST PASS: {details}")
        return 0
    print(f"VERIFY_DIST FAIL: {details}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
