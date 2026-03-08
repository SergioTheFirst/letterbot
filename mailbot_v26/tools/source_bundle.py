from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

EXCLUDED_TOP_LEVEL_DIRS = {
    ".git",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "data",
    "dist",
    "logs",
    "runtime",
}
EXCLUDED_DIR_NAMES = {
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
}
EXCLUDED_SUFFIXES = {
    ".db",
    ".log",
    ".pyc",
    ".pyo",
    ".sqlite",
    ".sqlite3",
}
EXCLUDED_FILENAMES = {
    "accounts.ini",
    "settings.ini",
}


def _is_excluded(path: Path, root: Path, output_zip: Path) -> bool:
    if path == output_zip:
        return True
    rel = path.relative_to(root)
    rel_posix = rel.as_posix()
    if not rel.parts:
        return False
    if rel.parts[0] in EXCLUDED_TOP_LEVEL_DIRS:
        return True
    if any(part in EXCLUDED_DIR_NAMES for part in rel.parts):
        return True
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    if path.name in EXCLUDED_FILENAMES:
        return True
    if rel_posix.startswith("runtime/tmp_") and path.suffix.lower() == ".ps1":
        return True
    if path.name.endswith(".local.ini") or path.name.endswith(".local.yaml"):
        return True
    if path.name == "config.local.yaml":
        return True
    return False


def build_source_bundle(output_zip: Path, repo_root: Path | None = None) -> Path:
    root = (repo_root or Path.cwd()).resolve()
    out = output_zip.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if _is_excluded(path, root, out):
            continue
        files.append(path)
    files.sort(key=lambda p: p.relative_to(root).as_posix())

    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in files:
            archive.write(file_path, file_path.relative_to(root).as_posix())
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build clean Letterbot source bundle without runtime artifacts."
    )
    parser.add_argument(
        "--output",
        default="dist/letterbot-source.zip",
        help="Output ZIP path (default: dist/letterbot-source.zip)",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root path (default: current directory)",
    )
    args = parser.parse_args()

    output = Path(args.output)
    root = Path(args.repo_root)
    built = build_source_bundle(output, repo_root=root)
    print(f"Created source bundle: {built}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
