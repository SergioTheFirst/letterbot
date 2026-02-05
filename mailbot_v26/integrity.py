"""Integrity checks for tamper-evident builds."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable


def _hash_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    sha256 = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_manifest(root_dir: Path) -> dict[str, str]:
    root_dir = Path(root_dir)
    manifest: dict[str, str] = {}
    for path in sorted(root_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root_dir).as_posix()
        manifest[rel] = _hash_file(path)
    return manifest


def _normalize_ignore_paths(root_dir: Path, manifest_path: Path) -> set[str]:
    ignored = {"config.yaml", "manifest.sha256.json"}
    try:
        ignored.add(manifest_path.relative_to(root_dir).as_posix())
    except ValueError:
        pass
    return ignored


def _load_manifest(manifest_path: Path) -> dict[str, str]:
    manifest_raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest_raw, dict):
        raise ValueError("manifest.sha256.json must contain a JSON object")
    manifest: dict[str, str] = {}
    for key, value in manifest_raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("manifest.sha256.json must map strings to strings")
        manifest[key] = value
    return manifest


def _collect_changed(
    expected: dict[str, str],
    current: dict[str, str],
    ignored: Iterable[str],
) -> list[str]:
    ignored_set = set(ignored)
    changed: list[str] = []
    for rel, expected_hash in expected.items():
        current_hash = current.pop(rel, None)
        if current_hash is None or current_hash != expected_hash:
            changed.append(rel)
    for extra in sorted(current.keys()):
        if extra in ignored_set:
            continue
        changed.append(extra)
    return changed


def verify_manifest(root_dir: Path, manifest_path: Path) -> tuple[bool, list[str]]:
    root_dir = Path(root_dir)
    manifest_path = Path(manifest_path)
    expected = _load_manifest(manifest_path)
    current = compute_manifest(root_dir)
    ignored = _normalize_ignore_paths(root_dir, manifest_path)
    for ignore in ignored:
        current.pop(ignore, None)
    changed = _collect_changed(expected, current, ignored)
    return not changed, changed
