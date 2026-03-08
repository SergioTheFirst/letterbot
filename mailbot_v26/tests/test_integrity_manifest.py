from __future__ import annotations

import json
from pathlib import Path

from mailbot_v26.integrity import (
    compute_manifest,
    manifest_ignore_paths,
    verify_manifest,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_compute_manifest_hashes_files(tmp_path: Path) -> None:
    file_a = tmp_path / "alpha.txt"
    file_b = tmp_path / "nested" / "beta.txt"
    _write(file_a, "hello")
    _write(file_b, "world")

    manifest = compute_manifest(tmp_path)

    assert "alpha.txt" in manifest
    assert "nested/beta.txt" in manifest
    assert manifest["alpha.txt"] != manifest["nested/beta.txt"]


def test_verify_manifest_ignores_runtime_mutable_files(tmp_path: Path) -> None:
    shipped = tmp_path / "Letterbot.exe"
    _write(shipped, "binary")

    manifest = compute_manifest(tmp_path)
    manifest_path = tmp_path / "manifest.sha256.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    for rel in manifest_ignore_paths():
        if rel == "manifest.sha256.json":
            continue
        _write(tmp_path / rel, "runtime")

    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert ok
    assert changed == []


def test_verify_manifest_flags_unexpected_extra_file(tmp_path: Path) -> None:
    payload = tmp_path / "payload.txt"
    _write(payload, "original")

    manifest = compute_manifest(tmp_path)
    manifest_path = tmp_path / "manifest.sha256.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    _write(tmp_path / "extra.bin", "x")

    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert not ok
    assert "extra.bin" in changed


def test_verify_manifest_detects_modified_shipped_file(tmp_path: Path) -> None:
    payload = tmp_path / "payload.txt"
    _write(payload, "original")

    manifest = compute_manifest(tmp_path)
    manifest_path = tmp_path / "manifest.sha256.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    _write(payload, "modified")

    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert not ok
    assert "payload.txt" in changed
