from __future__ import annotations

import json
from pathlib import Path

from mailbot_v26.integrity import compute_manifest, verify_manifest


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_compute_manifest_hashes_files(tmp_path: Path) -> None:
    file_a = tmp_path / "alpha.txt"
    file_b = tmp_path / "nested" / "beta.txt"
    file_b.parent.mkdir(parents=True)
    _write(file_a, "hello")
    _write(file_b, "world")

    manifest = compute_manifest(tmp_path)

    assert "alpha.txt" in manifest
    assert "nested/beta.txt" in manifest
    assert manifest["alpha.txt"] != manifest["nested/beta.txt"]


def test_verify_manifest_detects_changes_and_ignores_config(tmp_path: Path) -> None:
    payload = tmp_path / "payload.txt"
    _write(payload, "original")

    manifest = compute_manifest(tmp_path)
    manifest_path = tmp_path / "manifest.sha256.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert ok
    assert changed == []

    _write(payload, "modified")
    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert not ok
    assert "payload.txt" in changed

    _write(tmp_path / "config.yaml", "user config")
    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert not ok
    assert "config.yaml" not in changed


def test_verify_manifest_flags_extra_files(tmp_path: Path) -> None:
    payload = tmp_path / "payload.txt"
    _write(payload, "original")

    manifest = compute_manifest(tmp_path)
    manifest_path = tmp_path / "manifest.sha256.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    extra = tmp_path / "extra.bin"
    extra.write_bytes(b"x")

    ok, changed = verify_manifest(tmp_path, manifest_path)
    assert not ok
    assert "extra.bin" in changed
