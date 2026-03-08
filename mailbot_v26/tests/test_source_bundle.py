from __future__ import annotations

import zipfile
from pathlib import Path

from mailbot_v26.tools.source_bundle import build_source_bundle


def _touch(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _zip_names(path: Path) -> set[str]:
    with zipfile.ZipFile(path, "r") as archive:
        return set(archive.namelist())


def test_source_bundle_excludes_runtime_artifacts(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _touch(repo / "mailbot_v26" / "start.py", "print('ok')\n")
    _touch(repo / "README.md", "# Letterbot\n")

    _touch(repo / "database.sqlite")
    _touch(repo / "data" / "mailbot.sqlite")
    _touch(repo / "logs" / "decision_trace_failures.ndjson")
    _touch(repo / "mailbot_v26" / "mailbot.log")
    _touch(repo / ".pytest_cache" / "v" / "cache" / "nodeids")
    _touch(repo / ".ruff_cache" / "cache")
    _touch(repo / "mailbot_v26" / "__pycache__" / "start.cpython-310.pyc")
    _touch(repo / "runtime" / "tmp_fix.ps1")
    _touch(repo / "mailbot_v26" / "config" / "settings.ini")
    _touch(repo / "mailbot_v26" / "config" / "accounts.ini")
    _touch(repo / "config.local.yaml")
    _touch(repo / "mailbot_v26" / "config" / "settings.local.ini")

    output = repo / "dist" / "letterbot-source.zip"
    build_source_bundle(output, repo_root=repo)
    names = _zip_names(output)

    assert "mailbot_v26/start.py" in names
    assert "README.md" in names
    assert "database.sqlite" not in names
    assert "data/mailbot.sqlite" not in names
    assert "logs/decision_trace_failures.ndjson" not in names
    assert "mailbot_v26/mailbot.log" not in names
    assert ".pytest_cache/v/cache/nodeids" not in names
    assert ".ruff_cache/cache" not in names
    assert "mailbot_v26/__pycache__/start.cpython-310.pyc" not in names
    assert "runtime/tmp_fix.ps1" not in names
    assert "mailbot_v26/config/settings.ini" not in names
    assert "mailbot_v26/config/accounts.ini" not in names
    assert "config.local.yaml" not in names
    assert "mailbot_v26/config/settings.local.ini" not in names


def test_source_bundle_includes_required_docs_examples_and_templates(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _touch(repo / "docs" / "RELEASE_ARTIFACT_CONTRACT.md")
    _touch(repo / "examples" / "sample.txt")
    _touch(repo / "mailbot_v26" / "config" / "settings.ini.example")
    _touch(repo / "mailbot_v26" / "config" / "accounts.ini.example")
    _touch(repo / "mailbot_v26" / "web_observability" / "static" / "style.css")
    _touch(repo / "mailbot_v26" / "tests" / "test_smoke.py", "def test_ok(): pass\n")

    output = repo / "dist" / "letterbot-source.zip"
    build_source_bundle(output, repo_root=repo)
    names = _zip_names(output)

    assert "docs/RELEASE_ARTIFACT_CONTRACT.md" in names
    assert "examples/sample.txt" in names
    assert "mailbot_v26/config/settings.ini.example" in names
    assert "mailbot_v26/config/accounts.ini.example" in names
    assert "mailbot_v26/web_observability/static/style.css" in names
    assert "mailbot_v26/tests/test_smoke.py" in names
