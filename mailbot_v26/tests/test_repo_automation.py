from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_ci_workflow_exists_and_runs_quality_gates() -> None:
    text = _read(".github/workflows/ci.yml")

    assert "ubuntu-latest" in text
    assert "windows-latest" in text
    assert 'python-version: "3.10"' in text or "matrix.python-version" in text
    assert "python -m compileall mailbot_v26 -q" in text
    assert "python -m pytest mailbot_v26/tests/ -q --tb=short" in text
    assert "python -m mailbot_v26.tools.eval_golden_corpus" in text
    assert "python -m mailbot_v26.tools.cleanup --status" in text


def test_release_smoke_workflow_exists() -> None:
    text = _read(".github/workflows/release-smoke.yml")

    assert "windows-latest" in text
    assert "python -m venv .venv" in text
    assert "requirements-build.txt" in text
    assert "build_windows_onefolder.bat" in text
    assert "verify_dist.bat" in text
    assert "dist\\\\Letterbot\\\\run.bat" in text or "dist\\Letterbot\\run.bat" in text


def test_dependabot_config_exists_and_covers_pip_and_actions() -> None:
    text = _read(".github/dependabot.yml")

    assert 'package-ecosystem: "pip"' in text
    assert 'package-ecosystem: "github-actions"' in text
    assert 'interval: "weekly"' in text


def test_codeql_workflow_exists_for_python() -> None:
    text = _read(".github/workflows/codeql.yml")

    assert "github/codeql-action/init@v3" in text
    assert "languages: python" in text
    assert "github/codeql-action/analyze@v3" in text


def test_scorecards_workflow_exists() -> None:
    text = _read(".github/workflows/scorecards.yml")

    assert "ossf/scorecard-action" in text
    assert "upload-sarif" in text
    assert "publish_results: true" in text


def test_security_md_exists_and_has_reporting_guidance() -> None:
    text = _read("SECURITY.md")

    assert "Reporting a vulnerability" in text
    assert "Do not open a public GitHub issue" in text
    assert "Rotate or revoke any real leaked credential" in text


def test_release_docs_reference_current_windows_contract() -> None:
    docs = [
        "RELEASE_ARTIFACT.md",
        "docs/RELEASE_ARTIFACT_CONTRACT.md",
        "docs/RELEASE_CHECKLIST_WINDOWS.md",
        "README_QUICKSTART_WINDOWS.md",
        "docs/PRODUCTION_GATES.md",
    ]

    for rel in docs:
        text = _read(rel)
        assert "run.bat" in text
        assert "Letterbot.exe" in text
        assert "manifest.sha256.json" in text
        assert "install_and_run.bat" not in text
        assert "update_and_run.bat" not in text
        assert "MANIFEST.json" not in text
