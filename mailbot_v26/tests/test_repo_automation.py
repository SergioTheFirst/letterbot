from __future__ import annotations

import configparser
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
    # Plain pytest runs every pytest.ini testpath, so the root tests/ suite runs too.
    assert "python -m pytest -q --tb=short" in text
    assert "python -m pytest mailbot_v26/tests" not in text
    pytest_ini = configparser.ConfigParser()
    pytest_ini.read_string(_read("pytest.ini"))
    assert set(pytest_ini["pytest"]["testpaths"].split()) >= {"tests", "mailbot_v26/tests"}
    assert "python -m mailbot_v26.tools.eval_golden_corpus" in text
    assert "python -m mailbot_v26.tools.cleanup --status" in text


def test_ci_test_job_runs_pinned_ruff_gate_on_linux() -> None:
    test_job = _read(".github/workflows/ci.yml").split("build_windows:", 1)[0]

    assert "- name: Lint (ruff)" in test_job
    step = test_job.split("- name: Lint (ruff)", 1)[1].split("- name:", 1)[0]
    assert "if: runner.os == 'Linux'" in step
    assert 'python -m pip install "ruff==0.15.5"' in step
    assert (
        "ruff check mailbot_v26 tests conftest.py --select F821,F811,F841,B023,PLE"
        in step
    )
    # ruff is a dev-only CI tool, never a runtime dependency.
    assert "ruff" not in _read("requirements.txt").lower()


def test_release_smoke_workflow_exists() -> None:
    text = _read(".github/workflows/release-smoke.yml")

    assert "windows-latest" in text
    assert "python -m venv .venv" in text
    assert "requirements-build.txt" in text
    assert "build_windows_onefolder.bat" in text
    assert "verify_dist.bat" in text
    assert "dist\\\\Letterbot\\\\run.bat" in text or "dist\\Letterbot\\run.bat" in text
    assert "dist\\\\Letterbot-Setup.exe" in text or "dist\\Letterbot-Setup.exe" in text
    assert "Letterbot-windows-installer-smoke" in text


def test_ci_windows_artifacts_are_onefolder_zip_and_smoked_installer() -> None:
    text = _read(".github/workflows/ci.yml")

    assert "build_windows_onefolder.bat" in text
    assert "verify_dist.bat" in text
    assert "Compress-Archive dist/Letterbot dist/Letterbot.zip" in text
    assert "Letterbot-windows-onefolder" in text
    assert "dist\\\\Letterbot-Setup.exe" in text or "dist\\Letterbot-Setup.exe" in text
    assert "Letterbot-windows-installer" in text
    # The installer is proven end to end: quiet install, then verify the installed tree.
    assert '-ArgumentList "/Q" -Wait' in text
    assert "mailbot_v26.tools.verify_dist $target" in text
    assert "timeout-minutes" in text


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
        assert "Letterbot-Setup.exe" in text
        assert "install_and_run.bat" not in text
        assert "update_and_run.bat" not in text
        assert "MANIFEST.json" not in text
