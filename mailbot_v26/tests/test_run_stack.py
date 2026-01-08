from __future__ import annotations

import subprocess
import sys


def _run_stack(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "mailbot_v26.tools.run_stack", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_run_stack_help() -> None:
    result = _run_stack("--help")
    assert result.returncode == 0


def test_run_stack_dry_run_all() -> None:
    result = _run_stack("--dry-run")
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "worker:" in combined
    assert "web:" in combined
    assert "mailbot_v26" in combined
    assert "mailbot_v26.web_observability.app" in combined


def test_run_stack_dry_run_worker_only() -> None:
    result = _run_stack("worker", "--dry-run")
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "worker:" in combined
    assert "web:" not in combined


def test_run_stack_dry_run_web_only() -> None:
    result = _run_stack("web", "--dry-run")
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "web:" in combined
    assert "worker:" not in combined
