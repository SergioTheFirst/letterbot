from __future__ import annotations

import subprocess
import sys
from pathlib import Path


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


def test_run_stack_dry_run_uses_web_values_from_settings_ini(tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text(
        "[web]\nhost=0.0.0.0\nport=9321\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8"
    )

    result = _run_stack("web", "--dry-run", "--config-dir", str(tmp_path))

    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "--bind 0.0.0.0" in combined
    assert "--port 9321" in combined


def test_build_web_command_uses_config_directory_argument(tmp_path: Path) -> None:
    from mailbot_v26.tools.run_stack import _build_web_command

    cmd = _build_web_command(
        sys.executable,
        config_dir=tmp_path,
        db_path=tmp_path / "knowledge.db",
        bind="127.0.0.1",
        port=8787,
    )

    assert "--config" in cmd.args
    assert str(tmp_path.resolve()) in cmd.args


def test_web_module_does_not_exit_with_code_1_on_minimal_config_dir(
    tmp_path: Path,
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[web]\nhost=127.0.0.1\nport=0\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[telegram]\nbot_token=t\n", encoding="utf-8"
    )
    (tmp_path / "config.ini").write_text(
        "[general]\nweb_secret_key=test-secret\n", encoding="utf-8"
    )
    db_path = tmp_path / "mailbot.sqlite"
    db_path.touch()

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "mailbot_v26.web_observability.app",
            "--config",
            str(tmp_path),
            "--db",
            str(db_path),
            "--bind",
            "127.0.0.1",
            "--port",
            "0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        process.wait(timeout=1.5)
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=5)
    else:
        combined = (process.stdout.read() if process.stdout else "") + (
            process.stderr.read() if process.stderr else ""
        )
        assert process.returncode != 1, combined


def test_run_processes_reports_log_tail_on_child_failure(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    from mailbot_v26.tools import run_stack

    class DummyProcess:
        _next_pid = 100

        def __init__(self, _args, **_kwargs):
            self.args = _args
            self.pid = DummyProcess._next_pid
            DummyProcess._next_pid += 1
            self._calls = 0

        def poll(self):
            self._calls += 1
            return 1 if self._calls >= 1 else None

    log_file = tmp_path / "worker.log"
    log_file.write_text("line1\nline2\n", encoding="utf-8")
    monkeypatch.setattr(run_stack, "_prepare_log_path", lambda _name: log_file)
    monkeypatch.setattr(subprocess, "Popen", DummyProcess)

    code = run_stack._run_processes(
        [run_stack.StackCommand("worker", [sys.executable, "-m", "mailbot_v26"])],
        open_browser=False,
        web_url="http://127.0.0.1:8787/login",
        web_timeout=0.1,
    )

    out = capsys.readouterr().out
    assert code == 1
    assert "worker exited with code 1" in out
    assert "worker log:" in out
    assert "worker log tail" in out
    assert "line1" in out
    assert "line2" in out
