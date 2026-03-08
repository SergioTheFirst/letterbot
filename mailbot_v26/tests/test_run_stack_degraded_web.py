from __future__ import annotations

import sys

from mailbot_v26.tools import run_stack


def test_web_bind_error_is_non_fatal(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        run_stack,
        "build_commands",
        lambda **_kwargs: [
            run_stack.StackCommand("worker", ["python", "-m", "mailbot_v26"]),
            run_stack.StackCommand(
                "web", ["python", "-m", "mailbot_v26.web_observability.app"]
            ),
        ],
    )
    monkeypatch.setattr(run_stack, "_is_port_busy", lambda _host, _port: True)

    captured = {}

    def _fake_run_processes(commands, **_kwargs):
        captured["names"] = [command.name for command in commands]
        return 0

    monkeypatch.setattr(run_stack, "_run_processes", _fake_run_processes)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_stack", "all", "--bind", "127.0.0.1", "--port", "8787", "--no-browser"],
    )

    code = run_stack.main()

    out = capsys.readouterr().out
    assert code == 0
    assert captured["names"] == ["worker"]
    assert "DEGRADED_NO_WEB" in out
    assert "settings.ini" in out
