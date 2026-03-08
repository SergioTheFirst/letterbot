import runpy
import sys

import mailbot_v26.deps
import mailbot_v26.start


def test_module_entrypoint_runs_start(monkeypatch):
    called = {}

    def fake_main(config_dir=None):
        called["config_dir"] = config_dir

    monkeypatch.setattr(mailbot_v26.start, "main", fake_main)
    monkeypatch.setattr(
        mailbot_v26.deps, "require_runtime_for", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(sys, "argv", ["mailbot_v26"])

    runpy.run_module("mailbot_v26", run_name="__main__")

    assert str(called["config_dir"]) == "."


def test_module_entrypoint_config_ready_command(monkeypatch):
    called = {}

    def fake_run_config_ready(config_dir, verbose=False):
        called["config_dir"] = config_dir
        called["verbose"] = verbose
        return 0

    monkeypatch.setattr(
        mailbot_v26.deps, "require_runtime_for", lambda *_args, **_kwargs: None
    )
    import mailbot_v26.tools.config_bootstrap as config_bootstrap

    monkeypatch.setattr(config_bootstrap, "run_config_ready", fake_run_config_ready)
    monkeypatch.setattr(
        sys, "argv", ["mailbot_v26", "config-ready", "--config-dir", "x", "--verbose"]
    )

    try:
        runpy.run_module("mailbot_v26", run_name="__main__")
    except SystemExit as exc:
        assert exc.code == 0

    assert str(called["config_dir"]) == "x"
    assert called["verbose"] is True
