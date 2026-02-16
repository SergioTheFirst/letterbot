import runpy
import sys

import mailbot_v26.deps
import mailbot_v26.start


def test_module_entrypoint_runs_start(monkeypatch):
    called = {}

    def fake_main(config_dir=None):
        called["config_dir"] = config_dir

    monkeypatch.setattr(mailbot_v26.start, "main", fake_main)
    monkeypatch.setattr(mailbot_v26.deps, "require_runtime_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sys, "argv", ["mailbot_v26"])

    runpy.run_module("mailbot_v26", run_name="__main__")

    assert called["config_dir"] is None
