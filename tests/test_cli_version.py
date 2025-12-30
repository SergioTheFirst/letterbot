import runpy
import sys

from mailbot_v26.version import __version__


def test_cli_version_prints_version(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mailbot_v26", "--version"])
    runpy.run_module("mailbot_v26", run_name="__main__")
    out = capsys.readouterr().out.strip()
    assert __version__ in out
