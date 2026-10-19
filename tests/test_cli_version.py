import re
import runpy
import sys

import pytest

from mailbot_v26.version import __version__

pytestmark = pytest.mark.filterwarnings(
    r"error:.*found in sys\.modules after import of package:RuntimeWarning"
)


@pytest.fixture(autouse=True)
def _drop_imported_main_module(monkeypatch):
    # Other tests import mailbot_v26.__main__; runpy warns when it is already loaded.
    monkeypatch.delitem(sys.modules, "mailbot_v26.__main__", raising=False)


def test_version_module_semver_format() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+(?:-rc(?:\.?\d+)?)?", __version__)


def test_cli_version_prints_version_flag(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mailbot_v26", "--version"])
    runpy.run_module("mailbot_v26", run_name="__main__")
    out = capsys.readouterr().out.strip()
    assert __version__ in out


def test_cli_version_prints_version_command(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mailbot_v26", "version"])
    runpy.run_module("mailbot_v26", run_name="__main__")
    out = capsys.readouterr().out.strip()
    assert __version__ in out
