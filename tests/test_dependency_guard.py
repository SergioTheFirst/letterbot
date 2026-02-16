from __future__ import annotations

import subprocess
import sys

import pytest

from mailbot_v26 import deps


def test_require_runtime_for_reports_single_clean_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deps, "has", lambda _module: False)

    with pytest.raises(deps.DependencyError) as exc_info:
        deps.require_runtime_for("runtime")

    message = str(exc_info.value)
    assert message.startswith("Missing dependency: yaml")
    assert "Install: python -m pip install PyYAML" in message
    assert "install_and_run.bat" in message


def test_entrypoint_validate_config_fails_cleanly_without_yaml() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "mailbot_v26", "validate-config"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "Missing dependency: yaml" in result.stderr
    assert "Traceback" not in result.stderr
