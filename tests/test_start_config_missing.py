from __future__ import annotations

import subprocess
import sys


def test_missing_config_path_exits_cleanly() -> None:
    script = "\n".join(
        [
            "from pathlib import Path",
            "from mailbot_v26.start import _load_yaml_config_or_exit",
            "_load_yaml_config_or_exit(Path('missing_config.yaml'))",
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "Traceback" not in combined
