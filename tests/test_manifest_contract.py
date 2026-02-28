from __future__ import annotations

import json
from pathlib import Path

from mailbot_v26.version import __version__


def test_manifest_json_is_valid_and_version_matches() -> None:
    payload = json.loads(Path("MANIFEST.json").read_text(encoding="utf-8"))

    assert isinstance(payload, dict)
    assert payload["version"] == __version__
    assert isinstance(payload.get("files"), list)
    assert len(payload["files"]) >= 6
    for item in payload["files"]:
        assert isinstance(item.get("path"), str)
        assert isinstance(item.get("sha256"), str)
        assert len(item["sha256"]) == 64
