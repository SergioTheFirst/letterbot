from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from pathlib import Path

from mailbot_v26.version import __version__, get_version
from mailbot_v26.web_observability.doctor_export import build_diagnostics_zip
from mailbot_v26.web_observability.flask_stub import render_template


def _create_min_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS events_v1 (id INTEGER PRIMARY KEY)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS system_health_snapshots (payload_json TEXT, ts_utc TEXT)"
        )


def test_web_base_template_renders_version_footer() -> None:
    template_path = Path("mailbot_v26/web_observability/templates/base.html")
    rendered = render_template(str(template_path), app_version=__version__)
    assert f"Letterbot v{get_version()}" in rendered


def test_doctor_diagnostics_metadata_contains_version(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    _create_min_db(db_path)
    config_path = tmp_path / "config.yaml"
    config_path.write_text("web_ui:\n  enabled: true\n", encoding="utf-8")
    log_path = tmp_path / "mailbot.log"
    log_path.write_text("line-1\n", encoding="utf-8")
    dist_root = tmp_path / "dist"
    dist_root.mkdir()
    (dist_root / "manifest.sha256.json").write_text(json.dumps({}, sort_keys=True), encoding="utf-8")

    archive_bytes = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=db_path,
        dist_root=dist_root,
    )

    with zipfile.ZipFile(io.BytesIO(archive_bytes), mode="r") as archive:
        metadata_text = archive.read("metadata.txt").decode("utf-8")
        runtime_text = archive.read("versions/runtime.txt").decode("utf-8")

    assert f"app_version={__version__}" in metadata_text
    assert f"app_version={__version__}" in runtime_text
