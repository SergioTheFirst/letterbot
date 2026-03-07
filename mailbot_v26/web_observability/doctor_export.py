from __future__ import annotations

import io
import json
import platform
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from mailbot_v26.integrity import verify_manifest
from mailbot_v26.version import __version__

REDACTED = "***REDACTED***"
MAX_LOG_BYTES = 10 * 1024 * 1024
TAIL_LINE_LIMIT = 4000
SECRET_KEY_MARKERS = (
    "password",
    "pass",
    "api_token",
    "api_key",
    "token",
    "secret",
    "key",
    "imap_password",
    "smtp_password",
    "telegram_token",
)


def _is_secret_key(key: str) -> bool:
    lowered = key.strip().lower().replace("-", "_")
    return any(marker in lowered for marker in SECRET_KEY_MARKERS)


def _redact_yaml_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in text.splitlines():
        if ":" not in raw_line:
            lines.append(raw_line)
            continue
        prefix, value = raw_line.split(":", 1)
        key = prefix.strip().strip('"').strip("'")
        if _is_secret_key(key):
            spacing = "" if value.startswith(" ") else " "
            lines.append(f"{prefix}:{spacing}{REDACTED}")
            continue
        lines.append(raw_line)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def _tail_text(path: Path, max_lines: int) -> str:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        all_lines = handle.readlines()
    selected = all_lines[-max_lines:]
    return "".join(selected)


def _latest_health_snapshot(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {}
    query = (
        "SELECT payload_json, ts_utc FROM system_health_snapshots "
        "ORDER BY ts_utc DESC LIMIT 1"
    )
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            row = conn.execute(query).fetchone()
    except sqlite3.Error:
        return {}
    if not row:
        return {}
    payload_raw = row[0]
    payload: dict[str, Any] = {}
    if isinstance(payload_raw, str) and payload_raw.strip():
        try:
            loaded = json.loads(payload_raw)
            if isinstance(loaded, dict):
                payload = loaded
        except json.JSONDecodeError:
            payload = {}
    payload.setdefault("ts_utc", row[1])
    return payload


def _manifest_status(dist_root: Path) -> dict[str, Any]:
    manifest_path = dist_root / "manifest.sha256.json"
    if not manifest_path.exists():
        return {"status": "NO_MANIFEST", "changed_files": []}
    try:
        ok, changed = verify_manifest(dist_root, manifest_path)
    except Exception as exc:
        return {
            "status": "MODIFIED",
            "changed_files": [],
            "error": f"manifest verification failed: {exc}",
        }
    return {
        "status": "OK" if ok else "MODIFIED",
        "changed_files": sorted(changed),
    }


def _runtime_versions_text() -> str:
    deps: list[str] = []
    for dist in sorted(metadata.distributions(), key=lambda d: (d.metadata.get("Name") or "").lower()):
        name = dist.metadata.get("Name")
        if not name:
            continue
        deps.append(f"{name}=={dist.version}")
        if len(deps) >= 30:
            break
    return "\n".join(
        [
            f"app_version={__version__}",
            f"python={sys.version}",
            f"platform={platform.platform()}",
            "dependencies:",
            *deps,
        ]
    ) + "\n"


def build_diagnostics_zip(
    config_path: Path,
    log_path: Path,
    db_path: Path,
    dist_root: Path,
    *,
    web_ui_bind: str = "",
    web_ui_port: int = 0,
    uptime_seconds: int = 0,
) -> bytes:
    config_path = Path(config_path)
    log_path = Path(log_path)
    db_path = Path(db_path)
    dist_root = Path(dist_root)
    missing_sources: list[str] = []

    manifest_status = _manifest_status(dist_root)
    health_payload = _latest_health_snapshot(db_path)
    health_payload["web_ui"] = {
        "bind": web_ui_bind,
        "port": int(web_ui_port),
        "uptime_seconds": max(0, int(uptime_seconds)),
    }
    health_payload.setdefault("generated_at_utc", datetime.now(timezone.utc).isoformat())
    health_payload.setdefault("app_version", __version__)

    metadata_lines: list[str] = [f"app_version={__version__}"]
    out = io.BytesIO()
    with zipfile.ZipFile(out, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        if log_path.exists():
            if log_path.stat().st_size > MAX_LOG_BYTES:
                archive.writestr(f"logs/{log_path.name}", _tail_text(log_path, TAIL_LINE_LIMIT))
                metadata_lines.append(
                    f"logs/{log_path.name}: truncated_to_tail_lines={TAIL_LINE_LIMIT}"
                )
            else:
                archive.write(log_path, arcname=f"logs/{log_path.name}")
        else:
            missing_sources.append(str(log_path))

        rotated = log_path.with_name(f"{log_path.name}.1")
        if rotated.exists():
            if rotated.stat().st_size > MAX_LOG_BYTES:
                archive.writestr(f"logs/{rotated.name}", _tail_text(rotated, TAIL_LINE_LIMIT))
                metadata_lines.append(
                    f"logs/{rotated.name}: truncated_to_tail_lines={TAIL_LINE_LIMIT}"
                )
            else:
                archive.write(rotated, arcname=f"logs/{rotated.name}")

        archive.writestr(
            "health/health.json",
            json.dumps(health_payload, ensure_ascii=False, indent=2, sort_keys=True),
        )
        archive.writestr(
            "build/manifest_status.json",
            json.dumps(manifest_status, ensure_ascii=False, indent=2, sort_keys=True),
        )

        if config_path.exists():
            redacted = _redact_yaml_text(config_path.read_text(encoding="utf-8"))
            archive.writestr("config/config.redacted.yaml", redacted)
        else:
            missing_sources.append(str(config_path))

        archive.writestr("versions/runtime.txt", _runtime_versions_text())

        if metadata_lines:
            archive.writestr("metadata.txt", "\n".join(metadata_lines) + "\n")
        if missing_sources:
            archive.writestr("missing.txt", "\n".join(sorted(missing_sources)) + "\n")

    return out.getvalue()
