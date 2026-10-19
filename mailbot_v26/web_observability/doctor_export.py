from __future__ import annotations

import io
import json
import platform
import re
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from mailbot_v26.integrity import verify_shipped_files
from mailbot_v26.observability.redact import redact_secrets
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
SUPPORT_VALUE_PLACEHOLDER = "<redacted>"
# An e-mail address; the first local-part char and the domain survive masking.
_SUPPORT_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*"
    r"@([A-Za-z0-9.-]+\.[A-Za-z]{2,})"
)
# ``subject=...`` / ``from=...`` log fields: the value runs to the next
# ``key=`` field or the end of the line (quoted values end at the quote).
_SUPPORT_KEYED_VALUE_RE = re.compile(
    r"\b(subject|from)=(?:\"[^\"\n]*\"|'[^'\n]*'|[^\n]*?)"
    r"(?=[ \t]+[A-Za-z_][\w.-]*=|$)",
    re.IGNORECASE | re.MULTILINE,
)


def _scrub_support_text(text: str) -> str:
    """Mask secrets, mail subjects/senders and e-mail addresses for the zip."""
    scrubbed = redact_secrets(text)
    scrubbed = _SUPPORT_KEYED_VALUE_RE.sub(
        lambda match: f"{match.group(1)}={SUPPORT_VALUE_PLACEHOLDER}", scrubbed
    )
    return _SUPPORT_EMAIL_RE.sub("\\1\u2026@\\2", scrubbed)


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
    return _scrub_support_text("".join(selected))


def _add_log_to_archive(
    archive: zipfile.ZipFile, path: Path, metadata_lines: list[str]
) -> None:
    """Add one log file to the zip, always scrubbed and never copied raw."""
    arcname = f"logs/{path.name}"
    if path.stat().st_size > MAX_LOG_BYTES:
        archive.writestr(arcname, _tail_text(path, TAIL_LINE_LIMIT))
        metadata_lines.append(f"{arcname}: truncated_to_tail_lines={TAIL_LINE_LIMIT}")
        return
    text = path.read_text(encoding="utf-8", errors="replace")
    archive.writestr(arcname, _scrub_support_text(text))


def _latest_health_snapshot(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {}
    query = (
        "SELECT payload_json, ts_utc FROM system_health_snapshots "
        "ORDER BY ts_utc DESC LIMIT 1"
    )
    try:
        uri = Path(db_path).resolve().as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
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
        changed = verify_shipped_files(dist_root, manifest_path)
        ok = not changed
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
    for dist in sorted(
        metadata.distributions(), key=lambda d: (d.metadata.get("Name") or "").lower()
    ):
        name = dist.metadata.get("Name")
        if not name:
            continue
        deps.append(f"{name}=={dist.version}")
        if len(deps) >= 30:
            break
    return (
        "\n".join(
            [
                f"app_version={__version__}",
                f"python={sys.version}",
                f"platform={platform.platform()}",
                "dependencies:",
                *deps,
            ]
        )
        + "\n"
    )


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
    health_payload.setdefault(
        "generated_at_utc", datetime.now(timezone.utc).isoformat()
    )
    health_payload.setdefault("app_version", __version__)

    metadata_lines: list[str] = [f"app_version={__version__}"]
    out = io.BytesIO()
    with zipfile.ZipFile(out, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        if log_path.exists():
            _add_log_to_archive(archive, log_path, metadata_lines)
        else:
            missing_sources.append(str(log_path))

        rotated = log_path.with_name(f"{log_path.name}.1")
        if rotated.exists():
            _add_log_to_archive(archive, rotated, metadata_lines)

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
