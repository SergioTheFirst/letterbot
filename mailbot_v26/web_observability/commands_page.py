"""The Commands page (/commands): every command-line command and its place here.

GET /commands lists commands_registry.ENTRIES by group (Developer and release
tools collapsed) and shows the results of the commands that only read,
computed on this request:
- validate-config: config_bootstrap.validate_config_findings (the CLI's own
  lines), section names masked like on the Settings page, plus the config.yaml
  schema check of --compat (versions and status only, never file text);
- config-ready: config_bootstrap.config_ready_findings;
- cleanup --status: tools.cleanup.cleanup_status on a read-only connection;
- version, and doctor --print-lan-url from settings.ini [web].
GET /api/commands/<command> returns the same results as JSON.

Nothing here writes the database or a file or starts a process (CONSTITUTION
IV). Access is the Settings rule: this computer, or signed in over the
network. No option value of accounts.ini/settings.ini reaches a result:
findings name sections only (masked), and a failed check shows only the
error class, the details going to the log.
"""

from __future__ import annotations

import logging
import os
import sys
from functools import partial
from pathlib import Path
from typing import Any, Callable

from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.config_loader import load_web_config
from mailbot_v26.config_yaml import (
    SUPPORTED_SCHEMA_VERSION,
    get_schema_version,
    load_config as load_yaml_config,
    validate_config_with_hints,
)
from mailbot_v26.tools.cleanup import PROTECTED_EVENT_TYPES, PROTECTED_TABLES, cleanup_status
from mailbot_v26.tools.config_bootstrap import (
    ConfigFinding,
    config_ready_findings,
    validate_config_findings,
)
from mailbot_v26.tools.networking import get_primary_ipv4
from mailbot_v26.version import get_version
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.accounts_status import _section_label
from mailbot_v26.web_observability.commands_registry import (
    COLLAPSED_GROUPS,
    ENTRIES,
    GROUPS,
    entry_view,
    text,
)
from mailbot_v26.web_observability.settings_api import (
    _access_denial,
    _config_dir,
    _json,
    _no_store,
    _where,
)
from mailbot_v26.web_observability.settings_text import resolve_locale

logger = logging.getLogger(__name__)

TEMPLATE_NAME = "commands.html"


def _failed(command: str, exc: BaseException) -> dict[str, object]:
    """Log a failed check (class and place only) and return its visible state."""
    logger.error(
        "commands_check_failed command=%s error_class=%s at=%s",
        command,
        type(exc).__name__,
        _where(exc),
    )
    return {"command": command, "state": "unavailable", "error": type(exc).__name__}


def _shown(finding: ConfigFinding) -> str:
    return finding.shown(_section_label)


def _compat_result(config_dir: Path) -> dict[str, object]:
    """validate-config --compat, reduced to versions and status."""
    yaml_path = resolve_config_paths(config_dir).yaml_path
    if yaml_path is None:
        return {"state": "absent"}
    try:
        raw = load_yaml_config(yaml_path)
        ok = validate_config_with_hints(raw)[0]
        version = get_schema_version(raw)
    except Exception as exc:
        failed = _failed("validate-config --compat", exc)
        return {"state": "unreadable", "error": failed["error"]}
    return {
        "state": "ok" if ok else "fail",
        "supported": SUPPORTED_SCHEMA_VERSION,
        "version": version,
    }


def validate_result(config_dir: Path | None) -> dict[str, object]:
    command = "validate-config"
    if config_dir is None:
        return {"command": command, "state": "no_config_dir", "findings": []}
    try:
        findings = validate_config_findings(config_dir)
    except Exception as exc:
        result = _failed(command, exc)
        result["findings"] = []
        return result
    return {
        "command": command,
        "state": "warn" if findings else "ok",
        "findings": [
            {"text": _shown(finding), "section": _section_label(finding.section) if finding.section else ""}
            for finding in findings
        ],
        "compat": _compat_result(config_dir),
    }


def ready_result(config_dir: Path | None) -> dict[str, object]:
    command = "config-ready"
    if config_dir is None:
        return {"command": command, "state": "no_config_dir", "critical": [], "warnings": []}
    try:
        critical, warnings = config_ready_findings(config_dir)
    except Exception as exc:
        result = _failed(command, exc)
        result.update(critical=[], warnings=[])
        return result
    return {
        "command": command,
        "state": "not_ready" if critical else "ready",
        "critical": [_shown(finding) for finding in critical],
        "warnings": [_shown(finding) for finding in warnings],
    }


def cleanup_result(db_path: Path) -> dict[str, object]:
    command = "cleanup --status"
    try:
        status = cleanup_status(db_path)
    except Exception as exc:
        return _failed(command, exc)
    if status.vacuum_reason == "database_missing":
        return {"command": command, "state": "database_missing", "db_path": str(status.db_path)}
    return {
        "command": command,
        "state": "ok",
        "db_path": str(status.db_path),
        "eligible_total": sum(entry.eligible_rows for entry in status.plan),
        "freelist": status.freelist_before,
        "targets": [
            {
                "table": entry.target.table,
                "name": entry.target.name,
                "retention_days": entry.target.retention_days,
                "eligible": entry.eligible_rows,
                "missing": entry.target.table in status.missing_tables,
            }
            for entry in status.plan
        ],
        "missing_tables": list(status.missing_tables),
        "protected_event_types": sorted(PROTECTED_EVENT_TYPES),
        "protected_tables": sorted(PROTECTED_TABLES),
    }


def version_result() -> dict[str, object]:
    return {"command": "version", "version": get_version()}


def lan_result(config_dir: Path | None) -> dict[str, object]:
    """doctor --print-lan-url: the saved [web] host/port of settings.ini."""
    command = "doctor --print-lan-url"
    if config_dir is None:
        return {"command": command, "state": "no_config_dir"}
    try:
        web = load_web_config(config_dir)
        host, port = str(web.host), int(web.port)
        if web_app._is_loopback_bind(host):
            return {"command": command, "state": "local_only", "url": f"http://127.0.0.1:{port}/", "port": port}
        detected = get_primary_ipv4() if host == "0.0.0.0" else host
    except Exception as exc:
        return _failed(command, exc)
    if not detected:
        return {"command": command, "state": "lan_undetected", "url": "", "port": port}
    return {"command": command, "state": "lan", "url": f"http://{detected}:{port}/", "port": port}


def _results(app: Any) -> dict[str, dict[str, object]]:
    config_dir = _config_dir(app)
    return {
        "validate": validate_result(config_dir),
        "ready": ready_result(config_dir),
        "cleanup": cleanup_result(Path(app.config["DB_PATH"])),
        "version": version_result(),
        "lan": lan_result(config_dir),
    }


def _groups(app: Any, locale: str) -> list[dict[str, object]]:
    config_dir = _config_dir(app)
    frozen = bool(getattr(sys, "frozen", False))
    shown_config = os.path.abspath(config_dir) if config_dir is not None else None
    shown_db = os.path.abspath(app.config["DB_PATH"])
    groups: list[dict[str, object]] = []
    for group in GROUPS:
        groups.append(
            {
                "id": group,
                "title": text(f"group.{group}", locale),
                "collapsed": group in COLLAPSED_GROUPS,
                "entries": [
                    entry_view(
                        entry,
                        locale,
                        frozen=frozen,
                        config_dir=shown_config,
                        db_path=shown_db,
                    )
                    for entry in ENTRIES
                    if entry.group == group
                ],
            }
        )
    return groups


def _api(app: Any, build: Callable[[], dict[str, object]]) -> Any:
    denial = _access_denial(app)
    if denial is not None:
        return denial
    return _json(build())


def register_commands_routes(app: Any) -> None:
    """Add /commands and its read-only JSON results to ``app``."""

    @app.route("/commands", methods=["GET"])
    def commands():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        locale = resolve_locale(app.config.get("UI_LOCALE"))
        title = text("page.title", locale)
        body = web_app._render_template(
            app,
            TEMPLATE_NAME,
            title=f"{title} · LetterBot.ru",
            page_title=title,
            ct=partial(text, locale=locale),
            groups=_groups(app, locale),
            results=_results(app),
            config_dir_known=_config_dir(app) is not None,
            hide_limit=True,
        )
        return _no_store(web_app._text_response(body, 200))

    @app.route("/api/commands/validate-config", methods=["GET"])
    def api_commands_validate_config():
        return _api(app, lambda: validate_result(_config_dir(app)))

    @app.route("/api/commands/config-ready", methods=["GET"])
    def api_commands_config_ready():
        return _api(app, lambda: ready_result(_config_dir(app)))

    @app.route("/api/commands/cleanup-status", methods=["GET"])
    def api_commands_cleanup_status():
        return _api(app, lambda: cleanup_result(Path(app.config["DB_PATH"])))

    @app.route("/api/commands/version", methods=["GET"])
    def api_commands_version():
        return _api(app, version_result)

    @app.route("/api/commands/lan-url", methods=["GET"])
    def api_commands_lan_url():
        return _api(app, lambda: lan_result(_config_dir(app)))


__all__ = ["register_commands_routes"]
