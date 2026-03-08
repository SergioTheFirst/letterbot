from __future__ import annotations

import configparser
import logging
from pathlib import Path

_LOGGER = logging.getLogger(__name__)
_WARNED_CONFIG_ONCE: set[tuple[Path, str]] = set()


def read_user_ini_with_defaults(
    ini_path: Path,
    *,
    logger: logging.Logger | None = None,
    scope_label: str = "config settings",
    template_path: Path | None = None,
) -> configparser.ConfigParser:
    """Read user-provided INI safely and return an empty parser on any read/parse issue."""
    parser = configparser.ConfigParser()
    active_logger = logger or _LOGGER
    resolved_template = template_path or (ini_path.parent / f"{ini_path.name}.example")

    if not ini_path.exists():
        _warn_once(
            active_logger,
            ini_path,
            reason="missing",
            scope_label=scope_label,
            template_path=resolved_template,
        )
        return parser

    try:
        parsed, used_legacy = _read_ini_with_legacy_support(parser, ini_path)
        if used_legacy:
            _warn_once(
                active_logger,
                ini_path,
                reason="invalid",
                scope_label=scope_label,
                template_path=resolved_template,
            )
        return parsed
    except (
        configparser.MissingSectionHeaderError,
        configparser.ParsingError,
        OSError,
    ) as exc:
        _warn_once(
            active_logger,
            ini_path,
            reason="invalid",
            scope_label=scope_label,
            template_path=resolved_template,
        )
        active_logger.debug("INI read failure details for %s", ini_path, exc_info=exc)
        return configparser.ConfigParser()


def _read_ini_with_legacy_support(
    parser: configparser.ConfigParser,
    ini_path: Path,
) -> tuple[configparser.ConfigParser, bool]:
    raw_text = ini_path.read_text(encoding="utf-8")
    if not raw_text.strip():
        return parser, False
    try:
        parser.read_string(raw_text, source=str(ini_path))
        return parser, False
    except (configparser.MissingSectionHeaderError, configparser.ParsingError):
        parser.read_string(f"[main]\n{raw_text}", source=str(ini_path))
        return parser, True


def _warn_once(
    logger: logging.Logger,
    ini_path: Path,
    *,
    reason: str,
    scope_label: str,
    template_path: Path,
) -> None:
    key = (ini_path.resolve(), reason)
    if key in _WARNED_CONFIG_ONCE:
        return
    _WARNED_CONFIG_ONCE.add(key)

    if reason == "missing":
        logger.warning(
            "%s missing at %s; using deterministic defaults. Template: %s. Windows command: copy %s %s",
            ini_path.name,
            ini_path,
            template_path,
            template_path,
            ini_path,
        )
        return

    logger.warning(
        "%s is invalid at %s; using deterministic defaults for %s. "
        "Template: %s. Windows command: copy %s %s",
        ini_path.name,
        ini_path,
        scope_label,
        template_path,
        template_path,
        ini_path,
    )


__all__ = ["read_user_ini_with_defaults"]
