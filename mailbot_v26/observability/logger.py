from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Protocol, TextIO

_CONFIGURED = False
_FILE_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
_PLAIN_FORMAT = "%(message)s"
_MAX_BYTES_DEFAULT = 5 * 1024 * 1024
_BACKUP_COUNT_DEFAULT = 5


class LoggerLike(Protocol):
    def info(self, event: str, **fields: object) -> None: ...

    def warning(self, event: str, **fields: object) -> None: ...

    def error(self, event: str, **fields: object) -> None: ...


class ObservabilityLogger:
    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def info(self, event: str, **fields: object) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: object) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: object) -> None:
        self._log(logging.ERROR, event, **fields)

    def _log(self, level: int, event: str, **fields: object) -> None:
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": logging.getLevelName(level),
            "event": event,
            **fields,
        }
        message = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self._logger.log(level, message)

def _is_letterbot_console_handler(handler: logging.Handler) -> bool:
    return (
        type(handler) is logging.StreamHandler
        and bool(getattr(handler, "_letterbot_console", False))
    )


def _is_letterbot_file_handler(handler: logging.Handler) -> bool:
    return isinstance(handler, RotatingFileHandler) and bool(
        getattr(handler, "_letterbot_file", False)
    )


def _configure_console_handler(
    root_logger: logging.Logger,
    *,
    stream: TextIO | None,
    formatter: logging.Formatter,
) -> None:
    for handler in root_logger.handlers:
        if _is_letterbot_console_handler(handler):
            if stream is not None:
                handler.stream = stream
            handler.setFormatter(formatter)
            return
    if stream is None and root_logger.handlers:
        return
    handler = logging.StreamHandler(stream)
    handler._letterbot_console = True  # type: ignore[attr-defined]
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def _configure_file_handler(
    root_logger: logging.Logger,
    *,
    log_path: Path,
    max_bytes: int,
    backup_count: int,
) -> None:
    resolved = log_path.resolve()
    formatter = logging.Formatter(_FILE_FORMAT)
    for handler in root_logger.handlers:
        if not _is_letterbot_file_handler(handler):
            continue
        base_filename = Path(getattr(handler, "baseFilename", "")).resolve()
        if base_filename != resolved:
            continue
        handler.maxBytes = max_bytes
        handler.backupCount = backup_count
        handler.setFormatter(formatter)
        return

    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
        delay=True,
    )
    handler._letterbot_file = True  # type: ignore[attr-defined]
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def configure_logging(
    *,
    log_path: Path | None = None,
    console_stream: TextIO | None = None,
    level: int = logging.INFO,
    max_bytes: int = _MAX_BYTES_DEFAULT,
    backup_count: int = _BACKUP_COUNT_DEFAULT,
) -> None:
    global _CONFIGURED
    if max_bytes <= 0:
        raise ValueError("max_bytes must be > 0")
    if backup_count <= 0:
        raise ValueError("backup_count must be > 0")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    console_formatter = logging.Formatter(
        _FILE_FORMAT if console_stream is not None else _PLAIN_FORMAT
    )
    _configure_console_handler(
        root_logger, stream=console_stream, formatter=console_formatter
    )
    if log_path is not None:
        _configure_file_handler(
            root_logger,
            log_path=Path(log_path),
            max_bytes=max_bytes,
            backup_count=backup_count,
        )
    _CONFIGURED = True


def get_logger(name: str = "mailbot") -> LoggerLike:
    configure_logging()
    return ObservabilityLogger(name)

__all__ = [
    "configure_logging",
    "get_logger",
    "ObservabilityLogger",
    "_BACKUP_COUNT_DEFAULT",
    "_MAX_BYTES_DEFAULT",
]
