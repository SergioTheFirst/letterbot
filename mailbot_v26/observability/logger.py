from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Protocol

_CONFIGURED = False


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


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    root_logger = logging.getLogger()
    formatter = logging.Formatter("%(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    if not root_logger.handlers:
        root_logger.addHandler(handler)
    else:
        for existing_handler in root_logger.handlers:
            existing_handler.setFormatter(formatter)
    root_logger.setLevel(logging.INFO)

    _CONFIGURED = True


def get_logger(name: str = "mailbot") -> LoggerLike:
    configure_logging()
    return ObservabilityLogger(name)


__all__ = ["configure_logging", "get_logger", "ObservabilityLogger"]
