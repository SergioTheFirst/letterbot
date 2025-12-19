from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Protocol

try:
    import structlog

    STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in tests
    structlog = None
    STRUCTLOG_AVAILABLE = False

_CONFIGURED = False


class LoggerLike(Protocol):
    def info(self, event: str, *args: Any, **kwargs: Any) -> None: ...

    def warning(self, event: str, *args: Any, **kwargs: Any) -> None: ...

    def error(self, event: str, *args: Any, **kwargs: Any) -> None: ...


class _FallbackLogger:
    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def info(self, event: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.INFO, event, *args, **kwargs)

    def warning(self, event: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.WARNING, event, *args, **kwargs)

    def error(self, event: str, *args: Any, **kwargs: Any) -> None:
        self._log(logging.ERROR, event, *args, **kwargs)

    def _log(self, level: int, event: str, *args: Any, **kwargs: Any) -> None:
        if args:
            try:
                event = event % args
            except (TypeError, ValueError):
                pass
        payload = {
            "event": event,
            "level": logging.getLevelName(level).lower(),
            "logger": self._logger.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        message = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self._logger.log(level, message)


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    root_logger = logging.getLogger()
    if STRUCTLOG_AVAILABLE:
        timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
        pre_chain = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            timestamper,
        ]

        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=pre_chain,
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        if not root_logger.handlers:
            root_logger.addHandler(handler)
        else:
            for existing_handler in root_logger.handlers:
                existing_handler.setFormatter(formatter)
        root_logger.setLevel(logging.INFO)

        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                timestamper,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    else:
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
    if STRUCTLOG_AVAILABLE:
        return structlog.get_logger(name)
    return _FallbackLogger(name)


__all__ = ["configure_logging", "get_logger"]
