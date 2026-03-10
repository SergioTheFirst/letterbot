from __future__ import annotations

import io
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.observability import logger as observability_logger
from mailbot_v26.telegram.inbound import TelegramInboundClient


@pytest.fixture()
def isolated_root_logger() -> logging.Logger:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    observability_logger._CONFIGURED = False
    try:
        yield root_logger
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
        observability_logger._CONFIGURED = False


def _rotating_handlers(root_logger: logging.Logger) -> list[RotatingFileHandler]:
    return [
        handler
        for handler in root_logger.handlers
        if isinstance(handler, RotatingFileHandler)
    ]


def test_log_rotation_handler_is_rotating_not_plain_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_root_logger: logging.Logger
) -> None:
    monkeypatch.setattr(start_module, "LOG_PATH", tmp_path / "logs" / "letterbot.log")

    start_module._configure_logging()  # noqa: SLF001

    rotating = _rotating_handlers(isolated_root_logger)
    plain_file_handlers = [
        handler
        for handler in isolated_root_logger.handlers
        if isinstance(handler, logging.FileHandler)
        and not isinstance(handler, RotatingFileHandler)
    ]

    assert len(rotating) == 1
    assert plain_file_handlers == []


def test_log_rotation_max_bytes_within_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_root_logger: logging.Logger
) -> None:
    monkeypatch.setattr(start_module, "LOG_PATH", tmp_path / "logs" / "letterbot.log")

    start_module._configure_logging()  # noqa: SLF001

    handler = _rotating_handlers(isolated_root_logger)[0]
    assert 0 < handler.maxBytes <= 10 * 1024 * 1024


def test_log_rotation_backup_count_within_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_root_logger: logging.Logger
) -> None:
    monkeypatch.setattr(start_module, "LOG_PATH", tmp_path / "logs" / "letterbot.log")

    start_module._configure_logging()  # noqa: SLF001

    handler = _rotating_handlers(isolated_root_logger)[0]
    assert 3 <= handler.backupCount <= 7


def test_log_rotation_log_dir_created_if_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_root_logger: logging.Logger
) -> None:
    log_path = tmp_path / "nested" / "logs" / "letterbot.log"
    monkeypatch.setattr(start_module, "LOG_PATH", log_path)

    start_module._configure_logging()  # noqa: SLF001

    assert log_path.parent.exists()


def test_log_no_sensitive_data_in_output(
    tmp_path: Path, isolated_root_logger: logging.Logger
) -> None:
    stream = io.StringIO()
    observability_logger.configure_logging(
        log_path=tmp_path / "logs" / "masked.log",
        console_stream=stream,
    )

    token = "123456789:AASECRET_TOKEN_VALUE"
    secret_body = "private-body-from-telegram"

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "ok": False,
                "description": f"token={token} password=secret",
                "result": [{"message": {"text": secret_body}}],
            }

    class _Requests:
        @staticmethod
        def get(*_args, **_kwargs) -> _Response:
            return _Response()

    client = TelegramInboundClient(bot_token=token, _requests=_Requests())

    result = client.get_updates(offset=None)

    assert result == []
    output = stream.getvalue()
    assert token not in output
    assert "password=secret" not in output
    assert secret_body not in output
    assert "telegram_inbound_poll_error" in output


def test_log_setup_idempotent_no_duplicate_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_root_logger: logging.Logger
) -> None:
    monkeypatch.setattr(start_module, "LOG_PATH", tmp_path / "logs" / "letterbot.log")

    start_module._configure_logging()  # noqa: SLF001
    start_module._configure_logging()  # noqa: SLF001

    rotating = _rotating_handlers(isolated_root_logger)
    console_handlers = [
        handler
        for handler in isolated_root_logger.handlers
        if type(handler) is logging.StreamHandler
    ]
    assert len(rotating) == 1
    assert len(console_handlers) == 1
