"""Bot tokens must never reach logs, delivery errors or support exports (A7/B4, QW2)."""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from mailbot_v26.observability import logger as observability_logger
from mailbot_v26.observability.redact import RedactingFormatter, redact_secrets
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.web_observability import doctor_export
from mailbot_v26.worker import telegram_sender

TOKEN = "987654321:AAHfakeTokenValueForTestsOnly_1234567"
TOKEN_SECRET = TOKEN.split(":", 1)[1]
URL_PATH = f"/bot{TOKEN}/sendMessage"


def _connection_error() -> requests.ConnectionError:
    return requests.ConnectionError(
        "HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries "
        f"exceeded with url: {URL_PATH} (Caused by NewConnectionError('boom'))"
    )


def _payload() -> TelegramPayload:
    return TelegramPayload(
        html_text="hello",
        priority="\U0001f535",
        metadata={"bot_token": TOKEN, "chat_id": "42"},
    )


def _assert_records_clean(records: list[logging.LogRecord]) -> None:
    assert records, "expected the sender to log the failure"
    for record in records:
        assert TOKEN_SECRET not in record.getMessage()
        assert TOKEN_SECRET not in str(getattr(record, "error", "") or "")


@pytest.fixture()
def isolated_root_logger():
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    for handler in original_handlers:
        root_logger.removeHandler(handler)
    observability_logger._CONFIGURED = False
    try:
        yield root_logger
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
        observability_logger._CONFIGURED = False


def test_redact_secrets_masks_bot_url_path_and_bare_token() -> None:
    redacted_url = redact_secrets(f"failed with url: {URL_PATH} (boom)")
    assert TOKEN_SECRET not in redacted_url
    assert "/bot<redacted>/sendMessage" in redacted_url

    assert redact_secrets(f"token {TOKEN} end") == "token <redacted-token> end"
    assert redact_secrets("ratio 12:34 at 10:15") == "ratio 12:34 at 10:15"
    assert redact_secrets(None) == ""


def test_send_exception_error_and_logs_are_redacted(monkeypatch, caplog) -> None:
    def _raise(**_kwargs):
        raise _connection_error()

    monkeypatch.setattr(telegram_sender, "_post_message", _raise)
    caplog.set_level(logging.DEBUG)

    result = telegram_sender.send_telegram(_payload())

    assert result.delivered is False
    assert result.retryable is True
    assert TOKEN_SECRET not in (result.error or "")
    assert "bot<redacted>" in (result.error or "")
    _assert_records_clean(caplog.records)


def test_salvage_exception_error_and_logs_are_redacted(monkeypatch, caplog) -> None:
    calls: list[dict[str, object]] = []

    def _post(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return SimpleNamespace(
                status_code=400, text="Bad Request: can't parse entities"
            )
        raise _connection_error()

    monkeypatch.setattr(telegram_sender, "_post_message", _post)
    caplog.set_level(logging.DEBUG)

    result = telegram_sender.send_telegram(_payload())

    assert result.delivered is False
    assert result.mode == "plain_salvage"
    assert TOKEN_SECRET not in (result.error or "")
    _assert_records_clean(caplog.records)


def test_edit_exception_logs_are_redacted(monkeypatch, caplog) -> None:
    def _raise(*_args, **_kwargs):
        raise _connection_error()

    monkeypatch.setattr(telegram_sender.requests, "post", _raise)
    caplog.set_level(logging.DEBUG)

    result = telegram_sender.edit_telegram_message(
        bot_token=TOKEN, chat_id="42", message_id=7, html_text="hello"
    )

    assert not result
    _assert_records_clean(caplog.records)


def test_ping_exception_detail_and_logs_are_redacted(monkeypatch, caplog) -> None:
    def _raise(*_args, **_kwargs):
        raise _connection_error()

    monkeypatch.setattr(telegram_sender.requests, "get", _raise)
    caplog.set_level(logging.DEBUG)

    ok, detail = telegram_sender.ping_telegram(TOKEN)

    assert ok is False
    assert TOKEN_SECRET not in detail
    _assert_records_clean(caplog.records)


def test_redacting_formatter_redacts_exception_traceback() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(RedactingFormatter("%(levelname)s %(message)s"))
    test_logger = logging.getLogger("mailbot.tests.redaction")
    test_logger.addHandler(handler)
    test_logger.propagate = False
    try:
        try:
            raise _connection_error()
        except requests.ConnectionError:
            test_logger.exception("send failed via %s", URL_PATH)
    finally:
        test_logger.removeHandler(handler)
        test_logger.propagate = True

    output = stream.getvalue()
    assert "Traceback" in output
    assert TOKEN_SECRET not in output
    assert "bot<redacted>" in output


def test_runtime_logging_handlers_redact_tokens(
    tmp_path: Path, isolated_root_logger: logging.Logger
) -> None:
    stream = io.StringIO()
    log_path = tmp_path / "logs" / "letterbot.log"
    observability_logger.configure_logging(log_path=log_path, console_stream=stream)

    try:
        raise _connection_error()
    except requests.ConnectionError:
        logging.getLogger("mailbot.tests.runtime").exception(
            "Telegram send exception: %s", URL_PATH
        )
    observability_logger.get_logger("mailbot").error("tg_failed", error=TOKEN)
    for handler in isolated_root_logger.handlers:
        handler.flush()

    console_text = stream.getvalue()
    file_text = log_path.read_text(encoding="utf-8")
    for text in (console_text, file_text):
        assert "Traceback" in text
        assert "tg_failed" in text
        assert TOKEN_SECRET not in text
        assert "bot<redacted>" in text


def test_doctor_export_log_tail_is_redacted(tmp_path: Path, monkeypatch) -> None:
    log_path = tmp_path / "letterbot.log"
    log_path.write_text(
        "first line\n"
        f"ERROR Telegram send exception: Max retries exceeded with url: {URL_PATH}\n"
        f"ERROR raw token {TOKEN}\n",
        encoding="utf-8",
    )

    tail = doctor_export._tail_text(log_path, 10)
    assert "first line" in tail
    assert TOKEN_SECRET not in tail
    assert "bot<redacted>" in tail

    monkeypatch.setattr(doctor_export, "MAX_LOG_BYTES", 0)
    archive_bytes = doctor_export.build_diagnostics_zip(
        tmp_path / "missing-config.yaml",
        log_path,
        tmp_path / "missing.sqlite",
        tmp_path,
    )
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        exported = archive.read(f"logs/{log_path.name}").decode("utf-8")
    assert TOKEN_SECRET not in exported
