"""Telegram edit results must be honoured by inbound callers (B10 / QW6)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.tests.test_telegram_inbound import (
    _build_processor,
    _insert_email,
    _norm,
)
from mailbot_v26.worker import telegram_sender

NOT_MODIFIED_BODY = json.dumps(
    {
        "ok": False,
        "error_code": 400,
        "description": (
            "Bad Request: message is not modified: specified new message content "
            "and reply markup are exactly the same as a content and reply markup "
            "of the current message"
        ),
    }
)
NOT_FOUND_BODY = json.dumps(
    {
        "ok": False,
        "error_code": 400,
        "description": "Bad Request: message to edit not found",
    }
)


class _Response:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def json(self) -> dict[str, object]:
        return json.loads(self.text)


class _TelegramHttpStub:
    """Fake Bot API: scripted editMessageText reply, records callback acks."""

    def __init__(self, edit_status: int, edit_body: str) -> None:
        self.edit_status = edit_status
        self.edit_body = edit_body
        self.edits: list[dict[str, object]] = []
        self.acks: list[dict[str, object]] = []

    def post(self, url: str, json: dict[str, object], timeout: int) -> _Response:
        if url.endswith("/editMessageText"):
            self.edits.append(json)
            return _Response(self.edit_status, self.edit_body)
        if url.endswith("/answerCallbackQuery"):
            self.acks.append(json)
            return _Response(200, '{"ok": true}')
        raise AssertionError(f"unexpected Telegram call: {url}")


def _gate_result() -> GateResult:
    return GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=1,
        correction_rate=0.01,
        engine="priority_v2_auto",
    )


def _edit(monkeypatch, status: int, body: str):
    monkeypatch.setattr(
        telegram_sender,
        "requests",
        SimpleNamespace(post=lambda url, json, timeout: _Response(status, body)),
    )
    return telegram_sender.edit_telegram_message(
        bot_token="token", chat_id="123", message_id=7, html_text="text"
    )


def test_edit_not_modified_is_treated_as_success(monkeypatch, caplog) -> None:
    caplog.set_level(logging.INFO)

    result = _edit(monkeypatch, 400, NOT_MODIFIED_BODY)

    assert result is True
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_edit_other_bad_request_is_failure(monkeypatch) -> None:
    assert _edit(monkeypatch, 400, NOT_FOUND_BODY) is False


def _run_priority_callback(
    tmp_path: Path, monkeypatch, stub: _TelegramHttpStub
) -> None:
    monkeypatch.setattr(telegram_sender, "requests", stub)
    processor = _build_processor(tmp_path, [], _gate_result())
    email_id = _insert_email(processor.knowledge_db.path)
    processor.handle_callback_query(
        {
            "id": "cb-prio-edit",
            "data": f"prio_set:{email_id}:Y",
            "message": {"chat": {"id": "chat"}, "message_id": 202, "text": "old"},
        }
    )


def test_priority_callback_rejected_edit_is_not_confirmed(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    stub = _TelegramHttpStub(400, NOT_FOUND_BODY)
    caplog.set_level(logging.INFO)

    _run_priority_callback(tmp_path, monkeypatch, stub)

    assert len(stub.edits) == 1
    assert stub.acks
    assert _norm(str(stub.acks[0]["text"])) == _norm("Не могу отредактировать")
    assert "tg_priority_edit_ok" not in caplog.text
    assert "tg_priority_edit_failed" in caplog.text


def test_priority_callback_not_modified_edit_is_confirmed(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    stub = _TelegramHttpStub(400, NOT_MODIFIED_BODY)
    caplog.set_level(logging.INFO)

    _run_priority_callback(tmp_path, monkeypatch, stub)

    assert len(stub.edits) == 1
    assert _norm(str(stub.acks[0]["text"])) == _norm("Приоритет обновлён")
    assert "tg_priority_edit_ok" in caplog.text
    assert "tg_priority_edit_failed" not in caplog.text


def test_trace_toggle_rejected_edit_is_logged(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    stub = _TelegramHttpStub(400, NOT_FOUND_BODY)
    monkeypatch.setattr(telegram_sender, "requests", stub)
    processor = _build_processor(tmp_path, [], _gate_result())
    email_id = _insert_email(processor.knowledge_db.path)
    caplog.set_level(logging.INFO)

    processor.handle_callback_query(
        {
            "id": "cb-trace",
            "data": f"mb:d:{email_id}",
            "message": {"message_id": 55, "chat": {"id": "chat"}},
        }
    )

    assert len(stub.edits) == 1
    assert "telegram_inbound_edit_failed" in caplog.text
