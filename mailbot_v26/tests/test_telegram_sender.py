import logging
from types import SimpleNamespace

import pytest

from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker import telegram_sender


class DummyResponse:
    def __init__(
        self, status_code: int = 200, text: str = "ok", payload: dict | None = None
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.content = text.encode()
        self._payload = payload

    def json(self) -> dict:
        if self._payload is not None:
            return self._payload
        raise ValueError("no payload")


def _payload(
    text: str, *, token: str = "token", chat_id: str = "123"
) -> TelegramPayload:
    return TelegramPayload(
        html_text=text,
        priority="🔵",
        metadata={"bot_token": token, "chat_id": chat_id},
    )


def test_send_telegram_empty_text_logs(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR):
        result = telegram_sender.send_telegram(
            _payload("", token="token", chat_id="chat")
        )
        assert result.delivered is False
    assert "empty" in caplog.text.lower()


def test_send_telegram_success(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {}

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        called["url"] = url
        called["json"] = json
        called["timeout"] = timeout
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    result = telegram_sender.send_telegram(_payload("hello"))
    assert result.delivered is True
    assert called["json"]["text"] == "hello"
    assert called["json"]["chat_id"] == "123"
    assert called["json"]["parse_mode"] == "HTML"
    assert called["json"]["disable_web_page_preview"] is True


def test_send_telegram_non_200_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        telegram_sender,
        "requests",
        SimpleNamespace(
            post=lambda url, json, timeout: DummyResponse(status_code=401, text="bad")
        ),
    )
    with caplog.at_level(logging.ERROR):
        result = telegram_sender.send_telegram(_payload("hello"))
        assert result.delivered is False
    assert "401" in caplog.text
    assert "bad" in caplog.text


def test_send_telegram_exception(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def raising_post(url: str, json: dict, timeout: int) -> DummyResponse:
        raise RuntimeError("network error")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=raising_post))
    with caplog.at_level(logging.ERROR):
        result = telegram_sender.send_telegram(_payload("hello"))
        assert result.delivered is False
    assert "network error" in caplog.text


def test_send_telegram_requests_missing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(telegram_sender, "requests", None)
    with caplog.at_level(logging.ERROR):
        result = telegram_sender.send_telegram(_payload("hello"))
        assert result.delivered is False
    assert "requests module not available" in caplog.text


def test_send_telegram_does_not_escape_html(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        captured.update(json)
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    text = "HQ\\MedvevSS <b>hi</b> & \"quote\" 'single'"
    result = telegram_sender.send_telegram(_payload(text))
    assert result.delivered is True
    assert captured["text"] == text


def test_worker_does_not_modify_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        captured.update(json)
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    text = "⏰ Сделать: <b>NO CHANGE</b> & keep \\ slashes"
    result = telegram_sender.send_telegram(_payload(text))
    assert result.delivered is True
    assert captured["text"] == text


def test_salvage_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        calls.append(json)
        if len(calls) == 1:
            return DummyResponse(
                status_code=400, text="Bad Request: unsupported start tag"
            )
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    text = "hello <b>world</b>"
    result = telegram_sender.send_telegram(_payload(text))
    assert result.delivered is True
    assert calls[0]["parse_mode"] == "HTML"
    assert "parse_mode" not in calls[1]


def test_send_telegram_extracts_message_id(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        return DummyResponse(
            status_code=200, text="ok", payload={"result": {"message_id": 4242}}
        )

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    result = telegram_sender.send_telegram(_payload("hello"))
    assert result.delivered is True
    assert result.message_id == 4242


def test_edit_telegram_message_success(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        captured.update(json)
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    ok = telegram_sender.edit_telegram_message(
        bot_token="token", chat_id="123", message_id=7, html_text="<b>text</b>"
    )
    assert ok is True
    assert captured["chat_id"] == "123"
    assert captured["message_id"] == 7
    assert captured["text"] == "<b>text</b>"


def test_edit_telegram_message_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        return DummyResponse(status_code=500, text="server error")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    ok = telegram_sender.edit_telegram_message(
        bot_token="token", chat_id="123", message_id=7, html_text="<b>text</b>"
    )
    assert ok is False


def test_outbound_sanitizer_fixes_common_mojibake_sequences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        captured.update(json)
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    clean_text = "\U0001f535 from sender@example.com:\nCheck payment by 15.04 — urgent…\n• Action"
    mojibake_text = clean_text.encode("utf-8").decode("cp1251")

    result = telegram_sender.send_telegram(_payload(mojibake_text))

    assert result.delivered is True
    sent_text = str(captured.get("text", ""))
    assert sent_text == clean_text
    for token in ("вЂ", "Р", "рџ", "СЃ", "РѕС‚"):
        assert token not in sent_text


def test_no_mojibake_in_regular_telegram_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
        captured.update(json)
        return DummyResponse(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=fake_post))
    clean_text = "\U0001f7e1 from alerts@example.com:\nTotal payable 1200 USD"
    mojibake_text = clean_text.encode("utf-8").decode("cp1251")

    result = telegram_sender.send_telegram(_payload(mojibake_text))

    assert result.delivered is True
    sent_text = str(captured.get("text", ""))
    assert sent_text == clean_text
    for token in ("вЂ", "Р", "рџ", "СЃ", "РѕС‚"):
        assert token not in sent_text
