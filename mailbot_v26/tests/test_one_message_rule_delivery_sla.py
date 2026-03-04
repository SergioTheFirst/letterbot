from __future__ import annotations

from mailbot_v26.pipeline.processor import _apply_delivery_sla
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _payload(text: str, *, markup: dict[str, object] | None = None) -> TelegramPayload:
    return TelegramPayload(
        html_text=text,
        priority="🔵",
        metadata={"bot_token": "token", "chat_id": "chat"},
        reply_markup=markup,
    )


def test_initial_delivery_sends_once_with_final_payload() -> None:
    sent: list[TelegramPayload] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=77)

    outcome = _apply_delivery_sla(
        processing_started_at=95.0,
        wait_budget_seconds=10.0,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=None,
        on_edit_failure=None,
        monotonic=lambda: 100.0,
    )

    assert outcome.delivery_mode == "final_first_send"
    assert len(sent) == 1
    assert sent[0].html_text == "final"


def test_delayed_followup_edits_same_message_and_preserves_reply_markup() -> None:
    sent: list[TelegramPayload] = []
    edited: list[tuple[int, TelegramPayload]] = []

    final_markup = {"inline_keyboard": [[{"text": "Действия", "callback_data": "mb:ok:1"}]]}

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=501)

    def _edit(message_id: int, payload: TelegramPayload) -> bool:
        edited.append((message_id, payload))
        return True

    outcome = _apply_delivery_sla(
        processing_started_at=80.0,
        wait_budget_seconds=5.0,
        minimal_payload=_payload("minimal", markup=final_markup),
        final_payload=_payload("final", markup=final_markup),
        send_func=_send,
        edit_func=_edit,
        on_edit_failure=None,
        monotonic=lambda: 100.0,
    )

    assert outcome.delivery_mode == "minimal_then_edit"
    assert len(sent) == 1
    assert sent[0].html_text == "minimal"
    assert len(edited) == 1
    assert edited[0][0] == 501
    assert edited[0][1].html_text == "final"
    assert edited[0][1].reply_markup == final_markup


def test_edit_failure_does_not_send_second_message() -> None:
    sent: list[TelegramPayload] = []
    edit_failures: list[str] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=12)

    outcome = _apply_delivery_sla(
        processing_started_at=10.0,
        wait_budget_seconds=0.1,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=lambda _message_id, _payload: False,
        on_edit_failure=edit_failures.append,
        monotonic=lambda: 100.0,
    )

    assert outcome.delivery_mode == "minimal_then_edit"
    assert outcome.edit_applied is False
    assert edit_failures == ["edit_failed"]
    assert len(sent) == 1


def test_missing_message_id_skips_edit_without_second_send() -> None:
    sent: list[TelegramPayload] = []
    edit_failures: list[str] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=None)

    outcome = _apply_delivery_sla(
        processing_started_at=10.0,
        wait_budget_seconds=0.1,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=lambda _message_id, _payload: True,
        on_edit_failure=edit_failures.append,
        monotonic=lambda: 100.0,
    )

    assert outcome.delivery_mode == "minimal_then_edit"
    assert outcome.edit_applied is False
    assert edit_failures == ["missing_message_id"]
    assert len(sent) == 1
