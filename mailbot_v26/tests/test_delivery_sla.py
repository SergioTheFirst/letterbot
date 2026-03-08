from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _payload(text: str) -> TelegramPayload:
    return TelegramPayload(
        html_text=text, priority="🔵", metadata={"chat_id": "1", "bot_token": "t"}
    )


def _monotonic_sequence(values: list[float]):
    calls = iter(values)

    def _next() -> float:
        return next(calls)

    return _next


def test_delivery_full_within_budget_sends_once() -> None:
    sends: list[str] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sends.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False, message_id=101)

    outcome = processor._apply_delivery_sla(
        processing_started_at=0.0,
        wait_budget_seconds=10.0,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=lambda message_id, payload: False,
        on_edit_failure=None,
        monotonic=_monotonic_sequence([1.0, 1.0]),
    )

    assert sends == ["final"]
    assert outcome.delivery_mode == "final_first_send"
    assert outcome.edit_applied is False
    assert outcome.result.message_id == 101


def test_delivery_timeout_sends_minimal_then_edits_same_message() -> None:
    sends: list[str] = []
    edited: list[tuple[int, str]] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sends.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False, message_id=202)

    def _edit(message_id: int, payload: TelegramPayload) -> bool:
        edited.append((message_id, payload.html_text))
        return True

    outcome = processor._apply_delivery_sla(
        processing_started_at=0.0,
        wait_budget_seconds=0.5,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=_edit,
        on_edit_failure=None,
        monotonic=_monotonic_sequence([1.0, 1.0]),
    )

    assert sends == ["minimal"]
    assert edited == [(202, "final")]
    assert outcome.delivery_mode == "minimal_then_edit"
    assert outcome.edit_applied is True


def test_edit_failure_does_not_trigger_second_send() -> None:
    sends: list[str] = []
    edit_calls: list[int] = []
    edit_failures: list[str] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sends.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False, message_id=303)

    def _edit(message_id: int, payload: TelegramPayload) -> bool:
        edit_calls.append(message_id)
        return False

    outcome = processor._apply_delivery_sla(
        processing_started_at=0.0,
        wait_budget_seconds=0.1,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=_edit,
        on_edit_failure=lambda reason: edit_failures.append(reason),
        monotonic=_monotonic_sequence([0.2, 0.2]),
    )

    assert sends == ["minimal"]
    assert edit_calls == [303]
    assert edit_failures == ["edit_failed"]
    assert outcome.result.delivered is True
    assert outcome.edit_applied is False


def test_progressive_message_edit() -> None:
    sends: list[str] = []
    edited: list[tuple[int, str]] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sends.append(payload.html_text)
        return DeliveryResult(delivered=True, retryable=False, message_id=404)

    def _edit(message_id: int, payload: TelegramPayload) -> bool:
        edited.append((message_id, payload.html_text))
        return True

    outcome = processor._apply_delivery_sla(
        processing_started_at=0.0,
        wait_budget_seconds=10.0,
        minimal_payload=_payload("📩 Письмо получено\nОбрабатываю вложения…"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=_edit,
        on_edit_failure=None,
        force_minimal_then_edit=True,
        monotonic=_monotonic_sequence([0.1, 0.2]),
    )

    assert sends == ["📩 Письмо получено\nОбрабатываю вложения…"]
    assert edited == [(404, "final")]
    assert outcome.delivery_mode == "minimal_then_edit"
    assert outcome.edit_applied is True


def test_progressive_message_only_one_message_id() -> None:
    sent_ids: list[int | None] = []
    edited_ids: list[int] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        result = DeliveryResult(delivered=True, retryable=False, message_id=505)
        sent_ids.append(result.message_id)
        return result

    def _edit(message_id: int, payload: TelegramPayload) -> bool:
        edited_ids.append(message_id)
        return True

    processor._apply_delivery_sla(
        processing_started_at=0.0,
        wait_budget_seconds=10.0,
        minimal_payload=_payload("progress"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=_edit,
        on_edit_failure=None,
        force_minimal_then_edit=True,
        monotonic=_monotonic_sequence([0.1, 0.2]),
    )

    assert sent_ids == [505]
    assert edited_ids == [505]
