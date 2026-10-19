"""One Telegram delivery per email: reservation around every per-email send.

Every path that sends an email's notification (premium processor, legacy TG
stage, fail-open) asks the guard right before the POST and reports the result
right after it. The reservation row (``telegram_delivery_log``, key
``email:{id}``) is committed before the POST, so a crash leaves a visible
tombstone instead of a silent resend.

Outcomes (``guard.outcome``):
- ``none``: nothing was posted (yet).
- ``delivered``: Telegram returned 200; the reservation is confirmed.
- ``duplicate``: an earlier, confirmed delivery exists; nothing was posted.
- ``unknown``: the message may be in the chat (reply lost, or an unconfirmed
  reservation from a crash); never resend, never send a "failed" notice.
- ``failed``: Telegram rejected it (definitely not shown); key released.
- ``retryable``: definitely not sent (429/5xx/connect error, or the
  reservation store is unavailable); key released, retry later.

Trade-off: at-most-once. When the store cannot reserve, nothing is posted
(fail closed) and the caller retries through its normal backoff.
"""

from __future__ import annotations

import logging
from typing import Any

from mailbot_v26.worker.telegram_sender import DeliveryResult

logger = logging.getLogger("mailbot")

DELIVERY_LOG_UNAVAILABLE = "delivery_log_unavailable"
DUPLICATE_SUPPRESSED = "duplicate_suppressed"
DELIVERY_UNKNOWN = "delivery_unknown"
# Outcomes after which the email must never be posted again.
SETTLED_OUTCOMES = frozenset({"delivered", "duplicate", "unknown"})


def email_delivery_key(email_id: int) -> str:
    return f"email:{email_id}"


def delivery_unknown_error_class(error: str | None) -> str:
    """Exception class behind a maybe-delivered result (no message text)."""
    text = str(error or "")
    prefix = f"{DELIVERY_UNKNOWN}:"
    if text.startswith(prefix):
        return text[len(prefix) :] or "unknown"
    if text == DUPLICATE_SUPPRESSED:
        return "UnconfirmedReservation"
    return "unknown"


def _blocked_result(error: str, *, retryable: bool) -> DeliveryResult:
    return DeliveryResult(
        delivered=False,
        retryable=retryable,
        error=error,
        maybe_delivered=not retryable,
    )


class EmailDeliveryGuard:
    def __init__(
        self,
        storage: Any,
        *,
        email_id: int,
        account_id: str | None,
        chat_id: str | None,
        delivery_key: str | None = None,
    ) -> None:
        self._storage = storage
        self.email_id = int(email_id)
        self.account_id = account_id
        self.chat_id = chat_id
        self.delivery_key = delivery_key or email_delivery_key(self.email_id)
        self._reserved = False
        self._outcome = "none"
        self._error: str | None = None
        self._retry_after: float | None = None
        self._message_id: int | None = None

    @property
    def outcome(self) -> str:
        return self._outcome

    @property
    def error(self) -> str | None:
        return self._error

    @property
    def retry_after(self) -> float | None:
        return self._retry_after

    @property
    def message_id(self) -> int | None:
        return self._message_id

    @property
    def is_settled(self) -> bool:
        return self._outcome in SETTLED_OUTCOMES

    def before_send(self) -> DeliveryResult | None:
        """None: reservation held, go POST. Otherwise the result to use instead."""
        if self.is_settled:
            return _blocked_result(DUPLICATE_SUPPRESSED, retryable=False)
        if self._reserved:
            return None
        try:
            state = self._storage.reserve_telegram_delivery(
                delivery_key=self.delivery_key,
                email_id=self.email_id,
                account_id=self.account_id,
                chat_id=self.chat_id,
                kind="email",
            )
        except Exception:
            logger.exception(
                "telegram_delivery_reserve_failed email_id=%s delivery_key=%s",
                self.email_id,
                self.delivery_key,
            )
            state = "unavailable"
        if state == "reserved":
            self._reserved = True
            return None
        if state == "duplicate":
            return self._on_duplicate()
        return self._on_unavailable()

    def _on_duplicate(self) -> DeliveryResult:
        try:
            state = self._storage.get_telegram_delivery_state(
                self.delivery_key, self.email_id
            )
        except Exception:
            logger.exception(
                "telegram_delivery_state_read_failed email_id=%s delivery_key=%s",
                self.email_id,
                self.delivery_key,
            )
            return self._on_unavailable()
        self._outcome = "duplicate" if state == "confirmed" else "unknown"
        self._error = DUPLICATE_SUPPRESSED
        logger.warning(
            "telegram_delivery_reservation_exists email_id=%s delivery_key=%s state=%s",
            self.email_id,
            self.delivery_key,
            state or "missing",
        )
        return _blocked_result(DUPLICATE_SUPPRESSED, retryable=False)

    def _on_unavailable(self) -> DeliveryResult:
        self._outcome = "retryable"
        self._error = DELIVERY_LOG_UNAVAILABLE
        logger.warning(
            "telegram_delivery_dedup_unavailable email_id=%s account_id=%s delivery_key=%s",
            self.email_id,
            self.account_id or "",
            self.delivery_key,
        )
        return _blocked_result(DELIVERY_LOG_UNAVAILABLE, retryable=True)

    def after_send(self, result: Any) -> None:
        """Record what the POST returned (call exactly once per POST)."""
        delivered = bool(getattr(result, "delivered", False))
        error = getattr(result, "error", None)
        if delivered:
            self._outcome = "delivered"
            self._error = None
            self._message_id = getattr(result, "message_id", None)
            self._confirm()
            return
        self._error = str(error or "telegram delivery failed")
        if getattr(result, "maybe_delivered", False):
            # Keep the reservation as a tombstone: the message may be in the chat.
            self._outcome = "unknown"
            logger.error(
                "telegram_delivery_unknown email_id=%s delivery_key=%s error_class=%s",
                self.email_id,
                self.delivery_key,
                delivery_unknown_error_class(self._error),
            )
            return
        retryable = bool(getattr(result, "retryable", False))
        self._outcome = "retryable" if retryable else "failed"
        self._retry_after = getattr(result, "retry_after", None)
        self._release()

    def abort(self) -> None:
        """The caller did not POST after all (e.g. rendering failed): drop the key."""
        if self._outcome == "none":
            self._release()

    def _confirm(self) -> None:
        message_id = self._message_id
        try:
            self._storage.mark_telegram_delivered(self.email_id)
            if self._reserved:
                self._storage.finalize_telegram_delivery(
                    delivery_key=self.delivery_key,
                    telegram_message_id=(
                        str(message_id) if message_id is not None else None
                    ),
                )
        except Exception:
            # The message is in the chat; never retry the send because of this.
            logger.exception(
                "telegram_delivery_confirm_failed email_id=%s delivery_key=%s",
                self.email_id,
                self.delivery_key,
            )

    def _release(self) -> None:
        if not self._reserved:
            return
        self._reserved = False
        try:
            self._storage.release_telegram_delivery(delivery_key=self.delivery_key)
        except Exception:
            # The row stays: a later attempt sees an unconfirmed reservation and
            # reports DELIVERY_UNKNOWN instead of sending twice.
            logger.exception(
                "telegram_delivery_release_failed email_id=%s delivery_key=%s",
                self.email_id,
                self.delivery_key,
            )


__all__ = [
    "DELIVERY_LOG_UNAVAILABLE",
    "DELIVERY_UNKNOWN",
    "DUPLICATE_SUPPRESSED",
    "EmailDeliveryGuard",
    "SETTLED_OUTCOMES",
    "delivery_unknown_error_class",
    "email_delivery_key",
]
