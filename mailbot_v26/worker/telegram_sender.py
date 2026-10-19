from __future__ import annotations

import importlib.util
import logging
import json
import re
import ssl
from dataclasses import dataclass

from mailbot_v26.observability.redact import redact_secrets
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.telegram_utils import cap_telegram_text, telegram_text_units
from mailbot_v26.text.mojibake import normalize_mojibake_text

requests_spec = importlib.util.find_spec("requests")
if requests_spec is None:
    requests = None
    _requests_exceptions = None
    _urllib3_exceptions = None
else:
    import requests
    from requests import exceptions as _requests_exceptions

    try:
        from urllib3 import exceptions as _urllib3_exceptions
    except ImportError:  # pragma: no cover - urllib3 ships with requests
        _urllib3_exceptions = None

log = logging.getLogger(__name__)

_SEND_NOT_SENT_RETRYABLE = "not_sent_retryable"
_SEND_NOT_SENT_FINAL = "not_sent_final"
_SEND_MAYBE_DELIVERED = "maybe_delivered"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    delivered: bool
    retryable: bool
    error: str | None = None
    mode: str = "html"
    retry_count: int = 0
    message_id: int | None = None
    status_code: int | None = None
    retry_after: float | None = None
    # Telegram may have shown the message (request sent, reply lost): never resend.
    maybe_delivered: bool = False


def _is_retryable_status(status_code: int) -> bool:
    return status_code >= 500 or status_code == 429


def _exception_chain(exc: BaseException) -> list[BaseException]:
    """exc plus what it wraps (args[0], urllib3 .reason, __cause__/__context__)."""
    chain: list[BaseException] = []
    pending: list[object] = [exc]
    while pending and len(chain) < 16:
        current = pending.pop(0)
        if not isinstance(current, BaseException) or any(
            current is seen for seen in chain
        ):
            continue
        chain.append(current)
        args = getattr(current, "args", ())
        pending.extend(
            [
                args[0] if args else None,
                getattr(current, "reason", None),
                current.__cause__,
                current.__context__,
            ]
        )
    return chain


def _wraps_connect_failure(exc: BaseException) -> bool:
    if _urllib3_exceptions is None:
        return False
    args = getattr(exc, "args", ())
    wrapped = args[0] if args else None
    if not isinstance(wrapped, _urllib3_exceptions.MaxRetryError):
        return False
    return isinstance(
        wrapped.reason,
        (
            _urllib3_exceptions.NewConnectionError,
            _urllib3_exceptions.ConnectTimeoutError,
        ),
    )


def _classify_send_exception(exc: BaseException) -> str:
    """Whether a failed sendMessage POST can have reached Telegram.

    Only errors raised before the request left this machine count as not sent;
    anything else (read timeout, reset after sending, unknown) may have been
    delivered and must never be resent.
    """
    rexc = _requests_exceptions
    if rexc is None:
        return _SEND_MAYBE_DELIVERED
    if isinstance(exc, (rexc.InvalidURL, rexc.MissingSchema, rexc.InvalidHeader)):
        return _SEND_NOT_SENT_FINAL
    if isinstance(exc, rexc.ReadTimeout):
        return _SEND_MAYBE_DELIVERED
    if isinstance(exc, (rexc.ConnectTimeout, rexc.ProxyError)):
        return _SEND_NOT_SENT_RETRYABLE
    if isinstance(exc, rexc.SSLError):
        if any(
            isinstance(item, ssl.SSLCertVerificationError)
            for item in _exception_chain(exc)
        ):
            return _SEND_NOT_SENT_RETRYABLE
        return _SEND_MAYBE_DELIVERED
    if isinstance(exc, rexc.ConnectionError) and _wraps_connect_failure(exc):
        return _SEND_NOT_SENT_RETRYABLE
    return _SEND_MAYBE_DELIVERED


def _send_exception_result(exc: BaseException, *, mode: str) -> DeliveryResult:
    # requests puts the URL (with /bot<TOKEN>/) into the exception text.
    error_text = redact_secrets(exc)
    kind = _classify_send_exception(exc)
    if kind == _SEND_MAYBE_DELIVERED:
        log.error(
            "Telegram send outcome unknown (%s): %s", type(exc).__name__, error_text
        )
        result = DeliveryResult(
            delivered=False,
            retryable=False,
            error=f"delivery_unknown:{type(exc).__name__}",
            mode=mode,
            retry_count=1,
            message_id=None,
            maybe_delivered=True,
        )
    else:
        log.error("Telegram send exception: %s", error_text)
        result = DeliveryResult(
            delivered=False,
            retryable=kind == _SEND_NOT_SENT_RETRYABLE,
            error=error_text,
            mode=mode,
            retry_count=1,
            message_id=None,
        )
    log.info(
        "tg_delivery_final",
        extra={
            "delivered": False,
            "retryable": result.retryable,
            "maybe_delivered": result.maybe_delivered,
            "error": result.error,
        },
    )
    return result


def _strip_html_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")


def _sanitize_user_facing_text(text: str) -> str:
    return normalize_mojibake_text(text)


def _cap_outgoing_text(text: str, *, kind: str) -> str:
    # Safety net: callers cap already; an over-long text would be a lost 400.
    capped = cap_telegram_text(text)
    if capped != text:
        log.warning(
            "telegram_text_capped kind=%s original_units=%s capped_units=%s",
            kind,
            telegram_text_units(text),
            telegram_text_units(capped),
        )
    return capped


def _looks_like_parse_error(error_text: str) -> bool:
    lowered = (error_text or "").lower()
    return "unsupported start tag" in lowered or "can't parse" in lowered


def _is_message_not_modified(status_code: int, error_text: str) -> bool:
    # Telegram rejects an edit whose text and markup are unchanged; the message
    # already shows the requested content, so the edit counts as applied.
    lowered = (error_text or "").lower()
    return status_code == 400 and "message is not modified" in lowered


def _post_message(
    *,
    url: str,
    chat_id: str,
    text: str,
    parse_mode: str | None,
    reply_markup: dict[str, object] | None,
) -> "requests.Response":
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return requests.post(url, json=payload, timeout=15)


def _extract_message_id(resp: "requests.Response") -> int | None:
    json_loader = getattr(resp, "json", None)
    if not callable(json_loader):
        return None
    try:
        payload = json_loader()
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    result = payload.get("result")
    if isinstance(result, dict):
        message_id = result.get("message_id")
        try:
            return int(message_id)
        except (TypeError, ValueError):
            return None
    return None


def send_telegram(payload: TelegramPayload) -> DeliveryResult:
    """
    Отправляет сообщение в Telegram.
    Возвращает явный результат (успех/ошибка).
    НИКОГДА не бросает исключения.
    """
    bot_token = payload.metadata.get("bot_token")
    chat_id = payload.metadata.get("chat_id")
    normalized_text = _sanitize_user_facing_text(payload.html_text)
    if not bot_token or not chat_id or not normalized_text:
        log.error("Telegram send failed: empty token, chat_id or text")
        return DeliveryResult(
            delivered=False,
            retryable=False,
            error="missing required fields",
            mode="html",
            retry_count=0,
            message_id=None,
        )

    if requests is None:
        log.error("Telegram send failed: requests module not available")
        return DeliveryResult(
            delivered=False,
            retryable=False,
            error="requests module not available",
            mode="html",
            retry_count=0,
            message_id=None,
        )

    normalized_text = _cap_outgoing_text(normalized_text, kind="send")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    try:
        resp = _post_message(
            url=url,
            chat_id=chat_id,
            text=normalized_text,
            parse_mode="HTML",
            reply_markup=payload.reply_markup,
        )
    except Exception as exc:
        return _send_exception_result(exc, mode="html")

    if resp.status_code != 200:
        log.error(
            "Telegram HTTP error %s: %s",
            resp.status_code,
            resp.text,
        )
        if resp.status_code == 400 and _looks_like_parse_error(resp.text):
            log.warning(
                "[TG-SALVAGE] parse error, retrying as plain text",
            )
            stripped_text = _strip_html_tags(normalized_text)
            try:
                salvage_resp = _post_message(
                    url=url,
                    chat_id=chat_id,
                    text=stripped_text,
                    parse_mode=None,
                    reply_markup=payload.reply_markup,
                )
            except Exception as exc:
                return _send_exception_result(exc, mode="plain_salvage")
            if salvage_resp.status_code == 200:
                salvage_message_id = _extract_message_id(salvage_resp)
                log.info(
                    "tg_salvage_sent",
                    extra={"chat_id": chat_id},
                )
                log.info(
                    "tg_delivery_final",
                    extra={
                        "delivered": True,
                        "retryable": False,
                        "error": None,
                        "message_id": salvage_message_id,
                    },
                )
                return DeliveryResult(
                    delivered=True,
                    retryable=False,
                    error=None,
                    mode="plain_salvage",
                    retry_count=1,
                    message_id=salvage_message_id,
                    status_code=salvage_resp.status_code,
                )
            log.error(
                "Telegram salvage HTTP error %s: %s",
                salvage_resp.status_code,
                salvage_resp.text,
            )
            retryable = _is_retryable_status(salvage_resp.status_code)
            log.info(
                "tg_delivery_final",
                extra={
                    "delivered": False,
                    "retryable": retryable,
                    "error": salvage_resp.text,
                },
            )
            return DeliveryResult(
                delivered=False,
                retryable=retryable,
                error=salvage_resp.text,
                mode="plain_salvage",
                retry_count=1,
                message_id=None,
                status_code=salvage_resp.status_code,
            )
        retryable = _is_retryable_status(resp.status_code)
        log.info(
            "tg_delivery_final",
            extra={
                "delivered": False,
                "retryable": retryable,
                "error": resp.text,
            },
        )
        return DeliveryResult(
            delivered=False,
            retryable=retryable,
            error=resp.text,
            mode="html",
            retry_count=0,
            message_id=None,
            status_code=resp.status_code,
        )

    message_id = _extract_message_id(resp)
    log.info(
        "tg_delivery_final",
        extra={
            "delivered": True,
            "retryable": False,
            "error": None,
            "message_id": message_id,
        },
    )
    return DeliveryResult(
        delivered=True,
        retryable=False,
        error=None,
        mode="html",
        retry_count=0,
        message_id=message_id,
        status_code=resp.status_code,
    )


def edit_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    html_text: str,
    reply_markup: dict[str, object] | None = None,
) -> bool:
    """Edit a sent message; True when it now shows the requested content.

    Callers must check the result: False means Telegram did not apply the edit
    (the reason is logged here).
    """
    normalized_text = _sanitize_user_facing_text(html_text)
    if not bot_token or not chat_id or not message_id or not normalized_text:
        log.error(
            "telegram_edit_failed chat_id=%s message_id=%s reason=%s",
            chat_id,
            message_id,
            "missing required fields",
        )
        return False
    if requests is None:
        log.error("Telegram edit failed: requests module not available")
        return False
    normalized_text = _cap_outgoing_text(normalized_text, kind="edit")
    url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": normalized_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=15)
    except Exception as exc:
        log.error("Telegram edit exception: %s", redact_secrets(exc))
        return False
    if resp.status_code != 200:
        if _is_message_not_modified(resp.status_code, resp.text):
            log.info(
                "tg_edit_not_modified",
                extra={"chat_id": chat_id, "message_id": message_id},
            )
            return True
        log.error(
            "Telegram edit HTTP error %s: %s",
            resp.status_code,
            resp.text,
        )
        return False
    log.info(
        "tg_edit_applied",
        extra={"chat_id": chat_id, "message_id": message_id},
    )
    return True


def ping_telegram(bot_token: str) -> tuple[bool, str]:
    """
    Проверяет доступность Telegram API для бота.
    НИЧЕГО не отправляет.
    НИКОГДА не бросает исключения.
    """
    if not bot_token:
        log.error("Telegram ping failed: empty token")
        return False, "missing bot token"

    if not requests:
        log.error("Telegram ping failed: requests module not available")
        return False, "requests module not available"

    url = f"https://api.telegram.org/bot{bot_token}/getMe"
    try:
        resp = requests.get(url, timeout=10)
    except Exception as exc:
        error_text = redact_secrets(exc)
        log.error("Telegram ping exception: %s", error_text)
        return False, f"exception: {error_text}"

    if resp.status_code != 200:
        log.error("Telegram ping HTTP error %s: %s", resp.status_code, resp.text)
        return False, f"http {resp.status_code}"

    return True, "reachable"
