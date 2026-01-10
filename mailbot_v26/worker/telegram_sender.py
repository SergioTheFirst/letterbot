from __future__ import annotations

import importlib.util
import logging
import json
import re
from dataclasses import dataclass

from mailbot_v26.pipeline.telegram_payload import TelegramPayload

requests_spec = importlib.util.find_spec("requests")
if requests_spec is None:
    requests = None
else:
    import requests

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    delivered: bool
    retryable: bool
    error: str | None = None
    mode: str = "html"
    retry_count: int = 0
    message_id: int | None = None


def _is_retryable_status(status_code: int) -> bool:
    return status_code >= 500 or status_code == 429


def _strip_html_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")


def _looks_like_parse_error(error_text: str) -> bool:
    lowered = (error_text or "").lower()
    return "unsupported start tag" in lowered or "can't parse" in lowered


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
    if not bot_token or not chat_id or not payload.html_text:
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

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    try:
        resp = _post_message(
            url=url,
            chat_id=chat_id,
            text=payload.html_text,
            parse_mode="HTML",
            reply_markup=payload.reply_markup,
        )
    except Exception as exc:
        log.error("Telegram send exception: %s", exc)
        log.info(
            "tg_delivery_final",
            extra={
                "delivered": False,
                "retryable": True,
                "error": str(exc),
            },
        )
        return DeliveryResult(
            delivered=False,
            retryable=True,
            error=str(exc),
            mode="html",
            retry_count=1,
            message_id=None,
        )

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
            stripped_text = _strip_html_tags(payload.html_text)
            try:
                salvage_resp = _post_message(
                    url=url,
                    chat_id=chat_id,
                    text=stripped_text,
                    parse_mode=None,
                    reply_markup=payload.reply_markup,
                )
            except Exception as exc:
                log.error("Telegram salvage exception: %s", exc)
                log.info(
                    "tg_delivery_final",
                    extra={
                        "delivered": False,
                        "retryable": True,
                        "error": str(exc),
                    },
                )
                return DeliveryResult(
                    delivered=False,
                    retryable=True,
                    error=str(exc),
                    mode="plain_salvage",
                    retry_count=1,
                    message_id=None,
                )
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
    )


def edit_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    html_text: str,
    reply_markup: dict[str, object] | None = None,
) -> bool:
    if not bot_token or not chat_id or not message_id or not html_text:
        log.error(
            "telegram_edit_failed",
            chat_id=chat_id,
            message_id=message_id,
            reason="missing required fields",
        )
        return False
    if requests is None:
        log.error("Telegram edit failed: requests module not available")
        return False
    url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": html_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=15)
    except Exception as exc:
        log.error("Telegram edit exception: %s", exc)
        return False
    if resp.status_code != 200:
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
        log.error("Telegram ping exception: %s", exc)
        return False, f"exception: {exc}"

    if resp.status_code != 200:
        log.error("Telegram ping HTTP error %s: %s", resp.status_code, resp.text)
        return False, f"http {resp.status_code}"

    return True, "reachable"
