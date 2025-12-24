from __future__ import annotations

import importlib.util
import logging
from dataclasses import dataclass

from mailbot_v26.pipeline.telegram_payload import TelegramPayload

requests_spec = importlib.util.find_spec("requests")
if requests_spec is None:
    requests = None
else:
    import requests

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TelegramSendResult:
    success: bool
    error: str | None = None
    status_code: int | None = None


def send_telegram(payload: TelegramPayload) -> TelegramSendResult:
    """
    Отправляет сообщение в Telegram.
    Возвращает явный результат (успех/ошибка).
    НИКОГДА не бросает исключения.
    """
    bot_token = payload.metadata.get("bot_token")
    chat_id = payload.metadata.get("chat_id")
    if not bot_token or not chat_id or not payload.html_text:
        log.error("Telegram send failed: empty token, chat_id or text")
        return TelegramSendResult(success=False, error="missing required fields")

    if requests is None:
        log.error("Telegram send failed: requests module not available")
        return TelegramSendResult(success=False, error="requests module not available")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": payload.html_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
    except Exception as exc:
        log.error("Telegram send exception: %s", exc)
        return TelegramSendResult(success=False, error=str(exc))

    if resp.status_code != 200:
        log.error(
            "Telegram HTTP error %s: %s",
            resp.status_code,
            resp.text,
        )
        return TelegramSendResult(
            success=False,
            error=resp.text,
            status_code=resp.status_code,
        )

    return TelegramSendResult(success=True)


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
