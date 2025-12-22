import logging

try:
    import requests
except Exception:
    requests = None

log = logging.getLogger(__name__)


def send_telegram(bot_token: str, chat_id: str, text: str) -> bool:
    """
    Отправляет сообщение в Telegram.
    НИЧЕГО не форматирует.
    НИКОГДА не бросает исключения.
    """
    if not bot_token or not chat_id or not text:
        log.error("Telegram send failed: empty token, chat_id or text")
        return False

    if not requests:
        log.error("Telegram send failed: requests module not available")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
    except Exception as exc:
        log.error("Telegram send exception: %s", exc)
        return False

    if resp.status_code != 200:
        log.error(
            "Telegram HTTP error %s: %s",
            resp.status_code,
            resp.text,
        )
        return False

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
