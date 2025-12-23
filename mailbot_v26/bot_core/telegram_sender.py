"""Minimal Telegram send helper."""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request

from mailbot_v26.telegram_utils import telegram_safe


def send_telegram_message(bot_token: str, chat_id: str, text: str) -> bool:
    """Send message via Telegram HTTP API. Returns success flag."""
    if not bot_token or not chat_id or not text:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": telegram_safe(text),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
    )
    request = urllib.request.Request(url, data=payload.encode("utf-8"), method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            parsed = json.loads(body)
            return bool(parsed.get("ok"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError, ValueError):
        return False


__all__ = ["send_telegram_message"]
