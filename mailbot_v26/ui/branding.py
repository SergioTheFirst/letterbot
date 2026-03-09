from functools import lru_cache

from mailbot_v26.config_loader import load_branding_config

PRODUCT_LABEL = "Letterbot Premium"
WATERMARK_LINE = "🔹 Powered by LetterBot.ru"
WATERMARK_HTML_LINE = "<i>🔹 Powered by LetterBot.ru</i>"
_LEGACY_WATERMARK_LINE = "🔹 Powered by Letterbot Premium"
_PLAIN_WATERMARK_LINE = "Powered by LetterBot.ru"
_PLAIN_WATERMARK_HTML_LINE = "<i>Powered by LetterBot.ru</i>"


@lru_cache(maxsize=1)
def _watermark_enabled() -> bool:
    return load_branding_config().show_watermark


def reset_branding_cache() -> None:
    _watermark_enabled.cache_clear()


def append_watermark(text: str, *, html: bool = False) -> str:
    if not _watermark_enabled():
        return text
    watermark = WATERMARK_HTML_LINE if html else WATERMARK_LINE
    if (
        WATERMARK_LINE in text
        or WATERMARK_HTML_LINE in text
        or _LEGACY_WATERMARK_LINE in text
        or _PLAIN_WATERMARK_LINE in text
        or _PLAIN_WATERMARK_HTML_LINE in text
    ):
        return text
    if not text:
        return watermark
    return f"{text}\n{watermark}"


__all__ = [
    "PRODUCT_LABEL",
    "WATERMARK_LINE",
    "WATERMARK_HTML_LINE",
    "append_watermark",
    "reset_branding_cache",
]
