PRODUCT_LABEL = "Letterbot Premium"
WATERMARK_LINE = "🔹 Powered by LetterBot.ru"
WATERMARK_HTML_LINE = "<i>🔹 Powered by LetterBot.ru</i>"
_LEGACY_WATERMARK_LINE = "🔹 Powered by Letterbot Premium"


def append_watermark(text: str, *, html: bool = False) -> str:
    watermark = WATERMARK_HTML_LINE if html else WATERMARK_LINE
    if (
        WATERMARK_LINE in text
        or WATERMARK_HTML_LINE in text
        or _LEGACY_WATERMARK_LINE in text
    ):
        return text
    if not text:
        return watermark
    return f"{text}\n{watermark}"
