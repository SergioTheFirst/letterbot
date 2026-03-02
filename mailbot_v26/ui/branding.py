PRODUCT_LABEL = "Letterbot Premium"
WATERMARK_LINE = "🔹 Powered by Letterbot Premium"


def append_watermark(text: str) -> str:
    if WATERMARK_LINE in text:
        return text
    if not text:
        return WATERMARK_LINE
    return f"{text}\n{WATERMARK_LINE}"
