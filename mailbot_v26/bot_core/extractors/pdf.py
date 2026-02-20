"""
PDF extractor for MailBot v26.

DLL-free, Windows-friendly, RAM-safe.

Использует только:
- pypdf (основной путь для текстовых PDF)
- pikepdf (fallback для странных/частично сломанных PDF)
- опционально OCR через EasyOCR (если RAM позволяет)

Соответствует КОНСТИТУЦИИ:
- никаких внешних PDF-to-text EXE / poppler / DLL
- только чистый Python и pip-зависимости
- OCR включается ТОЛЬКО при достаточном объёме свободной RAM
"""

from __future__ import annotations

import io
import logging
from typing import Optional, List

try:
    from pypdf import PdfReader
except ImportError:  # fail-safe, обработаем ниже
    PdfReader = None  # type: ignore

try:
    import pikepdf
except ImportError:
    pikepdf = None  # type: ignore


logger = logging.getLogger(__name__)


def _safe_join(chunks: List[str], limit: int = 50_000) -> str:
    """Аккуратно склеивает куски текста с жёстким лимитом длины."""
    out: List[str] = []
    total = 0
    for part in chunks:
        if not part:
            continue
        remaining = limit - total
        if remaining <= 0:
            break
        if len(part) > remaining:
            out.append(part[:remaining])
            total += remaining
            break
        out.append(part)
        total += len(part)
    return "\n".join(out)


def _extract_with_pypdf(file_bytes: bytes) -> str:
    """Попытаться вытащить текст с помощью pypdf."""
    if PdfReader is None:
        return ""

    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except Exception as e:
        logger.warning("pypdf open failed: %s", e)
        return ""

    chunks: List[str] = []
    for page in reader.pages:
        try:
            txt = page.extract_text() or ""
        except Exception as e:
            logger.debug("pypdf page extract failed: %s", e)
            txt = ""
        if txt.strip():
            chunks.append(txt)

    text = _safe_join(chunks, limit=50_000)
    return text


def _extract_with_pikepdf(file_bytes: bytes) -> str:
    """Fallback-доставка: пробуем pikepdf, если pypdf не дал текста."""
    if pikepdf is None:
        return ""

    try:
        pdf = pikepdf.open(io.BytesIO(file_bytes))
    except Exception as e:
        logger.warning("pikepdf open failed: %s", e)
        return ""
    chunks: List[str] = []

    try:
        for page in pdf.pages:
            try:
                # это очень грубый способ, но иногда вытаскивает текст
                contents = page.get("/Contents", None)
                if contents is None:
                    continue
                s = str(contents)
                if s.strip():
                    chunks.append(s)
            except Exception:
                continue
    finally:
        pdf.close()

    return _safe_join(chunks, limit=50_000)


def _ocr_pdf_if_possible(file_bytes: bytes) -> str:
    """OCR disabled per CONSTITUTION (torch forbidden)."""
    return ""


def extract_pdf(file_bytes: bytes, filename: str) -> str:
    """
    Главная точка входа.

    Порядок:
    1. pypdf — быстрый, дешёвый, для 90% PDF
    2. pikepdf — fallback для странных/сломаных PDF
    3. EasyOCR — только если RAM ≥ 800 МБ и два первых способа дали пустоту
    """
    name = (filename or "").lower()
    if not name.endswith(".pdf"):
        # Защита от неправильного вызова
        return ""

    # 1. pypdf
    text = _extract_with_pypdf(file_bytes)
    if text.strip():
        return text

    # 2. pikepdf fallback
    text = _extract_with_pikepdf(file_bytes)
    if text.strip():
        return text

    # 3. OCR как последний шанс
    ocr_text = _ocr_pdf_if_possible(file_bytes)
    return ocr_text or ""


extract_pdf_text = extract_pdf
