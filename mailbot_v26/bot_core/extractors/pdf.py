"""
PDF extractor for MailBot v26.

DLL-free, Windows-friendly, RAM-safe.

Использует только:
- pypdf (основной путь для текстовых PDF)
- pikepdf (fallback для странных/частично сломанных PDF)
- pdfminer.six (fallback для XFA/ЭЦП документов)
- опционально OCR через EasyOCR (если RAM позволяет)

Соответствует КОНСТИТУЦИИ:
- никаких внешних утилит извлечения текста из PDF (EXE/poppler/DLL)
- только чистый Python и pip-зависимости
- OCR включается ТОЛЬКО при достаточном объёме свободной RAM
"""

from __future__ import annotations

import io
import logging
from typing import List

try:
    from pypdf import PdfReader
except ImportError:  # fail-safe, обработаем ниже
    PdfReader = None  # type: ignore

try:
    import pikepdf
except ImportError:
    pikepdf = None  # type: ignore


logger = logging.getLogger(__name__)

PDF_ZERO_REASON_PYPDF_EMPTY = "pypdf_empty"
PDF_ZERO_REASON_PIKEPDF_EMPTY = "pikepdf_empty"
PDF_ZERO_REASON_PDFMINER_EMPTY = "pdfminer_empty"
PDF_ZERO_REASON_IMAGE_ONLY = "image_only"
PDF_ZERO_REASON_ENCRYPTED = "encrypted"
PDF_ZERO_REASON_BROKEN = "broken_pdf"
PDF_ZERO_REASON_ALL_EMPTY = "all_extractors_empty"


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


def _extract_with_pdfminer(file_bytes: bytes) -> str:
    """
    Третий fallback: pdfminer для XFA/encrypted/signed PDF.
    Использует LAParams для лучшего извлечения таблиц.
    """
    try:
        from pdfminer.high_level import extract_text_to_fp
        from pdfminer.layout import LAParams
        import io as _io

        output = _io.StringIO()
        extract_text_to_fp(
            _io.BytesIO(file_bytes),
            output,
            laparams=LAParams(),
            output_type="text",
            codec="utf-8",
        )
        return output.getvalue().strip()
    except ImportError:
        return ""
    except Exception as e:
        logger.warning("pdfminer extract failed: %s", e)
        return ""


def _ocr_pdf_if_possible(file_bytes: bytes) -> str:
    """OCR disabled per CONSTITUTION (torch forbidden)."""
    return ""


def _looks_encrypted_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "encrypt" in message or "password" in message


def _is_image_only_pdf(file_bytes: bytes) -> bool:
    if PdfReader is None:
        return False
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except Exception:
        return False
    pages = list(reader.pages)
    if not pages:
        return False

    for page in pages:
        try:
            resources = page.get("/Resources") or {}
            fonts = resources.get("/Font") if hasattr(resources, "get") else None
            if fonts:
                return False
            extracted = page.extract_text() or ""
            if extracted.strip():
                return False
        except Exception:
            return False
    return True


def extract_pdf(file_bytes: bytes, filename: str) -> tuple[str, str]:
    """
    Returns (text, zero_reason).
    zero_reason is "" if text is non-empty.
    """
    name = (filename or "").lower()
    if not name.endswith(".pdf"):
        return "", PDF_ZERO_REASON_BROKEN

    try:
        text = _extract_with_pypdf(file_bytes)
    except Exception as exc:
        reason = PDF_ZERO_REASON_ENCRYPTED if _looks_encrypted_error(exc) else PDF_ZERO_REASON_BROKEN
        logger.warning("pypdf extraction exception for %s: %s", filename, exc)
        return "", reason
    if text.strip():
        return text, ""

    try:
        text = _extract_with_pikepdf(file_bytes)
    except Exception as exc:
        reason = PDF_ZERO_REASON_ENCRYPTED if _looks_encrypted_error(exc) else PDF_ZERO_REASON_BROKEN
        logger.warning("pikepdf extraction exception for %s: %s", filename, exc)
        return "", reason
    if text.strip():
        return text, ""

    try:
        text = _extract_with_pdfminer(file_bytes)
    except Exception:
        text = ""
    if text.strip():
        return text, ""

    ocr_text = _ocr_pdf_if_possible(file_bytes)
    if ocr_text.strip():
        return ocr_text, ""

    if _is_image_only_pdf(file_bytes):
        return "", PDF_ZERO_REASON_IMAGE_ONLY

    return "", PDF_ZERO_REASON_ALL_EMPTY


def extract_pdf_text(file_bytes: bytes, filename: str) -> str:
    text, _reason = extract_pdf(file_bytes, filename)
    return text
