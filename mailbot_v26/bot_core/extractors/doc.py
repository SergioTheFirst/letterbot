from __future__ import annotations

import io
import logging
import zipfile
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)
WORD_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def _safe_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _normalize_text(value: str) -> str:
    normalized = (value or "").encode("utf-8", "ignore").decode("utf-8", "ignore")
    return normalized.strip()


def _load_docx_parser():
    try:
        from docx import Document

        return Document
    except Exception:
        return None


def _load_docx2txt():
    try:
        import docx2txt

        return docx2txt
    except Exception:
        return None


def _extract_via_zip(file_bytes: bytes) -> str:
    """Основной метод - чистый ZIP без зависимостей"""
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            data = zf.read("word/document.xml")
    except Exception as e:
        logger.debug("ZIP extraction failed: %s", e)
        return ""

    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        logger.debug("XML parsing failed: %s", e)
        return ""

    runs = []
    # Извлекаем текст из всех параграфов
    for node in root.findall(".//w:t", WORD_NS):
        text = _safe_text(node.text)
        if text:
            runs.append(text)

    # Извлекаем текст из таблиц
    for table in root.findall(".//w:tbl", WORD_NS):
        for row in table.findall(".//w:tr", WORD_NS):
            row_texts = []
            for cell in row.findall(".//w:tc", WORD_NS):
                cell_text = " ".join(
                    _safe_text(t.text)
                    for t in cell.findall(".//w:t", WORD_NS)
                    if t.text
                )
                if cell_text:
                    row_texts.append(cell_text)
            if row_texts:
                runs.append(" | ".join(row_texts))

    return " ".join(runs)


def extract_doc(file_bytes: bytes, filename: str) -> str:
    """
    Извлечение текста из DOC/DOCX файлов.
    Приоритет: ZIP -> python-docx -> docx2txt
    """
    name = (filename or "").lower()

    # DOCX/DOCM - современный формат
    if name.endswith((".docx", ".docm")):
        # ПОПЫТКА 1: Чистый ZIP (без зависимостей)
        zipped_text = _extract_via_zip(file_bytes)
        if zipped_text.strip():
            text = _normalize_text(zipped_text)
            logger.debug("Extracted %d chars from %s (ZIP method)", len(text), filename)
            return text

        # ПОПЫТКА 2: python-docx (если установлен)
        document_cls = _load_docx_parser()
        if document_cls:
            try:
                doc = document_cls(io.BytesIO(file_bytes))
                parts: list[str] = []

                # Параграфы
                for p in doc.paragraphs:
                    if p.text and p.text.strip():
                        parts.append(_safe_text(p.text))

                # Таблицы
                for table in getattr(doc, "tables", []):
                    for row in table.rows:
                        cells = [
                            _safe_text(cell.text) for cell in row.cells if cell.text
                        ]
                        if any(cells):
                            parts.append(" | ".join([c for c in cells if c]))

                if parts:
                    text = _normalize_text("\n".join(parts))
                    logger.debug(
                        "Extracted %d chars from %s (python-docx)", len(text), filename
                    )
                    return text

            except Exception as e:
                logger.debug("python-docx failed: %s", e)

    # DOC - старый формат (только через docx2txt)
    if name.endswith(".doc"):
        docx2txt = _load_docx2txt()
        if docx2txt:
            try:
                raw = docx2txt.process(io.BytesIO(file_bytes)) or ""
                text = _normalize_text(str(raw))
                if text:
                    logger.debug(
                        "Extracted %d chars from %s (docx2txt)", len(text), filename
                    )
                    return text
            except Exception as e:
                logger.debug("docx2txt failed: %s", e)

    logger.warning("Failed to extract text from %s", filename)
    return ""


extract_docx_text = extract_doc
