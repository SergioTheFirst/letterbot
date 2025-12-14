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
        from docx import Document  # type: ignore

        return Document
    except Exception:
        return None


def _load_docx2txt():
    try:
        import docx2txt  # type: ignore

        return docx2txt
    except Exception:
        return None


def _extract_via_zip(file_bytes: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            data = zf.read("word/document.xml")
    except Exception:
        return ""

    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return ""

    runs = []
    for node in root.findall(".//w:t", WORD_NS):
        text = _safe_text(node.text)
        if text:
            runs.append(text)
    return " ".join(runs)


def extract_doc(file_bytes: bytes, filename: str) -> str:
    name = (filename or "").lower()

    if name.endswith((".docx", ".docm")):
        zipped_text = _extract_via_zip(file_bytes)
        if zipped_text.strip():
            text = _normalize_text(zipped_text)
            logger.debug("Extracted %d chars from %s", len(text), filename)
            return text

        document_cls = _load_docx_parser()
        if document_cls is None:
            logger.debug("Extracted 0 chars from %s", filename)
            return ""
        try:
            doc = document_cls(io.BytesIO(file_bytes))
            parts: list[str] = []
            parts.extend(
                _safe_text(p.text)
                for p in doc.paragraphs
                if p.text and p.text.strip()
            )
            for table in getattr(doc, "tables", []):
                for row in table.rows:
                    cells = [_safe_text(cell.text) for cell in row.cells if cell.text]
                    if any(cells):
                        parts.append(" | ".join([c for c in cells if c]))
            text = _normalize_text("\n".join([p for p in parts if p]))
            logger.debug("Extracted %d chars from %s", len(text), filename)
            return text
        except Exception:
            logger.debug("Extracted 0 chars from %s", filename)
            return ""

    if name.endswith(".doc"):
        docx2txt = _load_docx2txt()
        if docx2txt is None:
            logger.debug("Extracted 0 chars from %s", filename)
            return ""
        try:
            raw = docx2txt.process(io.BytesIO(file_bytes)) or ""
            text = _normalize_text(str(raw))
            logger.debug("Extracted %d chars from %s", len(text), filename)
            return text
        except Exception:
            logger.debug("Extracted 0 chars from %s", filename)
            return ""

    logger.debug("Extracted 0 chars from %s", filename)
    return ""


extract_docx_text = extract_doc
