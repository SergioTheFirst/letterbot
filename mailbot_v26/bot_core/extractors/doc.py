from __future__ import annotations

import io
import zipfile
from xml.etree import ElementTree as ET


WORD_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def _safe_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


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
            return zipped_text

        document_cls = _load_docx_parser()
        if document_cls is None:
            return ""
        try:
            doc = document_cls(io.BytesIO(file_bytes))
            return "\n".join(
                p.text for p in doc.paragraphs if p.text and p.text.strip()
            )
        except Exception:
            return ""

    if name.endswith(".doc"):
        docx2txt = _load_docx2txt()
        if docx2txt is None:
            return ""
        try:
            return docx2txt.process(io.BytesIO(file_bytes))
        except Exception:
            return ""

    return ""


extract_docx_text = extract_doc
