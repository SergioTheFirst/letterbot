"""Deterministic attachment classifier with Constitution safeguards.

Section IV: attachments are classified purely by heuristics (no ML).
Section II.3: deterministic, low-memory rules only.
Section VIII: guaranteed mode—safe handling of corrupt files.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple


Category = str

# Extension → category mapping (deterministic, no guessing)
_EXTENSION_MAP: dict[str, Category] = {
    ".pdf": "PDF",
    ".docx": "DOCX",
    ".doc": "DOC",
    ".xlsx": "XLSX",
    ".xls": "XLS",
    ".png": "IMAGE",
    ".jpg": "IMAGE",
    ".jpeg": "IMAGE",
    ".tif": "IMAGE",
    ".tiff": "IMAGE",
    ".bmp": "IMAGE",
    ".gif": "IMAGE",
    ".txt": "TEXT",
}

# MIME → category mapping
_MIME_MAP: dict[str, Category] = {
    "application/pdf": "PDF",
    "application/msword": "DOC",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "DOCX",
    "application/vnd.ms-excel": "XLS",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "XLSX",
    "image/png": "IMAGE",
    "image/jpeg": "IMAGE",
    "image/tiff": "IMAGE",
    "image/gif": "IMAGE",
    "image/bmp": "IMAGE",
    "text/plain": "TEXT",
}

# Magic prefix bytes for quick sniffing
_PDF_MAGIC = b"%PDF-"
_ZIP_MAGIC = b"PK\x03\x04"
_DOC_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"  # OLE Compound File (legacy DOC/XLS)
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_JPEG_MAGIC = b"\xff\xd8\xff"
_GIF_MAGIC = b"GIF8"
_TIFF_MAGIC = (b"II*\x00", b"MM\x00*")
_TEXT_PRINTABLE = set(range(0x20, 0x7F)) | {0x09, 0x0A, 0x0D}


@dataclass(frozen=True)
class AttachmentProbe:
    filename: str
    mime_type: str = ""
    content_bytes: bytes = b""

    def suffix(self) -> str:
        return Path(self.filename or "").suffix.lower()

    def prefix(self, length: int = 12) -> bytes:
        return (self.content_bytes or b"")[:length]


def classify_attachment(filename: str = "", mime_type: str = "", content_bytes: bytes = b"") -> Category:
    """Classify an attachment without ML.

    Priority order: MIME → extension → magic sniff → text heuristic → UNKNOWN.
    """

    probe = AttachmentProbe(filename=filename or "", mime_type=(mime_type or "").lower(), content_bytes=content_bytes or b"")

    # 1) MIME type (already authoritative)
    if probe.mime_type in _MIME_MAP:
        return _MIME_MAP[probe.mime_type]

    # 2) Extension mapping
    suffix = probe.suffix()
    if suffix in _EXTENSION_MAP:
        return _EXTENSION_MAP[suffix]

    # 3) Magic prefix sniffing (small prefix only)
    head = probe.prefix(16)
    if head.startswith(_PDF_MAGIC):
        return "PDF"
    if head.startswith(_PNG_MAGIC):
        return "IMAGE"
    if head.startswith(_JPEG_MAGIC):
        return "IMAGE"
    if head.startswith(_GIF_MAGIC):
        return "IMAGE"
    if any(head.startswith(sig) for sig in _TIFF_MAGIC):
        return "IMAGE"
    if head.startswith(_DOC_MAGIC):
        # Legacy OLE container: decide between DOC/XLS only if filename hints; else UNKNOWN.
        if suffix == ".doc":
            return "DOC"
        if suffix == ".xls":
            return "XLS"
        return "UNKNOWN"
    if head.startswith(_ZIP_MAGIC):
        # Could be DOCX/XLSX/other ZIP—use filename hint only.
        if suffix == ".docx":
            return "DOCX"
        if suffix == ".xlsx":
            return "XLSX"
        return "UNKNOWN"

    # 4) Light text heuristic for plain text fallbacks
    if head and _looks_like_text(probe.content_bytes):
        return "TEXT"

    return "UNKNOWN"


def classify_by_keywords(filename: str, text_sample: str = "", content_type: str = "") -> Tuple[Category | None, float]:
    """Compatibility wrapper returning deterministic attachment categories.

    Returns (category, confidence) with 1.0 for known deterministic matches,
    or (None, 0.0) when no supported category is detected.
    """

    category = classify_attachment(filename=filename, mime_type=content_type)
    if category == "UNKNOWN":
        return (None, 0.0)
    return (category, 1.0)


def _looks_like_text(data: bytes, sample: int = 256, binary_threshold: float = 0.10) -> bool:
    sample_bytes = data[:sample]
    if not sample_bytes:
        return False
    non_printable = sum(1 for b in sample_bytes if b not in _TEXT_PRINTABLE)
    return (non_printable / len(sample_bytes)) <= binary_threshold


# -------------------- Self-test --------------------

def _self_test_entries() -> List[Tuple[str, str, bytes]]:
    return [
        ("report.pdf", "application/pdf", _PDF_MAGIC + b" test"),
        ("contract.DOCX", "", _ZIP_MAGIC + b"..."),
        ("legacy_form.doc", "application/msword", _DOC_MAGIC + b"..."),
        ("sheet.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", _ZIP_MAGIC + b"..."),
        ("legacy.xls", "", _DOC_MAGIC + b"..."),
        ("photo.jpeg", "image/jpeg", _JPEG_MAGIC + b"morebytes"),
        ("diagram.png", "", _PNG_MAGIC + b"..."),
        ("notes.txt", "text/plain", b"hello world"),
        ("readme", "", b"Plain ASCII text snippet"),
        ("unknown.bin", "application/octet-stream", b"\x00\x01\x02\x03"),
    ]


def _run_self_test() -> bool:
    entries = _self_test_entries()
    results: List[Tuple[str, Category]] = []
    for name, mime, data in entries:
        category = classify_attachment(filename=name, mime_type=mime, content_bytes=data)
        results.append((name, category))
        print(f"{name:15s} | mime={mime or '-':45s} | -> {category}")

    # Assertions (deterministic expectations)
    assert results[0][1] == "PDF"
    assert results[1][1] == "DOCX"
    assert results[2][1] == "DOC"
    assert results[3][1] == "XLSX"
    assert results[4][1] == "XLS"
    assert results[5][1] == "IMAGE"
    assert results[6][1] == "IMAGE"
    assert results[7][1] == "TEXT"
    assert results[8][1] == "TEXT"
    assert results[9][1] == "UNKNOWN"

    print("\nSelf-test passed: 10/10 deterministic checks")
    return True


if __name__ == "__main__":
    _run_self_test()
