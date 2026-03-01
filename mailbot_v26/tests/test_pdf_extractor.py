from __future__ import annotations

import sys
from unittest import mock

from mailbot_v26.bot_core.extractors.pdf import _extract_with_pdfminer, extract_pdf


def test_extract_pdf_falls_back_to_pdfminer_when_pypdf_empty() -> None:
    dummy_bytes = b"%PDF-1.4 fake"
    with mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="Извлечённый текст УПД",
    ) as mock_pdfminer:
        result = extract_pdf(dummy_bytes, "document.pdf")

    mock_pdfminer.assert_called_once_with(dummy_bytes)
    assert result == "Извлечённый текст УПД"


def test_pdfminer_gracefully_handles_import_error() -> None:
    with mock.patch.dict(
        sys.modules,
        {
            "pdfminer": None,
            "pdfminer.high_level": None,
            "pdfminer.layout": None,
        },
    ):
        result = _extract_with_pdfminer(b"fake bytes")

    assert result == ""
