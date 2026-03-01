from __future__ import annotations

import sys
from unittest import mock

from mailbot_v26.bot_core.extractors.pdf import (
    PDF_ZERO_REASON_ALL_EMPTY,
    PDF_ZERO_REASON_ENCRYPTED,
    _extract_with_pdfminer,
    extract_pdf,
    extract_pdf_text,
)


def test_extract_pdf_returns_reason_when_zero() -> None:
    with mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._is_image_only_pdf",
        return_value=False,
    ):
        text, reason = extract_pdf(b"fake", "test.pdf")

    assert text == ""
    assert reason != ""


def test_extract_pdf_returns_empty_reason_when_text_found() -> None:
    with mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="Извлечённый текст",
    ):
        text, reason = extract_pdf(b"fake", "test.pdf")

    assert text == "Извлечённый текст"
    assert reason == ""


def test_extract_pdf_text_alias_returns_string() -> None:
    with mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="text",
    ):
        result = extract_pdf_text(b"fake", "test.pdf")

    assert isinstance(result, str)
    assert result == "text"


def test_extract_pdf_classifies_encrypted() -> None:
    def raise_encrypted(_bytes: bytes) -> str:
        raise RuntimeError("PdfReadError: encrypted")

    with mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        side_effect=raise_encrypted,
    ):
        text, reason = extract_pdf(b"fake", "test.pdf")

    assert text == ""
    assert "encrypt" in reason.lower() or reason == PDF_ZERO_REASON_ENCRYPTED


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
        result, reason = extract_pdf(dummy_bytes, "document.pdf")

    mock_pdfminer.assert_called_once_with(dummy_bytes)
    assert result == "Извлечённый текст УПД"
    assert reason == ""


def test_extract_pdf_all_extractors_empty_reason() -> None:
    with mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._is_image_only_pdf",
        return_value=False,
    ):
        _text, reason = extract_pdf(b"fake", "empty.pdf")

    assert reason == PDF_ZERO_REASON_ALL_EMPTY


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
