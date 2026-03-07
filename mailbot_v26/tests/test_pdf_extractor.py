from __future__ import annotations

import sys
from unittest import mock

from mailbot_v26.bot_core.extractors.pdf import (
    PDF_ZERO_REASON_ALL_EMPTY,
    PDF_ZERO_REASON_BROKEN,
    PDF_ZERO_REASON_ENCRYPTED,
    PDF_ZERO_REASON_IMAGE_ONLY,
    _extract_with_pdfminer,
    extract_pdf,
    extract_pdf_text,
)


def test_extract_pdf_returns_tuple_contract() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf._probe_pdf_state", return_value=(True, False, 1, False)), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._pikepdf_can_open",
        return_value=(True, False),
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._is_image_only_pdf",
        return_value=False,
    ):
        result = extract_pdf(b"fake", "test.pdf")

    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], str)
    assert isinstance(result[1], str)


def test_extract_pdf_returns_empty_reason_when_text_found() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf._probe_pdf_state", return_value=(True, False, 1, False)), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="Извлечённый текст",
    ):
        text, reason = extract_pdf(b"fake", "test.pdf")

    assert text == "Извлечённый текст"
    assert reason == ""


def test_extract_pdf_returns_non_empty_reason_when_extractors_empty() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf._probe_pdf_state", return_value=(True, False, 1, False)), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._pikepdf_can_open",
        return_value=(True, False),
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._is_image_only_pdf",
        return_value=False,
    ):
        text, reason = extract_pdf(b"fake", "test.pdf")

    assert text == ""
    assert reason != ""


def test_extract_pdf_text_alias_returns_string() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf.extract_pdf", return_value=("text", "")):
        result = extract_pdf_text(b"fake", "test.pdf")

    assert isinstance(result, str)
    assert result == "text"


def test_extract_pdf_classifies_encrypted() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf._probe_pdf_state", return_value=(True, True, 0, True)):
        text, reason = extract_pdf(b"fake", "test.pdf")

    assert text == ""
    assert reason == PDF_ZERO_REASON_ENCRYPTED


def test_extract_pdf_classifies_broken_pdf() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf._probe_pdf_state", return_value=(False, False, 0, False)), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._pikepdf_can_open",
        return_value=(False, False),
    ):
        text, reason = extract_pdf(b"not-pdf", "broken.pdf")

    assert text == ""
    assert reason == PDF_ZERO_REASON_BROKEN


def test_extract_pdf_image_or_all_empty_contract() -> None:
    with mock.patch("mailbot_v26.bot_core.extractors.pdf._probe_pdf_state", return_value=(True, False, 2, False)), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pypdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pikepdf",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._extract_with_pdfminer",
        return_value="",
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._pikepdf_can_open",
        return_value=(True, False),
    ), mock.patch(
        "mailbot_v26.bot_core.extractors.pdf._is_image_only_pdf",
        return_value=True,
    ):
        text, reason = extract_pdf(b"fake", "image.pdf")

    assert text == ""
    assert reason in {PDF_ZERO_REASON_IMAGE_ONLY, PDF_ZERO_REASON_ALL_EMPTY}


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
