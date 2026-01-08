import mailbot_v26.start as start
from email.message import EmailMessage

from mailbot_v26.bot_core import pipeline as core_pipeline
from mailbot_v26.pipeline.processor import Attachment


def test_png_attachment_extraction_is_empty():
    data = b"\x89PNG\r\n\x1a\nIHDRbinarydata"
    att = Attachment(filename="image.png", content=data, content_type="image/png")
    text = start._extract_attachment_text(att)
    assert text == ""


def test_zip_like_attachment_not_leaking_pk():
    data = b"PK\x03\x04fakezipdata"
    att = Attachment(filename="file.docx", content=data, content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    text = start._extract_attachment_text(att)
    assert "PK" not in text
    assert text in {"", None}


def _build_minimal_xlsx():
    import io

    from openpyxl import Workbook

    buf = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.active
    sheet["A1"] = "Цена"
    sheet["B1"] = 123
    workbook.save(buf)
    return buf.getvalue()


def _build_minimal_docx():
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "word/document.xml",
            """<?xml version='1.0' encoding='UTF-8'?>
<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'><w:body><w:p><w:r><w:t>Договор поставки</w:t></w:r></w:p></w:body></w:document>""",
        )
    return buf.getvalue()


def test_excel_text_is_extracted():
    data = _build_minimal_xlsx()
    att = Attachment(filename="table.xlsx", content=data, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    text = start._extract_attachment_text(att)
    assert "Цена" in text
    assert "123" in text


def test_docx_text_is_extracted():
    data = _build_minimal_docx()
    att = Attachment(filename="offer.docx", content=data, content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    text = start._extract_attachment_text(att)
    assert "Договор" in text


def test_async_attachment_extraction_order_and_workers(monkeypatch):
    captured = {}

    class ImmediateFuture:
        def __init__(self, value):
            self._value = value

        def result(self):
            return self._value

    class SpyExecutor:
        def __init__(self, max_workers):
            captured["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            return ImmediateFuture(fn(*args, **kwargs))

    monkeypatch.setattr(core_pipeline, "ThreadPoolExecutor", SpyExecutor)

    message = EmailMessage()
    message.set_content("body")
    message.add_attachment("first", subtype="plain", filename="first.txt")
    message.add_attachment("second", subtype="plain", filename="second.txt")

    attachments = core_pipeline._extract_attachments(message, max_mb=5)

    assert captured["max_workers"] <= 2
    assert [att.filename for att in attachments] == ["first.txt", "second.txt"]
    assert [att.text for att in attachments] == ["first", "second"]
