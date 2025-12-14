import mailbot_v26.start as start
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
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "xl/sharedStrings.xml",
            """<?xml version='1.0' encoding='UTF-8'?>
<sst xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'><si><t>Цена</t></si></sst>""",
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            """<?xml version='1.0' encoding='UTF-8'?>
<worksheet xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'><sheetData><row r='1'><c r='A1' t='s'><v>0</v></c><c r='B1'><v>123</v></c></row></sheetData></worksheet>""",
        )
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
