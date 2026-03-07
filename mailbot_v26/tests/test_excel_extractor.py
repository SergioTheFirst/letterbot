import io

import pytest

from mailbot_v26.bot_core.extractors import excel
from mailbot_v26.bot_core.extractors.excel import extract_excel_text


def _build_workbook_bytes(fill_rows: int, fill_cols: int) -> bytes:
    openpyxl = pytest.importorskip("openpyxl")
    Workbook = openpyxl.Workbook
    buffer = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.active
    for row in range(1, fill_rows + 1):
        for col in range(1, fill_cols + 1):
            sheet.cell(row=row, column=col, value=f"R{row}C{col}")
    workbook.save(buffer)
    return buffer.getvalue()


def test_extract_excel_text_openpyxl_values():
    openpyxl = pytest.importorskip("openpyxl")
    Workbook = openpyxl.Workbook
    buffer = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.active
    sheet["A1"] = "Alpha"
    sheet["B1"] = "Beta"
    workbook.save(buffer)

    text = extract_excel_text(buffer.getvalue(), "sample.xlsx")

    assert "Alpha" in text
    assert "Beta" in text


def test_extract_excel_text_row_limit():
    data = _build_workbook_bytes(fill_rows=300, fill_cols=1)

    text = extract_excel_text(data, "rows.xlsx")
    lines = [line for line in text.splitlines() if line.strip()]

    assert len(lines) <= 200
    assert "R1C1" in text
    assert "R200C1" in text
    assert "R201C1" not in text


def test_extract_excel_text_column_limit():
    data = _build_workbook_bytes(fill_rows=1, fill_cols=35)

    text = extract_excel_text(data, "cols.xlsx")
    first_line = text.splitlines()[0]
    columns = first_line.split("\t")

    assert len(columns) <= 30
    assert "R1C30" in first_line
    assert "R1C31" not in first_line


def test_extract_excel_text_routes_xls_to_xlrd(monkeypatch):
    calls = []

    def _fail_openpyxl(_file_bytes: bytes) -> list[str]:
        calls.append("openpyxl")
        return ["unexpected"]

    def _collect_xlrd(_file_bytes: bytes) -> list[str]:
        calls.append("xlrd")
        return ["XLS_ROW"]

    monkeypatch.setattr(excel, "_collect_rows_from_openpyxl", _fail_openpyxl)
    monkeypatch.setattr(excel, "_collect_rows_from_xlrd", _collect_xlrd)

    text = extract_excel_text(b"xls-bytes", "legacy.xls")

    assert text == "XLS_ROW"
    assert calls == ["xlrd"]


def test_collect_rows_from_xlrd_missing_dependency_returns_empty(monkeypatch):
    real_import = __import__

    def _import_without_xlrd(name, *args, **kwargs):
        if name == "xlrd":
            raise ImportError("xlrd missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _import_without_xlrd)

    assert excel._collect_rows_from_xlrd(b"broken") == []


def test_extract_excel_text_xlsx_still_uses_openpyxl_branch(monkeypatch):
    calls = []

    def _collect_openpyxl(_file_bytes: bytes) -> list[str]:
        calls.append("openpyxl")
        return ["XLSX_ROW"]

    def _collect_xlrd(_file_bytes: bytes) -> list[str]:
        calls.append("xlrd")
        return ["legacy"]

    monkeypatch.setattr(excel, "_collect_rows_from_openpyxl", _collect_openpyxl)
    monkeypatch.setattr(excel, "_collect_rows_from_xlrd", _collect_xlrd)

    text = extract_excel_text(b"xlsx-bytes", "current.xlsx")

    assert text == "XLSX_ROW"
    assert calls == ["openpyxl"]


def test_extract_excel_text_corrupt_xls_returns_empty_with_warning(monkeypatch, caplog):
    def _collect_xlrd(_file_bytes: bytes) -> list[str]:
        return []

    monkeypatch.setattr(excel, "_collect_rows_from_xlrd", _collect_xlrd)

    with caplog.at_level("WARNING"):
        text = extract_excel_text(b"not-a-real-xls", "broken.xls")

    assert text == ""
    assert "Failed to extract text from broken.xls" in caplog.text
