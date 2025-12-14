from __future__ import annotations

import io
import zipfile
from xml.etree import ElementTree as ET


XL_MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _safe_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _load_pandas():
    try:
        import pandas as pd  # type: ignore

        return pd
    except Exception:
        return None


def _extract_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        data = zf.read("xl/sharedStrings.xml")
    except Exception:
        return []

    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return []

    values: list[str] = []
    for node in root.findall(f".//{XL_MAIN_NS}t"):
        text = _safe_text(node.text or "")
        if text:
            values.append(text)
    return values


def _extract_cells(zf: zipfile.ZipFile, shared_strings: list[str]) -> list[str]:
    rows: list[str] = []
    for name in zf.namelist():
        if not name.startswith("xl/worksheets/sheet"):
            continue

        try:
            sheet_xml = zf.read(name)
            root = ET.fromstring(sheet_xml)
        except Exception:
            continue

        for row in root.findall(f".//{XL_MAIN_NS}row"):
            values: list[str] = []
            for cell in row.findall(f"{XL_MAIN_NS}c"):
                cell_type = cell.attrib.get("t")
                value_node = cell.find(f"{XL_MAIN_NS}v")
                if cell_type == "s" and value_node is not None:
                    try:
                        idx = int(value_node.text or "0")
                        cell_value = shared_strings[idx] if idx < len(shared_strings) else ""
                    except Exception:
                        cell_value = ""
                else:
                    if value_node is not None and value_node.text:
                        cell_value = _safe_text(value_node.text)
                    else:
                        inline = cell.find(f"{XL_MAIN_NS}is/{XL_MAIN_NS}t")
                        cell_value = _safe_text(inline.text if inline is not None else "")

                if cell_value:
                    values.append(cell_value)

            if values:
                rows.append(" | ".join(values))

    return rows


def _extract_via_zip(file_bytes: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            shared_strings = _extract_shared_strings(zf)
            rows = _extract_cells(zf, shared_strings)
    except Exception:
        return ""

    if not rows:
        return ""

    return "\n".join(rows)


def extract_excel(file_bytes: bytes, filename: str) -> str:
    name = (filename or "").lower()

    if not name.endswith((".xls", ".xlsx")):
        return ""

    zip_text = _extract_via_zip(file_bytes)
    if zip_text.strip():
        return zip_text

    pandas = _load_pandas()
    if pandas is None:
        return ""

    try:
        df = pandas.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        return df.to_string(index=False, header=True)
    except Exception:
        return ""


extract_excel_text = extract_excel
