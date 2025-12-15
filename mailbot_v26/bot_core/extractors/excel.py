from __future__ import annotations

import io
import logging
import zipfile
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)
XL_MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _safe_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def _normalize_text(value: str) -> str:
    normalized = (value or "").encode("utf-8", "ignore").decode("utf-8", "ignore")
    return normalized.strip()


def _load_pandas():
    try:
        import pandas as pd
        return pd
    except Exception:
        return None


def _extract_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    """Извлечь общие строки из XLSX"""
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
    """Извлечь ячейки из всех листов"""
    rows: list[str] = []
    
    for name in sorted(zf.namelist()):
        if not name.startswith("xl/worksheets/sheet"):
            continue

        try:
            sheet_xml = zf.read(name)
            root = ET.fromstring(sheet_xml)
        except Exception as e:
            logger.debug("Failed to parse %s: %s", name, e)
            continue

        for row in root.findall(f".//{XL_MAIN_NS}row"):
            values: list[str] = []
            
            for cell in row.findall(f"{XL_MAIN_NS}c"):
                cell_type = cell.attrib.get("t")
                value_node = cell.find(f"{XL_MAIN_NS}v")
                
                # Общая строка
                if cell_type == "s" and value_node is not None:
                    try:
                        idx = int(value_node.text or "0")
                        cell_value = shared_strings[idx] if idx < len(shared_strings) else ""
                    except (ValueError, IndexError):
                        cell_value = ""
                
                # Прямое значение
                elif value_node is not None and value_node.text:
                    cell_value = _safe_text(value_node.text)
                
                # Inline строка
                else:
                    inline = cell.find(f"{XL_MAIN_NS}is/{XL_MAIN_NS}t")
                    cell_value = _safe_text(inline.text if inline is not None else "")

                if cell_value:
                    values.append(cell_value)

            if values:
                rows.append(" | ".join(values))

    return rows


def _extract_via_zip(file_bytes: bytes) -> list[str]:
    """Основной метод - чистый ZIP"""
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            shared_strings = _extract_shared_strings(zf)
            rows = _extract_cells(zf, shared_strings)
    except Exception as e:
        logger.debug("ZIP extraction failed: %s", e)
        return []

    return rows


def _limit_rows(rows: list[str], max_rows: int = 20) -> list[str]:
    """Ограничить количество строк, но взять больше для информативности"""
    preview = [row for row in rows if row and row.strip()]
    return preview[:max_rows]


def extract_excel(file_bytes: bytes, filename: str) -> str:
    """
    Извлечение из XLS/XLSX.
    Приоритет: ZIP -> pandas
    """
    name = (filename or "").lower()

    if not name.endswith((".xls", ".xlsx")):
        return ""

    # ПОПЫТКА 1: Чистый ZIP (для XLSX)
    if name.endswith(".xlsx"):
        zip_rows = _extract_via_zip(file_bytes)
        limited_rows = _limit_rows(zip_rows, max_rows=20)
        
        if limited_rows:
            text = _normalize_text("\n".join(limited_rows))
            logger.debug("Extracted %d chars from %s (ZIP method)", len(text), filename)
            return text

    # ПОПЫТКА 2: pandas (для XLS и как фоллбэк для XLSX)
    pandas = _load_pandas()
    if pandas:
        try:
            # Для XLS используем xlrd engine
            engine = "xlrd" if name.endswith(".xls") else "openpyxl"
            df = pandas.read_excel(io.BytesIO(file_bytes), engine=engine)
            df = df.dropna(how="all")
            preview = df.head(20)
            
            if not preview.empty:
                text = _normalize_text(preview.to_string(index=False, header=True))
                logger.debug("Extracted %d chars from %s (pandas)", len(text), filename)
                return text
                
        except Exception as e:
            logger.debug("pandas extraction failed: %s", e)

    logger.warning("Failed to extract text from %s", filename)
    return ""


extract_excel_text = extract_excel
