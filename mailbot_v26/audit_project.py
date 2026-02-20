"""
Audit-скрипт для Letterbot v26.

Проверяет:
- отсутствие использования запрещённых EXE-строк в КОДЕ
- отсутствие прямых импортов torch/tensorflow/stanza/spacy/transformers
- отсутствие использования catdoc/xlhtml/pdftotext в Python-файлах

ВАЖНО:
Сам audit НЕ содержит запрещённых слов напрямую.
Поэтому строки для проверки собираются динамически.
"""

from __future__ import annotations
from pathlib import Path
import re

PROJECT_ROOT = Path(__file__).resolve().parent

# Формируем запрещённые строки так, чтобы они НЕ находились в исходнике audit'а
BAD1 = "pd" + "ftotext"
BAD2 = "cat" + "doc"
BAD3 = "xl" + "html"

FORBIDDEN_STRINGS = [BAD1, BAD2, BAD3]

FORBIDDEN_IMPORTS = [
    r"\bimport\s+torch\b",
    r"\bfrom\s+torch\b",
    r"\bimport\s+tensorflow\b",
    r"\bimport\s+spacy\b",
    r"\bimport\s+stanza\b",
    r"\bimport\s+transformers\b",
]

def scan_code() -> list[str]:
    problems = []
    for file in PROJECT_ROOT.rglob("*.py"):
        if file.name == "audit_project.py":
            continue  # не проверяем самого себя

        text = file.read_text("utf-8", errors="ignore").lower()

        # 1. Запрещённые exe-строки
        for bad in FORBIDDEN_STRINGS:
            if bad in text:
                problems.append(f"ERROR: В файле {file} найдено запрещённое слово: {bad}")

        # 2. Запрещённые импорты
        for pattern in FORBIDDEN_IMPORTS:
            if re.search(pattern, text):
                problems.append(
                    f"ERROR: В файле {file} найден запрещённый импорт: {pattern}"
                )

    return problems


def main():
    print("=== Letterbot v26 Audit ===")
    print(f"Проект: {PROJECT_ROOT}")

    problems = scan_code()
    if not problems:
        print("OK: Запрещённые зависимости не обнаружены.")
        return 0

    print("\nОТЧЁТ:")
    for p in problems:
        print(p)

    print("\nИТОГ: ERROR: Исправь нарушения перед запуском.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
