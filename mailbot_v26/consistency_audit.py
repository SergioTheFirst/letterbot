"""Full-project consistency audit aligned with the Constitution.

This module walks the entire ``mailbot_v26`` directory, prints per-file line
counts with one-line summaries, flags empty or unexpected files, detects
potentially unused Python modules, and searches for dependencies banned by
Constitution Section IX. Constitution Sections II and VIII are used as
guardrails for architecture and Guaranteed Mode readiness.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Set

PROJECT_ROOT = Path(__file__).resolve().parent
PACKAGE_NAME = PROJECT_ROOT.name

FORBIDDEN_DEPENDENCIES: Set[str] = {
    "spacy",
    "stanza",
    "bert",
    "bert-base",
    "bert-large",
    "transformers",
    "torch",
    "tensorflow",
}

ALLOWED_SUFFIXES: Set[str] = {
    ".py",
    ".txt",
    ".ini",
    ".md",
    ".json",
    ".yaml",
    ".yml",
    ".cfg",
    ".conf",
    ".lock",
    ".req",
}

IGNORED_DIR_NAMES: Set[str] = {"__pycache__", ".git", ".venv", ".idea"}


@dataclass
class FileInfo:
    path: Path
    line_count: int
    description: str
    is_empty: bool
    unexpected: bool


@dataclass
class AuditReport:
    files: List[FileInfo] = field(default_factory=list)
    empty_files: List[FileInfo] = field(default_factory=list)
    unexpected_files: List[FileInfo] = field(default_factory=list)
    unused_modules: List[FileInfo] = field(default_factory=list)
    forbidden_dependencies: Set[str] = field(default_factory=set)
    constitution_matches: List[str] = field(default_factory=list)
    constitution_violations: List[str] = field(default_factory=list)


def iter_project_files(base_path: Path) -> Iterable[Path]:
    for path in base_path.rglob("*"):
        if path.is_dir() and path.name in IGNORED_DIR_NAMES:
            continue
        if path.is_file():
            yield path.resolve()


def count_lines(path: Path) -> int:
    try:
        with path.open(encoding="utf-8", errors="ignore") as handle:
            return sum(1 for _ in handle)
    except OSError:
        return 0


def describe_file(path: Path) -> str:
    if path.suffix == ".py":
        try:
            module = ast.parse(path.read_text(encoding="utf-8"))
            doc = ast.get_docstring(module)
            if doc:
                return doc.strip().splitlines()[0]
        except (OSError, SyntaxError):
            pass
    try:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned[:160]
    except OSError:
        return "Unreadable file"
    return "(empty file)"


def is_unexpected_file(path: Path) -> bool:
    if path.name.startswith("."):
        return False
    if path.suffix in ALLOWED_SUFFIXES:
        return False
    return True


def gather_file_infos(base_path: Path) -> List[FileInfo]:
    infos: List[FileInfo] = []
    for path in sorted(iter_project_files(base_path)):
        line_count = count_lines(path)
        description = describe_file(path)
        unexpected = is_unexpected_file(path)
        info = FileInfo(
            path=path,
            line_count=line_count,
            description=description,
            is_empty=line_count == 0,
            unexpected=unexpected,
        )
        infos.append(info)
    return infos


def python_module_name(path: Path) -> str:
    relative = path.with_suffix("").relative_to(PROJECT_ROOT)
    return ".".join([PACKAGE_NAME, *relative.parts])


def collect_python_files(infos: Iterable[FileInfo]) -> List[Path]:
    return [info.path for info in infos if info.path.suffix == ".py"]


def collect_import_targets(py_files: Iterable[Path]) -> Set[str]:
    imports: Set[str] = set()
    for file_path in py_files:
        try:
            tree = ast.parse(file_path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    target = module if alias.name == "*" else f"{module}.{alias.name}" if module else alias.name
                    imports.add(target)
    return imports


def detect_unused_modules(infos: List[FileInfo]) -> List[FileInfo]:
    py_files = collect_python_files(infos)
    modules: Dict[str, Path] = {python_module_name(path): path for path in py_files}
    imports = collect_import_targets(py_files)

    unused: List[FileInfo] = []
    for module_name, path in modules.items():
        if path.name == "__init__.py":
            continue
        if path.name.startswith("test_") or "tests" in path.parts:
            continue
        if any(module_name.startswith(target) or target.startswith(module_name) for target in imports):
            continue
        if "__main__" in path.read_text(encoding="utf-8", errors="ignore"):
            continue
        description = describe_file(path)
        unused.append(
            FileInfo(
                path=path,
                line_count=count_lines(path),
                description=description,
                is_empty=False,
                unexpected=False,
            )
        )
    return sorted(unused, key=lambda info: str(info.path))


def detect_forbidden_dependencies(base_path: Path) -> Set[str]:
    detected: Set[str] = set()
    for path in base_path.rglob("requirements*.txt"):
        content = path.read_text(encoding="utf-8", errors="ignore").lower()
        for forbidden in FORBIDDEN_DEPENDENCIES:
            if forbidden in content:
                detected.add(forbidden)
    return detected


def summarize_constitution(report: AuditReport) -> None:
    if not report.unexpected_files and not report.empty_files:
        report.constitution_matches.append(
            "Section II — архитектурная целостность: каталог очищен, пустые и лишние файлы не обнаружены"
        )
    if not report.unused_modules:
        report.constitution_matches.append(
            "Section VIII — Guaranteed Mode: все модули задействованы в рабочем коде или тестах"
        )
    if not report.forbidden_dependencies:
        report.constitution_matches.append(
            "Section IX — запрещённые зависимости: spaCy/Stanza/BERT/seq2seq/NN фреймворки не найдены"
        )

    if report.unexpected_files or report.empty_files:
        report.constitution_violations.append(
            "Section II — найденные пустые или неожиданные файлы требуют очистки"
        )
    if report.unused_modules:
        report.constitution_violations.append(
            "Section VIII — обнаружены потенциально неиспользуемые модули, требуется проверка их роли"
        )
    if report.forbidden_dependencies:
        report.constitution_violations.append(
            "Section IX — найдены запрещённые зависимости, их необходимо удалить"
        )


def build_report(base_path: Path) -> AuditReport:
    infos = gather_file_infos(base_path)
    report = AuditReport()
    report.files = infos
    report.empty_files = [info for info in infos if info.is_empty]
    report.unexpected_files = [info for info in infos if info.unexpected]
    report.unused_modules = detect_unused_modules(infos)
    report.forbidden_dependencies = detect_forbidden_dependencies(base_path)
    summarize_constitution(report)
    return report


def format_file_line(info: FileInfo) -> str:
    return f"{info.path} | {info.line_count:4d} lines | {info.description}"


def print_report(report: AuditReport) -> None:
    print("=== CONSTITUTION CHECK ===")
    print("Section II — Архитектурная целостность")
    print("Section VIII — Guaranteed Mode: ошибки не должны ломать процесс")
    print("Section IX — Запрещённые зависимости")
    print()

    print("=== FILES ===")
    for info in report.files:
        print(format_file_line(info))

    print()
    print("EMPTY FILES:")
    if report.empty_files:
        for info in report.empty_files:
            print(f" - {info.path}")
    else:
        print(" - none")

    print("UNEXPECTED FILES:")
    if report.unexpected_files:
        for info in report.unexpected_files:
            print(f" - {info.path}")
    else:
        print(" - none")

    print("UNUSED MODULES:")
    if report.unused_modules:
        for info in report.unused_modules:
            print(f" - {info.path}")
    else:
        print(" - none")

    print("FORBIDDEN DEPENDENCIES:")
    if report.forbidden_dependencies:
        for item in sorted(report.forbidden_dependencies):
            print(f" - {item}")
    else:
        print(" - none")

    print()
    print("МОДУЛИ СООТВЕТСТВУЮТ КОНСТИТУЦИИ:")
    if report.constitution_matches:
        for line in report.constitution_matches:
            print(f" - {line}")
    else:
        print(" - нет подтверждений")

    print("МОДУЛИ НАРУШАЮТ КОНСТИТУЦИЮ:")
    if report.constitution_violations:
        for line in report.constitution_violations:
            print(f" - {line}")
    else:
        print(" - нет нарушений")


# === SELF-TEST ===

def run_self_test(base_path: Path) -> None:
    report = build_report(base_path)
    print_report(report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full project consistency audit")
    parser.add_argument(
        "--root",
        type=Path,
        default=PROJECT_ROOT,
        help="Path to the mailbot_v26 project root (defaults to this file's directory)",
    )
    args = parser.parse_args()
    run_self_test(args.root)
