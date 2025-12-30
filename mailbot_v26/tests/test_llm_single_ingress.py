from __future__ import annotations

import ast
from pathlib import Path

ALLOWLIST = {
    "mailbot_v26/llm/providers.py",
    "mailbot_v26/llm/router.py",
    "mailbot_v26/tests/test_llm_router.py",
    "mailbot_v26/tests/test_gigachat_global_lock.py",
    "mailbot_v26/tests/test_llm_single_ingress.py",
    "tests/test_event_contract.py",
}

GIGACHAT_HTTP_TOKENS = {
    "gigachat.devices.sberbank.ru",
    "api/v1/chat/completions",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _iter_python_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.py"):
        if any(part in {".git", ".venv", "__pycache__", "venv"} for part in path.parts):
            continue
        files.append(path)
    return files


def _has_gigachat_import(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if any(alias.name == "GigaChatProvider" for alias in node.names):
                return True
        if isinstance(node, ast.Import):
            if any(alias.name == "GigaChatProvider" for alias in node.names):
                return True
    return False


def _has_gigachat_constructor(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "GigaChatProvider":
                return True
            if isinstance(func, ast.Attribute) and func.attr == "GigaChatProvider":
                return True
    return False


def test_gigachat_single_ingress_guard() -> None:
    root = _repo_root()
    violations: list[str] = []
    for path in _iter_python_files(root):
        rel = path.relative_to(root).as_posix()
        if rel in ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        if any(token in source for token in GIGACHAT_HTTP_TOKENS):
            violations.append(f"{rel}: direct GigaChat endpoint reference")
            continue
        tree = ast.parse(source, filename=rel)
        if _has_gigachat_import(tree):
            violations.append(f"{rel}: GigaChatProvider import")
        elif _has_gigachat_constructor(tree):
            violations.append(f"{rel}: GigaChatProvider construction")
    assert not violations, "GigaChat must be called only via LLMRouter: " + ", ".join(
        violations
    )
