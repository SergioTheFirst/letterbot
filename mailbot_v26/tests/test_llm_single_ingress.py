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


SOURCE_ROOTS = ("mailbot_v26", "tests")
_SKIPPED_DIRS = {"__pycache__", "venv"}


def _iter_python_files(root: Path) -> list[Path]:
    # Scan only the source roots and skip hidden dirs (.venv, .git, tool
    # worktrees under .claude/) so copies of the repo are not reported.
    files: list[Path] = []
    for source_root in SOURCE_ROOTS:
        for path in sorted((root / source_root).rglob("*.py")):
            parts = path.relative_to(root).parts[:-1]
            if any(part.startswith(".") or part in _SKIPPED_DIRS for part in parts):
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
    files = _iter_python_files(root)
    assert files, f"no Python files found under {SOURCE_ROOTS} in {root}"
    for path in files:
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


_BAD_SOURCE = "from mailbot_v26.llm.providers import GigaChatProvider\n"


def _write(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_scan_covers_only_source_roots_and_skips_hidden_dirs(tmp_path: Path) -> None:
    root = tmp_path / ".claude" / "worktrees" / "wt"
    _write(root / "mailbot_v26" / "pkg" / "ok.py", "x = 1\n")
    _write(root / "tests" / "test_ok.py", "x = 1\n")
    _write(root / "mailbot_v26" / ".hidden" / "bad.py", _BAD_SOURCE)
    _write(root / "tests" / ".venv" / "bad.py", _BAD_SOURCE)
    _write(root / ".claude" / "worktrees" / "x" / "mailbot_v26" / "bad.py", _BAD_SOURCE)
    _write(root / ".venv" / "lib" / "bad.py", _BAD_SOURCE)
    _write(root / "scratch" / "bad.py", _BAD_SOURCE)

    scanned = {path.relative_to(root).as_posix() for path in _iter_python_files(root)}

    assert scanned == {"mailbot_v26/pkg/ok.py", "tests/test_ok.py"}


def test_scan_still_reports_violation_in_source_root(tmp_path: Path) -> None:
    _write(tmp_path / "mailbot_v26" / "pkg" / "bad.py", _BAD_SOURCE)

    rels = [path.relative_to(tmp_path).as_posix() for path in _iter_python_files(tmp_path)]

    assert rels == ["mailbot_v26/pkg/bad.py"]
    assert _has_gigachat_import(ast.parse(_BAD_SOURCE))
