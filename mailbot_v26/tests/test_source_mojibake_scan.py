from __future__ import annotations

import ast
from pathlib import Path

from mailbot_v26.text.mojibake import normalize_mojibake_text

_USER_FACING_SOURCE_FILES = (
    Path("mailbot_v26/pipeline/processor.py"),
    Path("mailbot_v26/start.py"),
    Path("mailbot_v26/telegram/inbound.py"),
    Path("mailbot_v26/ui/i18n.py"),
    Path("mailbot_v26/pipeline/tg_renderer.py"),
    Path("mailbot_v26/doctor.py"),
    Path("mailbot_v26/pipeline/daily_digest.py"),
    Path("mailbot_v26/telegram/decision_trace_ui.py"),
    Path("mailbot_v26/llm/prompts_ru.py"),
)

_TECHNICAL_LITERAL_ALLOWLIST: dict[str, frozenset[str]] = {
    "mailbot_v26/pipeline/processor.py": frozenset({"Ð", "Ñ"}),
    "mailbot_v26/telegram/inbound.py": frozenset(
        {
            "\u0440\u045f\u201d\u0491",
            "\u0440\u045f\u201f\u0160",
            "\u0440\u045f\u201d\u00b5",
            "\u0441\u0452\u0441\u045f\u0432\u0402\u045c\u0422\u2018",
            "\u0441\u0452\u0441\u045f\u0421\u045f\u045f\u0420\u040b",
            "\u0441\u0452\u0441\u045f\u0432\u0402\u045c\u0412\u00b5",
        }
    ),
}


def _collect_unexpected_mojibake_literals(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    allowed = _TECHNICAL_LITERAL_ALLOWLIST.get(path.as_posix(), frozenset())
    findings: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        literal = node.value
        repaired = normalize_mojibake_text(literal)
        if literal == repaired or literal in allowed:
            continue
        line = getattr(node, "lineno", 0)
        bad = literal.encode("unicode_escape").decode("ascii")
        fixed = repaired.encode("unicode_escape").decode("ascii")
        findings.append(f"{path.as_posix()}:{line}: {bad} -> {fixed}")

    return findings


def test_user_facing_source_files_have_no_mojibake_literals() -> None:
    findings: list[str] = []
    for path in _USER_FACING_SOURCE_FILES:
        assert path.exists()
        findings.extend(_collect_unexpected_mojibake_literals(path))

    assert findings == [], "Unexpected mojibake literals:\n" + "\n".join(findings)
