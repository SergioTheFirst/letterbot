from __future__ import annotations

import re

from mailbot_v26.ui import action_templates


_LATIN_RE = re.compile(r"[A-Za-z]")


def _assert_template(text: str) -> None:
    assert text
    assert len(text) <= 120
    assert not _LATIN_RE.search(text)


def test_deadlock_template_rules() -> None:
    text = action_templates.template_for_deadlock(
        from_email="boss@example.com",
        subject="Счёт",
    )
    _assert_template(text)


def test_silence_template_rules() -> None:
    text = action_templates.template_for_silence(contact="client@example.com")
    _assert_template(text)
