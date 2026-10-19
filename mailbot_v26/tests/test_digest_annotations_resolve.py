from __future__ import annotations

import typing

import pytest

from mailbot_v26.pipeline import daily_digest, weekly_digest

_ATTENTION_HELPERS = (
    "_format_attention_top_sinks_localized",
    "_format_attention_risks_localized",
    "_format_attention_best_localized",
)


@pytest.mark.parametrize("module", [daily_digest, weekly_digest])
@pytest.mark.parametrize("name", _ATTENTION_HELPERS)
def test_attention_helper_annotations_resolve(module, name: str) -> None:
    func = getattr(module, name)

    hints = typing.get_type_hints(func)

    assert "entities" in hints
    assert "locale" in hints
