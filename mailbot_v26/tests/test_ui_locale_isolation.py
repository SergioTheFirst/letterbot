"""The autouse fixture in conftest.py undoes UI-locale changes between tests.

/lang, configure_processor_locale() and process_message() write module-level
locale globals; without the fixture a test that ends in "ru" turns later
tests' English expectations Russian (order-dependent failures).

The two tests below rely on pytest's in-file order: the first one switches
the locale, the second one checks that it starts from the original state.
"""

from __future__ import annotations

from mailbot_v26.pipeline import processor
from mailbot_v26.telegram import inbound

_BASELINE: dict[str, tuple[object, object, object]] = {}


def _ui_locale_state() -> tuple[object, object, object]:
    return (
        processor._UI_LOCALE,
        processor._UI_LOCALE_CONFIGURED,
        inbound._UI_LOCALE,
    )


def test_switching_ui_locale_mutates_module_globals() -> None:
    baseline = _ui_locale_state()
    _BASELINE["state"] = baseline
    target = "en" if str(baseline[0]).startswith("ru") else "ru"

    processor.configure_processor_locale(target)
    inbound.set_inbound_locale(target)

    assert _ui_locale_state() == (target, True, target)
    assert _ui_locale_state() != baseline


def test_next_test_starts_from_the_original_ui_locale() -> None:
    assert "state" in _BASELINE, "must run after the mutating test above"
    assert _ui_locale_state() == _BASELINE["state"]
