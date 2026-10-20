"""HYG3: a test that re-points the processor database must not leak it.

configure_processor_db_path() rebinds DB_PATH and every database-backed
singleton. Tests that point it at a tmp_path database and never switch back
used to leave later tests writing through singletons bound to a deleted file.
The autouse fixture in conftest.py restores the processor's DB configuration
after every test. The two tests below run in file order: the first leaks on
purpose, the second checks that nothing of it survived.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mailbot_v26.pipeline import processor

_LEAKED: dict[str, object] = {}


def test_configure_without_restoring_leaks_on_purpose(tmp_path: Path) -> None:
    _LEAKED["before"] = {
        name: getattr(processor, name) for name in processor._DB_BOUND_GLOBALS
    }
    leaked_path = tmp_path / "leaked.sqlite"
    _LEAKED["db_path"] = leaked_path

    processor.configure_processor_db_path(leaked_path)
    # A stand-in another module imported by name caches a singleton set too.
    stand_in = processor._LazyDbResource("event_emitter")

    assert processor.DB_PATH == leaked_path
    assert processor._PROCESSOR_DB_CONFIGURED is True
    assert Path(stand_in.path) == leaked_path
    assert leaked_path in processor._DB_SINGLETONS


def test_next_test_gets_the_processor_db_configuration_back() -> None:
    if "before" not in _LEAKED:
        pytest.skip("runs after test_configure_without_restoring_leaks_on_purpose")
    before = _LEAKED["before"]
    assert isinstance(before, dict)

    for name, value in before.items():
        assert getattr(processor, name) is value, name
    assert processor.DB_PATH != _LEAKED["db_path"]
    assert _LEAKED["db_path"] not in processor._DB_SINGLETONS
    # Stand-ins resolve against the restored path again, not the deleted file.
    assert Path(processor._LazyDbResource("event_emitter").path) == processor.DB_PATH
