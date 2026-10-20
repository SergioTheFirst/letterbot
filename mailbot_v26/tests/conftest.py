import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator

import pytest

# Ensure repository root is on sys.path for test imports
ROOT = Path(__file__).resolve().parents[1]
PARENT = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))


# Module-level UI-locale globals that /lang, configure_processor_locale(),
# configure_processor_config_dir() and process_message() rewrite at runtime.
_UI_LOCALE_GLOBALS: dict[str, tuple[str, ...]] = {
    "mailbot_v26.pipeline.processor": ("_UI_LOCALE", "_UI_LOCALE_CONFIGURED"),
    "mailbot_v26.telegram.inbound": ("_UI_LOCALE",),
}
# Every module object seen so far: a test that re-imports a module puts a new
# object into sys.modules while other test modules keep using the old one.
_tracked_locale_modules: dict[int, tuple[ModuleType, str]] = {}


def _track_loaded_locale_modules() -> None:
    for name in _UI_LOCALE_GLOBALS:
        module = sys.modules.get(name)
        if module is not None:
            _tracked_locale_modules.setdefault(id(module), (module, name))


def _import_time_locale_values(name: str) -> dict[str, object]:
    from mailbot_v26.ui.i18n import DEFAULT_LOCALE

    defaults: dict[str, object] = {
        "_UI_LOCALE": DEFAULT_LOCALE,
        "_UI_LOCALE_CONFIGURED": False,
    }
    return {attr: defaults[attr] for attr in _UI_LOCALE_GLOBALS[name]}


@pytest.fixture(autouse=True)
def _restore_ui_locale_state() -> Iterator[None]:
    """Undo UI-locale changes a test leaves in module globals.

    Without this, a test ending in "ru" (e.g. the /lang tests) makes later
    tests that expect English output fail depending on file order.
    """
    _track_loaded_locale_modules()
    snapshot = {
        key: (module, {attr: getattr(module, attr) for attr in _UI_LOCALE_GLOBALS[name]})
        for key, (module, name) in _tracked_locale_modules.items()
    }
    yield
    _track_loaded_locale_modules()
    for key, (module, name) in _tracked_locale_modules.items():
        # A module first imported during the test goes back to import-time values.
        _, values = snapshot.get(key, (module, _import_time_locale_values(name)))
        for attr, value in values.items():
            setattr(module, attr, value)


# configure_processor_db_path() rebinds the processor globals listed in its
# _DB_BOUND_GLOBALS (A22); its lazy stand-ins cache one singleton set per
# database in _DB_SINGLETONS.
_PROCESSOR_MODULE = "mailbot_v26.pipeline.processor"
_tracked_processor_modules: dict[int, ModuleType] = {}


def _track_loaded_processor_modules() -> None:
    module = sys.modules.get(_PROCESSOR_MODULE)
    # A stub standing in for the processor has nothing to restore.
    if module is not None and hasattr(module, "_DB_BOUND_GLOBALS"):
        _tracked_processor_modules.setdefault(id(module), module)


@pytest.fixture(autouse=True)
def _restore_processor_db_state() -> Iterator[None]:
    """Undo processor database re-pointing a test leaves behind (HYG3).

    Many tests call configure_processor_db_path(tmp_path / ...) and never switch
    back, so later tests wrote through singletons bound to a deleted file. Every
    DB-bound global goes back to its value before the test (the import-time
    lazy stand-ins, unless a broader-scoped fixture configured it), and
    singleton sets first cached during the test are dropped. A processor module
    first imported during a test is tracked from the next test on.
    """
    _track_loaded_processor_modules()
    snapshot = {
        key: (
            {name: getattr(module, name) for name in module._DB_BOUND_GLOBALS},
            module._DB_SINGLETONS,
            set(module._DB_SINGLETONS),
        )
        for key, module in _tracked_processor_modules.items()
    }
    yield
    for key, (bound, singletons, cached_paths) in snapshot.items():
        module = _tracked_processor_modules[key]
        for name, value in bound.items():
            setattr(module, name, value)
        with module._DB_SINGLETONS_LOCK:
            for db_path in set(singletons) - cached_paths:
                del singletons[db_path]
