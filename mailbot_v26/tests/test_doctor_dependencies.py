from __future__ import annotations

import pytest

from mailbot_v26 import doctor


@pytest.fixture(autouse=True)
def _reset_doctor_locale() -> None:
    doctor._set_doctor_locale("en")
    yield
    doctor._set_doctor_locale("en")


def test_check_dependencies_optional_missing_is_warn(monkeypatch) -> None:
    def fake_import(module: str):
        if module == "ldap":
            raise ImportError(module)
        return object()

    monkeypatch.setattr(doctor.importlib, "import_module", fake_import)

    entry = doctor._check_dependencies()

    assert entry.component == "Dependencies"
    assert entry.status == "WARN"
    assert "ldap" in entry.details
    assert "non-blocking" in entry.details


def test_check_dependencies_required_missing_is_fail(monkeypatch) -> None:
    def fake_import(module: str):
        if module == "imapclient":
            raise ImportError(module)
        return object()

    monkeypatch.setattr(doctor.importlib, "import_module", fake_import)

    entry = doctor._check_dependencies()

    assert entry.component == "Dependencies"
    assert entry.status == "FAIL"
    assert "imapclient" in entry.details
