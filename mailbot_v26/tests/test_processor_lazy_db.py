"""A22: importing the pipeline processor must not open, create or migrate a database.

The database-backed singletons stay lazy until first use or until
configure_processor_db_path() points them at the configured database.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from mailbot_v26.pipeline import processor
from mailbot_v26.storage.knowledge_db import KnowledgeDB

REPO_ROOT = Path(__file__).resolve().parents[2]

# Singleton -> attribute holding the database path it writes to or reads from.
DB_SINGLETON_PATH_ATTRS = {
    "knowledge_db": "path",
    "analytics": "path",
    "decision_trace_writer": "path",
    "trust_snapshot_writer": "path",
    "relationship_health_snapshot_writer": "path",
    "context_store": "path",
    "event_emitter": "path",
    "contract_event_emitter": "db_path",
    "runtime_override_store": "_path",
    "metrics_aggregator": "path",
    "processing_span_recorder": "path",
    "notification_alert_store": "path",
    "budget_gate": "_db_path",
}


@pytest.fixture
def restore_processor_globals():
    """Undo every module-global rebinding a test makes (configure re-points many)."""
    saved = dict(vars(processor))
    yield
    for name, value in saved.items():
        if vars(processor).get(name) is not value:
            setattr(processor, name, value)


def _run_isolated(args: list[str], cwd: Path, temp_dir: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("LETTERBOT_DEFAULT_DB_PATH", None)
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PYTHONIOENCODING"] = "utf-8"
    env["TEMP"] = env["TMP"] = env["TMPDIR"] = str(temp_dir)
    return subprocess.run(
        [sys.executable, *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=600,
    )


def _sqlite_files(root: Path) -> list[str]:
    return sorted(
        str(path.relative_to(root)) for path in root.rglob("*") if "sqlite" in path.name
    )


@pytest.fixture
def isolated_dirs(tmp_path: Path) -> SimpleNamespace:
    cwd = tmp_path / "cwd"
    temp_dir = tmp_path / "temp"
    cwd.mkdir()
    temp_dir.mkdir()
    return SimpleNamespace(cwd=cwd, temp=temp_dir)


def test_importing_processor_and_start_creates_no_database(isolated_dirs) -> None:
    proc = _run_isolated(
        ["-c", "import mailbot_v26.pipeline.processor, mailbot_v26.start"],
        isolated_dirs.cwd,
        isolated_dirs.temp,
    )

    assert proc.returncode == 0, proc.stderr[-2000:]
    assert _sqlite_files(isolated_dirs.cwd) == []
    assert _sqlite_files(isolated_dirs.temp) == []


def test_lazy_resource_builds_on_first_use_from_current_db_path(
    monkeypatch, tmp_path: Path
) -> None:
    calls: list[Path] = []

    def fake_build(path: Path) -> dict[str, SimpleNamespace]:
        calls.append(path)
        return {"probe": SimpleNamespace(path=path, value=7)}

    monkeypatch.setattr(processor, "_build_db_singletons", fake_build)
    monkeypatch.setattr(processor, "_DB_SINGLETONS", {})
    resource = processor._LazyDbResource("probe")
    late_path = tmp_path / "configured-later.sqlite"
    monkeypatch.setattr(processor, "DB_PATH", late_path)

    assert calls == []
    assert resource.value == 7
    assert resource.path == late_path
    assert calls == [late_path]
    # Every stand-in for the same path shares one built set.
    assert processor._LazyDbResource("probe").path == late_path
    assert calls == [late_path]
    # A stand-in follows DB_PATH, e.g. after configure_processor_db_path().
    configured_path = tmp_path / "configured.sqlite"
    monkeypatch.setattr(processor, "DB_PATH", configured_path)
    assert resource.path == configured_path
    assert calls == [late_path, configured_path]


def test_first_use_of_any_lazy_singleton_creates_the_schema(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "first-use.sqlite"
    monkeypatch.setattr(processor, "DB_PATH", db_path)
    monkeypatch.setattr(processor, "_DB_SINGLETONS", {})

    # A reader that never creates tables itself still finds KnowledgeDB's schema.
    assert Path(processor._LazyDbResource("metrics_aggregator").path) == db_path

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
    assert {"emails", "commitments", "email_importance_scores"} <= tables


def test_lazy_resource_forwards_attribute_writes(monkeypatch) -> None:
    target = SimpleNamespace(save=lambda: "real")
    monkeypatch.setattr(processor, "_build_db_singletons", lambda _path: {"probe": target})
    monkeypatch.setattr(processor, "_DB_SINGLETONS", {})
    resource = processor._LazyDbResource("probe")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(resource, "save", lambda: "patched")
        assert resource.save() == "patched"
        assert target.save() == "patched"

    assert resource.save() == "real"


def test_configure_repoints_every_db_singleton(
    tmp_path: Path, restore_processor_globals
) -> None:
    db_path = tmp_path / "configured.sqlite"

    processor.configure_processor_db_path(db_path)

    assert processor.DB_PATH == db_path
    assert db_path.exists()
    for name, attr in DB_SINGLETON_PATH_ATTRS.items():
        singleton = getattr(processor, name)
        assert not isinstance(singleton, processor._LazyDbResource), name
        assert Path(getattr(singleton, attr)) == db_path, name
    assert processor.auto_priority_gate_state_store._knowledge_db is processor.knowledge_db
    assert processor.auto_priority_quality_gate._analytics is processor.analytics
    assert processor.budget_gate._emitter is processor.contract_event_emitter
    assert processor.budget_consumer._gate is processor.budget_gate


def test_configure_materializes_lazy_singletons_for_the_import_default_path(
    tmp_path: Path, restore_processor_globals
) -> None:
    default_path = tmp_path / "import-default.sqlite"
    processor.DB_PATH = default_path
    processor._PROCESSOR_DB_CONFIGURED = False
    processor.knowledge_db = processor._LazyDbResource("knowledge_db")

    processor.configure_processor_db_path(default_path)

    assert isinstance(processor.knowledge_db, KnowledgeDB)
    assert default_path.exists()
