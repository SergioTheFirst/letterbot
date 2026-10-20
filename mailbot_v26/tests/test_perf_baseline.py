from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tools import eval_golden_corpus
from mailbot_v26.tools import perf_baseline

SECRET_SENDER = "perf-secret-sender@example.invalid"
SECRET_NUMBER = 987654
MARKER_ACCOUNT = "perf-marker-account@example.invalid"
MARKER_SENDER = "perf-marker-sender@example.invalid"
MARKER_SUBJECT = "PERF-MARKER-SUBJECT-7f3a"
MARKER_BODY = "PERF-MARKER-BODY-91c2"


def _make_items_db(path: Path) -> Path:
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                sender TEXT,
                amount INTEGER,
                note TEXT
            );
            CREATE INDEX idx_items_sender ON items(sender);
            CREATE INDEX idx_items_note_unused ON items(note);
            """
        )
        conn.execute(
            "INSERT INTO items (sender, amount, note) VALUES (?, ?, ?)",
            (SECRET_SENDER, 5, "note"),
        )
        conn.commit()
    finally:
        conn.close()
    return path


def test_normalize_sql_replaces_literals_and_keeps_identifiers() -> None:
    raw = (
        "SELECT id, 'x' FROM events_v1 WHERE account = 'alice@example.com' "
        "AND n > 42 AND r < 3.5e2 AND b = x'DEADBEEF' AND q = 'it''s' "
        "AND k = ?1 AND j = :name -- tail 'secret-comment'\n"
        "/* comment 99 */ AND t IN (1, 2, 3) AND d >= datetime('now', '-7 days')"
    )

    normalized = perf_baseline.normalize_sql(raw)

    for leaked in (
        "alice@example.com",
        "42",
        "3.5e2",
        "DEADBEEF",
        "it''s",
        "secret-comment",
        "99",
        "comment",
        "-7 days",
    ):
        assert leaked not in normalized
    assert "events_v1" in normalized
    assert "?1" in normalized
    assert ":name" in normalized
    assert "\n" not in normalized
    assert "t IN (?, ...)" in normalized
    assert "datetime(?, ?)" in normalized


def test_normalize_sql_strips_unterminated_string_literal() -> None:
    normalized = perf_baseline.normalize_sql(
        "SELECT * FROM t WHERE a = 'cut off secret"
    )

    assert "secret" not in normalized
    assert normalized.startswith("SELECT * FROM t WHERE a = ?")


def test_temporary_copy_reads_source_read_only_and_is_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _make_items_db(tmp_path / "source.sqlite")
    source_bytes = source.read_bytes()
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    real_connect = perf_baseline._raw_connect

    def _spy(*args: object, **kwargs: object) -> sqlite3.Connection:
        calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(perf_baseline, "_raw_connect", _spy)
    work_root = tmp_path / "work"

    with perf_baseline.temporary_db_copy(source, work_root=work_root) as copy_path:
        copy_dir = copy_path.parent
        assert copy_path.exists()
        assert copy_path.resolve() != source.resolve()
        check = sqlite3.connect(str(copy_path))
        try:
            assert check.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1
        finally:
            check.close()

    assert not copy_dir.exists()
    assert list(work_root.iterdir()) == []
    assert source.read_bytes() == source_bytes
    source_args, source_kwargs = calls[0]
    assert str(source_args[0]).startswith("file:")
    assert str(source_args[0]).endswith("?mode=ro")
    assert source_kwargs.get("uri") is True


def test_temporary_copy_is_deleted_when_measurement_fails(tmp_path: Path) -> None:
    source = _make_items_db(tmp_path / "source.sqlite")

    with pytest.raises(RuntimeError, match="boom"):
        with perf_baseline.temporary_db_copy(
            source, work_root=tmp_path / "work"
        ) as copy_path:
            copy_dir = copy_path.parent
            raise RuntimeError("boom")

    assert not copy_dir.exists()


def test_tracer_closes_leaked_connections_and_restores_connect(tmp_path: Path) -> None:
    source = _make_items_db(tmp_path / "source.sqlite")
    original_connect = sqlite3.connect

    with perf_baseline.temporary_db_copy(
        source, work_root=tmp_path / "work"
    ) as copy_path:
        copy_dir = copy_path.parent
        with perf_baseline.SqlTracer(copy_path):
            assert sqlite3.connect is not original_connect
            leaked = sqlite3.connect(str(copy_path))
            leaked.execute("SELECT COUNT(*) FROM items").fetchall()

    assert sqlite3.connect is original_connect
    assert not copy_dir.exists()


def test_sql_report_flags_full_scans_and_index_usage(tmp_path: Path) -> None:
    source = _make_items_db(tmp_path / "source.sqlite")

    with perf_baseline.temporary_db_copy(
        source, work_root=tmp_path / "work"
    ) as copy_path:
        with perf_baseline.SqlTracer(copy_path) as tracer:
            tracer.section = "probe"
            conn = sqlite3.connect(str(copy_path))
            conn.execute(
                "SELECT id FROM items WHERE sender = ?", (SECRET_SENDER,)
            ).fetchall()
            conn.execute(
                f"SELECT id FROM items WHERE amount > {SECRET_NUMBER}"
            ).fetchall()
            conn.close()
        report = perf_baseline.build_sql_report(tracer, copy_path)

    usage = {row["name"]: row["used"] for row in report["index_usage"]}
    assert usage["idx_items_sender"] is True
    assert usage["idx_items_note_unused"] is False
    assert "idx_items_note_unused" in report["unused_indexes"]
    assert "idx_items_sender" not in report["unused_indexes"]
    full_scans = report["full_scan_statements"]
    assert any(
        entry["tables"] == ["items"] and "amount > ?" in entry["sql"]
        for entry in full_scans
    )
    assert not any("sender = ?" in entry["sql"] for entry in full_scans)
    assert all("probe" in entry["sources"] for entry in full_scans)
    serialized = json.dumps(report)
    assert SECRET_SENDER not in serialized
    assert str(SECRET_NUMBER) not in serialized


def test_sql_report_resolves_table_aliases_in_full_scans(tmp_path: Path) -> None:
    source = _make_items_db(tmp_path / "source.sqlite")

    with perf_baseline.temporary_db_copy(
        source, work_root=tmp_path / "work"
    ) as copy_path:
        with perf_baseline.SqlTracer(copy_path) as tracer:
            conn = sqlite3.connect(str(copy_path))
            conn.execute("SELECT i.id FROM items AS i WHERE i.amount > 3").fetchall()
            conn.close()
        report = perf_baseline.build_sql_report(tracer, copy_path)

    assert [entry["tables"] for entry in report["full_scan_statements"]] == [["items"]]


def test_index_created_during_measurement_is_not_reported_unused(
    tmp_path: Path,
) -> None:
    source = _make_items_db(tmp_path / "source.sqlite")

    with perf_baseline.temporary_db_copy(
        source, work_root=tmp_path / "work"
    ) as copy_path:
        source_indexes = perf_baseline.index_names(copy_path)
        with perf_baseline.SqlTracer(copy_path) as tracer:
            conn = sqlite3.connect(str(copy_path))
            conn.execute(
                "CREATE INDEX idx_items_created_during_run ON items(note, sender)"
            )
            conn.close()
        report = perf_baseline.build_sql_report(
            tracer, copy_path, source_indexes=source_indexes
        )

    in_source = {row["name"]: row["in_source"] for row in report["index_usage"]}
    assert in_source["idx_items_created_during_run"] is False
    assert in_source["idx_items_note_unused"] is True
    assert "idx_items_created_during_run" not in report["unused_indexes"]
    assert "idx_items_note_unused" in report["unused_indexes"]


def test_main_reports_missing_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out_path = tmp_path / "out.json"

    exit_code = perf_baseline.main(
        ["--db", str(tmp_path / "missing.sqlite"), "--out", str(out_path)]
    )

    assert exit_code == 2
    assert "database not found" in capsys.readouterr().err
    assert not out_path.exists()


def test_main_writes_privacy_safe_baseline_json(tmp_path: Path) -> None:
    db_path = tmp_path / "live.sqlite"
    KnowledgeDB(db_path).save_email(
        account_email=MARKER_ACCOUNT,
        from_email=MARKER_SENDER,
        subject=MARKER_SUBJECT,
        received_at="2026-10-19T10:00:00+00:00",
        priority="\U0001f535",
        action_line="Check",
        body_summary=MARKER_BODY,
        raw_body=MARKER_BODY,
        attachment_summaries=[],
    )
    corpus_payload = json.loads(
        Path(eval_golden_corpus.DEFAULT_CORPUS_PATH).read_text(encoding="utf-8")
    )
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps(corpus_payload[:2]), encoding="utf-8")
    out_path = tmp_path / "out" / "baseline.json"
    work_root = tmp_path / "work"
    logging_disable_before = logging.root.manager.disable

    exit_code = perf_baseline.main(
        [
            "--db",
            str(db_path),
            "--runs",
            "1",
            "--out",
            str(out_path),
            "--corpus",
            str(corpus_path),
            "--work-dir",
            str(work_root),
        ]
    )

    assert exit_code == 0
    text = out_path.read_text(encoding="utf-8")
    data = json.loads(text)
    assert {
        "schema_version",
        "generated_at",
        "runs",
        "db",
        "web",
        "metrics_snapshot",
        "notification_sla",
        "golden_corpus",
        "sql",
        "errors",
    } <= set(data)
    assert data["runs"] == 1
    routes = {route["rule"]: route for route in data["web"]["routes"]}
    assert "/cockpit" in routes
    assert all(
        {"rule", "endpoint", "status", "median_ms", "statements"} <= set(route)
        for route in routes.values()
    )
    assert any(
        route.get("query_params") == ["account_email"] and route["status"] == 200
        for route in routes.values()
    )
    assert logging.root.manager.disable == logging_disable_before
    assert "median_ms" in data["metrics_snapshot"]
    assert "median_ms" in data["notification_sla"]
    assert data["golden_corpus"]["cases"] == 2
    assert 0 < len(data["golden_corpus"]["top_functions"]) <= 10
    tables = {table["name"]: table for table in data["db"]["tables"]}
    assert tables["emails"]["rows"] == 1
    assert data["sql"]["distinct_statements"] > 0
    assert isinstance(data["sql"]["index_usage"], list)
    for marker in (MARKER_ACCOUNT, MARKER_SENDER, MARKER_SUBJECT, MARKER_BODY):
        assert marker not in text
    assert list(work_root.iterdir()) == []


def test_measure_golden_never_touches_the_configured_processor_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # HYG3: the golden-corpus section must run on the processor's throwaway
    # database; the configured one (cwd-relative database.sqlite when the CLI
    # runs from the repo root) and its runtime overrides stay out of it.
    from mailbot_v26.pipeline import processor as pipeline_processor

    configured_path = tmp_path / "configured.sqlite"
    pipeline_processor.configure_processor_db_path(configured_path)
    pipeline_processor.runtime_override_store.set_value("ui_locale", "xx-user")
    real_evaluate = eval_golden_corpus.evaluate_golden_corpus
    seen: list[tuple[Path, str | None]] = []

    def _spy_evaluate(cases):
        seen.append(
            (
                pipeline_processor.DB_PATH,
                pipeline_processor.runtime_override_store.get_value("ui_locale"),
            )
        )
        return real_evaluate(cases)

    monkeypatch.setattr(eval_golden_corpus, "evaluate_golden_corpus", _spy_evaluate)
    corpus_payload = json.loads(
        Path(eval_golden_corpus.DEFAULT_CORPUS_PATH).read_text(encoding="utf-8")
    )
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps(corpus_payload[:2]), encoding="utf-8")
    tracer = perf_baseline.SqlTracer(tmp_path / "copy.sqlite")

    # The tracer keeps every connection open, as during a real baseline run.
    with tracer:
        result = perf_baseline._measure_golden(corpus_path, 1, tracer)

    assert result["cases"] == 2
    assert result["failed_cases"] == 0
    assert len(seen) == 2  # the timed run and the profile run
    for db_path, ui_locale in seen:
        assert db_path != configured_path
        assert ui_locale is None
        assert not db_path.parent.exists()  # the offline database is deleted
    assert pipeline_processor.DB_PATH == configured_path
    assert pipeline_processor.runtime_override_store.get_value("ui_locale") == "xx-user"
