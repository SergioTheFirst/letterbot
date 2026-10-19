"""Developer performance baseline for Letterbot (dev CLI, not on the runtime path).

    python -m mailbot_v26.tools.perf_baseline [--db PATH] [--runs 5] [--out PATH]

The database is opened read-only (``mode=ro``) and copied with the SQLite backup
API into a temporary directory; every measurement runs against that copy and the
copy is always deleted afterwards because it contains mail metadata.

Measured (median of ``--runs``): every parameterless web GET route through the
Flask test client, ``MetricsAggregator.snapshot()``, ``compute_notification_sla()``
and the deterministic golden-corpus pipeline (plus one cProfile run). Every SQL
statement executed on the copy is captured, EXPLAIN QUERY PLAN is run for each
distinct SELECT, and full-table scans and unused indexes are reported.

Privacy: the JSON report never contains row content. Captured SQL is normalised
(string, blob and numeric literals become ``?``) before it is stored, and errors
are reported by exception type only.
"""

from __future__ import annotations

import argparse
import cProfile
import gc
import json
import logging
import os
import platform
import pstats
import re
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence
from urllib.parse import unquote

from mailbot_v26.storage.sqlite_uri import sqlite_readonly_uri

# Untraced connect used for the harness's own connections (copy, EXPLAIN, stats).
_raw_connect = sqlite3.connect

SCHEMA_VERSION = 1
DEFAULT_RUNS = 5
TOP_FUNCTIONS = 10
_COPY_NAME = "perf-copy.sqlite"
_WORK_PREFIX = "letterbot-perf-"
_DELETE_ATTEMPTS = 5
_MEASURE_LOG_LEVEL = logging.WARNING
_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_EXPLAINED_KINDS = frozenset({"SELECT", "WITH"})

_SQL_TOKEN_RE = re.compile(
    r"""
    (?P<blob>[xX]'[^']*(?:'|\Z))
    |(?P<string>'(?:[^']|'')*(?:'|\Z))
    |(?P<ident>"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\])
    |(?P<line_comment>--[^\n]*)
    |(?P<block_comment>/\*.*?(?:\*/|\Z))
    |(?P<param>[?:@$][A-Za-z0-9_]*)
    |(?P<word>[A-Za-z_][A-Za-z0-9_$]*)
    |(?P<number>0[xX][0-9A-Fa-f]+|(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)
    """,
    re.VERBOSE | re.DOTALL,
)
_PLACEHOLDER_LIST_RE = re.compile(r"\bIN\s*\(\s*\?(?:\s*,\s*\?)+\s*\)", re.IGNORECASE)
_BINDINGS_NEEDED_RE = re.compile(r"uses (\d+), and there are 0 supplied")
_INDEX_IN_PLAN_RE = re.compile(r"\bINDEX ([A-Za-z_][A-Za-z0-9_$]*)")
_FULL_SCAN_RE = re.compile(
    r"^SCAN (?:TABLE )?([A-Za-z_][A-Za-z0-9_$]*)(?: AS [A-Za-z_][A-Za-z0-9_$]*)?$"
)
_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$]*)(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$]*))?",
    re.IGNORECASE,
)
_NOT_ALIASES = frozenset(
    {
        "AS",
        "CROSS",
        "EXCEPT",
        "FULL",
        "GROUP",
        "HAVING",
        "INDEXED",
        "INNER",
        "INTERSECT",
        "JOIN",
        "LEFT",
        "LIMIT",
        "NATURAL",
        "NOT",
        "ON",
        "ORDER",
        "OUTER",
        "RIGHT",
        "UNION",
        "USING",
        "WHERE",
        "WINDOW",
    }
)


def _replace_sql_token(match: re.Match[str]) -> str:
    kind = match.lastgroup
    if kind in {"blob", "string", "number"}:
        return "?"
    if kind in {"line_comment", "block_comment"}:
        return " "
    return match.group(0)


def normalize_sql(sql: str) -> str:
    """Return ``sql`` with literals replaced by ``?``, comments removed, spaces collapsed."""
    stripped = _SQL_TOKEN_RE.sub(_replace_sql_token, sql)
    collapsed = " ".join(stripped.split())
    return _PLACEHOLDER_LIST_RE.sub("IN (?, ...)", collapsed)


def _statement_kind(normalized: str) -> str:
    words = normalized.lstrip("( ").split(None, 1)
    return words[0].upper() if words else ""


def _same_file(database: object, target: Path) -> bool:
    if database is None:
        return False
    if isinstance(database, bytes):
        text = os.fsdecode(database)
    elif isinstance(database, os.PathLike):
        text = os.fspath(database)
    else:
        text = str(database)
    if text.startswith("file:"):
        text = unquote(text[len("file:") :].split("?", 1)[0])
        if text.startswith("///"):
            text = text[2:]
        elif text.startswith("//localhost/"):
            text = text[len("//localhost") :]
        if re.match(r"^/[A-Za-z]:", text):
            text = text[1:]
    if text in {"", ":memory:"}:
        return False
    return os.path.normcase(os.path.realpath(text)) == os.path.normcase(
        os.path.realpath(target)
    )


def _read_only_uri(path: Path) -> str:
    return sqlite_readonly_uri(path)


def _backup_read_only(source: Path, copy_path: Path) -> None:
    source_conn = _raw_connect(_read_only_uri(source), uri=True)
    try:
        copy_conn = _raw_connect(str(copy_path))
        try:
            source_conn.backup(copy_conn)
            # Rollback journal: no -wal/-shm next to the copy, read-only opens always work.
            copy_conn.execute("PRAGMA journal_mode=DELETE").fetchone()
        finally:
            copy_conn.close()
    finally:
        source_conn.close()


def _remove_tree(path: Path) -> None:
    last_error: OSError | None = None
    for attempt in range(_DELETE_ATTEMPTS):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            last_error = exc
            gc.collect()
            time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(
        f"could not delete the temporary database copy at {path}; "
        "delete it manually (it contains mail metadata)"
    ) from last_error


@contextmanager
def temporary_db_copy(source: Path, *, work_root: Path | None = None) -> Iterator[Path]:
    """Yield a backup-API copy of ``source`` (opened read-only); always delete it."""
    source = Path(source)
    if not source.is_file():
        raise FileNotFoundError(f"database not found: {source}")
    if work_root is not None:
        Path(work_root).mkdir(parents=True, exist_ok=True)
    work_dir = Path(
        tempfile.mkdtemp(
            prefix=_WORK_PREFIX, dir=str(work_root) if work_root is not None else None
        )
    )
    try:
        copy_path = work_dir / _COPY_NAME
        _backup_read_only(source, copy_path)
        yield copy_path
    finally:
        gc.collect()
        _remove_tree(work_dir)


@dataclass
class _Capture:
    count: int = 0
    sources: set[str] = field(default_factory=set)
    on_copy: bool = False
    raw_example: str | None = None  # kept in memory for EXPLAIN only, never serialised


class SqlTracer:
    """Wrap ``sqlite3.connect`` so every new connection traces its SQL statements.

    Connections opened while active are closed on exit, so the temporary copy can
    be deleted on Windows even when measured code leaks a connection.
    """

    def __init__(self, copy_path: Path) -> None:
        self.copy_path = Path(copy_path)
        self.section = "unlabelled"
        self.statement_count = 0
        self._raw: dict[tuple[str, bool, str], int] = {}
        self._on_copy_cache: dict[str, bool] = {}
        self._connections: list[sqlite3.Connection] = []
        self._original_connect: Callable[..., sqlite3.Connection] | None = None

    def __enter__(self) -> SqlTracer:
        self._original_connect = sqlite3.connect
        sqlite3.connect = self._connect  # type: ignore[assignment]
        return self

    def __exit__(self, *exc_info: object) -> bool:
        if self._original_connect is not None:
            sqlite3.connect = self._original_connect  # type: ignore[assignment]
        self.close_connections()
        return False

    def _connect(self, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        assert self._original_connect is not None
        conn = self._original_connect(*args, **kwargs)
        database = args[0] if args else kwargs.get("database")
        # Cached: realpath() on every connect would add file I/O to every timing.
        cache_key = repr(database)
        on_copy = self._on_copy_cache.get(cache_key)
        if on_copy is None:
            on_copy = _same_file(database, self.copy_path)
            self._on_copy_cache[cache_key] = on_copy

        def _trace(statement: str) -> None:
            self.statement_count += 1
            key = (self.section, on_copy, statement)
            self._raw[key] = self._raw.get(key, 0) + 1

        conn.set_trace_callback(_trace)
        self._connections.append(conn)
        return conn

    def close_connections(self) -> None:
        for conn in self._connections:
            try:
                conn.close()
            except sqlite3.Error as exc:  # e.g. created in another thread
                print(
                    f"perf_baseline: could not close a traced connection: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
        self._connections.clear()

    def captures(self) -> dict[str, _Capture]:
        result: dict[str, _Capture] = {}
        for (section, on_copy, raw), count in self._raw.items():
            entry = result.setdefault(normalize_sql(raw), _Capture())
            entry.count += count
            entry.sources.add(section)
            if on_copy:
                entry.on_copy = True
                if entry.raw_example is None:
                    entry.raw_example = raw
        return result


def _explain(conn: sqlite3.Connection, sql: str) -> list[str]:
    statement = f"EXPLAIN QUERY PLAN {sql}"
    try:
        rows = conn.execute(statement).fetchall()
    except sqlite3.ProgrammingError as exc:
        # Unexpanded SQL (placeholders kept): bind NULLs, the plan does not depend on values.
        match = _BINDINGS_NEEDED_RE.search(str(exc))
        if match is None:
            raise
        rows = conn.execute(statement, (None,) * int(match.group(1))).fetchall()
    return [str(row[3]) for row in rows]


def _table_aliases(normalized_sql: str, tables: set[str]) -> dict[str, str]:
    """Map lower-cased table names and their aliases (EQP prints aliases) to tables."""
    aliases: dict[str, str] = {}
    for match in _TABLE_REF_RE.finditer(normalized_sql):
        table, alias = match.group(1), match.group(2)
        if table.lower() not in tables:
            continue
        aliases[table.lower()] = table
        if alias and alias.upper() not in _NOT_ALIASES:
            aliases[alias.lower()] = table
    return aliases


def _full_scan_tables(details: Sequence[str], aliases: dict[str, str]) -> list[str]:
    found: set[str] = set()
    for detail in details:
        match = _FULL_SCAN_RE.match(detail.strip())
        if match and match.group(1).lower() in aliases:
            found.add(aliases[match.group(1).lower()])
    return sorted(found)


def _plan_record(
    conn: sqlite3.Connection,
    raw_sql: str,
    normalized_sql: str,
    tables: set[str],
    used: set[str],
) -> dict[str, Any]:
    try:
        details = _explain(conn, raw_sql)
    except sqlite3.Error as exc:
        return {"plan_error": type(exc).__name__}
    used.update(
        name.lower() for detail in details for name in _INDEX_IN_PLAN_RE.findall(detail)
    )
    return {
        "plan": [normalize_sql(detail) for detail in details],
        "full_scan_tables": _full_scan_tables(
            details, _table_aliases(normalized_sql, tables)
        ),
        "automatic_index": any("AUTOMATIC" in detail for detail in details),
        "temp_btree": any("TEMP B-TREE" in detail for detail in details),
    }


def index_names(db_path: Path) -> set[str]:
    conn = _raw_connect(_read_only_uri(db_path), uri=True)
    try:
        return {
            str(name)
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }
    finally:
        conn.close()


def build_sql_report(
    tracer: SqlTracer, copy_path: Path, *, source_indexes: set[str] | None = None
) -> dict[str, Any]:
    """Explain captured SELECTs on the copy; report full scans and index usage.

    ``source_indexes`` are the indexes present before measuring: measured code may
    create indexes on the copy (schema init), which then do not exist in the source.
    """
    captures = tracer.captures()
    conn = _raw_connect(_read_only_uri(copy_path), uri=True)
    try:
        tables = {
            str(name).lower()
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        indexes = conn.execute(
            "SELECT name, tbl_name, sql IS NULL FROM sqlite_master "
            "WHERE type = 'index' ORDER BY tbl_name, name"
        ).fetchall()
        used: set[str] = set()
        statements: list[dict[str, Any]] = []
        for normalized in sorted(captures):
            entry = captures[normalized]
            record: dict[str, Any] = {
                "sql": normalized,
                "kind": _statement_kind(normalized),
                "count": entry.count,
                "sources": sorted(entry.sources),
                "on_copy": entry.on_copy,
            }
            if record["kind"] in _EXPLAINED_KINDS and entry.raw_example is not None:
                record.update(
                    _plan_record(conn, entry.raw_example, normalized, tables, used)
                )
            statements.append(record)
    finally:
        conn.close()
    return _assemble_sql_report(tracer, statements, indexes, used, source_indexes)


def _assemble_sql_report(
    tracer: SqlTracer,
    statements: list[dict[str, Any]],
    indexes: Sequence[tuple[str, str, int]],
    used: set[str],
    source_indexes: set[str] | None,
) -> dict[str, Any]:
    index_usage = [
        {
            "name": name,
            "table": table,
            "auto": bool(auto),
            "used": name.lower() in used,
            "in_source": source_indexes is None or name in source_indexes,
        }
        for name, table, auto in indexes
    ]
    full_scans = sorted(
        (
            {
                "sql": record["sql"],
                "tables": record["full_scan_tables"],
                "count": record["count"],
                "sources": record["sources"],
            }
            for record in statements
            if record.get("full_scan_tables")
        ),
        key=lambda item: -item["count"],
    )
    return {
        "statements_executed": tracer.statement_count,
        "distinct_statements": len(statements),
        "explained_selects": sum(1 for record in statements if "plan" in record),
        "explain_errors": sum(1 for record in statements if "plan_error" in record),
        "full_scan_statements": full_scans,
        "unused_indexes": [
            item["name"]
            for item in index_usage
            if not item["used"] and not item["auto"] and item["in_source"]
        ],
        "index_usage": index_usage,
        "statements": statements,
    }


def _summarize(samples: Sequence[float]) -> dict[str, float]:
    return {
        "median_ms": round(statistics.median(samples), 3),
        "min_ms": round(min(samples), 3),
        "max_ms": round(max(samples), 3),
        "first_ms": round(samples[0], 3),
    }


def _measure_callable(
    tracer: SqlTracer, label: str, func: Callable[[], Any], runs: int
) -> dict[str, Any]:
    tracer.section = label
    samples: list[float] = []
    statements: list[int] = []
    for _ in range(runs):
        before = tracer.statement_count
        start = time.perf_counter()
        func()
        samples.append((time.perf_counter() - start) * 1000.0)
        statements.append(tracer.statement_count - before)
    return {
        "statements": statements[0],
        "statements_last": statements[-1],
        **_summarize(samples),
    }


def _time_requests(
    client: Any,
    path: str,
    query: dict[str, str],
    headers: dict[str, str],
    runs: int,
    tracer: SqlTracer,
) -> dict[str, Any]:
    samples: list[float] = []
    statements: list[int] = []
    status: int | None = None
    for _ in range(runs):
        before = tracer.statement_count
        start = time.perf_counter()
        response = client.get(path, query_string=query, headers=headers)
        response.get_data()
        samples.append((time.perf_counter() - start) * 1000.0)
        statements.append(tracer.statement_count - before)
        status = response.status_code
        response.close()
    # First run is cold; later runs may be served from the web cache.
    return {
        "status": status,
        "statements": statements[0],
        "statements_last": statements[-1],
        **_summarize(samples),
    }


def _measure_route(
    client: Any,
    rule: Any,
    headers: dict[str, str],
    runs: int,
    tracer: SqlTracer,
    account: str | None,
) -> dict[str, Any]:
    tracer.section = f"web GET {rule.rule}"
    result: dict[str, Any] = {"rule": rule.rule, "endpoint": rule.endpoint}
    try:
        result.update(_time_requests(client, rule.rule, {}, headers, runs, tracer))
        if result["status"] == 400 and account:
            # Account-scoped API: re-measure with the UI's default account (value never stored).
            scoped = _time_requests(
                client, rule.rule, {"account_email": account}, headers, runs, tracer
            )
            result.update(scoped, unscoped_status=400, query_params=["account_email"])
    except Exception as exc:  # keep measuring the other routes; reported per route
        traceback.print_exc(file=sys.stderr)
        result.update(
            status=None, statements=None, median_ms=None, error_type=type(exc).__name__
        )
    return result


def _measure_web(copy_path: Path, runs: int, tracer: SqlTracer) -> dict[str, Any]:
    from mailbot_v26.web_observability.app import _available_accounts, create_app

    # Doctor paths point into the temp dir, never at real logs or config.
    work_dir = copy_path.parent
    tracer.section = "web create_app"
    start = time.perf_counter()
    app = create_app(
        db_path=copy_path,
        password="perf",
        secret_key="perf",
        api_token="perf",
        config_path=work_dir / "config.yaml",
        log_path=work_dir / "mailbot.log",
        dist_root=work_dir,
    )
    create_ms = (time.perf_counter() - start) * 1000.0
    token = str(app.config.get("COCKPIT_API_TOKEN") or "")
    headers = {"X-Api-Token": token} if token else {}
    rules = sorted(
        (
            rule
            for rule in app.url_map.iter_rules()
            if "GET" in (rule.methods or ())
            and not rule.arguments
            and rule.endpoint != "static"
        ),
        key=lambda rule: rule.rule,
    )
    client = app.test_client()
    with client.session_transaction() as session:
        session["authenticated"] = True
        session["csrf_token"] = "perf"
    tracer.section = "web default account"
    accounts = _available_accounts(copy_path)
    account = accounts[0] if accounts else None
    routes = [
        _measure_route(client, rule, headers, runs, tracer, account) for rule in rules
    ]
    return {"create_app_ms": round(create_ms, 3), "routes": routes}


def _package_function(filename: str, lineno: int, funcname: str) -> str | None:
    if not filename.endswith(".py"):
        return None
    try:
        relative = Path(filename).resolve().relative_to(_PACKAGE_ROOT)
    except (OSError, ValueError):
        return None
    if relative.parts and relative.parts[0] == "tools":
        return None  # golden-corpus driver and this harness, not the runtime pipeline
    return f"{_PACKAGE_ROOT.name}/{relative.as_posix()}:{lineno}({funcname})"


def _top_functions(profiler: cProfile.Profile, limit: int) -> list[dict[str, Any]]:
    stats = pstats.Stats(profiler)
    rows: list[dict[str, Any]] = []
    for key, value in stats.stats.items():  # type: ignore[attr-defined]
        filename, lineno, funcname = key
        _cc, ncalls, tottime, cumtime, _callers = value
        name = _package_function(filename, lineno, funcname)
        if name is None:
            continue
        rows.append(
            {
                "function": name,
                "ncalls": ncalls,
                "tottime_s": round(tottime, 6),
                "cumtime_s": round(cumtime, 6),
            }
        )
    rows.sort(key=lambda row: row["cumtime_s"], reverse=True)
    return rows[:limit]


def _measure_golden(
    corpus_path: Path | None, runs: int, tracer: SqlTracer
) -> dict[str, Any]:
    from mailbot_v26.tools import eval_golden_corpus as golden

    tracer.section = "golden_corpus"
    path = (
        Path(corpus_path)
        if corpus_path is not None
        else Path(golden.DEFAULT_CORPUS_PATH)
    )
    start = time.perf_counter()
    cases = list(golden.load_golden_corpus(str(path)))
    load_ms = (time.perf_counter() - start) * 1000.0
    samples: list[float] = []
    failed_cases = 0
    for _ in range(runs):
        start = time.perf_counter()
        summary = golden.evaluate_golden_corpus(cases)
        samples.append((time.perf_counter() - start) * 1000.0)
        failed_cases = summary.failed_cases
    profiler = cProfile.Profile()
    profiler.enable()
    try:
        golden.evaluate_golden_corpus(cases)
    finally:
        profiler.disable()
    return {
        "cases": len(cases),
        "failed_cases": failed_cases,
        "load_ms": round(load_ms, 3),
        **_summarize(samples),
        "profile_run": "separate run after the timed runs (warm caches); tools/* frames excluded",
        "top_functions": _top_functions(profiler, TOP_FUNCTIONS),
    }


def _db_stats(source: Path, copy_path: Path) -> dict[str, Any]:
    conn = _raw_connect(_read_only_uri(copy_path), uri=True)
    try:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        index_counts = dict(
            conn.execute(
                "SELECT tbl_name, COUNT(*) FROM sqlite_master WHERE type = 'index' GROUP BY tbl_name"
            ).fetchall()
        )
        names = [
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        ]
        tables = [
            _table_stats(conn, name, int(index_counts.get(name, 0))) for name in names
        ]
    finally:
        conn.close()
    wal_path = source.with_name(f"{source.name}-wal")
    return {
        "source_name": source.name,
        "file_bytes": source.stat().st_size,
        "wal_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "freelist_bytes": freelist_count * page_size,
        "table_count": len(tables),
        "index_count": sum(table["indexes"] for table in tables),
        "tables": tables,
    }


def _table_stats(
    conn: sqlite3.Connection, name: str, index_count: int
) -> dict[str, Any]:
    quoted = '"' + name.replace('"', '""') + '"'
    entry: dict[str, Any] = {"name": name, "indexes": index_count}
    try:
        entry["rows"] = int(
            conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
        )
    except sqlite3.Error as exc:  # e.g. virtual table whose module is unavailable
        entry["rows"] = None
        entry["error_type"] = type(exc).__name__
    return entry


def _run_section(
    name: str, errors: list[dict[str, str]], func: Callable[[], dict[str, Any]]
) -> dict[str, Any]:
    try:
        return func()
    except Exception as exc:  # report and continue with the other sections
        traceback.print_exc(file=sys.stderr)
        errors.append({"section": name, "error_type": type(exc).__name__})
        return {"error_type": type(exc).__name__}


def _report_header(runs: int) -> dict[str, Any]:
    from mailbot_v26.version import get_version

    return {
        "schema_version": SCHEMA_VERSION,
        "tool": "mailbot_v26.tools.perf_baseline",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "app_version": get_version(),
        "python": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "runs": runs,
        "conditions": {
            "copy_journal_mode": "delete",
            "log_level_during_measurement": logging.getLevelName(_MEASURE_LOG_LEVEL),
            "sql_trace_during_timing": True,
        },
    }


def _measure_all(
    copy_path: Path,
    runs: int,
    corpus_path: Path | None,
    tracer: SqlTracer,
    errors: list[dict[str, str]],
) -> dict[str, Any]:
    from mailbot_v26.observability.metrics import MetricsAggregator
    from mailbot_v26.observability.notification_sla import compute_notification_sla
    from mailbot_v26.storage.analytics import KnowledgeAnalytics

    return {
        "web": _run_section(
            "web", errors, lambda: _measure_web(copy_path, runs, tracer)
        ),
        "metrics_snapshot": _run_section(
            "metrics_snapshot",
            errors,
            lambda: _measure_callable(
                tracer,
                "metrics_snapshot",
                lambda: MetricsAggregator(copy_path).snapshot(),
                runs,
            ),
        ),
        "notification_sla": _run_section(
            "notification_sla",
            errors,
            lambda: _measure_callable(
                tracer,
                "notification_sla",
                lambda: compute_notification_sla(
                    analytics=KnowledgeAnalytics(copy_path)
                ),
                runs,
            ),
        ),
        "golden_corpus": _run_section(
            "golden_corpus", errors, lambda: _measure_golden(corpus_path, runs, tracer)
        ),
    }


def run_baseline(
    *,
    db_path: Path,
    runs: int,
    out_path: Path,
    corpus_path: Path | None = None,
    work_root: Path | None = None,
) -> dict[str, Any]:
    """Measure on a temporary copy of ``db_path`` and write the JSON report to ``out_path``."""
    report = _report_header(runs)
    errors: list[dict[str, str]] = []
    previous_disable = logging.root.manager.disable
    with temporary_db_copy(Path(db_path), work_root=work_root) as copy_path:
        report["db"] = _db_stats(Path(db_path), copy_path)
        source_indexes = index_names(copy_path)
        tracer = SqlTracer(copy_path)
        # Console I/O would dominate timings; get_logger() resets the root level, so disable.
        logging.disable(max(previous_disable, _MEASURE_LOG_LEVEL - 1))
        try:
            with tracer:
                report.update(
                    _measure_all(copy_path, runs, corpus_path, tracer, errors)
                )
        finally:
            logging.disable(previous_disable)
        report["sql"] = build_sql_report(
            tracer, copy_path, source_indexes=source_indexes
        )
    report["errors"] = errors
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )
    return report


def render_summary(report: dict[str, Any], out_path: Path) -> str:
    lines = [f"perf baseline written to {out_path}"]
    routes = [
        r
        for r in report.get("web", {}).get("routes", [])
        if r.get("median_ms") is not None
    ]
    for route in sorted(routes, key=lambda r: -r["median_ms"])[:5]:
        lines.append(
            f"  route {route['rule']}: {route['median_ms']} ms (status {route['status']}, "
            f"{route.get('statements')} SQL)"
        )
    for key in ("metrics_snapshot", "notification_sla", "golden_corpus"):
        lines.append(f"  {key}: {report.get(key, {}).get('median_ms')} ms")
    sql = report.get("sql", {})
    lines.append(
        f"  SQL: {sql.get('distinct_statements')} distinct, "
        f"{len(sql.get('full_scan_statements', []))} with full-table scans, "
        f"unused indexes: {', '.join(sql.get('unused_indexes', [])) or 'none'}"
    )
    if report.get("errors"):
        lines.append(f"  section errors: {report['errors']}")
    return "\n".join(lines)


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return number


def main(argv: list[str] | None = None) -> int:
    from mailbot_v26.config.paths import app_root

    parser = argparse.ArgumentParser(
        description="Measure Letterbot web/SQL/pipeline performance on a temporary DB copy."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite DB (opened read-only). Default: <app root>/database.sqlite",
    )
    parser.add_argument(
        "--runs",
        type=_positive_int,
        default=DEFAULT_RUNS,
        help="Runs per measurement (median reported).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Report path. Default: <app root>/data/perf/baseline-<date>.json",
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=None,
        help="Golden corpus JSON (default: the bundled corpus).",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Parent folder for the temporary copy (default: system temp).",
    )
    args = parser.parse_args(argv)
    db_path = args.db or app_root() / "database.sqlite"
    out_path = (
        args.out
        or app_root() / "data" / "perf" / f"baseline-{date.today().isoformat()}.json"
    )
    try:
        report = run_baseline(
            db_path=db_path,
            runs=args.runs,
            out_path=out_path,
            corpus_path=args.corpus,
            work_root=args.work_dir,
        )
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"perf_baseline: cannot measure {db_path}: {exc}", file=sys.stderr)
        return 2
    print(render_summary(report, out_path))
    return 0 if not report["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
