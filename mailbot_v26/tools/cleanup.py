from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from mailbot_v26.config_loader import ConfigError, load_storage_config
from mailbot_v26.events.contract import EventType

DEFAULT_BATCH_SIZE = 500
DEFAULT_VACUUM_THRESHOLD_ROWS = 50_000

EVENT_RETENTION_DAYS = {
    EventType.IMAP_HEALTH.value: 30,
    EventType.TG_RENDER_RECORDED.value: 30,
    EventType.DECISION_TRACE_RECORDED.value: 30,
    EventType.TRUST_SCORE_UPDATED.value: 90,
    EventType.RELATIONSHIP_HEALTH_UPDATED.value: 90,
}

TABLE_RETENTION_DAYS = {
    "decision_traces": ("created_at", 30),
    "trust_snapshots": ("created_at", 90),
    "relationship_health_snapshots": ("created_at", 90),
}

PROTECTED_EVENT_TYPES = {
    EventType.EMAIL_RECEIVED.value,
    EventType.MESSAGE_INTERPRETATION.value,
    EventType.TELEGRAM_DELIVERED.value,
    EventType.TELEGRAM_FAILED.value,
    EventType.PRIORITY_DECISION_RECORDED.value,
    EventType.PRIORITY_CORRECTION_RECORDED.value,
    EventType.BUDGET_CONSUMED.value,
    EventType.BUDGET_LIMIT_EXCEEDED.value,
    EventType.BUDGET_LIMIT_NEAR.value,
    EventType.COMMITMENT_CREATED.value,
    EventType.COMMITMENT_STATUS_CHANGED.value,
    EventType.COMMITMENT_EXPIRED.value,
    EventType.DAILY_DIGEST_SENT.value,
    EventType.WEEKLY_DIGEST_SENT.value,
    EventType.DEADLOCK_DETECTED.value,
    EventType.ANOMALY_DETECTED.value,
}

PROTECTED_TABLES = {
    "emails",
    "events",
    "events_v1",
    "priority_feedback",
    "action_feedback",
    "commitments",
    "budget_consumption",
}


@dataclass(frozen=True)
class CleanupTarget:
    scope: str
    name: str
    table: str
    ts_column: str
    retention_days: int


@dataclass(frozen=True)
class CleanupPlanEntry:
    target: CleanupTarget
    eligible_rows: int


@dataclass(frozen=True)
class CleanupResult:
    db_path: Path
    mode: str
    plan: tuple[CleanupPlanEntry, ...]
    deleted_rows: int
    freelist_before: int
    freelist_after: int
    vacuum_performed: bool
    vacuum_reason: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_db_path(base_dir: Path) -> Path:
    config_dir = base_dir / "mailbot_v26" / "config"
    try:
        return load_storage_config(config_dir).db_path
    except ConfigError:
        return base_dir / "data" / "mailbot.sqlite"


def _event_target(event_type: str, retention_days: int) -> CleanupTarget:
    return CleanupTarget(
        scope="event",
        name=event_type,
        table="events_v1",
        ts_column="ts_utc",
        retention_days=retention_days,
    )


def _table_target(table: str, ts_column: str, retention_days: int) -> CleanupTarget:
    return CleanupTarget(
        scope="table",
        name=table,
        table=table,
        ts_column=ts_column,
        retention_days=retention_days,
    )


def resolve_event_cleanup_types(
    requested: Iterable[str] | None = None,
) -> tuple[str, ...]:
    if requested is None:
        return tuple(EVENT_RETENTION_DAYS.keys())
    return tuple(
        event_type
        for event_type in requested
        if event_type in EVENT_RETENTION_DAYS and event_type not in PROTECTED_EVENT_TYPES
    )


def resolve_table_cleanup_targets(
    requested: Iterable[str] | None = None,
) -> tuple[CleanupTarget, ...]:
    items = requested if requested is not None else TABLE_RETENTION_DAYS.keys()
    targets: list[CleanupTarget] = []
    for table in items:
        if table in PROTECTED_TABLES or table not in TABLE_RETENTION_DAYS:
            continue
        ts_column, retention_days = TABLE_RETENTION_DAYS[table]
        targets.append(_table_target(table, ts_column, retention_days))
    return tuple(targets)


def resolve_cleanup_targets() -> tuple[CleanupTarget, ...]:
    event_targets = [
        _event_target(event_type, EVENT_RETENTION_DAYS[event_type])
        for event_type in resolve_event_cleanup_types()
    ]
    return tuple(event_targets) + resolve_table_cleanup_targets()


def _cutoff_utc(target: CleanupTarget) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=target.retention_days)


def _build_count_sql(target: CleanupTarget) -> str:
    if target.scope == "event":
        return (
            "SELECT COUNT(*) FROM events_v1 "
            "WHERE event_type = ? AND ts_utc < ?"
        )
    return (
        f"SELECT COUNT(*) FROM {target.table} "
        f"WHERE datetime({target.ts_column}) < datetime(?)"
    )


def _build_delete_sql(target: CleanupTarget) -> str:
    if target.scope == "event":
        return (
            "DELETE FROM events_v1 WHERE id IN ("
            "SELECT id FROM events_v1 "
            "WHERE event_type = ? AND ts_utc < ? "
            "ORDER BY ts_utc ASC, id ASC LIMIT ?"
            ")"
        )
    return (
        f"DELETE FROM {target.table} WHERE rowid IN ("
        f"SELECT rowid FROM {target.table} "
        f"WHERE datetime({target.ts_column}) < datetime(?) "
        f"ORDER BY datetime({target.ts_column}) ASC, rowid ASC LIMIT ?"
        ")"
    )


def _count_target(conn: sqlite3.Connection, target: CleanupTarget) -> int:
    cutoff = _cutoff_utc(target)
    if target.scope == "event":
        row = conn.execute(
            _build_count_sql(target),
            (target.name, cutoff.timestamp()),
        ).fetchone()
    else:
        row = conn.execute(
            _build_count_sql(target),
            (cutoff.isoformat(),),
        ).fetchone()
    return int(row[0]) if row else 0


def _freelist_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA freelist_count").fetchone()
    return int(row[0]) if row else 0


def build_cleanup_plan(db_path: Path) -> tuple[CleanupPlanEntry, ...]:
    if not db_path.exists():
        return ()
    with sqlite3.connect(db_path) as conn:
        return tuple(
            CleanupPlanEntry(target=target, eligible_rows=_count_target(conn, target))
            for target in resolve_cleanup_targets()
        )


def _delete_batch(
    conn: sqlite3.Connection, target: CleanupTarget, *, batch_size: int
) -> int:
    cutoff = _cutoff_utc(target)
    sql = _build_delete_sql(target)
    if " WHERE " not in sql.upper():
        raise ValueError("Unsafe cleanup SQL without WHERE clause")
    if target.scope == "event":
        cursor = conn.execute(sql, (target.name, cutoff.timestamp(), batch_size))
    else:
        cursor = conn.execute(sql, (cutoff.isoformat(), batch_size))
    return int(cursor.rowcount or 0)


def _run_vacuum(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM")


def run_cleanup(
    *,
    db_path: Path,
    run: bool,
    status_only: bool,
    batch_size: int = DEFAULT_BATCH_SIZE,
    vacuum: bool = False,
    vacuum_threshold_rows: int = DEFAULT_VACUUM_THRESHOLD_ROWS,
) -> CleanupResult:
    db_path = Path(db_path)
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if vacuum_threshold_rows < 0:
        raise ValueError("vacuum_threshold_rows must be >= 0")

    if not db_path.exists():
        return CleanupResult(
            db_path=db_path,
            mode="status" if status_only else ("run" if run else "dry-run"),
            plan=(),
            deleted_rows=0,
            freelist_before=0,
            freelist_after=0,
            vacuum_performed=False,
            vacuum_reason="database_missing",
        )

    mode = "status" if status_only else ("run" if run else "dry-run")
    with sqlite3.connect(db_path) as conn:
        freelist_before = _freelist_count(conn)
        plan = tuple(
            CleanupPlanEntry(target=target, eligible_rows=_count_target(conn, target))
            for target in resolve_cleanup_targets()
        )

    if not run:
        return CleanupResult(
            db_path=db_path,
            mode=mode,
            plan=plan,
            deleted_rows=0,
            freelist_before=freelist_before,
            freelist_after=freelist_before,
            vacuum_performed=False,
            vacuum_reason="not_requested",
        )

    deleted_rows = 0
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("BEGIN")
            for entry in plan:
                if entry.eligible_rows <= 0:
                    continue
                while True:
                    deleted = _delete_batch(
                        conn, entry.target, batch_size=batch_size
                    )
                    deleted_rows += deleted
                    if deleted < batch_size:
                        break
            conn.commit()
            freelist_after = _freelist_count(conn)
        except Exception:
            conn.rollback()
            raise

    should_vacuum = vacuum or (
        vacuum_threshold_rows > 0 and deleted_rows >= vacuum_threshold_rows
    )
    vacuum_reason = "not_requested"
    if vacuum:
        vacuum_reason = "explicit"
    elif should_vacuum:
        vacuum_reason = "threshold"
    if should_vacuum and deleted_rows > 0:
        _run_vacuum(db_path)

    return CleanupResult(
        db_path=db_path,
        mode=mode,
        plan=plan,
        deleted_rows=deleted_rows,
        freelist_before=freelist_before,
        freelist_after=freelist_after,
        vacuum_performed=bool(should_vacuum and deleted_rows > 0),
        vacuum_reason=vacuum_reason,
    )


def format_cleanup_report(result: CleanupResult) -> str:
    lines = [
        "Letterbot cleanup",
        f"Mode: {result.mode}",
        f"DB: {result.db_path}",
    ]
    if not result.plan and result.vacuum_reason == "database_missing":
        lines.append("Database not found; nothing to clean.")
        return "\n".join(lines)

    total_eligible = sum(entry.eligible_rows for entry in result.plan)
    lines.append(f"Eligible rows: {total_eligible}")
    lines.append(f"Freelist before: {result.freelist_before}")
    if result.mode == "run":
        lines.append(f"Deleted rows: {result.deleted_rows}")
        lines.append(f"Freelist after: {result.freelist_after}")
    for entry in result.plan:
        lines.append(
            f"- {entry.target.table}:{entry.target.name} "
            f"older_than_days={entry.target.retention_days} "
            f"eligible={entry.eligible_rows}"
        )
    lines.append(
        "Protected event types: "
        + ", ".join(sorted(PROTECTED_EVENT_TYPES))
    )
    lines.append("Protected tables: " + ", ".join(sorted(PROTECTED_TABLES)))
    lines.append(
        "Vacuum: "
        + (
            f"performed ({result.vacuum_reason})"
            if result.vacuum_performed
            else f"skipped ({result.vacuum_reason})"
        )
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline cleanup for low-value runtime artifacts."
    )
    parser.add_argument("--db", type=Path, help="Override SQLite database path.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true", help="Show cleanup status only.")
    mode.add_argument("--run", action="store_true", help="Execute cleanup.")
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after cleanup.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Delete rows in bounded batches.",
    )
    parser.add_argument(
        "--vacuum-threshold-rows",
        type=int,
        default=DEFAULT_VACUUM_THRESHOLD_ROWS,
        help="Auto-vacuum only when deleted rows reach this threshold.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    db_path = Path(args.db) if args.db else _resolve_db_path(_repo_root())
    result = run_cleanup(
        db_path=db_path,
        run=bool(args.run),
        status_only=bool(args.status),
        batch_size=int(args.batch_size),
        vacuum=bool(args.vacuum),
        vacuum_threshold_rows=int(args.vacuum_threshold_rows),
    )
    print(format_cleanup_report(result))
    if args.run and not db_path.exists():
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
