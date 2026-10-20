from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.config.paths import import_time_db_path
from mailbot_v26.storage.sqlite_uri import sqlite_readonly_uri

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = import_time_db_path()
PROJECT_ROOT = Path(__file__).resolve().parent.parent

EMAILS_REQUIRED_COLUMNS: set[str] = {
    "id",
    "account_email",
    "from_email",
    "subject",
    "received_at",
    "priority",
    "priority_reason",
    "action_line",
    "body_summary",
    "raw_body_hash",
    "created_at",
}


def run_self_check(
    db_path: Path | str | None = None, project_root: Path | None = None
) -> None:
    """
    Выполняет диагностический self-check CRM.
    Никогда не бросает исключения наружу и не меняет поведение бота.

    Read-only: every query opens db_path with mode=ro, so the check never
    writes and never creates a missing database (it is reported instead).
    The worker passes its configured storage.db_path.
    """
    resolved_db = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
    resolved_root = project_root or PROJECT_ROOT

    _safe_call(_check_schema, resolved_db)
    _safe_call(_check_priority_reason_leaks, resolved_root)
    _safe_call(_log_crm_metrics, resolved_db)


def _safe_call(fn: Callable[..., object], *args: object) -> None:
    try:
        fn(*args)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(
            "[SELF-CHECK] Unexpected failure in %s: %s",
            getattr(fn, "__name__", fn),
            exc,
            exc_info=True,
        )


def _check_schema(db_path: Path) -> None:
    if not db_path.exists():
        logger.error("[SELF-CHECK] CRM DB not found at %s", db_path)
        return

    try:
        with sqlite3.connect(sqlite_readonly_uri(db_path), uri=True) as conn:
            columns = _fetch_columns(conn, "emails")
    except Exception as exc:
        logger.error("[SELF-CHECK] Unable to read schema: %s", exc)
        return

    missing = EMAILS_REQUIRED_COLUMNS - columns
    if missing:
        logger.error(
            "[SELF-CHECK] Missing email columns: %s", ", ".join(sorted(missing))
        )
    else:
        logger.info("[SELF-CHECK] emails schema OK (%d columns)", len(columns))


def _check_priority_reason_leaks(project_root: Path) -> None:
    candidate_paths = _collect_candidate_files(project_root)
    leaks: list[Path] = []

    for path in candidate_paths:
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            continue
        if "priority_reason" in content:
            leaks.append(path)

    if leaks:
        joined = ", ".join(str(p.relative_to(project_root)) for p in leaks)
        logger.error("[SELF-CHECK] priority_reason leaks into user output: %s", joined)
    else:
        logger.info("[SELF-CHECK] priority_reason not exposed in Telegram/user output")


def _log_crm_metrics(db_path: Path) -> None:
    if not db_path.exists():
        logger.error("[CRM] DB not found at %s", db_path)
        return

    try:
        with sqlite3.connect(sqlite_readonly_uri(db_path), uri=True) as conn:
            total = _fetch_single_int(conn, "SELECT COUNT(*) FROM emails;")
            priority_counts = {
                "🔴": _fetch_single_int(
                    conn, "SELECT COUNT(*) FROM emails WHERE priority = '🔴';"
                ),
                "🟡": _fetch_single_int(
                    conn, "SELECT COUNT(*) FROM emails WHERE priority = '🟡';"
                ),
                "🔵": _fetch_single_int(
                    conn, "SELECT COUNT(*) FROM emails WHERE priority = '🔵';"
                ),
            }
            escalations = _fetch_single_int(
                conn,
                """
                SELECT COUNT(*) FROM emails
                WHERE TRIM(COALESCE(priority_reason, '')) != '';
                """,
            )
    except Exception as exc:
        logger.error("[CRM] Unable to read metrics: %s", exc)
        return

    summary_parts = [f"{emoji}={count}" for emoji, count in priority_counts.items()]
    summary = "; ".join(summary_parts)
    logger.info(
        "[CRM] total=%d; priorities=%s; escalations=%d",
        total,
        summary,
        escalations,
    )


def _fetch_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}


def _fetch_single_int(conn: sqlite3.Connection, query: str) -> int:
    cur = conn.execute(query)
    row = cur.fetchone()
    try:
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _collect_candidate_files(project_root: Path) -> Iterable[Path]:
    files = [
        project_root / "worker" / "telegram_sender.py",
        project_root / "bot_core" / "pipeline.py",
        project_root / "formatter.py",
    ]
    return [path for path in files if path.exists()]


__all__ = ["run_self_check"]
