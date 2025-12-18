from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("database.sqlite")
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


def run_self_check(db_path: Path | str | None = None, project_root: Path | None = None) -> None:
    """
    Выполняет диагностический self-check CRM.
    Никогда не бросает исключения наружу и не меняет поведение бота.
    """
    resolved_db = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
    resolved_root = project_root or PROJECT_ROOT

    _safe_call(_check_schema, resolved_db)
    _safe_call(_check_priority_reason_persistence, resolved_db)
    _safe_call(_check_priority_reason_leaks, resolved_root)
    _safe_call(_log_crm_metrics, resolved_db)


def _safe_call(fn: Callable[..., object], *args: object) -> None:
    try:
        fn(*args)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("[SELF-CHECK] Unexpected failure in %s: %s", getattr(fn, "__name__", fn), exc, exc_info=True)


def _check_schema(db_path: Path) -> None:
    if not db_path.exists():
        logger.error("[SELF-CHECK] CRM DB not found at %s", db_path)
        return

    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            columns = _fetch_columns(conn, "emails")
    except Exception as exc:
        logger.error("[SELF-CHECK] Unable to read schema: %s", exc)
        return

    missing = EMAILS_REQUIRED_COLUMNS - columns
    if missing:
        logger.error("[SELF-CHECK] Missing email columns: %s", ", ".join(sorted(missing)))
    else:
        logger.info("[SELF-CHECK] emails schema OK (%d columns)", len(columns))


def _check_priority_reason_persistence(db_path: Path) -> None:
    if not db_path.exists():
        logger.error("[SELF-CHECK] priority_reason check skipped: DB not found at %s", db_path)
        return

    savepoint = "sc_priority_reason"
    now = datetime.utcnow().isoformat()
    sample_reason = "SELF-CHECK: priority_reason echo"
    sample_subject = "SELF-CHECK priority_reason probe"

    try:
        with sqlite3.connect(f"file:{db_path}?mode=rw", uri=True) as conn:
            conn.execute(f"SAVEPOINT {savepoint};")
            conn.execute(
                """
                INSERT INTO emails (
                    account_email,
                    from_email,
                    subject,
                    received_at,
                    priority,
                    priority_reason,
                    action_line,
                    body_summary,
                    raw_body_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    "self-check@example.com",
                    "diagnostic@example.com",
                    sample_subject,
                    now,
                    "🟡",
                    sample_reason,
                    "Self-check action line",
                    "Self-check body summary",
                    "self-check-hash",
                ),
            )

            row = conn.execute(
                """
                SELECT priority_reason
                FROM emails
                WHERE subject = ?
                ORDER BY id DESC
                LIMIT 1;
                """,
                (sample_subject,),
            ).fetchone()

            conn.execute(f"ROLLBACK TO {savepoint};")
            conn.execute(f"RELEASE {savepoint};")

        if row and (row[0] or "").strip() == sample_reason:
            logger.info("[SELF-CHECK] priority_reason persistence OK")
        else:
            logger.error("[SELF-CHECK] priority_reason is not persisted correctly")
    except sqlite3.OperationalError as exc:
        logger.error("[SELF-CHECK] priority_reason check failed: %s", exc)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("[SELF-CHECK] priority_reason check error: %s", exc, exc_info=True)


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
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            total = _fetch_single_int(conn, "SELECT COUNT(*) FROM emails;")
            priority_counts = {
                "🔴": _fetch_single_int(conn, "SELECT COUNT(*) FROM emails WHERE priority = '🔴';"),
                "🟡": _fetch_single_int(conn, "SELECT COUNT(*) FROM emails WHERE priority = '🟡';"),
                "🔵": _fetch_single_int(conn, "SELECT COUNT(*) FROM emails WHERE priority = '🔵';"),
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
