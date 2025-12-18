from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from pytest import LogCaptureFixture

from mailbot_v26.storage.self_check import run_self_check


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "database.sqlite"
    schema_src = Path(__file__).resolve().parents[1] / "storage" / "schema.sql"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_src.read_text(encoding="utf-8"))
    return db_path


def test_self_check_logs_and_preserves_data(tmp_path: Path, caplog: LogCaptureFixture) -> None:
    db_path = _init_db(tmp_path)

    with sqlite3.connect(db_path) as conn:
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "acc@example.com",
                "from@example.com",
                "Initial",
                datetime.utcnow().isoformat(),
                "🟡",
                "Escalated due to history",
                "Do something",
                "Summary",
                "hash1",
            ),
        )
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "acc@example.com",
                "from@example.com",
                "Secondary",
                datetime.utcnow().isoformat(),
                "🔵",
                None,
                "Action",
                "Body",
                "hash2",
            ),
        )
        conn.commit()
        initial_count = conn.execute("SELECT COUNT(*) FROM emails;").fetchone()[0]

    caplog.set_level(logging.INFO)
    run_self_check(db_path=db_path, project_root=Path(__file__).resolve().parents[1])

    joined = " | ".join(caplog.messages)
    assert "[SELF-CHECK] emails schema OK" in joined
    assert "priority_reason persistence OK" in joined
    assert "priority_reason not exposed in Telegram/user output" in joined
    assert "[CRM]" in joined

    with sqlite3.connect(db_path) as conn:
        final_count = conn.execute("SELECT COUNT(*) FROM emails;").fetchone()[0]

    assert final_count == initial_count
