from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.tools.export_data import export_data


def _init_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE events_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                ts_utc REAL NOT NULL,
                ts TEXT,
                account_id TEXT NOT NULL,
                entity_id TEXT,
                email_id INTEGER,
                payload JSON,
                payload_json JSON,
                schema_version INTEGER NOT NULL,
                fingerprint TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE commitments (
                id INTEGER PRIMARY KEY,
                email_row_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                commitment_text TEXT NOT NULL,
                deadline_iso TEXT,
                status TEXT NOT NULL,
                confidence REAL NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE relationship_health_snapshots (
                id TEXT PRIMARY KEY,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                entity_id TEXT NOT NULL,
                health_score REAL,
                reason TEXT,
                components_breakdown JSON,
                data_window_days INTEGER
            );
            """
        )
        conn.execute(
            """
            INSERT INTO events_v1 (
                event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "telegram_delivered",
                1700000000.0,
                "2023-11-14T00:00:00Z",
                "acc",
                None,
                None,
                json.dumps({"bot_token": "secret", "status": "ok"}, ensure_ascii=False),
                json.dumps({"bot_token": "secret", "status": "ok"}, ensure_ascii=False),
                1,
                "fp1",
            ),
        )
        conn.execute(
            """
            INSERT INTO commitments (
                id, email_row_id, source, commitment_text, deadline_iso, status, confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 10, "parser", "deliver", "2023-11-15", "open", 0.9, "2023-11-14 10:00:00"),
        )
        conn.execute(
            """
            INSERT INTO relationship_health_snapshots (
                id, created_at, entity_id, health_score, reason, components_breakdown, data_window_days
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "snap1",
                "2023-11-14 09:00:00",
                "entity",
                75.0,
                "ok",
                json.dumps({"trend": 1}),
                30,
            ),
        )
        conn.commit()


def test_export_determinism(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    _init_db(db_path)

    output_a = tmp_path / "export_a.jsonl"
    output_b = tmp_path / "export_b.jsonl"
    since_dt = datetime(2023, 11, 1, tzinfo=timezone.utc)

    export_data(db_path=db_path, output_path=output_a, since_dt=since_dt)
    export_data(db_path=db_path, output_path=output_b, since_dt=since_dt)

    content_a = output_a.read_text(encoding="utf-8")
    content_b = output_b.read_text(encoding="utf-8")

    assert content_a == content_b
    assert "secret" not in content_a
    assert "***" in content_a
