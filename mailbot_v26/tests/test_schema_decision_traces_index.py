import sqlite3
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_fresh_schema_has_decision_traces_created_at_index(tmp_path: Path) -> None:
    db_path = tmp_path / "schema.sqlite"
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT tbl_name FROM sqlite_master "
            "WHERE type='index' AND name = 'idx_decision_traces_created_at'"
        ).fetchone()
        columns = [
            info[2]
            for info in conn.execute(
                "PRAGMA index_info('idx_decision_traces_created_at')"
            ).fetchall()
        ]
    assert row is not None
    assert row[0] == "decision_traces"
    assert columns == ["created_at"]


def test_schema_sql_script_alone_creates_decision_traces_index() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "storage" / "schema.sql"
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        row = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name = 'idx_decision_traces_created_at'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
