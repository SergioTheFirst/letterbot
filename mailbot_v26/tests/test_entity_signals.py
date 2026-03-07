import sqlite3
from datetime import datetime, timezone

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_entity_signal_upsert_updates_row(tmp_path) -> None:
    db_path = tmp_path / "crm.sqlite"
    db = KnowledgeDB(db_path)
    computed_at = datetime.now(timezone.utc).isoformat()

    previous = db.upsert_entity_signal(
        entity_id="entity-1",
        signal_type="commitment_reliability",
        score=90,
        label="🟢 Надёжен",
        computed_at=computed_at,
        sample_size=3,
    )
    assert previous is None

    previous = db.upsert_entity_signal(
        entity_id="entity-1",
        signal_type="commitment_reliability",
        score=60,
        label="🟡 Нестабилен",
        computed_at=computed_at,
        sample_size=4,
    )
    assert previous == "🟢 Надёжен"

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT score, label, sample_size
            FROM entity_signals
            WHERE entity_id = ? AND signal_type = ?
            """,
            ("entity-1", "commitment_reliability"),
        ).fetchone()
    assert row == (60, "🟡 Нестабилен", 4)
