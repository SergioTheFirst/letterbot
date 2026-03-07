import sqlite3
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_schema_indexes_include_cockpit_support(tmp_path: Path) -> None:
    db_path = tmp_path / "schema.sqlite"
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        for index_name in (
            "idx_emails_account_received_at",
            "idx_emails_received_at_id",
            "idx_emails_account_received_at_id",
            "idx_events_v1_event_account_ts",
            "idx_events_v1_email_event_ts",
            "idx_events_v1_email_ts_id",
            "idx_events_v1_account_ts_id",
            "idx_processing_spans_email_ts",
            "idx_commitments_email_status_created_id",
        ):
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
                (index_name,),
            ).fetchone()
            assert row is not None
