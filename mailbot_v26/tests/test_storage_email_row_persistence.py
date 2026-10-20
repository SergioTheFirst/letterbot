"""B24: the worker's analytics land on the Storage row of the email.

The worker stores every email through Storage (``emails.uid NOT NULL``)
before the processor runs. process_message must update that row (priority,
action line, summary, shadow and confidence columns) and link commitments to
it: an INSERT of a second row fails on ``uid``, which left priority,
summaries and commitments unsaved and sender history empty.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.pipeline import processor
from mailbot_v26.storage.knowledge_db import KnowledgeDB

ACCOUNT = "account@example.com"
SENDER = "sender@example.com"
SUBJECT = "Quarterly report canary-subject"
COMMITMENT_BODY = "Вышлю отчет до 25.12.2025."


def _columns(db_path: Path) -> set[str]:
    with closing(sqlite3.connect(db_path)) as conn:
        return {str(row[1]) for row in conn.execute("PRAGMA table_info(emails);")}


def _storage_schema_db(tmp_path: Path) -> tuple[Path, Storage]:
    """A worker database after its first restart: Storage's emails table
    (uid NOT NULL) plus the KnowledgeDB columns."""
    db_path = tmp_path / "mailbot.sqlite"
    storage = Storage(db_path)
    KnowledgeDB(db_path)
    return db_path, storage


def _store_email(storage: Storage, *, uid: int, from_email: str = SENDER) -> int:
    email_id, created = storage.persist_email_and_enqueue(
        account_email=ACCOUNT,
        uid=uid,
        message_id=f"<m{uid}@example.com>",
        from_email=from_email,
        from_name="Sender",
        subject=SUBJECT,
        received_at="2026-10-01T09:00:00+00:00",
        attachments_count=0,
    )
    assert created
    return email_id


def _save(kdb: KnowledgeDB, **overrides: object) -> int | None:
    values: dict[str, object] = {
        "account_email": ACCOUNT,
        "from_email": SENDER,
        "subject": SUBJECT,
        "received_at": "2026-10-01T09:00:00+00:00",
        "priority": "🟡",
        "action_line": "Reply today",
        "body_summary": "Short summary",
        "raw_body": "Body",
        "attachment_summaries": [("report.pdf", "Quarterly numbers")],
    }
    values.update(overrides)
    return kdb.save_email(**values)  # type: ignore[arg-type]


# --- KnowledgeDB.save_email -------------------------------------------------


def test_save_email_updates_the_storage_row_and_keeps_its_id(tmp_path: Path) -> None:
    db_path, storage = _storage_schema_db(tmp_path)
    try:
        email_id = _store_email(storage, uid=10)
    finally:
        storage.close()

    saved = _save(KnowledgeDB(db_path), storage_email_id=email_id)

    assert saved == email_id
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute(
            "SELECT id, uid, priority, action_line, body_summary FROM emails"
        ).fetchall()
        attachments = conn.execute("SELECT email_id, filename FROM attachments").fetchall()
    assert rows == [(email_id, 10, "🟡", "Reply today", "Short summary")]
    assert attachments == [(email_id, "report.pdf")]


def test_save_email_missing_storage_row_is_logged_and_not_inserted(
    tmp_path: Path, caplog
) -> None:
    db_path, storage = _storage_schema_db(tmp_path)
    storage.close()
    caplog.set_level(logging.INFO)

    saved = _save(KnowledgeDB(db_path), storage_email_id=999)

    assert saved is None
    assert "knowledge_email_row_missing email_id=999" in caplog.text
    assert SUBJECT not in caplog.text
    with closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM attachments").fetchone()[0] == 0


def test_save_email_on_legacy_db_without_uid_still_inserts(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    kdb = KnowledgeDB(db_path)
    assert "uid" not in _columns(db_path)

    saved = _save(kdb, storage_email_id=7)

    assert saved is not None
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute("SELECT id, priority FROM emails").fetchall()
    assert rows == [(saved, "🟡")]


def test_save_email_on_legacy_db_never_overwrites_another_row(tmp_path: Path) -> None:
    # Without uid the table holds no Storage rows: an id is no reason to
    # update an unrelated legacy email.
    db_path = tmp_path / "legacy.sqlite"
    kdb = KnowledgeDB(db_path)
    first = _save(kdb, priority="🔵", action_line="Earlier email")

    second = _save(kdb, storage_email_id=first)

    assert second is not None and second != first
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute("SELECT id, action_line FROM emails ORDER BY id").fetchall()
    assert rows == [(first, "Earlier email"), (second, "Reply today")]


def test_ensure_schema_restores_columns_after_storage_recreated_emails(
    tmp_path: Path,
) -> None:
    # Fresh install order in start.main: KnowledgeDB first (emails without
    # uid), then Storage, which recreates the empty table with its own columns.
    db_path = tmp_path / "fresh.sqlite"
    kdb = KnowledgeDB(db_path)
    Storage(db_path).close()
    assert {"uid"} <= _columns(db_path)
    assert "priority" not in _columns(db_path)

    kdb.ensure_schema()

    assert {"uid", "priority", "action_line", "body_summary"} <= _columns(db_path)


# --- process_message on a Storage-schema database ---------------------------


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Send the report",
        body_summary="Report due on Friday.",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )


def _patch_processor(monkeypatch, db_path: Path) -> list[object]:
    processor.configure_processor_db_path(db_path)
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: _llm_result())
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=True,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_COMMITMENT_TRACKER=True,
        ),
    )
    sent: list[object] = []
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda *, email_id, payload: sent.append(payload),
    )
    return sent


def _process(email_id: int, *, body: str = COMMITMENT_BODY) -> None:
    processor.process_message(
        account_email=ACCOUNT,
        message_id=email_id,
        from_email=SENDER,
        subject=SUBJECT,
        received_at=datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc),
        body_text=body,
        attachments=[],
        telegram_chat_id="chat",
    )


def test_process_message_persists_priority_and_commitments_on_storage_row(
    monkeypatch, tmp_path: Path
) -> None:
    db_path, storage = _storage_schema_db(tmp_path)
    try:
        email_id = _store_email(storage, uid=21)
    finally:
        storage.close()
    sent = _patch_processor(monkeypatch, db_path)

    _process(email_id)

    assert len(sent) == 1
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT id, uid, priority, action_line, body_summary, shadow_priority
            FROM emails
            """
        ).fetchall()
        commitments = conn.execute(
            "SELECT DISTINCT email_row_id FROM commitments"
        ).fetchall()
        stats = conn.execute(
            """
            SELECT emails_total, red_count + yellow_count + blue_count
            FROM v_sender_stats WHERE sender_email = ?
            """,
            (SENDER,),
        ).fetchall()
    assert len(rows) == 1, "the Storage row is updated, never duplicated"
    row_id, uid, priority, action_line, body_summary, shadow_priority = rows[0]
    assert (row_id, uid) == (email_id, 21)
    assert priority in {"🔴", "🟡", "🔵"}
    assert action_line.strip()
    assert body_summary.strip()
    assert shadow_priority is not None
    assert commitments == [(email_id,)]
    assert stats == [(1, 1)]


def test_process_message_first_run_after_fresh_install_persists(
    monkeypatch, tmp_path: Path
) -> None:
    # start.main order on a new database: processor singletons (KnowledgeDB)
    # first, then Storage recreates emails, then the KnowledgeDB columns are
    # restored before the first email.
    db_path = tmp_path / "fresh.sqlite"
    sent = _patch_processor(monkeypatch, db_path)
    storage = Storage(db_path)
    try:
        processor.knowledge_db.ensure_schema()
        email_id = _store_email(storage, uid=5)
    finally:
        storage.close()

    _process(email_id, body="Please review the attached numbers.")

    assert len(sent) == 1
    with closing(sqlite3.connect(db_path)) as conn:
        row = conn.execute(
            "SELECT id, priority, body_summary FROM emails WHERE id = ?", (email_id,)
        ).fetchone()
        count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    assert count == 1
    assert row[0] == email_id
    assert row[1] in {"🔴", "🟡", "🔵"}
    assert row[2].strip()
