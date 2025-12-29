from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _due_time() -> datetime:
    return datetime(2025, 1, 6, 9, 0, tzinfo=timezone.utc)


def _patch_due_config(monkeypatch) -> None:
    monkeypatch.setattr(
        weekly_digest,
        "_load_weekly_digest_config",
        lambda: weekly_digest.WeeklyDigestConfig(weekday=0, hour=9, minute=0),
    )


def _insert_email(
    conn: sqlite3.Connection,
    *,
    account_email: str,
    from_email: str,
    created_at: str,
    body_summary: str = "",
    subject: str = "",
    deferred: bool = False,
    attachment_count: int = 0,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO emails (
            account_email,
            from_email,
            subject,
            received_at,
            priority,
            body_summary,
            deferred_for_digest,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            account_email,
            from_email,
            subject,
            created_at,
            "🔵",
            body_summary,
            1 if deferred else 0,
            created_at,
        ),
    )
    email_id = int(cur.lastrowid)
    for idx in range(attachment_count):
        conn.execute(
            "INSERT INTO attachments (email_id, filename, summary) VALUES (?, ?, ?)",
            (email_id, f"file-{idx}.txt", "summary"),
        )
    return email_id


def _ensure_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trust_snapshots (
            id TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            entity_id TEXT NOT NULL,
            trust_score REAL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS relationship_health_snapshots (
            id TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            entity_id TEXT NOT NULL,
            health_score REAL
        );
        """
    )


def test_weekly_digest_sent_once_per_week(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events.sqlite")

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(weekly_digest, "enqueue_tg", _enqueue_tg)
    _patch_due_config(monkeypatch)

    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=100,
        now=_due_time(),
    )
    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=101,
        now=_due_time(),
    )

    assert len(sent) == 1
    assert db.get_last_weekly_digest_key(account_email="account@example.com") == "2025-W02"


def test_weekly_digest_empty_content_is_deterministic(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events.sqlite")

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(weekly_digest, "enqueue_tg", _enqueue_tg)
    _patch_due_config(monkeypatch)

    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=200,
        now=_due_time(),
    )

    assert len(sent) == 1
    html_text = sent[0]["payload"].html_text
    assert "Объём: всего 0, в дайджест 0" in html_text
    assert "Просроченные (топ-5): нет" in html_text
    assert "Trust score: недостаточно истории" in html_text


def test_weekly_digest_flag_disabled_in_config(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text(
        """
[features]
enable_weekly_digest = false
""".strip(),
        encoding="utf-8",
    )
    flags = FeatureFlags(base_dir=config_dir)
    assert flags.ENABLE_WEEKLY_DIGEST is False


def _event_types(path: Path) -> list[str]:
    with sqlite3.connect(path) as conn:
        cur = conn.execute("SELECT type, payload FROM events ORDER BY timestamp ASC")
        return [str(row[0]) for row in cur.fetchall()]


def test_weekly_digest_attention_block_added_when_flag_on(monkeypatch, tmp_path) -> None:
    now = _due_time()
    created_at = datetime.utcnow().isoformat()
    earlier = (datetime.utcnow() - timedelta(days=5)).isoformat()
    db_path = tmp_path / "weekly.sqlite"
    events_path = tmp_path / "events.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(events_path)

    with sqlite3.connect(db_path) as conn:
        _ensure_snapshot_tables(conn)
        for _ in range(3):
            _insert_email(
                conn,
                account_email="account@example.com",
                from_email="alice@example.com",
                created_at=created_at,
                body_summary=" ".join(["word"] * 400),
                attachment_count=1,
            )
        for _ in range(2):
            _insert_email(
                conn,
                account_email="account@example.com",
                from_email="bob@example.com",
                created_at=created_at,
                body_summary=" ".join(["note"] * 50),
                deferred=True,
            )
        conn.execute(
            "INSERT INTO trust_snapshots (id, entity_id, trust_score, created_at) VALUES (?, ?, ?, ?)",
            ("t1", "alice@example.com", 0.4, earlier),
        )
        conn.execute(
            "INSERT INTO trust_snapshots (id, entity_id, trust_score, created_at) VALUES (?, ?, ?, ?)",
            ("t2", "alice@example.com", 0.6, created_at),
        )
        conn.execute(
            "INSERT INTO relationship_health_snapshots (id, entity_id, health_score, created_at) VALUES (?, ?, ?, ?)",
            ("h1", "bob@example.com", 80.0, earlier),
        )
        conn.execute(
            "INSERT INTO relationship_health_snapshots (id, entity_id, health_score, created_at) VALUES (?, ?, ?, ?)",
            ("h2", "bob@example.com", 60.0, created_at),
        )
        conn.commit()

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(weekly_digest, "enqueue_tg", _enqueue_tg)
    _patch_due_config(monkeypatch)

    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=300,
        now=now,
        include_attention_economics=True,
    )

    assert sent, "digest should be delivered"
    html_text = sent[0]["payload"].html_text
    assert "⏱ Куда ушло внимание" in html_text
    assert "alice@example.com" in html_text
    assert "bob@example.com" in html_text

    types = _event_types(events_path)
    assert "attention_economics_computed" in types
    assert "weekly_digest_attention_block_added" in types


def test_weekly_digest_attention_block_skipped_on_small_sample(monkeypatch, tmp_path) -> None:
    now = _due_time()
    created_at = datetime.utcnow().isoformat()
    db_path = tmp_path / "weekly.sqlite"
    events_path = tmp_path / "events.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(events_path)

    with sqlite3.connect(db_path) as conn:
        _ensure_snapshot_tables(conn)
        _insert_email(
            conn,
            account_email="account@example.com",
            from_email="carol@example.com",
            created_at=created_at,
            body_summary="short text",
        )
        conn.commit()

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(weekly_digest, "enqueue_tg", _enqueue_tg)
    _patch_due_config(monkeypatch)

    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=301,
        now=now,
        include_attention_economics=True,
    )

    assert sent, "digest should still be delivered"
    html_text = sent[0]["payload"].html_text
    assert "⏱ Куда ушло внимание" not in html_text
    assert "Attention economics:" in html_text

    with sqlite3.connect(events_path) as conn:
        cur = conn.execute("SELECT type, payload FROM events WHERE type LIKE 'attention_economics_%'")
        rows = cur.fetchall()
    assert any(row[0] == "attention_economics_skipped" for row in rows)
