from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.processor import PriorityResultV2
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup(monkeypatch, tmp_path: Path) -> tuple[Path, dict[str, object]]:
    db_path = tmp_path / "priority_source_guard.sqlite"
    sent: dict[str, object] = {}

    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(processor, "DB_PATH", db_path)
    monkeypatch.setattr(
        processor,
        "analytics",
        SimpleNamespace(sender_relationship_profile=lambda **kwargs: None),
    )
    monkeypatch.setattr(
        processor,
        "decision_trace_writer",
        SimpleNamespace(write=lambda **kwargs: None),
    )
    monkeypatch.setattr(
        processor,
        "context_store",
        SimpleNamespace(
            resolve_sender_entity=lambda **kwargs: None,
            record_interaction_event=lambda **kwargs: (None, None),
            recompute_email_frequency=lambda **kwargs: (0.0, 0),
        ),
    )
    monkeypatch.setattr(
        processor,
        "runtime_flag_store",
        SimpleNamespace(get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=False), False)),
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_PRIORITY_V2=False,
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.8,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_COMMITMENT_TRACKER=False,
        ),
    )
    monkeypatch.setattr(
        processor,
        "shadow_priority_engine",
        SimpleNamespace(compute=lambda **kwargs: ("🔵", "shadow")),
    )
    monkeypatch.setattr(
        processor,
        "shadow_action_engine",
        SimpleNamespace(compute=lambda **kwargs: []),
    )

    def _enqueue_tg(*, email_id: int, payload):
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    monkeypatch.setattr(processor, "send_system_notice", lambda **kwargs: None)
    monkeypatch.setattr(
        processor,
        "run_llm_stage",
        lambda **kwargs: {
            "summary": "summary",
            "action_line": "Проверить",
            "priority": "🔴",
            "attachment_summaries": [],
        },
    )

    return db_path, sent


def _insert_snapshot_row(*, db_path: Path, email_id: int, priority: str, priority_source: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (
                id,
                account_email,
                from_email,
                subject,
                received_at,
                priority,
                priority_source,
                action_line,
                body_summary,
                raw_body_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                email_id,
                "account@example.com",
                "sender@example.com",
                "Subject",
                datetime(2024, 1, 1, 12, 0).isoformat(),
                priority,
                priority_source,
                "Проверить",
                "",
                "hash",
            ),
        )
        conn.commit()


def _process_once(*, email_id: int) -> None:
    processor.process_message(
        account_email="account@example.com",
        message_id=email_id,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )


def test_user_priority_override_blocks_auto_priority(monkeypatch, tmp_path: Path) -> None:
    db_path, sent = _setup(monkeypatch, tmp_path)
    _insert_snapshot_row(db_path=db_path, email_id=101, priority="🟡", priority_source="user_override")

    monkeypatch.setattr(
        processor,
        "_compute_heuristic_priority",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not compute auto priority")),
    )

    _process_once(email_id=101)

    assert sent["payload"].priority == "🟡"
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT priority, priority_source FROM emails ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row == ("🟡", "user_override")


def test_enrichment_does_not_revert_priority(monkeypatch, tmp_path: Path) -> None:
    db_path, sent = _setup(monkeypatch, tmp_path)
    _insert_snapshot_row(db_path=db_path, email_id=202, priority="🟡", priority_source="user_override")

    monkeypatch.setattr(
        processor,
        "_compute_heuristic_priority",
        lambda **kwargs: PriorityResultV2(priority="🔴", score=100, breakdown=(), reason_codes=("HIGH",)),
    )

    _process_once(email_id=202)
    first_priority = sent["payload"].priority
    _process_once(email_id=202)
    second_priority = sent["payload"].priority

    assert first_priority == "🟡"
    assert second_priority == "🟡"


def test_new_email_still_uses_auto_priority(monkeypatch, tmp_path: Path) -> None:
    db_path, sent = _setup(monkeypatch, tmp_path)

    monkeypatch.setattr(
        processor,
        "_compute_heuristic_priority",
        lambda **kwargs: PriorityResultV2(priority="🔴", score=100, breakdown=(), reason_codes=("HIGH",)),
    )
    monkeypatch.setattr(
        processor,
        "run_llm_stage",
        lambda **kwargs: {
            "summary": "summary",
            "action_line": "Проверить",
            "attachment_summaries": [],
        },
    )

    _process_once(email_id=303)

    assert sent["payload"].priority == "🔴"
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT priority_source FROM emails ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row == ("auto",)
