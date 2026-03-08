from __future__ import annotations

import sqlite3
import sys
import types
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.storage.knowledge_db import KnowledgeDB

# Stub missing pipeline dependencies before importing the processor
if "mailbot_v26.pipeline.stage_llm" not in sys.modules:
    stage_llm = types.ModuleType("mailbot_v26.pipeline.stage_llm")
    stage_llm.run_llm_stage = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_llm"] = stage_llm

if "mailbot_v26.pipeline.stage_telegram" not in sys.modules:
    stage_telegram = types.ModuleType("mailbot_v26.pipeline.stage_telegram")
    stage_telegram.enqueue_tg = lambda **kwargs: None
    stage_telegram.send_preview_to_telegram = lambda **kwargs: None
    sys.modules["mailbot_v26.pipeline.stage_telegram"] = stage_telegram

from mailbot_v26.pipeline import processor


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔵",
        action_line="Action line",
        body_summary="Body summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
    )


def _common_monkeypatches(monkeypatch, db_path, *, enable_shadow: bool) -> None:
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=enable_shadow,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🟡", "reason1"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [("Перезвонить поставщику", "reason2")],
    )


def _process_once(monkeypatch, tmp_path, *, enable_shadow: bool):
    db_path = tmp_path / ("shadow_on.sqlite" if enable_shadow else "shadow_off.sqlite")
    sent: dict[str, object] = {}

    _common_monkeypatches(monkeypatch, db_path, enable_shadow=enable_shadow)

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=99 if enable_shadow else 98,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    return db_path, sent


def test_flag_off_skips_shadow_fields(monkeypatch, tmp_path):
    db_path = tmp_path / "shadow_disabled.sqlite"

    _common_monkeypatches(monkeypatch, db_path, enable_shadow=False)
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 2, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(emails);")}
        shadow_cols = {
            "shadow_priority",
            "shadow_priority_reason",
            "shadow_action_line",
            "shadow_action_reason",
        }

        if shadow_cols.issubset(columns):
            row = conn.execute("""
                SELECT
                    shadow_priority,
                    shadow_priority_reason,
                    shadow_action_line,
                    shadow_action_reason
                FROM emails ORDER BY id DESC LIMIT 1
                """).fetchone()
            assert row == (None, None, None, None)
        else:
            assert not shadow_cols & columns


def test_flag_on_persists_shadow_fields(monkeypatch, tmp_path):
    db_path = tmp_path / "shadow_enabled.sqlite"

    _common_monkeypatches(monkeypatch, db_path, enable_shadow=True)
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=2,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 2, 2, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("""
            SELECT
                shadow_priority,
                shadow_priority_reason,
                shadow_action_line,
                shadow_action_reason
            FROM emails ORDER BY id DESC LIMIT 1
            """).fetchone()

    assert row == ("🟡", "reason1", "Перезвонить поставщику", "reason2")


def test_telegram_payload_unchanged(monkeypatch, tmp_path):
    off_db, sent_off = _process_once(monkeypatch, tmp_path, enable_shadow=False)
    on_db, sent_on = _process_once(monkeypatch, tmp_path, enable_shadow=True)

    off_payload = sent_off["payload"]
    on_payload = sent_on["payload"]
    assert (off_payload.html_text, off_payload.priority, off_payload.metadata) == (
        on_payload.html_text,
        on_payload.priority,
        on_payload.metadata,
    )

    with sqlite3.connect(off_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
    with sqlite3.connect(on_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
