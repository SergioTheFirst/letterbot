from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _due_time() -> datetime:
    return datetime(2025, 1, 6, 9, 0, tzinfo=timezone.utc)


def _parse_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


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


def _emit_email_event(
    emitter: ContractEventEmitter,
    *,
    email_id: int,
    account_email: str,
    from_email: str,
    subject: str,
    body_summary: str,
    attachments_count: int,
    ts: datetime,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=ts.timestamp(),
            account_id=account_email,
            entity_id=None,
            email_id=email_id,
            payload={
                "from_email": from_email,
                "subject": subject,
                "body_summary": body_summary,
                "attachments_count": attachments_count,
            },
        )
    )


def test_weekly_digest_sent_once_per_week(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events.sqlite")
    contract_emitter = ContractEventEmitter(db_path)

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
        contract_event_emitter=contract_emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=100,
        now=_due_time(),
    )
    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        contract_event_emitter=contract_emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=101,
        now=_due_time(),
    )

    assert len(sent) == 1
    assert (
        db.get_last_weekly_digest_key(account_email="account@example.com") == "2025-W02"
    )


def test_weekly_digest_empty_content_is_deterministic(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events.sqlite")
    contract_emitter = ContractEventEmitter(db_path)

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
        contract_event_emitter=contract_emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=200,
        now=_due_time(),
    )

    assert len(sent) == 1
    html_text = sent[0]["payload"].html_text
    assert "За неделю 0 писем. Главное:" in html_text
    assert "• Спокойная неделя: критичных сигналов не было." in html_text


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


def test_weekly_digest_attention_block_added_when_flag_on(
    monkeypatch, tmp_path
) -> None:
    now = _due_time()
    created_at = datetime.utcnow().isoformat()
    earlier = (datetime.utcnow() - timedelta(days=5)).isoformat()
    db_path = tmp_path / "weekly.sqlite"
    events_path = tmp_path / "events.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(events_path)
    contract_emitter = ContractEventEmitter(db_path)

    with sqlite3.connect(db_path) as conn:
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
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, account_email, from_email, subject, body_summary, created_at FROM emails"
        ).fetchall()
        for row in rows:
            _emit_email_event(
                contract_emitter,
                email_id=int(row["id"]),
                account_email=str(row["account_email"]),
                from_email=str(row["from_email"]),
                subject=str(row["subject"] or ""),
                body_summary=str(row["body_summary"] or ""),
                attachments_count=1 if row["from_email"] == "alice@example.com" else 0,
                ts=_parse_ts(str(row["created_at"])),
            )
        deferred_rows = conn.execute(
            "SELECT id, account_email FROM emails WHERE deferred_for_digest = 1"
        ).fetchall()
        for row in deferred_rows:
            contract_emitter.emit(
                EventV1(
                    event_type=EventType.ATTENTION_DEFERRED_FOR_DIGEST,
                    ts_utc=now.timestamp(),
                    account_id=str(row["account_email"]),
                    entity_id=None,
                    email_id=int(row["id"]),
                    payload={
                        "reason": "test",
                        "attachments_only": False,
                        "attachments_count": 0,
                    },
                )
            )
        contract_emitter.emit(
            EventV1(
                event_type=EventType.TRUST_SCORE_UPDATED,
                ts_utc=_parse_ts(earlier).timestamp(),
                account_id="account@example.com",
                entity_id="alice@example.com",
                email_id=None,
                payload={"trust_score": 0.4},
            )
        )
        contract_emitter.emit(
            EventV1(
                event_type=EventType.TRUST_SCORE_UPDATED,
                ts_utc=_parse_ts(created_at).timestamp(),
                account_id="account@example.com",
                entity_id="alice@example.com",
                email_id=None,
                payload={"trust_score": 0.6},
            )
        )
        contract_emitter.emit(
            EventV1(
                event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
                ts_utc=_parse_ts(earlier).timestamp(),
                account_id="account@example.com",
                entity_id="bob@example.com",
                email_id=None,
                payload={"health_score": 80.0},
            )
        )
        contract_emitter.emit(
            EventV1(
                event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
                ts_utc=_parse_ts(created_at).timestamp(),
                account_id="account@example.com",
                entity_id="bob@example.com",
                email_id=None,
                payload={"health_score": 60.0},
            )
        )

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
        contract_event_emitter=contract_emitter,
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


def test_weekly_digest_attention_block_skipped_on_small_sample(
    monkeypatch, tmp_path
) -> None:
    now = _due_time()
    created_at = datetime.utcnow().isoformat()
    db_path = tmp_path / "weekly.sqlite"
    events_path = tmp_path / "events.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(events_path)
    contract_emitter = ContractEventEmitter(db_path)

    with sqlite3.connect(db_path) as conn:
        _insert_email(
            conn,
            account_email="account@example.com",
            from_email="carol@example.com",
            created_at=created_at,
            body_summary="short text",
        )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, account_email, from_email, subject, body_summary, created_at FROM emails"
        ).fetchone()
        _emit_email_event(
            contract_emitter,
            email_id=int(row["id"]),
            account_email=str(row["account_email"]),
            from_email=str(row["from_email"]),
            subject=str(row["subject"] or ""),
            body_summary=str(row["body_summary"] or ""),
            attachments_count=0,
            ts=_parse_ts(str(row["created_at"])),
        )

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
        contract_event_emitter=contract_emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=301,
        now=now,
        include_attention_economics=True,
    )

    assert sent, "digest should still be delivered"
    html_text = sent[0]["payload"].html_text
    assert "⏱ Куда ушло внимание" not in html_text
    assert "Спокойная неделя" in html_text

    with sqlite3.connect(events_path) as conn:
        cur = conn.execute(
            "SELECT type, payload FROM events WHERE type LIKE 'attention_economics_%'"
        )
        rows = cur.fetchall()
    assert any(row[0] == "attention_economics_skipped" for row in rows)


def test_weekly_uses_interpretation_events_only(tmp_path) -> None:
    test_weekly_uses_interpretation_events(tmp_path)


def test_weekly_uses_interpretation_not_raw_text(tmp_path) -> None:
    test_weekly_uses_interpretation_events(tmp_path)


def test_weekly_uses_interpretation_events(tmp_path) -> None:
    db_path = tmp_path / "weekly-interpretation.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for payload in (
            {
                "doc_kind": "invoice",
                "amount": 87500,
                "sender_email": "a@example.com",
                "due_date": "2026-04-15",
            },
            {
                "doc_kind": "invoice",
                "amount": 12500,
                "sender_email": "b@example.com",
                "due_date": "2026-04-16",
            },
            {"doc_kind": "contract", "sender_email": "c@example.com", "due_date": None},
        ):
            conn.execute(
                """
                INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "message_interpretation",
                    now.timestamp(),
                    now.isoformat(),
                    "account@example.com",
                    None,
                    1,
                    json.dumps(payload, ensure_ascii=False),
                    json.dumps(payload, ensure_ascii=False),
                    1,
                    f"mi-{payload.get('doc_kind')}-{payload.get('amount', 0)}",
                ),
            )
        conn.commit()

    invoice_count, invoice_total, contract_count = (
        weekly_digest._collect_weekly_human_signals(  # noqa: SLF001
            analytics=analytics,
            account_email="account@example.com",
            account_emails=["account@example.com"],
        )
    )

    assert invoice_count == 2
    assert invoice_total == 100000
    assert contract_count == 1


def test_shareable_weekly_card_format() -> None:
    data = weekly_digest.WeeklyDigestData(
        week_key="2025-W02",
        total_emails=47,
        deferred_emails=0,
        attention_entities=(),
        commitment_counts={},
        overdue_commitments=(),
        trust_deltas={"up": [], "down": []},
        anomaly_alerts=[],
        weekly_accuracy_progress=weekly_digest.WeeklyAccuracyProgress(
            current_surprise_rate_pp=11.0,
            prev_surprise_rate_pp=14,
            delta_pp=3,
            current_decisions=12,
            prev_decisions=8,
            current_corrections=5,
        ),
        invoice_count=3,
        invoice_total_rub=387000,
        contract_count=2,
    )

    card = weekly_digest._build_shareable_weekly_card(data)

    lines = card.splitlines()
    assert len(lines) <= 6
    assert lines[0] == "📊 My Mail Week"
    assert "47 emails processed" in card
    assert "3 invoices detected (387000 ₽)" in card
    assert "2 contracts waiting" in card
    assert "accuracy: 89%" in card
    assert "letterbot.ru" in card


def test_shareable_card_deterministic() -> None:
    data = weekly_digest.WeeklyDigestData(
        week_key="2025-W02",
        total_emails=10,
        deferred_emails=0,
        attention_entities=(),
        commitment_counts={},
        overdue_commitments=(),
        trust_deltas={"up": [], "down": []},
        anomaly_alerts=[],
        invoice_count=1,
        invoice_total_rub=2500,
        contract_count=1,
    )

    first = weekly_digest._build_shareable_weekly_card(data)
    second = weekly_digest._build_shareable_weekly_card(data)

    assert first == second


def test_weekly_digest_contains_shareable_card(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly-share.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events-share.sqlite")
    contract_emitter = ContractEventEmitter(db_path)

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
        contract_event_emitter=contract_emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=999,
        now=_due_time(),
    )

    assert sent
    payload = sent[0]["payload"]
    assert "Powered by LetterBot.ru" in payload.html_text
    assert "Share this report" in payload.html_text
    assert "📊 My Mail Week" in payload.html_text
    assert payload.metadata["shareable_weekly_qr_url"] == "https://letterbot.ru"
    assert payload.reply_markup == {
        "inline_keyboard": [
            [
                {
                    "text": "📤 Поделиться отчётом",
                    "switch_inline_query_current_chat": payload.metadata[
                        "shareable_weekly_card"
                    ],
                }
            ]
        ]
    }
