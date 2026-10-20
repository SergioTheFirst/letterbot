"""B16: every Telegram command and reminder stays inside the requesting chat.

Two mail accounts route to two different chats. A chat must only see the
accounts whose ``telegram_chat_id`` is that chat (the admin chat sees all),
``/digest off`` must only silence the chat that sent it, and a snooze reminder
must go to the chat of the email's own account.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.telegram.inbound import (
    TelegramInboundProcessor,
    build_chat_account_scopes,
    read_chat_digest_override,
)
from mailbot_v26.text.mojibake import normalize_mojibake_text
from mailbot_v26.worker.telegram_sender import DeliveryResult

ACCOUNT_A = "alice@example.com"
ACCOUNT_B = "bob@example.com"
CHAT_A = "1001"
CHAT_B = "2002"
ADMIN_CHAT = "9009"


def _norm(text: str) -> str:
    return normalize_mojibake_text(str(text))


def _account(account_id: str, login: str, chat_id: str) -> AccountConfig:
    return AccountConfig(
        account_id=account_id,
        login=login,
        password="pass",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id=chat_id,
    )


def _accounts() -> list[AccountConfig]:
    return [_account("acc_a", ACCOUNT_A, CHAT_A), _account("acc_b", ACCOUNT_B, CHAT_B)]


def _gate() -> GateResult:
    return GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )


class _Stub:
    def __init__(self, result: GateResult) -> None:
        self._result = result

    def evaluate(self, **_kwargs) -> GateResult:
        return self._result


def _build_processor(
    tmp_path: Path, sent: list[tuple[str, str]], *, admin_chat_id: str = ""
) -> TelegramInboundProcessor:
    db_path = tmp_path / "knowledge.sqlite"

    def _send_reply(chat_id: str, text: str) -> DeliveryResult:
        sent.append((chat_id, text))
        return DeliveryResult(delivered=True, retryable=False)

    scopes = build_chat_account_scopes(_accounts(), admin_chat_id=admin_chat_id)
    allowed = {CHAT_A, CHAT_B} | ({admin_chat_id} if admin_chat_id else set())
    return TelegramInboundProcessor(
        knowledge_db=KnowledgeDB(db_path),
        analytics=KnowledgeAnalytics(db_path),
        event_emitter=EventEmitter(tmp_path / "events.sqlite"),
        contract_event_emitter=ContractEventEmitter(db_path),
        runtime_flag_store=RuntimeFlagStore(path=tmp_path / "runtime_flags.json"),
        auto_priority_gate=_Stub(_gate()),
        auto_priority_gate_config=AutoPriorityGateConfig(enabled=True),
        override_store=RuntimeOverrideStore(db_path),
        send_reply=_send_reply,
        feature_flags=FeatureFlags(base_dir=tmp_path),
        allowed_chat_ids=frozenset(allowed),
        bot_token="token",
        locale="ru",
        chat_account_emails=scopes,
    )


def _insert_email(db_path: Path, *, account_email: str, priority: str = "🔴") -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO emails (
                account_email, from_email, subject, received_at,
                priority, action_line, body_summary, raw_body_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_email,
                "sender@example.com",
                "Subject",
                datetime.now(timezone.utc).isoformat(),
                priority,
                "Проверить",
                "",
                f"hash-{account_email}",
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def _insert_commitment(db_path: Path, *, email_id: int, text: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO commitments (
                email_row_id, source, commitment_text, deadline_iso,
                status, confidence, created_at
            ) VALUES (?, 'llm', ?, NULL, 'pending', 0.9, ?)
            """,
            (email_id, text, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def _message(chat_id: str, text: str) -> dict[str, object]:
    return {"chat": {"id": chat_id}, "text": text}


def test_build_chat_account_scopes_groups_accounts_by_chat() -> None:
    accounts = _accounts() + [_account("acc_c", "carol@example.com", CHAT_A)]

    scopes = build_chat_account_scopes(accounts, admin_chat_id=ADMIN_CHAT)

    assert scopes[CHAT_A] == (ACCOUNT_A, "carol@example.com")
    assert scopes[CHAT_B] == (ACCOUNT_B,)
    assert scopes[ADMIN_CHAT] == (ACCOUNT_A, ACCOUNT_B, "carol@example.com")


def test_build_chat_account_scopes_without_admin_chat() -> None:
    scopes = build_chat_account_scopes(_accounts(), admin_chat_id="")

    assert set(scopes) == {CHAT_A, CHAT_B}


def test_status_lists_only_accounts_of_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_A)
    _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_B)

    processor.handle_message(_message(CHAT_A, "/status"))
    processor.handle_message(_message(CHAT_B, "/status"))

    (chat_a, status_a), (chat_b, status_b) = sent
    assert (chat_a, chat_b) == (CHAT_A, CHAT_B)
    assert ACCOUNT_A in status_a and ACCOUNT_B not in status_a
    assert ACCOUNT_B in status_b and ACCOUNT_A not in status_b


def test_admin_chat_status_lists_every_account(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent, admin_chat_id=ADMIN_CHAT)

    processor.handle_message(_message(ADMIN_CHAT, "/status"))

    assert ACCOUNT_A in sent[-1][1]
    assert ACCOUNT_B in sent[-1][1]


def test_commitments_scoped_to_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    email_a = _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_A)
    email_b = _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_B)
    _insert_commitment(processor.knowledge_db.path, email_id=email_a, text="Задача Алисы")
    _insert_commitment(processor.knowledge_db.path, email_id=email_b, text="Задача Боба")

    processor.handle_message(_message(CHAT_A, "/commitments"))

    reply = _norm(sent[-1][1])
    assert "Задача Алисы" in reply
    assert "Задача Боба" not in reply


def test_week_counts_only_accounts_of_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_A)
    for _ in range(3):
        _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_B)

    processor.handle_message(_message(CHAT_A, "/week"))
    processor.handle_message(_message(CHAT_B, "/week"))

    assert _norm("Важных: 1 ·") in _norm(sent[0][1])
    assert _norm("Важных: 3 ·") in _norm(sent[1][1])


def test_stats_transitions_scoped_to_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    email_b = _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_B)
    processor.contract_event_emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=datetime.now(timezone.utc).timestamp(),
            account_id=ACCOUNT_B,
            entity_id=None,
            email_id=email_b,
            payload={"old_priority": "🔵", "new_priority": "🔴"},
        )
    )

    processor.handle_message(_message(CHAT_A, "/stats"))
    processor.handle_message(_message(CHAT_B, "/stats"))

    assert "×1" not in sent[0][1]
    assert _norm("🔵→🔴 ×1") in _norm(sent[1][1])


def test_chat_without_accounts_sees_no_account_data(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    processor.allowed_chat_ids = frozenset({CHAT_A, CHAT_B, "3003"})
    email_a = _insert_email(processor.knowledge_db.path, account_email=ACCOUNT_A)
    _insert_commitment(processor.knowledge_db.path, email_id=email_a, text="Задача Алисы")

    processor.handle_message(_message("3003", "/status"))
    processor.handle_message(_message("3003", "/commitments"))

    assert ACCOUNT_A not in sent[0][1]
    assert "Задача Алисы" not in _norm(sent[1][1])


def test_digest_off_applies_only_to_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)

    processor.handle_message(_message(CHAT_A, "/digest off"))

    store = processor.override_store
    assert read_chat_digest_override(store, CHAT_A) is False
    assert read_chat_digest_override(store, CHAT_B) is None
    assert store.get_overrides().digest_enabled is None
    assert _norm(sent[-1][1]) == _norm("Дайджесты выключены.")


def test_digest_toggle_callback_applies_only_to_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)

    processor._apply_toggle(CHAT_B, {"flag": "digest", "value": "off"})

    store = processor.override_store
    assert read_chat_digest_override(store, CHAT_B) is False
    assert read_chat_digest_override(store, CHAT_A) is None


def test_status_digest_flag_reflects_requesting_chat(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    processor.handle_message(_message(CHAT_A, "/digest off"))
    processor.handle_message(_message(CHAT_B, "/digest on"))

    processor.handle_message(_message(CHAT_A, "/status"))
    processor.handle_message(_message(CHAT_B, "/status"))

    assert _norm("Дайджесты: выключены") in _norm(sent[-2][1])
    assert _norm("Дайджесты: включены") in _norm(sent[-1][1])


def test_chat_digest_override_falls_back_to_legacy_global_key(tmp_path: Path) -> None:
    store = RuntimeOverrideStore(tmp_path / "overrides.sqlite")
    store.set_digest_enabled(False)

    assert read_chat_digest_override(store, CHAT_A) is False

    sent: list[tuple[str, str]] = []
    processor = _build_processor(tmp_path, sent)
    processor.override_store = store
    processor.handle_message(_message(CHAT_A, "/digest on"))

    assert read_chat_digest_override(store, CHAT_A) is True
    assert read_chat_digest_override(store, CHAT_B) is False


# --- digest scheduler honours the per-chat override -------------------------


def _write_digest_config(path: Path) -> None:
    path.write_text(
        "[features]\n"
        "enable_daily_digest = true\n"
        "enable_weekly_digest = false\n\n"
        "[daily_digest]\nhour = 9\nminute = 0\n\n"
        "[weekly_digest]\nweekday = mon\nhour = 9\nminute = 0\n",
        encoding="utf-8",
    )


def _bot_config(tmp_path: Path, accounts: list[AccountConfig]) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=120,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50000,
            max_extracted_total_chars=120000,
            admin_chat_id="",
        ),
        accounts=accounts,
        keys=KeysConfig(telegram_bot_token="token", cf_account_id="", cf_api_token=""),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def _seed_deferred_email(db: KnowledgeDB, emitter: ContractEventEmitter, account: str) -> None:
    email_id = db.save_email(
        account_email=account,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        priority="🔵",
        action_line="Проверить",
        body_summary="",
        raw_body="",
        attachment_summaries=[("report.pdf", "summary")],
        deferred_for_digest=True,
    )
    now_ts = datetime.now(timezone.utc).timestamp()
    for event_type, payload in (
        (EventType.EMAIL_RECEIVED, {"attachments_count": 1}),
        (
            EventType.ATTENTION_DEFERRED_FOR_DIGEST,
            {"reason": "test", "attachments_only": True, "attachments_count": 1},
        ),
    ):
        emitter.emit(
            EventV1(
                event_type=event_type,
                ts_utc=now_ts,
                account_id=account,
                entity_id=None,
                email_id=email_id,
                payload=payload,
            )
        )


class _DigestLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        self.events.append((event, fields))

    warning = info
    error = info


def test_digest_tick_skips_only_the_chat_that_turned_digests_off(
    monkeypatch, tmp_path: Path
) -> None:
    from mailbot_v26.pipeline import digest_scheduler

    config_path = tmp_path / "config.ini"
    _write_digest_config(config_path)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)
    db_path = tmp_path / "knowledge.sqlite"
    storage = digest_scheduler.DigestStorage(
        knowledge_db=KnowledgeDB(db_path),
        analytics=KnowledgeAnalytics(db_path),
        event_emitter=EventEmitter(tmp_path / "events.sqlite"),
        contract_event_emitter=ContractEventEmitter(db_path),
    )
    for account in (ACCOUNT_A, ACCOUNT_B):
        _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter, account)
    sent_chats: list[str] = []
    processor = _build_processor(tmp_path, [])
    processor.handle_message(_message(CHAT_A, "/digest off"))

    def _sender(payload) -> DeliveryResult:
        sent_chats.append(str(payload.metadata.get("chat_id")))
        return DeliveryResult(delivered=True, retryable=False)

    logger = _DigestLogger()
    digest_scheduler.run_digest_tick(
        now=datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc),
        config=_bot_config(tmp_path, _accounts()),
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )

    assert sent_chats == [CHAT_B]
    assert any(
        event == "digest_tick_checked"
        and fields.get("reason") == "override_disabled"
        and fields.get("account_email") == ACCOUNT_A
        for event, fields in logger.events
    )


# --- snooze reminders go to the chat of the email's account -----------------


def _snooze_storage(tmp_path: Path, *, account_email: str):
    from mailbot_v26.bot_core.storage import Storage

    storage = Storage(tmp_path / "mailbot.sqlite")
    email_id = storage.upsert_email(
        account_email=account_email,
        uid=7,
        message_id="<m7@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        attachments_count=0,
    )
    due = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    with storage.conn:
        storage.conn.execute(
            """
            INSERT INTO telegram_snooze (
                email_id, deliver_at_utc, snoozed_at_utc, status,
                reminder_text, attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'pending', 'Reminder', 0, ?, ?)
            """,
            (email_id, due, now, now, now),
        )
    return storage, email_id


def _patch_snooze_send(monkeypatch, sent: list[object]) -> None:
    from mailbot_v26 import start as start_module

    def _fake_send(payload):
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False, message_id=55)

    monkeypatch.setattr(start_module, "send_telegram", _fake_send)
    monkeypatch.setattr(
        start_module,
        "_build_snooze_return_text",
        lambda *_args, **_kwargs: "Reminder body",
    )
    monkeypatch.setattr(
        start_module,
        "load_telegram_ui_config",
        lambda *_args, **_kwargs: SimpleNamespace(show_decision_trace=False),
    )


def test_snooze_reminder_goes_to_chat_of_email_account(monkeypatch, tmp_path: Path) -> None:
    from mailbot_v26 import start as start_module

    sent: list[object] = []
    _patch_snooze_send(monkeypatch, sent)
    storage, email_id = _snooze_storage(tmp_path, account_email=ACCOUNT_B)
    try:
        start_module._process_due_snoozes(
            storage=storage, config=_bot_config(tmp_path, _accounts())
        )
        row = storage.conn.execute(
            "SELECT status FROM telegram_snooze WHERE email_id = ?", (email_id,)
        ).fetchone()
        delivery = storage.conn.execute(
            "SELECT account_id, chat_id FROM telegram_delivery_log WHERE email_id = ?",
            (email_id,),
        ).fetchone()
    finally:
        storage.close()

    assert [payload.metadata["chat_id"] for payload in sent] == [CHAT_B]
    assert row[0] == "delivered"
    assert tuple(delivery) == (ACCOUNT_B, CHAT_B)


def test_snooze_reminder_for_unconfigured_account_is_not_sent_elsewhere(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    from mailbot_v26 import start as start_module

    sent: list[object] = []
    _patch_snooze_send(monkeypatch, sent)
    storage, email_id = _snooze_storage(tmp_path, account_email="gone@example.com")
    caplog.set_level("WARNING")
    try:
        start_module._process_due_snoozes(
            storage=storage, config=_bot_config(tmp_path, _accounts())
        )
        row = storage.conn.execute(
            "SELECT status, attempts, last_error, deliver_at_utc FROM telegram_snooze"
            " WHERE email_id = ?",
            (email_id,),
        ).fetchone()
    finally:
        storage.close()

    assert sent == []
    assert row[0] == "pending"
    assert row[1] == 1
    assert row[2] == "snooze account not configured"
    assert datetime.fromisoformat(row[3]) > datetime.now(timezone.utc)
    assert "snooze_account_unresolved" in caplog.text
