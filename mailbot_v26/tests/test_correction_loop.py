from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.config.learning import configure_learning_config, reset_learning_config
from mailbot_v26.domain.template_promotion import clear_runtime_template_promotion_cache
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import processor as pipeline_processor
from mailbot_v26.pipeline.processor import _build_message_decision
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.telegram.inbound import TelegramInboundProcessor
from mailbot_v26.worker.telegram_sender import DeliveryResult


@dataclass
class _StubGate:
    result: GateResult

    def evaluate(self, **_kwargs) -> GateResult:
        return self.result


def _build_processor(tmp_path: Path) -> TelegramInboundProcessor:
    db_path = tmp_path / "knowledge.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    event_emitter = EventEmitter(tmp_path / "events.sqlite")
    contract_emitter = ContractEventEmitter(db_path)
    runtime_flags = RuntimeFlagStore(path=tmp_path / "runtime_flags.json")
    override_store = RuntimeOverrideStore(db_path)
    feature_flags = FeatureFlags(base_dir=tmp_path)

    def _send_reply(_chat_id: str, _text: str) -> DeliveryResult:
        return DeliveryResult(delivered=True, retryable=False)

    return TelegramInboundProcessor(
        knowledge_db=knowledge_db,
        analytics=analytics,
        event_emitter=event_emitter,
        contract_event_emitter=contract_emitter,
        runtime_flag_store=runtime_flags,
        auto_priority_gate=_StubGate(
            GateResult(
                passed=True,
                reason="ok",
                window_days=30,
                samples=100,
                corrections=1,
                correction_rate=0.01,
                engine="priority_v2_auto",
            )
        ),
        auto_priority_gate_config=AutoPriorityGateConfig(enabled=True),
        override_store=override_store,
        send_reply=_send_reply,
        feature_flags=feature_flags,
        allowed_chat_ids=frozenset({"chat"}),
        bot_token="token",
    )


def _insert_email(
    db_path: Path,
    *,
    sender_email: str,
    subject: str,
    priority: str = "🟡",
    account_email: str = "account@example.com",
) -> int:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (
                account_email,
                from_email,
                subject,
                received_at,
                priority,
                action_line,
                body_summary,
                raw_body_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_email,
                sender_email,
                subject,
                datetime.now(timezone.utc).isoformat(),
                priority,
                "Проверить",
                "",
                f"hash-{sender_email}-{subject}",
            ),
        )
        row = conn.execute("SELECT id FROM emails ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    return int(row[0])


def _emit_interpretation(
    db_path: Path,
    *,
    email_id: int,
    sender_email: str,
    doc_kind: str,
    priority: str = "🟡",
    action: str = "Проверить",
) -> None:
    emitter = ContractEventEmitter(db_path)
    emitter.emit(
        EventV1(
            event_type=EventType.MESSAGE_INTERPRETATION,
            ts_utc=float(email_id),
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={
                "sender_email": sender_email,
                "doc_kind": doc_kind,
                "amount": 87500.0 if doc_kind == "invoice" else None,
                "due_date": "2026-04-15" if doc_kind == "invoice" else None,
                "priority": priority,
                "action": action,
                "confidence": 0.92,
                "context": "NEW_MESSAGE",
                "document_id": f"{doc_kind}-{email_id}",
                "issuer_label": "ООО Вектор",
            },
        )
    )


def _apply_priority_callback(
    processor: TelegramInboundProcessor,
    monkeypatch: pytest.MonkeyPatch,
    *,
    email_id: int,
    priority_token: str,
) -> None:
    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return {"json": json, "timeout": timeout}

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())
    processor.handle_callback_query(
        {
            "id": f"cb-{email_id}-{priority_token}",
            "data": f"prio_set:{email_id}:{priority_token}",
            "message": {"chat": {"id": "chat"}, "message_id": 1000 + email_id},
        }
    )


def _invoice_facts() -> dict[str, object]:
    return {
        "amount": "87 500 USD",
        "due_date": "15.04.2026",
        "doc_number": "INV-900",
        "doc_kind": "invoice",
        "invoice_signal": True,
        "payroll_signal": False,
        "contract_signal": False,
        "incident_signal": False,
        "amount_context_missing": False,
        "amount_window_hit": True,
    }


def _payroll_facts() -> dict[str, object]:
    return {
        "amount": "",
        "due_date": "",
        "doc_number": "PAY-900",
        "doc_kind": "payroll",
        "invoice_signal": False,
        "payroll_signal": True,
        "contract_signal": False,
        "incident_signal": False,
        "amount_context_missing": False,
        "amount_window_hit": False,
    }


def test_correction_event_written_on_telegram_inbound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = _build_processor(tmp_path)
    email_id = _insert_email(
        processor.knowledge_db.path,
        sender_email="billing@vendor.test",
        subject="Invoice 42",
    )
    _emit_interpretation(
        processor.knowledge_db.path,
        email_id=email_id,
        sender_email="billing@vendor.test",
        doc_kind="invoice",
    )

    _apply_priority_callback(
        processor,
        monkeypatch,
        email_id=email_id,
        priority_token="R",
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        row = conn.execute(
            """
            SELECT payload
            FROM events_v1
            WHERE event_type = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (EventType.PRIORITY_CORRECTION_RECORDED.value,),
        ).fetchone()

    assert row is not None
    payload = json.loads(row[0])
    assert payload["new_priority"] == "🔴"
    assert payload["old_priority"] == "🟡"
    assert payload["corrected_decision"] == "🔴"
    assert payload["confidence"] == 1.0
    assert payload["issuer_fingerprint"].startswith("issuer:")


def test_correction_changes_template_signal_on_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = _build_processor(tmp_path)
    sender_email = "billing@vendor.test"
    seeded_email_ids: list[int] = []
    for idx in range(5):
        email_id = _insert_email(
            processor.knowledge_db.path,
            sender_email=sender_email,
            subject=f"Invoice {idx}",
        )
        seeded_email_ids.append(email_id)
        _emit_interpretation(
            processor.knowledge_db.path,
            email_id=email_id,
            sender_email=sender_email,
            doc_kind="invoice",
        )
        _apply_priority_callback(
            processor,
            monkeypatch,
            email_id=email_id,
            priority_token="R",
        )

    original_db_path = pipeline_processor.DB_PATH
    clear_cache = clear_runtime_template_promotion_cache
    pipeline_processor.configure_processor_db_path(processor.knowledge_db.path)
    try:
        clear_cache()
        configure_learning_config(template_promotion_runtime=False)
        decision_without = _build_message_decision(
            priority="🟡",
            action_line="Проверить",
            summary="",
            message_facts=_invoice_facts(),
            account_id="account@example.com",
            sender_email=sender_email,
            subject="Invoice INV-900",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
        )
        clear_cache()
        configure_learning_config(template_promotion_runtime=True)
        decision_with = _build_message_decision(
            priority="🟡",
            action_line="Проверить",
            summary="",
            message_facts=_invoice_facts(),
            account_id="account@example.com",
            sender_email=sender_email,
            subject="Invoice INV-900",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_cache()

    assert decision_without.facts.get("template_promotion_applied") in (None, False)
    assert decision_with.facts.get("template_promotion_applied") is True
    assert decision_with.facts.get("template_id") == "russian_invoice_common"
    assert float(decision_with.facts.get("template_confidence_boost") or 0.0) > 0.0


def test_correction_loop_does_not_affect_different_issuer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = _build_processor(tmp_path)
    for idx in range(5):
        email_id = _insert_email(
            processor.knowledge_db.path,
            sender_email="billing@vendor.test",
            subject=f"Invoice {idx}",
        )
        _emit_interpretation(
            processor.knowledge_db.path,
            email_id=email_id,
            sender_email="billing@vendor.test",
            doc_kind="invoice",
        )
        _apply_priority_callback(processor, monkeypatch, email_id=email_id, priority_token="R")

    original_db_path = pipeline_processor.DB_PATH
    clear_cache = clear_runtime_template_promotion_cache
    pipeline_processor.configure_processor_db_path(processor.knowledge_db.path)
    try:
        clear_cache()
        configure_learning_config(template_promotion_runtime=True)
        decision = _build_message_decision(
            priority="🟡",
            action_line="Проверить",
            summary="",
            message_facts=_invoice_facts(),
            account_id="account@example.com",
            sender_email="another@vendor.test",
            subject="Invoice INV-901",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_cache()

    assert decision.facts.get("template_promotion_applied") in (None, False)


def test_correction_loop_payroll_correction_never_promotes_to_invoice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = _build_processor(tmp_path)
    sender_email = "hr@vendor.test"
    for idx in range(5):
        email_id = _insert_email(
            processor.knowledge_db.path,
            sender_email=sender_email,
            subject=f"Payroll {idx}",
        )
        _emit_interpretation(
            processor.knowledge_db.path,
            email_id=email_id,
            sender_email=sender_email,
            doc_kind="payroll",
            action="Проверить",
        )
        _apply_priority_callback(processor, monkeypatch, email_id=email_id, priority_token="R")

    original_db_path = pipeline_processor.DB_PATH
    clear_cache = clear_runtime_template_promotion_cache
    pipeline_processor.configure_processor_db_path(processor.knowledge_db.path)
    try:
        clear_cache()
        configure_learning_config(template_promotion_runtime=True)
        decision = _build_message_decision(
            priority="🟡",
            action_line="Оплатить",
            summary="",
            message_facts=_payroll_facts(),
            account_id="account@example.com",
            sender_email=sender_email,
            subject="Расчётный листок",
            body_text="Расчётный листок. Начислено 120 000 руб. Удержано 15 000 руб.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_cache()

    assert decision.facts.get("template_id") != "russian_invoice_common"
    assert "оплат" not in decision.action.casefold()


def test_correction_loop_below_threshold_no_runtime_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = _build_processor(tmp_path)
    sender_email = "billing@vendor.test"
    for idx in range(4):
        email_id = _insert_email(
            processor.knowledge_db.path,
            sender_email=sender_email,
            subject=f"Invoice weak {idx}",
        )
        _emit_interpretation(
            processor.knowledge_db.path,
            email_id=email_id,
            sender_email=sender_email,
            doc_kind="invoice",
        )
        _apply_priority_callback(processor, monkeypatch, email_id=email_id, priority_token="R")

    original_db_path = pipeline_processor.DB_PATH
    clear_cache = clear_runtime_template_promotion_cache
    pipeline_processor.configure_processor_db_path(processor.knowledge_db.path)
    try:
        clear_cache()
        configure_learning_config(template_promotion_runtime=True)
        decision = _build_message_decision(
            priority="🟡",
            action_line="Проверить",
            summary="",
            message_facts=_invoice_facts(),
            account_id="account@example.com",
            sender_email=sender_email,
            subject="Invoice INV-902",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_cache()

    assert decision.facts.get("template_promotion_applied") in (None, False)


def test_correction_event_idempotent_on_duplicate_tap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    processor = _build_processor(tmp_path)
    email_id = _insert_email(
        processor.knowledge_db.path,
        sender_email="billing@vendor.test",
        subject="Invoice duplicate",
    )
    _emit_interpretation(
        processor.knowledge_db.path,
        email_id=email_id,
        sender_email="billing@vendor.test",
        doc_kind="invoice",
    )

    _apply_priority_callback(processor, monkeypatch, email_id=email_id, priority_token="R")
    _apply_priority_callback(processor, monkeypatch, email_id=email_id, priority_token="R")

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        feedback_count = conn.execute(
            "SELECT COUNT(*) FROM priority_feedback WHERE email_id = ?",
            (str(email_id),),
        ).fetchone()[0]
        event_count = conn.execute(
            "SELECT COUNT(*) FROM events_v1 WHERE event_type = ? AND email_id = ?",
            (EventType.PRIORITY_CORRECTION_RECORDED.value, email_id),
        ).fetchone()[0]

    assert feedback_count == 1
    assert event_count == 1
