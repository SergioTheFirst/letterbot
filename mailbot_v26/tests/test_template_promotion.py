from __future__ import annotations

import sqlite3
from pathlib import Path

from mailbot_v26.config.learning import configure_learning_config, reset_learning_config
from mailbot_v26.domain.template_promotion import (
    analyze_template_promotion_candidates,
    clear_runtime_template_promotion_cache,
    find_runtime_template_promotion,
)
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter
from mailbot_v26.pipeline.processor import _build_message_decision
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_interpretation(
    emitter: EventEmitter,
    *,
    db_path: Path,
    email_id: int,
    sender_email: str,
    doc_kind: str,
    account_id: str = "account@example.com",
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.MESSAGE_INTERPRETATION,
            ts_utc=float(email_id),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "sender_email": sender_email,
                "doc_kind": doc_kind,
                "amount": 1000.0,
                "due_date": None,
                "priority": "🟡",
                "action": "Проверить",
                "confidence": 0.9,
                "context": "NEW_MESSAGE",
                "document_id": f"doc-{email_id}",
            },
        )
    )


def _emit_priority_correction(
    emitter: EventEmitter,
    *,
    email_id: int,
    new_priority: str,
    account_id: str = "account@example.com",
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=float(email_id) + 0.5,
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "new_priority": new_priority,
                "old_priority": "🟡",
                "engine": "priority_v2_auto",
                "source": "telegram_inbound",
            },
        )
    )


def test_template_promotion_helper_detects_repeated_consistent_corrections(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "promotion.sqlite"
    emitter = EventEmitter(db_path)

    for email_id in (101, 102, 103):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="🔴")

    signals = analyze_template_promotion_candidates(db_path)

    scope_map = {(signal.scope_kind, signal.scope_value): signal for signal in signals}
    sender_signal = scope_map[("sender_email", "billing@billing.vendor.test")]
    fingerprint_signal = next(
        signal for signal in signals if signal.scope_kind == "issuer_fingerprint"
    )

    assert sender_signal.template_id == "russian_invoice_common"
    assert sender_signal.correction_count == 3
    assert sender_signal.dominant_priority == "🔴"
    assert fingerprint_signal.template_id == "russian_invoice_common"
    assert fingerprint_signal.identity_confidence == "medium"
    assert fingerprint_signal.issuer_fingerprint == fingerprint_signal.scope_value


def test_template_promotion_helper_does_not_fire_on_weak_signal(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "promotion_weak.sqlite"
    emitter = EventEmitter(db_path)

    priorities = ["🔴", "🟡", "🔵"]
    for offset, priority in enumerate(priorities, start=201):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=offset,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=offset, new_priority=priority)

    signals = analyze_template_promotion_candidates(db_path)

    assert signals == ()


def test_template_promotion_helper_is_scoped_per_sender_or_pattern(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "promotion_scope.sqlite"
    emitter = EventEmitter(db_path)

    cases = (
        (301, "ops1@billing.vendor.test"),
        (302, "ops1@billing.vendor.test"),
        (303, "ops2@billing.vendor.test"),
        (304, "ops2@billing.vendor.test"),
    )
    for email_id, sender_email in cases:
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email=sender_email,
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="🔴")

    signals = analyze_template_promotion_candidates(db_path, min_corrections=2)
    scope_map = {(item.scope_kind, item.scope_value): item for item in signals}

    assert ("sender_email", "ops1@billing.vendor.test") in scope_map
    assert ("sender_email", "ops2@billing.vendor.test") in scope_map
    assert ("sender_domain", "billing.vendor.test") in scope_map
    assert scope_map[("sender_domain", "billing.vendor.test")].correction_count == 4


def test_template_promotion_helper_uses_canonical_corrections_not_raw_guesswork(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "promotion_canonical.sqlite"
    KnowledgeDB(db_path)
    emitter = EventEmitter(db_path)
    _emit_interpretation(
        emitter,
        db_path=db_path,
        email_id=401,
        sender_email="billing@billing.vendor.test",
        doc_kind="invoice",
    )

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO priority_feedback (
                id, email_id, kind, value, entity_id, sender_email, account_email
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "manual-feedback",
                "401",
                "priority_correction",
                "🔴",
                "entity-1",
                "billing@billing.vendor.test",
                "account@example.com",
            ),
        )
        conn.commit()

    signals = analyze_template_promotion_candidates(db_path, min_corrections=1)

    assert signals == ()


def test_runtime_template_promotion_detects_repeated_consistent_corrections(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "runtime_signal.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (501, 502, 503, 504, 505):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="billing@billing.vendor.test",
        doc_kind="invoice",
    )

    assert reason is None
    assert promotion is not None
    assert promotion.signal.template_id == "russian_invoice_common"
    assert promotion.signal.scope_kind == "issuer_fingerprint"


def test_runtime_template_promotion_does_not_fire_on_weak_invoice_signal(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "runtime_signal_weak.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (601, 602, 603, 604):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="billing@billing.vendor.test",
        doc_kind="invoice",
    )

    assert promotion is None
    assert reason == "insufficient_corrections"


def test_shadow_mode_writes_recommendation_does_not_change_pipeline(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shadow_mode.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (701, 702, 703, 704, 705):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    from mailbot_v26.pipeline import processor as pipeline_processor

    original_db_path = pipeline_processor.DB_PATH
    pipeline_processor.configure_processor_db_path(db_path)
    clear_runtime_template_promotion_cache()
    configure_learning_config(template_promotion_shadow=True, template_promotion_runtime=False)
    try:
        promotion, reason = find_runtime_template_promotion(
            db_path,
            account_id="account@example.com",
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        decision = _build_message_decision(
            priority="\U0001f7e1",
            action_line="Проверить",
            summary="",
            message_facts={
                "amount": "87 500 USD",
                "due_date": "15.04.2026",
                "doc_number": "INV-705",
                "doc_kind": "invoice",
                "invoice_signal": True,
                "payroll_signal": False,
                "contract_signal": False,
                "incident_signal": False,
                "amount_context_missing": False,
                "amount_window_hit": True,
            },
            account_id="account@example.com",
            sender_email="billing@billing.vendor.test",
            subject="Invoice INV-705",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_runtime_template_promotion_cache()

    assert promotion is not None
    assert reason is None
    assert decision.facts.get("template_promotion_applied") in (None, False)


def test_runtime_mode_off_keeps_report_only_behavior(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_off.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (711, 712, 713, 714, 715):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    from mailbot_v26.pipeline import processor as pipeline_processor

    original_db_path = pipeline_processor.DB_PATH
    pipeline_processor.configure_processor_db_path(db_path)
    clear_runtime_template_promotion_cache()
    try:
        configure_learning_config(template_promotion_runtime=False)
        promotion, reason = find_runtime_template_promotion(
            db_path,
            account_id="account@example.com",
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        shadow_decision = _build_message_decision(
            priority="\U0001f7e1",
            action_line="Проверить",
            summary="",
            message_facts={
                "amount": "87 500 USD",
                "due_date": "15.04.2026",
                "doc_number": "INV-715",
                "doc_kind": "invoice",
                "invoice_signal": True,
                "payroll_signal": False,
                "contract_signal": False,
                "incident_signal": False,
                "amount_context_missing": False,
                "amount_window_hit": True,
            },
            account_id="account@example.com",
            sender_email="billing@billing.vendor.test",
            subject="Invoice INV-715",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_runtime_template_promotion_cache()

    assert promotion is not None
    assert reason is None
    assert shadow_decision.facts.get("template_promotion_applied") in (None, False)


def test_runtime_mode_requires_all_gates_simultaneously(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_all_gates.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (721, 722, 723, 724, 725):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    from mailbot_v26.pipeline import processor as pipeline_processor

    original_db_path = pipeline_processor.DB_PATH
    pipeline_processor.configure_processor_db_path(db_path)
    clear_runtime_template_promotion_cache()
    configure_learning_config(template_promotion_runtime=True)
    try:
        decision = _build_message_decision(
            priority="\U0001f535",
            action_line="Ознакомиться",
            summary="",
            message_facts={
                "amount": "",
                "due_date": "",
                "doc_number": "",
                "doc_kind": "",
                "invoice_signal": False,
                "payroll_signal": False,
                "contract_signal": False,
                "incident_signal": False,
                "amount_context_missing": False,
            },
            account_id="account@example.com",
            sender_email="billing@billing.vendor.test",
            subject="Weekly status sync",
            body_text="Internal status update only.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_runtime_template_promotion_cache()

    assert decision.facts.get("template_promotion_applied") in (None, False)


def test_weak_issuer_identity_blocks_runtime_promotion(tmp_path: Path) -> None:
    db_path = tmp_path / "weak_identity_runtime.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (731, 732, 733, 734, 735):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="noreply@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="noreply@billing.vendor.test",
        doc_kind="invoice",
    )

    assert promotion is None
    assert reason == "weak_identity"


def test_few_corrections_below_threshold_no_promotion(tmp_path: Path) -> None:
    test_runtime_template_promotion_does_not_fire_on_weak_invoice_signal(tmp_path)


def test_inconsistent_corrections_no_promotion(tmp_path: Path) -> None:
    db_path = tmp_path / "inconsistent_runtime.sqlite"
    emitter = EventEmitter(db_path)
    for email_id, priority in zip((741, 742, 743, 744, 745), ("рџ”ґ", "рџџЎ", "рџ”ґ", "рџџЎ", "рџ”µ")):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority=priority)

    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="billing@billing.vendor.test",
        doc_kind="invoice",
    )

    assert promotion is None
    assert reason == "inconsistent_corrections"


def test_payroll_suppression_beats_invoice_promotion_in_runtime_mode(
    tmp_path: Path,
) -> None:
    from mailbot_v26.tests.test_pipeline_processor import (
        test_runtime_safe_promotion_contradictions_block_application,
    )

    test_runtime_safe_promotion_contradictions_block_application(tmp_path)


def test_reconciliation_suppression_beats_promotion(tmp_path: Path) -> None:
    db_path = tmp_path / "reconciliation_runtime.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (751, 752, 753, 754):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="recon@reconciliation.vendor.test",
            doc_kind="reconciliation",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџџЎ")

    from mailbot_v26.pipeline import processor as pipeline_processor

    original_db_path = pipeline_processor.DB_PATH
    pipeline_processor.configure_processor_db_path(db_path)
    clear_runtime_template_promotion_cache()
    configure_learning_config(template_promotion_runtime=True)
    try:
        decision = _build_message_decision(
            priority="\U0001f7e1",
            action_line="Оплатить",
            summary="",
            message_facts={
                "amount": "50 000 руб",
                "due_date": "",
                "doc_number": "",
                "doc_kind": "reconciliation",
                "invoice_signal": False,
                "payroll_signal": False,
                "contract_signal": False,
                "incident_signal": False,
                "amount_context_missing": False,
            },
            account_id="account@example.com",
            sender_email="recon@reconciliation.vendor.test",
            subject="Акт сверки",
            body_text="Акт сверки взаимных расчетов за март.",
            attachments=[],
            context="NEW_MESSAGE",
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_runtime_template_promotion_cache()

    assert "оплат" not in decision.action.lower()


def test_manual_override_never_overwritten_by_promotion(tmp_path: Path) -> None:
    db_path = tmp_path / "manual_override_runtime.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (761, 762, 763, 764, 765):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    from mailbot_v26.pipeline import processor as pipeline_processor

    original_db_path = pipeline_processor.DB_PATH
    pipeline_processor.configure_processor_db_path(db_path)
    clear_runtime_template_promotion_cache()
    configure_learning_config(template_promotion_runtime=True)
    try:
        decision = _build_message_decision(
            priority="\U0001f7e1",
            action_line="Проверить",
            summary="",
            message_facts={
                "amount": "87 500 USD",
                "due_date": "15.04.2026",
                "doc_number": "INV-765",
                "doc_kind": "invoice",
                "invoice_signal": True,
                "payroll_signal": False,
                "contract_signal": False,
                "incident_signal": False,
                "amount_context_missing": False,
                "amount_window_hit": True,
            },
            account_id="account@example.com",
            sender_email="billing@billing.vendor.test",
            subject="Invoice INV-765",
            body_text="Invoice total 87 500 USD. Payment due 15.04.2026.",
            attachments=[],
            context="NEW_MESSAGE",
            priority_locked_by_user=True,
        )
    finally:
        reset_learning_config()
        pipeline_processor.configure_processor_db_path(original_db_path)
        clear_runtime_template_promotion_cache()

    assert decision.facts.get("template_promotion_applied") in (None, False)


def test_runtime_promotes_confidence_within_same_class_only(tmp_path: Path) -> None:
    db_path = tmp_path / "same_class_runtime.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (771, 772, 773, 774, 775):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    clear_runtime_template_promotion_cache()
    promotion, reason = find_runtime_template_promotion(
        db_path,
        account_id="account@example.com",
        sender_email="billing@billing.vendor.test",
        doc_kind="invoice",
    )

    assert reason is None
    assert promotion is not None
    assert promotion.signal.template_id == "russian_invoice_common"
    assert promotion.confidence_boost > 0


def test_shadow_output_is_deterministic(tmp_path: Path) -> None:
    db_path = tmp_path / "shadow_deterministic.sqlite"
    emitter = EventEmitter(db_path)
    for email_id in (781, 782, 783, 784, 785):
        _emit_interpretation(
            emitter,
            db_path=db_path,
            email_id=email_id,
            sender_email="billing@billing.vendor.test",
            doc_kind="invoice",
        )
        _emit_priority_correction(emitter, email_id=email_id, new_priority="рџ”ґ")

    first = analyze_template_promotion_candidates(db_path)
    second = analyze_template_promotion_candidates(db_path)

    assert first == second

