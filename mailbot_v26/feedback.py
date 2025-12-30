from __future__ import annotations

from mailbot_v26.observability import get_logger
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode

logger = get_logger("mailbot")


def record_action_feedback(
    *,
    knowledge_db: KnowledgeDB,
    email_id: str,
    proposed_action: dict | None,
    decision: str,
    user_note: str | None = None,
    system_mode: OperationalMode = OperationalMode.FULL,
) -> str:
    feedback_id = knowledge_db.save_action_feedback(
        email_id=email_id,
        proposed_action=proposed_action,
        decision=decision,
        user_note=user_note,
    )
    action_type = ""
    confidence = 0.0
    if isinstance(proposed_action, dict):
        action_type = str(proposed_action.get("type") or "")
        confidence = float(proposed_action.get("confidence") or 0.0)

    event = "preview_feedback_recorded"
    if decision == "accepted":
        event = "preview_accepted"
    elif decision == "rejected":
        event = "preview_rejected"

    logger.info(
        event,
        email_id=email_id,
        action_type=action_type,
        confidence=confidence,
        system_mode=system_mode.value,
    )
    return feedback_id


def record_priority_correction(
    *,
    knowledge_db: KnowledgeDB,
    email_id: int | str,
    correction: str,
    entity_id: str | None = None,
    sender_email: str | None = None,
    account_email: str | None = None,
    system_mode: OperationalMode = OperationalMode.FULL,
    event_emitter: EventEmitter | None = None,
) -> str:
    feedback_id = knowledge_db.save_priority_feedback(
        email_id=email_id,
        kind="priority_correction",
        value=correction,
        entity_id=entity_id,
        sender_email=sender_email,
        account_email=account_email,
    )
    if event_emitter is not None:
        event_emitter.emit(
            type="priority_correction_recorded",
            entity_id=entity_id,
            email_id=email_id,
            payload={
                "correction": correction,
                "sender_email": sender_email or "",
                "account_email": account_email or "",
            },
        )
    logger.info(
        "priority_correction_recorded",
        email_id=str(email_id),
        correction=correction,
        entity_id=entity_id or "",
        sender_email=sender_email or "",
        account_email=account_email or "",
        system_mode=system_mode.value,
    )
    return feedback_id


__all__ = ["record_action_feedback", "record_priority_correction"]
