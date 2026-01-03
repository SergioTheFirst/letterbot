from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.config_loader import get_account_scope, resolve_account_scope
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode

logger = get_logger("mailbot")


def _priority_feedback_anchor_ts(
    *,
    knowledge_db: KnowledgeDB,
    email_id: int | str,
    correction: str,
    created_at: datetime | None,
) -> float:
    try:
        with sqlite3.connect(knowledge_db.path) as conn:
            row = conn.execute(
                """
                SELECT created_at
                FROM priority_feedback
                WHERE email_id = ? AND value = ?
                ORDER BY datetime(created_at) ASC, id ASC
                LIMIT 1
                """,
                (str(email_id), correction),
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("priority_feedback_anchor_failed", error=str(exc))
        row = None

    if row and row[0]:
        parsed = parse_sqlite_datetime(str(row[0]))
        if parsed:
            return parsed.timestamp()

    if created_at is not None:
        return created_at.timestamp()

    return datetime.now(timezone.utc).timestamp()


def _feedback_created_at(knowledge_db: KnowledgeDB, feedback_id: str) -> datetime | None:
    try:
        with sqlite3.connect(knowledge_db.path) as conn:
            row = conn.execute(
                "SELECT created_at FROM priority_feedback WHERE id = ?",
                (feedback_id,),
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("priority_feedback_created_at_read_failed", error=str(exc))
        return None
    if not row or not row[0]:
        return None
    return parse_sqlite_datetime(str(row[0]))


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
    contract_event_emitter: ContractEventEmitter | None = None,
    old_priority: str | None = None,
    engine: str | None = None,
    source: str | None = None,
    surprise_mode: str = "disabled",
) -> str:
    feedback_id, inserted = knowledge_db.save_priority_feedback(
        email_id=email_id,
        kind="priority_correction",
        value=correction,
        entity_id=entity_id,
        sender_email=sender_email,
        account_email=account_email,
    )
    created_at = _feedback_created_at(knowledge_db, feedback_id)
    event_ts = _priority_feedback_anchor_ts(
        knowledge_db=knowledge_db,
        email_id=email_id,
        correction=correction,
        created_at=created_at,
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
    if contract_event_emitter is not None and inserted:
        try:
            resolved_scope = resolve_account_scope(account_email or "")
            scope_chat_id = resolved_scope.chat_id if resolved_scope else None
            scope_emails = list(resolved_scope.account_emails) if resolved_scope else None
            if not scope_emails and account_email:
                scope_emails = [account_email]
            scope_payload = get_account_scope(
                chat_id=scope_chat_id,
                account_email=account_email,
                account_emails=scope_emails,
            )
            event = EventV1(
                event_type=EventType.PRIORITY_CORRECTION_RECORDED,
                ts_utc=event_ts,
                account_id=account_email or "",
                entity_id=entity_id,
                email_id=int(str(email_id)) if str(email_id).isdigit() else None,
                payload={
                    "old_priority": old_priority or "",
                    "new_priority": correction,
                    "engine": engine or "unknown",
                    "source": source or "unknown",
                    "sender_email": sender_email or "",
                    "account_email": account_email or "",
                    "system_mode": system_mode.value,
                    **scope_payload,
                },
            )
            contract_event_emitter.emit(event)
            if _surprise_enabled(surprise_mode) and old_priority and correction:
                contract_event_emitter.emit(
                    EventV1(
                        event_type=EventType.SURPRISE_DETECTED,
                        ts_utc=event_ts,
                        account_id=account_email or "",
                        entity_id=entity_id,
                        email_id=int(str(email_id)) if str(email_id).isdigit() else None,
                        payload={
                            "old_priority": old_priority or "",
                            "new_priority": correction,
                            "delta": _priority_delta(old_priority, correction),
                            "engine": engine or "unknown",
                            "source": source or "unknown",
                            **scope_payload,
                        },
                    )
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "contract_priority_correction_emit_failed",
                email_id=str(email_id),
                error=str(exc),
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


def _priority_delta(old_priority: str, new_priority: str) -> int | None:
    rank = _priority_rank
    old_rank = rank(old_priority)
    new_rank = rank(new_priority)
    if old_rank is None or new_rank is None:
        return None
    return new_rank - old_rank


def _priority_rank(value: str) -> int | None:
    normalized = str(value or "").strip().lower()
    mapping = {
        "🔴": 3,
        "🟠": 2,
        "🟡": 2,
        "🔵": 1,
        "high": 3,
        "medium": 2,
        "low": 1,
    }
    return mapping.get(normalized)


def _surprise_enabled(mode: str) -> bool:
    return str(mode or "").strip().lower() in {"enabled", "shadow", "true", "on"}


__all__ = ["record_action_feedback", "record_priority_correction"]
