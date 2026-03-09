from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any


class EventType(str, Enum):
    EMAIL_RECEIVED = "email_received"
    ATTACHMENT_EXTRACTED = "attachment_extracted"
    TELEGRAM_DELIVERED = "telegram_delivered"
    TELEGRAM_FAILED = "telegram_failed"
    IMAP_HEALTH = "imap_health_v1"
    PRIORITY_CORRECTION_RECORDED = "priority_correction_recorded"
    PRIORITY_DECISION_RECORDED = "priority_decision_recorded"
    # Payload (v1):
    # {
    #   "old_priority": "🔵|🟡|🔴" (string; may be empty if unknown),
    #   "new_priority": "🔵|🟡|🔴" (string; required),
    #   "engine": "priority_v2|shadow|auto|unknown" (string; best-effort),
    #   "source": "preview_buttons|telegram_inbound|cli|unknown",
    #   "sender_email": "...",
    #   "account_email": "...",
    #   "system_mode": "FULL|..." (string)
    # }
    COMMITMENT_CREATED = "commitment_created"
    COMMITMENT_STATUS_CHANGED = "commitment_status_changed"
    COMMITMENT_EXPIRED = "commitment_expired"
    TRUST_SCORE_UPDATED = "trust_score_updated"
    RELATIONSHIP_HEALTH_UPDATED = "relationship_health_updated"
    ATTENTION_DEFERRED_FOR_DIGEST = "attention_deferred_for_digest"
    DELIVERY_POLICY_APPLIED = "delivery_policy_applied"
    ATTENTION_DEBT_UPDATED = "attention_debt_updated"
    SURPRISE_DETECTED = "surprise_detected"
    SILENCE_SIGNAL_DETECTED = "silence_signal_detected"
    DEADLOCK_DETECTED = "deadlock_detected"
    DAILY_DIGEST_SENT = "daily_digest_sent"
    WEEKLY_DIGEST_SENT = "weekly_digest_sent"
    CALIBRATION_PROPOSALS_GENERATED = "calibration_proposals_generated"
    ANOMALY_DETECTED = "anomaly_detected"
    TG_RENDER_RECORDED = "tg_render_recorded"
    MESSAGE_INTERPRETATION = "message_interpretation"
    BUDGET_CONSUMED = "budget_consumed"
    BUDGET_LIMIT_EXCEEDED = "budget_limit_exceeded"
    BUDGET_LIMIT_NEAR = "budget_limit_near"
    GATE_FLIPPED = "gate_flipped"
    BUDGET_GATE_ERROR = "budget_gate_error"
    DECISION_TRACE_RECORDED = "DECISION_TRACE_RECORDED"


@dataclass(frozen=True, slots=True)
class EventV1:
    event_type: EventType
    ts_utc: float
    account_id: str
    entity_id: str | None
    email_id: int | None
    payload: dict[str, Any]
    payload_json: str | None = None
    schema_version: int = 1


def fingerprint(event: EventV1) -> str:
    payload_value = (
        event.payload_json if event.payload_json is not None else event.payload
    )
    stable = json.dumps(
        {
            "event_type": event.event_type.value,
            "ts_utc": event.ts_utc,
            "account_id": event.account_id,
            "entity_id": event.entity_id,
            "email_id": event.email_id,
            "payload": payload_value,
            "schema_version": event.schema_version,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


__all__ = ["EventType", "EventV1", "fingerprint"]
