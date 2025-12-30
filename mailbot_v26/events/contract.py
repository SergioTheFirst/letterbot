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
    PRIORITY_CORRECTION_RECORDED = "priority_correction_recorded"
    COMMITMENT_CREATED = "commitment_created"
    COMMITMENT_STATUS_CHANGED = "commitment_status_changed"
    TRUST_SCORE_UPDATED = "trust_score_updated"
    RELATIONSHIP_HEALTH_UPDATED = "relationship_health_updated"
    ATTENTION_DEFERRED_FOR_DIGEST = "attention_deferred_for_digest"
    DAILY_DIGEST_SENT = "daily_digest_sent"
    WEEKLY_DIGEST_SENT = "weekly_digest_sent"
    ANOMALY_DETECTED = "anomaly_detected"


@dataclass(frozen=True, slots=True)
class EventV1:
    event_type: EventType
    ts_utc: float
    account_id: str
    entity_id: str | None
    email_id: int | None
    payload: dict[str, Any]
    schema_version: int = 1


def fingerprint(event: EventV1) -> str:
    stable = json.dumps(
        {
            "event_type": event.event_type.value,
            "ts_utc": event.ts_utc,
            "account_id": event.account_id,
            "entity_id": event.entity_id,
            "email_id": event.email_id,
            "payload": event.payload,
            "schema_version": event.schema_version,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


__all__ = ["EventType", "EventV1", "fingerprint"]
