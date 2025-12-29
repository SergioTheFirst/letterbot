from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any


class EventType(str, Enum):
    EMAIL_RECEIVED = "EMAIL_RECEIVED"
    COMMITMENT_CREATED = "COMMITMENT_CREATED"
    COMMITMENT_STATUS_CHANGED = "COMMITMENT_STATUS_CHANGED"
    TRUST_SCORE_UPDATED = "TRUST_SCORE_UPDATED"
    RELATIONSHIP_HEALTH_UPDATED = "RELATIONSHIP_HEALTH_UPDATED"
    TG_DELIVERY_FINAL = "TG_DELIVERY_FINAL"
    LLM_PROVIDER_SELECTED = "LLM_PROVIDER_SELECTED"
    SYSTEM_MODE_CHANGED = "SYSTEM_MODE_CHANGED"


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
