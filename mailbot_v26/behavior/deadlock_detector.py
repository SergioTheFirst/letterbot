from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from mailbot_v26.config.deadlock_policy import DeadlockPolicyConfig
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


def maybe_emit_deadlock(
    *,
    knowledge_db,
    event_emitter,
    account_email: str,
    thread_key: str,
    policy: DeadlockPolicyConfig,
    now_ts: float,
) -> bool:
    if not thread_key:
        return False

    try:
        cutoff_ts = now_ts - (policy.window_days * 86400)
        cutoff_iso = datetime.fromtimestamp(
            cutoff_ts, tz=timezone.utc
        ).isoformat()
        with sqlite3.connect(knowledge_db.path) as conn:
            email_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM emails
                WHERE account_email = ?
                  AND thread_key = ?
                  AND received_at >= ?
                """,
                (account_email, thread_key, cutoff_iso),
            ).fetchone()[0]

            if email_count < policy.min_messages:
                return False

            cooldown_ts = now_ts - (policy.cooldown_hours * 3600)
            rows = conn.execute(
                """
                SELECT payload
                FROM events_v1
                WHERE event_type = ?
                  AND ts_utc >= ?
                """,
                (EventType.DEADLOCK_DETECTED.value, cooldown_ts),
            ).fetchall()
    except sqlite3.Error as exc:
        logger.error("deadlock_detector_failed", error=str(exc))
        return False

    for (payload_raw,) in rows:
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if payload.get("thread_key") == thread_key:
            return False

    event = EventV1(
        event_type=EventType.DEADLOCK_DETECTED,
        ts_utc=now_ts,
        account_id=account_email,
        entity_id=None,
        email_id=None,
        payload={
            "thread_key": thread_key,
            "count_window": int(email_count),
            "window_days": int(policy.window_days),
            "cooldown_hours": int(policy.cooldown_hours),
        },
    )
    try:
        return bool(event_emitter.emit(event))
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("deadlock_detector_emit_failed", error=str(exc))
        return False


__all__ = ["maybe_emit_deadlock"]
