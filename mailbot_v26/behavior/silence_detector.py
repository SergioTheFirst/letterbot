from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from mailbot_v26.config.silence_policy import SilencePolicyConfig
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


def run_silence_scan(
    *,
    knowledge_db,
    event_emitter,
    account_email: str,
    account_emails: Iterable[str] | None = None,
    now_ts: float,
    policy: SilencePolicyConfig,
) -> int:
    try:
        scope_emails = _normalize_account_scope(account_email, account_emails)
        placeholders = ",".join("?" for _ in scope_emails)
        cutoff_ts = now_ts - (policy.lookback_days * 86400)
        history_cutoff_ts = now_ts - (30 * 86400)
        cutoff_iso = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat()
        history_cutoff_iso = datetime.fromtimestamp(
            history_cutoff_ts, tz=timezone.utc
        ).isoformat()
        with sqlite3.connect(knowledge_db.path) as conn:
            rows = conn.execute(
                f"""
                SELECT
                    from_email,
                    MIN(received_at),
                    MAX(received_at),
                    COUNT(*),
                    SUM(CASE WHEN received_at >= ? THEN 1 ELSE 0 END)
                FROM emails
                WHERE account_email IN ({placeholders})
                  AND received_at >= ?
                  AND from_email IS NOT NULL
                  AND from_email != ''
                GROUP BY from_email
                HAVING COUNT(*) >= ?
                """,
                (history_cutoff_iso, *scope_emails, cutoff_iso, policy.min_messages),
            ).fetchall()

            cooldown_ts = now_ts - (policy.cooldown_hours * 3600)
            recent_events = conn.execute(
                f"""
                SELECT payload
                FROM events_v1
                WHERE event_type = ?
                  AND account_id IN ({placeholders})
                  AND ts_utc >= ?
                """,
                (
                    EventType.SILENCE_SIGNAL_DETECTED.value,
                    *scope_emails,
                    cooldown_ts,
                ),
            ).fetchall()
    except sqlite3.Error as exc:
        logger.error("silence_detector_failed", error=str(exc))
        return 0

    try:
        dedupe_contacts: set[str] = set()
        for (payload_raw,) in recent_events:
            contact = _load_contact(payload_raw)
            if contact:
                dedupe_contacts.add(contact)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        logger.error("silence_detector_parse_failed", error=str(exc))
        return 0

    candidates: list[dict[str, object]] = []
    for from_email, first_seen, last_seen, msg_count, recent_count in rows:
        if int(recent_count or 0) < int(policy.min_messages):
            continue
        try:
            first_dt = _parse_iso(first_seen)
            last_dt = _parse_iso(last_seen)
        except (TypeError, ValueError) as exc:
            logger.error("silence_detector_parse_failed", error=str(exc))
            return 0

        first_ts = first_dt.timestamp()
        last_ts = last_dt.timestamp()
        baseline_gap_hours = (last_ts - first_ts) / max(1, int(msg_count) - 1) / 3600.0
        silence_threshold_hours = max(
            policy.min_silence_days * 24.0,
            baseline_gap_hours * policy.silence_factor,
        )
        silence_gap_hours = (now_ts - last_ts) / 3600.0
        if silence_gap_hours < silence_threshold_hours:
            continue
        if from_email in dedupe_contacts:
            continue
        days_silent = (now_ts - last_ts) / 86400.0
        candidates.append(
            {
                "contact": from_email,
                "last_seen_ts": last_ts,
                "days_silent": days_silent,
                "baseline_gap_hours": baseline_gap_hours,
                "threshold_hours": silence_threshold_hours,
                "count_window": int(msg_count),
            }
        )

    candidates.sort(key=lambda item: item["days_silent"], reverse=True)
    emitted = 0
    for item in candidates[: max(0, int(policy.max_per_run))]:
        event = EventV1(
            event_type=EventType.SILENCE_SIGNAL_DETECTED,
            ts_utc=now_ts,
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload={
                "contact": item["contact"],
                "last_seen_ts": int(item["last_seen_ts"]),
                "days_silent": round(float(item["days_silent"]), 1),
                "baseline_gap_hours": int(round(float(item["baseline_gap_hours"]))),
                "threshold_hours": int(round(float(item["threshold_hours"]))),
                "lookback_days": int(policy.lookback_days),
                "count_window": int(item["count_window"]),
            },
        )
        try:
            if event_emitter.emit(event):
                emitted += 1
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("silence_detector_emit_failed", error=str(exc))
            continue

    return emitted


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _load_contact(payload_raw: str | None) -> str | None:
    if not payload_raw:
        return None
    payload = json.loads(payload_raw)
    contact = payload.get("contact")
    return str(contact) if contact else None


def _normalize_account_scope(
    account_email: str,
    account_emails: Iterable[str] | None,
) -> list[str]:
    if account_emails:
        cleaned = {
            str(email).strip() for email in account_emails if str(email or "").strip()
        }
    else:
        cleaned = set()
    scope = sorted(cleaned)
    if not scope:
        fallback = str(account_email or "").strip()
        if fallback:
            return [fallback]
    return scope


__all__ = ["run_silence_scan"]
