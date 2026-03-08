"""Lightweight health reporting for MailBot Premium v26.

This module is intentionally decoupled from the main pipeline. It exposes
helpers that can be used optionally by operators to observe recent error
rates and IMAP connectivity status without introducing new dependencies or
slowing down the runtime loop.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Deque, Dict

from mailbot_v26.state_manager import AccountState, StateManager

logger = logging.getLogger(__name__)

start_time = datetime.now()
_messages_processed = 0
_error_events: Deque[datetime] = deque()


def _prune_errors(now: datetime | None = None) -> None:
    current = now or datetime.now()
    cutoff = current - timedelta(hours=1)
    while _error_events and _error_events[0] < cutoff:
        _error_events.popleft()


def record_message_processed(count: int = 1) -> None:
    global _messages_processed
    if count <= 0:
        return
    _messages_processed += count


def record_error(event_time: datetime | None = None) -> None:
    current = event_time or datetime.now()
    _error_events.append(current)
    _prune_errors(current)


def errors_last_hour() -> int:
    _prune_errors()
    return len(_error_events)


def _imap_status_counts(state: StateManager | None) -> Dict[str, int]:
    counts = {"ok": 0, "error": 0, "unknown": 0}
    if state is None:
        return counts

    snapshot: Dict[str, AccountState] = {}
    try:
        snapshot = state.get_accounts_snapshot()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Health monitor could not read StateManager: %s", exc)

    for account in snapshot.values():
        status = (account.imap_status or "").lower()
        if status == "ok":
            counts["ok"] += 1
        elif status == "error":
            counts["error"] += 1
        else:
            counts["unknown"] += 1

    return counts


def health_status(state: StateManager | None = None) -> str:
    counts = _imap_status_counts(state)
    total_accounts = sum(counts.values())

    if total_accounts > 0 and counts["error"] == total_accounts:
        return "unhealthy"
    if counts["error"] > 0:
        return "degraded"
    return "healthy"


def snapshot(state: StateManager | None = None) -> Dict[str, object]:
    now = datetime.now()
    return {
        "start_time": start_time.isoformat(),
        "uptime_seconds": int((now - start_time).total_seconds()),
        "messages_processed": _messages_processed,
        "errors_last_hour": errors_last_hour(),
        "imap": _imap_status_counts(state),
        "status": health_status(state),
        "generated_at": now.isoformat(),
    }


def export_json(state: StateManager | None, destination: Path) -> Path:
    payload = snapshot(state)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    logger.info("Health JSON exported to %s", destination)
    return destination


def export_text_metrics(state: StateManager | None, destination: Path) -> Path:
    counts = _imap_status_counts(state)
    content_lines = [
        f"status: {health_status(state)}",
        f"start_time: {start_time.isoformat()}",
        f"messages_processed: {_messages_processed}",
        f"errors_last_hour: {errors_last_hour()}",
        f"imap_ok: {counts['ok']}",
        f"imap_error: {counts['error']}",
        f"imap_unknown: {counts['unknown']}",
    ]

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(content_lines), encoding="utf-8")
    logger.info("Health metrics exported to %s", destination)
    return destination


__all__ = [
    "export_json",
    "export_text_metrics",
    "health_status",
    "record_error",
    "record_message_processed",
    "errors_last_hour",
    "snapshot",
    "start_time",
]
