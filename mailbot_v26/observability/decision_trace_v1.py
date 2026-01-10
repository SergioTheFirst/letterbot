from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

from mailbot_v26.events.contract import EventV1, fingerprint
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


@dataclass(frozen=True, slots=True)
class DecisionTraceV1:
    decision_key: str
    decision_kind: str
    anchor_ts_utc: float
    signals_evaluated: list[str]
    signals_fired: list[str]
    evidence: dict[str, int]
    model_fingerprint: str
    explain_codes: list[str]
    trace_schema: str = "DecisionTraceV1"
    trace_version: int = 1


def _normalize_config(config_obj: Any) -> Any:
    if is_dataclass(config_obj):
        return asdict(config_obj)
    if isinstance(config_obj, dict):
        return config_obj
    if hasattr(config_obj, "__dict__"):
        return dict(vars(config_obj))
    return config_obj


def compute_decision_key(
    account_id: str,
    email_id: int,
    decision_kind: str,
    anchor_ts_utc: float,
) -> str:
    stable = json.dumps(
        {
            "account_id": account_id,
            "email_id": email_id,
            "decision_kind": decision_kind,
            "anchor_ts_utc": anchor_ts_utc,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()[:20]


def compute_model_fingerprint(config_obj: Any) -> str:
    normalized = _normalize_config(config_obj)
    stable = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def to_canonical_json(trace: DecisionTraceV1) -> str:
    return json.dumps(
        asdict(trace),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


@dataclass(slots=True)
class DecisionTraceEmitter:
    drop_threshold: int = 3
    attempted: int = 0
    succeeded: int = 0
    dropped: int = 0
    disabled: bool = False

    def emit(self, emitter: ContractEventEmitter, event: EventV1) -> bool:
        self.attempted += 1
        if self.disabled:
            self.dropped += 1
            return False
        try:
            emitted = emitter.emit(event)
        except Exception as exc:  # pragma: no cover - defensive logging
            self._record_drop(exc)
            return False
        if emitted:
            self.succeeded += 1
            return True
        if self._event_exists(emitter, event):
            return False
        self._record_drop(None)
        return False

    def _record_drop(self, exc: Exception | None) -> None:
        self.dropped += 1
        if self.dropped >= self.drop_threshold:
            self.disabled = True
        try:
            if exc is not None:
                logger.exception("decision_trace_emit_failed", error=str(exc))
            else:
                logger.error("decision_trace_emit_failed", error="emit_returned_false")
        except Exception:
            return

    def _event_exists(self, emitter: ContractEventEmitter, event: EventV1) -> bool:
        try:
            with sqlite3.connect(emitter.db_path) as conn:
                row = conn.execute(
                    "SELECT 1 FROM events_v1 WHERE fingerprint = ? LIMIT 1",
                    (fingerprint(event),),
                ).fetchone()
        except Exception:
            return False
        return row is not None


__all__ = [
    "DecisionTraceV1",
    "DecisionTraceEmitter",
    "compute_decision_key",
    "compute_model_fingerprint",
    "to_canonical_json",
]
