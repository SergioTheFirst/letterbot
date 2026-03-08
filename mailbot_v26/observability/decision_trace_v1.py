from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import sys
import threading
import time
from dataclasses import replace
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any

from mailbot_v26.events.contract import EventV1, fingerprint
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")

_CODE_PATTERN = re.compile(r"^[A-Z0-9_]{2,40}$")
_SANITIZED_CODE = "SANITIZED_CODE"
_FAILURE_KEY_PATTERN = re.compile(r"^[a-f0-9]{8,64}$")
_EMAIL_PATTERN = re.compile(r"[\w.+'-]+@[\w.-]+")


def sanitize_code(code: str | None) -> str:
    cleaned = str(code or "").strip()
    if _CODE_PATTERN.fullmatch(cleaned):
        return cleaned
    return _SANITIZED_CODE


def _sanitize_failure_text(value: object | None, *, limit: int) -> str:
    raw = str(value or "").strip()
    scrubbed = _EMAIL_PATTERN.sub("[redacted]", raw)
    return scrubbed[:limit]


def _sanitize_failure_key(value: object | None) -> str:
    raw = str(value or "").strip().lower()
    if _FAILURE_KEY_PATTERN.fullmatch(raw):
        return raw
    return ""


def sanitize_codes(codes: list[str] | tuple[str, ...]) -> list[str]:
    sanitized: list[str] = []
    for code in codes:
        sanitized.append(sanitize_code(code))
    return sanitized


def sanitize_trace(trace: DecisionTraceV1) -> DecisionTraceV1:
    return replace(
        trace,
        signals_evaluated=sanitize_codes(trace.signals_evaluated),
        signals_fired=sanitize_codes(trace.signals_fired),
        explain_codes=sanitize_codes(trace.explain_codes),
    )


def from_canonical_json(payload_json: str) -> DecisionTraceV1 | None:
    try:
        raw = json.loads(payload_json)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        trace = DecisionTraceV1(
            decision_key=str(raw.get("decision_key") or ""),
            decision_kind=str(raw.get("decision_kind") or ""),
            anchor_ts_utc=float(raw.get("anchor_ts_utc") or 0.0),
            signals_evaluated=[
                str(item)
                for item in raw.get("signals_evaluated", [])
                if item is not None
            ],
            signals_fired=[
                str(item) for item in raw.get("signals_fired", []) if item is not None
            ],
            evidence={
                "matched": int((raw.get("evidence") or {}).get("matched") or 0),
                "total": int((raw.get("evidence") or {}).get("total") or 0),
            },
            model_fingerprint=str(raw.get("model_fingerprint") or ""),
            explain_codes=[
                str(item) for item in raw.get("explain_codes", []) if item is not None
            ],
            trace_schema=str(raw.get("trace_schema") or "DecisionTraceV1"),
            trace_version=int(raw.get("trace_version") or 1),
        )
    except (TypeError, ValueError):
        return None
    return sanitize_trace(trace)


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
        return {key: _normalize_config(value) for key, value in config_obj.items()}
    if isinstance(config_obj, (list, tuple)):
        return [_normalize_config(item) for item in config_obj]
    if hasattr(config_obj, "__dict__"):
        return {
            key: _normalize_config(value) for key, value in vars(config_obj).items()
        }
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
    failure_log_path: Path | None = None
    breaker_until_ts: float | None = None
    last_drop_reason: str | None = None
    _lock: threading.Lock = field(
        init=False, repr=False, default_factory=threading.Lock
    )

    def emit(self, emitter: ContractEventEmitter, event: EventV1) -> bool:
        with self._lock:
            self.attempted += 1
            disabled = self.disabled
        if disabled:
            with self._lock:
                self.dropped += 1
                self.last_drop_reason = "circuit_breaker"
            self._write_failure_log(
                event,
                error_type="circuit_breaker",
                breaker_state="disabled",
            )
            return False
        try:
            emitted = emitter.emit(event)
        except Exception as exc:  # pragma: no cover - defensive logging
            self._record_drop(event, error_type=type(exc).__name__)
            return False
        if emitted:
            with self._lock:
                self.succeeded += 1
            return True
        if self._event_exists(emitter, event):
            return False
        self._record_drop(event, error_type="emit_returned_false")
        return False

    def _record_drop(
        self,
        event: EventV1,
        *,
        error_type: str,
    ) -> None:
        with self._lock:
            self.dropped += 1
            self.last_drop_reason = error_type
            if self.dropped >= self.drop_threshold:
                self.disabled = True
                self.breaker_until_ts = time.time()
        try:
            logger.error("decision_trace_emit_failed", error=error_type)
        except Exception:
            return
        self._write_failure_log(
            event,
            error_type=error_type,
            breaker_state="disabled" if self.disabled else "open",
        )

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

    def _write_failure_log(
        self,
        event: EventV1,
        *,
        error_type: str,
        breaker_state: str | None = None,
    ) -> None:
        if self.failure_log_path is None:
            self.failure_log_path = Path("logs/decision_trace_failures.ndjson")
        try:
            self.failure_log_path.parent.mkdir(parents=True, exist_ok=True)
            if self.failure_log_path.exists():
                if self.failure_log_path.stat().st_size > 256 * 1024:
                    rotated = self.failure_log_path.with_suffix(
                        self.failure_log_path.suffix + ".1"
                    )
                    try:
                        if rotated.exists():
                            rotated.unlink()
                        self.failure_log_path.rename(rotated)
                    except OSError:
                        pass
            payload = {
                "ts_utc": float(event.ts_utc),
                "decision_type": sanitize_code(
                    str(event.payload.get("decision_kind") or "")
                ),
                "decision_key": _sanitize_failure_key(
                    self._extract_decision_key(event)
                ),
                "error_type": _sanitize_failure_text(error_type, limit=60),
                "breaker_state": breaker_state,
            }
            line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            with self.failure_log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"{line}\n")
        except Exception as exc:
            try:
                sys.stderr.write(
                    "decision_trace_failure_log_failed: "
                    f"{_sanitize_failure_text(exc, limit=120)}\n"
                )
            except Exception:
                return

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            attempted = int(self.attempted)
            succeeded = int(self.succeeded)
            dropped = int(self.dropped)
            disabled = bool(self.disabled)
            breaker_until_ts = self.breaker_until_ts
            last_drop_reason = self.last_drop_reason or ""
            log_path = str(self.failure_log_path) if self.failure_log_path else ""
        return {
            "attempted": attempted,
            "recorded": succeeded,
            "dropped": dropped,
            "breaker_open": disabled,
            "breaker_until_ts": breaker_until_ts,
            "last_drop_reason": last_drop_reason,
            "drop_log_path": log_path,
        }

    def _extract_decision_key(self, event: EventV1) -> str:
        raw = event.payload_json or ""
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return ""
        if isinstance(payload, dict):
            return str(payload.get("decision_key") or "")
        return ""


_DEFAULT_DECISION_TRACE_EMITTER = DecisionTraceEmitter()


def get_default_decision_trace_emitter() -> DecisionTraceEmitter:
    return _DEFAULT_DECISION_TRACE_EMITTER


__all__ = [
    "DecisionTraceV1",
    "DecisionTraceEmitter",
    "get_default_decision_trace_emitter",
    "compute_decision_key",
    "compute_model_fingerprint",
    "to_canonical_json",
    "from_canonical_json",
    "sanitize_code",
    "sanitize_codes",
    "sanitize_trace",
]
