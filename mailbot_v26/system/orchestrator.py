from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Tuple

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter

logger = logging.getLogger(__name__)


class SystemMode(str, Enum):
    FULL = "FULL"
    DEGRADED_NO_LLM = "DEGRADED_NO_LLM"
    DEGRADED_NO_TELEGRAM = "DEGRADED_NO_TELEGRAM"
    EMERGENCY_READ_ONLY = "EMERGENCY_READ_ONLY"


@dataclass
class SystemOrchestrator:
    event_emitter: EventEmitter | None = None
    mode: SystemMode = SystemMode.FULL
    reasons: List[str] = field(default_factory=list)
    cooldown_until: Dict[str, float] = field(default_factory=dict)

    def update_component(self, name: str, healthy: bool, reason: str | None) -> None:
        if healthy:
            self.cooldown_until.pop(name, None)
        else:
            self.cooldown_until[name] = time.time() + 300
        if reason:
            self.reasons.append(f"{name}:{reason}")
        new_mode = self._derive_mode()
        if new_mode != self.mode:
            self.mode = new_mode
            self._emit_mode_changed()

    def decide_llm_allowed(self) -> Tuple[bool, str]:
        allowed = self.mode not in {SystemMode.DEGRADED_NO_LLM, SystemMode.EMERGENCY_READ_ONLY}
        reason = "" if allowed else "llm_disabled_by_mode"
        self._log_policy("llm", allowed, reason)
        return allowed, reason

    def decide_digest_send_allowed(self) -> Tuple[bool, str]:
        allowed = self.mode != SystemMode.EMERGENCY_READ_ONLY
        reason = "" if allowed else "digests_disabled_by_mode"
        self._log_policy("digest_send", allowed, reason)
        return allowed, reason

    def decide_auto_priority_allowed(self) -> Tuple[bool, str]:
        allowed = self.mode not in {SystemMode.EMERGENCY_READ_ONLY}
        reason = "" if allowed else "auto_priority_disabled_by_mode"
        self._log_policy("auto_priority", allowed, reason)
        return allowed, reason

    def snapshot(self) -> dict:
        return {
            "mode": self.mode.value,
            "reasons": list(self.reasons),
            "cooldown_until": dict(self.cooldown_until),
        }

    def _derive_mode(self) -> SystemMode:
        if any(name.startswith("db") for name in self.cooldown_until):
            return SystemMode.EMERGENCY_READ_ONLY
        if any(name.startswith("telegram") for name in self.cooldown_until):
            return SystemMode.DEGRADED_NO_TELEGRAM
        if any(name.startswith("llm") for name in self.cooldown_until):
            return SystemMode.DEGRADED_NO_LLM
        return SystemMode.FULL

    def _emit_mode_changed(self) -> None:
        logger.info("system_mode_changed mode=%s", self.mode.value)
        if self.event_emitter:
            event = EventV1(
                event_type=EventType.SYSTEM_MODE_CHANGED,
                ts_utc=time.time(),
                account_id="system",
                entity_id=None,
                email_id=None,
                payload={"mode": self.mode.value, "reasons": list(self.reasons)},
            )
            self.event_emitter.emit(event)

    def _log_policy(self, policy: str, allowed: bool, reason: str) -> None:
        logger.info(
            "system_policy_decision policy=%s allowed=%d reason=%s mode=%s",
            policy,
            int(allowed),
            reason,
            self.mode.value,
        )


__all__ = ["SystemOrchestrator", "SystemMode"]
