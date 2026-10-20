from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from mailbot_v26.observability import get_logger
from mailbot_v26.ui.i18n import humanize_mode


class OperationalMode(Enum):
    FULL = "FULL"
    DEGRADED_NO_LLM = "DEGRADED_NO_LLM"
    DEGRADED_NO_TELEGRAM = "DEGRADED_NO_TELEGRAM"
    EMERGENCY_READ_ONLY = "EMERGENCY_READ_ONLY"


@dataclass(frozen=True)
class ComponentHealth:
    name: str
    available: bool | None
    reason: str | None = None
    # Intentionally off (not configured / not wired): never degrades the mode.
    disabled: bool = False


COMPONENT_OFF = "off"
COMPONENT_DEGRADED = "degraded"
COMPONENT_OK = "ok"
COMPONENT_UNKNOWN = "unknown"


def _component_state(health: ComponentHealth | None) -> str:
    if health is None:
        return COMPONENT_UNKNOWN
    if health.disabled:
        return COMPONENT_OFF
    if health.available is False:
        return COMPONENT_DEGRADED
    if health.available is True:
        return COMPONENT_OK
    return COMPONENT_UNKNOWN


@dataclass(frozen=True)
class ModeChange:
    previous: OperationalMode
    current: OperationalMode
    reason: str


class SystemHealth:
    def __init__(self) -> None:
        self._logger = get_logger("mailbot")
        self._components: dict[str, ComponentHealth] = {}
        self._mode = OperationalMode.FULL

    @property
    def mode(self) -> OperationalMode:
        return self._mode

    def reset(self) -> None:
        self._components.clear()
        self._mode = OperationalMode.FULL

    def update_component(
        self, component: str, available: bool, reason: str | None = None
    ) -> ModeChange | None:
        self._set_component(ComponentHealth(component, available, reason))
        return self._evaluate_mode()

    def disable_component(self, component: str, reason: str) -> ModeChange | None:
        """Mark a component intentionally off (e.g. no LLM configured or wired).

        Unlike ``update_component(..., False)`` this is not a failure: the
        component reports "off" and never degrades the operational mode.
        """
        self._set_component(ComponentHealth(component, None, reason, disabled=True))
        return self._evaluate_mode()

    def component_state(self, component: str) -> str:
        """Return "off", "degraded", "ok" or "unknown" for one component."""
        return _component_state(self._components.get(component))

    def _set_component(self, health: ComponentHealth) -> None:
        previous = _component_state(self._components.get(health.name))
        self._components[health.name] = health
        current = _component_state(health)
        # "off" is the only state without its own mode: log its transitions
        # (mode changes are logged by _evaluate_mode).
        if previous != current and COMPONENT_OFF in (previous, current):
            self._logger.info(
                "system_component_state_changed",
                component=health.name,
                reason=health.reason or "",
                **{"from": previous, "to": current},
            )

    def update_components(
        self, updates: Iterable[ComponentHealth]
    ) -> ModeChange | None:
        for update in updates:
            self._set_component(update)
        return self._evaluate_mode()

    def system_notice(self, change: ModeChange) -> str:
        label = humanize_mode(change.current.value, locale="ru")
        if change.current == OperationalMode.FULL:
            return f"{label}\nВозможности восстановлены."
        if change.current == OperationalMode.DEGRADED_NO_LLM:
            return f"{label}\nAI-анализ временно недоступен."
        if change.current == OperationalMode.DEGRADED_NO_TELEGRAM:
            return f"{label}\nTelegram временно недоступен."
        return f"{label}\nCRM временно недоступен."

    def _evaluate_mode(self) -> ModeChange | None:
        new_mode, reason = self._determine_mode()
        if new_mode == self._mode:
            return None
        change = ModeChange(previous=self._mode, current=new_mode, reason=reason)
        self._mode = new_mode
        self._logger.info(
            "system_mode_changed",
            **{
                "from": change.previous.value,
                "to": change.current.value,
                "reason": reason,
            },
        )
        return change

    def _determine_mode(self) -> tuple[OperationalMode, str]:
        crm = self._components.get("CRM")
        mail = self._components.get("Mail")
        llm = self._components.get("LLM")
        telegram = self._components.get("Telegram")

        if crm is not None and crm.available is False:
            return OperationalMode.EMERGENCY_READ_ONLY, crm.reason or "CRM unavailable"
        if mail is not None and mail.available is False:
            return (
                OperationalMode.EMERGENCY_READ_ONLY,
                mail.reason or "Mail unavailable",
            )
        if llm is not None and llm.available is False:
            return OperationalMode.DEGRADED_NO_LLM, llm.reason or "LLM unavailable"
        if telegram is not None and telegram.available is False:
            return (
                OperationalMode.DEGRADED_NO_TELEGRAM,
                telegram.reason or "Telegram unavailable",
            )
        return OperationalMode.FULL, "All components healthy"


system_health = SystemHealth()

__all__ = [
    "COMPONENT_DEGRADED",
    "COMPONENT_OFF",
    "COMPONENT_OK",
    "COMPONENT_UNKNOWN",
    "ComponentHealth",
    "ModeChange",
    "OperationalMode",
    "SystemHealth",
    "system_health",
]
