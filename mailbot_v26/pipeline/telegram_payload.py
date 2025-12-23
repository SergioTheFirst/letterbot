from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TelegramPayload:
    html_text: str
    priority: str
    metadata: dict[str, Any]


__all__ = ["TelegramPayload"]
