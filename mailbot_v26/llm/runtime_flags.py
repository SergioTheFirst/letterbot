from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

DEFAULT_RUNTIME_FLAGS_PATH = Path(__file__).resolve().parents[1] / "runtime_flags.json"


@dataclass(frozen=True)
class RuntimeFlags:
    enable_gigachat: bool = False
    enable_auto_priority: bool = True


class RuntimeFlagStore:
    def __init__(
        self,
        path: Path = DEFAULT_RUNTIME_FLAGS_PATH,
        *,
        poll_interval_sec: float = 1.0,
    ) -> None:
        self.path = path
        self.poll_interval_sec = poll_interval_sec
        self._lock = threading.Lock()
        self._last_checked = 0.0
        self._last_mtime: float | None = None
        self._flags = RuntimeFlags()

    def get_flags(self, *, force: bool = False) -> Tuple[RuntimeFlags, bool]:
        now = time.monotonic()
        with self._lock:
            if not force and (now - self._last_checked) < self.poll_interval_sec:
                return self._flags, False
            self._last_checked = now
            return self._refresh_locked()

    def set_enable_gigachat(self, enabled: bool) -> None:
        with self._lock:
            flags = self._load_flags()
            updated = RuntimeFlags(
                enable_gigachat=bool(enabled),
                enable_auto_priority=flags.enable_auto_priority,
            )
            self._write_flags(updated)
            self._last_mtime = self._safe_mtime()
            self._flags = updated

    def set_enable_auto_priority(self, enabled: bool) -> None:
        with self._lock:
            flags = self._load_flags()
            updated = RuntimeFlags(
                enable_gigachat=flags.enable_gigachat,
                enable_auto_priority=bool(enabled),
            )
            self._write_flags(updated)
            self._last_mtime = self._safe_mtime()
            self._flags = updated

    def _refresh_locked(self) -> Tuple[RuntimeFlags, bool]:
        mtime = self._safe_mtime()
        if mtime == self._last_mtime:
            return self._flags, False
        self._last_mtime = mtime
        flags = self._load_flags()
        changed = flags != self._flags
        self._flags = flags
        return flags, changed

    def _safe_mtime(self) -> float | None:
        try:
            return self.path.stat().st_mtime
        except OSError:
            return None

    def _load_flags(self) -> RuntimeFlags:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, json.JSONDecodeError):
            return RuntimeFlags()

        enabled = raw.get("enable_gigachat", False)
        auto_priority = raw.get("enable_auto_priority", True)
        return RuntimeFlags(
            enable_gigachat=bool(enabled),
            enable_auto_priority=bool(auto_priority),
        )

    def _write_flags(self, flags: RuntimeFlags) -> None:
        payload = {
            "enable_gigachat": bool(flags.enable_gigachat),
            "enable_auto_priority": bool(flags.enable_auto_priority),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        tmp_path.replace(self.path)


__all__ = ["DEFAULT_RUNTIME_FLAGS_PATH", "RuntimeFlags", "RuntimeFlagStore"]
