from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")

_DIGEST_KEY = "digest_enabled"
_AUTO_PRIORITY_KEY = "auto_priority_enabled"
_INSIDER_SINCE_KEY = "insider_since"


@dataclass(frozen=True, slots=True)
class RuntimeOverrides:
    digest_enabled: bool | None
    auto_priority_enabled: bool | None


class RuntimeOverrideStore:
    def __init__(self, db_path: Path) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self._path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runtime_overrides (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TEXT
                    );
                    """
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("runtime_overrides_init_failed", error=str(exc))

    def get_overrides(self) -> RuntimeOverrides:
        return RuntimeOverrides(
            digest_enabled=self._read_bool(_DIGEST_KEY),
            auto_priority_enabled=self._read_bool(_AUTO_PRIORITY_KEY),
        )

    def set_digest_enabled(self, enabled: bool) -> None:
        self._write_bool(_DIGEST_KEY, enabled)

    def set_auto_priority_enabled(self, enabled: bool) -> None:
        self._write_bool(_AUTO_PRIORITY_KEY, enabled)


    def set_insider_since(self, value: str) -> None:
        cleaned = str(value or "").strip()
        if not cleaned:
            self._write_text(_INSIDER_SINCE_KEY, None)
            return
        self._write_text(_INSIDER_SINCE_KEY, cleaned)

    def get_insider_since(self) -> str | None:
        return self._read_text(_INSIDER_SINCE_KEY)



    def _read_text(self, key: str) -> str | None:
        try:
            with sqlite3.connect(self._path) as conn:
                row = conn.execute(
                    "SELECT value FROM runtime_overrides WHERE key = ?",
                    (key,),
                ).fetchone()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("runtime_overrides_read_failed", key=key, error=str(exc))
            return None
        if not row:
            return None
        value = str(row[0] or "").strip()
        return value or None

    def _read_bool(self, key: str) -> bool | None:
        try:
            with sqlite3.connect(self._path) as conn:
                row = conn.execute(
                    "SELECT value FROM runtime_overrides WHERE key = ?",
                    (key,),
                ).fetchone()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("runtime_overrides_read_failed", key=key, error=str(exc))
            return None
        if not row or row[0] is None:
            return None
        value = str(row[0]).strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        return None

    def _write_bool(self, key: str, enabled: bool) -> None:
        self._write_text(key, "1" if enabled else "0")

    def _write_text(self, key: str, value: str | None) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        try:
            with sqlite3.connect(self._path) as conn:
                conn.execute(
                    """
                    INSERT INTO runtime_overrides (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (key, value, ts),
                )
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("runtime_overrides_write_failed", key=key, error=str(exc))


__all__ = ["RuntimeOverrideStore", "RuntimeOverrides"]
