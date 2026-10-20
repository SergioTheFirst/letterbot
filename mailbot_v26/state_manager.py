"""Thread-safe persistence for Letterbot v26.

The state file keeps track of processed UIDs and service health so the
pipeline can resume after restarts without duplicating work. The design
follows the Constitution's Guaranteed Mode: operations fail gracefully
and prefer safe defaults over crashes.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from mailbot_v26.account_identity import normalize_login

DEFAULT_STATE_PATH = Path(__file__).resolve().parent / "state.json"

log = logging.getLogger(__name__)

# One state file write at a time in this process, across StateManager
# instances: Windows refuses a rename over a file another rename is replacing.
_SAVE_LOCK = threading.Lock()


@dataclass
class AccountState:
    last_uid: int = 0
    last_check_time: Optional[str] = None
    imap_status: str = "unknown"
    last_error: str = ""
    uidvalidity: Optional[int] = None
    mailbox: str = "INBOX"


@dataclass
class LLMState:
    tokens_used_today: int = 0
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    unavailable: bool = False
    last_error: str = ""


@dataclass
class MetaState:
    version: str = "v26"
    last_save: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class BotState:
    accounts: Dict[str, AccountState] = field(default_factory=dict)
    llm: LLMState = field(default_factory=LLMState)
    meta: MetaState = field(default_factory=MetaState)
    # Top-level keys, never AccountState fields: an older build loads
    # accounts with AccountState(**data) and ignores unknown top-level keys.
    uid_attempts: Dict[str, Dict[int, int]] = field(default_factory=dict)
    dead_letter_alert_at: Dict[str, str] = field(default_factory=dict)
    # login -> UIDVALIDITY whose Message-ID resync has not yet reached the
    # epoch's highest UID (WP2c).
    resync_epoch: Dict[str, int] = field(default_factory=dict)
    # login -> emails dead-lettered at CONTEXT (lost after a restart) that
    # the account's next poll folds into its aggregated alert (R3).
    context_dead_letters: Dict[str, int] = field(default_factory=dict)


class StateManager:
    """Persistent, thread-safe state handler."""

    def __init__(self, state_file: Path = DEFAULT_STATE_PATH) -> None:
        self.state_file = state_file
        self._lock = threading.Lock()
        self._state = self._load_state()
        self._dirty = False
        self._last_save_ts = datetime.now()

    def _load_state(self) -> BotState:
        if not self.state_file.exists():
            return BotState()
        try:
            with open(self.state_file, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (json.JSONDecodeError, OSError):
            return BotState()

        accounts: Dict[str, AccountState] = {}
        for login, data in raw.get("accounts", {}).items():
            normalized = normalize_login(login)
            if not normalized:
                continue
            candidate = AccountState(**data)
            existing = accounts.get(normalized)
            if existing is None:
                accounts[normalized] = candidate
                continue
            if candidate.last_uid > existing.last_uid:
                existing.last_uid = candidate.last_uid
            if candidate.last_check_time and (
                not existing.last_check_time
                or candidate.last_check_time > existing.last_check_time
            ):
                existing.last_check_time = candidate.last_check_time
            if candidate.last_error:
                existing.last_error = candidate.last_error
            if candidate.imap_status and candidate.imap_status != "unknown":
                existing.imap_status = candidate.imap_status
            if candidate.uidvalidity is not None:
                existing.uidvalidity = candidate.uidvalidity
            if candidate.mailbox:
                existing.mailbox = candidate.mailbox
        llm_raw = raw.get("llm", {})
        meta_raw = raw.get("meta", {})
        return BotState(
            accounts=accounts,
            llm=LLMState(**llm_raw),
            meta=MetaState(**meta_raw),
            uid_attempts=self._load_uid_attempts(raw.get("uid_attempts")),
            dead_letter_alert_at=self._load_alert_times(
                raw.get("dead_letter_alert_at")
            ),
            resync_epoch=self._load_resync_epochs(raw.get("resync_epoch")),
            context_dead_letters=self._load_context_dead_letters(
                raw.get("context_dead_letters")
            ),
        )

    def _load_uid_attempts(self, raw: Any) -> Dict[str, Dict[int, int]]:
        attempts: Dict[str, Dict[int, int]] = {}
        if not isinstance(raw, Mapping):
            return attempts
        for login, entries in raw.items():
            key = normalize_login(str(login))
            if not key or not isinstance(entries, Mapping):
                continue
            target = attempts.setdefault(key, {})
            for uid, count in entries.items():
                try:
                    uid_value, count_value = int(uid), int(count)
                except (TypeError, ValueError):
                    log.warning(
                        "state_uid_attempt_entry_ignored file=%s", self.state_file
                    )
                    continue
                target[uid_value] = max(target.get(uid_value, 0), count_value)
        return attempts

    @staticmethod
    def _load_alert_times(raw: Any) -> Dict[str, str]:
        if not isinstance(raw, Mapping):
            return {}
        times: Dict[str, str] = {}
        for login, value in raw.items():
            key = normalize_login(str(login))
            if key and isinstance(value, str) and value:
                times[key] = max(times.get(key, ""), value)
        return times

    def _load_resync_epochs(self, raw: Any) -> Dict[str, int]:
        if not isinstance(raw, Mapping):
            return {}
        epochs: Dict[str, int] = {}
        for login, value in raw.items():
            key = normalize_login(str(login))
            if key and isinstance(value, int) and not isinstance(value, bool):
                epochs[key] = value
            else:
                log.warning("state_resync_epoch_entry_ignored file=%s", self.state_file)
        return epochs

    def _load_context_dead_letters(self, raw: Any) -> Dict[str, int]:
        if not isinstance(raw, Mapping):
            return {}
        counts: Dict[str, int] = {}
        for login, value in raw.items():
            key = normalize_login(str(login))
            if key and isinstance(value, int) and not isinstance(value, bool) and value > 0:
                counts[key] = counts.get(key, 0) + value
            else:
                log.warning(
                    "state_context_dead_letter_entry_ignored file=%s", self.state_file
                )
        return counts

    def get_last_uid(self, login: str) -> int:
        key = normalize_login(login)
        with self._lock:
            return self._state.accounts.get(key, AccountState()).last_uid

    def update_last_uid(self, login: str, uid: int) -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            account = self._state.accounts.setdefault(key, AccountState())
            account.last_uid = uid
            self._mark_dirty()

    def reset_account_cursor(self, login: str) -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            account = self._state.accounts.setdefault(key, AccountState())
            account.last_uid = 0
            account.last_check_time = None
            self._mark_dirty()

    def get_uidvalidity(self, login: str) -> Optional[int]:
        key = normalize_login(login)
        with self._lock:
            return self._state.accounts.get(key, AccountState()).uidvalidity

    def update_uidvalidity(
        self, login: str, uidvalidity: Optional[int], mailbox: str | None = None
    ) -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            account = self._state.accounts.setdefault(key, AccountState())
            account.uidvalidity = uidvalidity
            if mailbox:
                account.mailbox = mailbox
            self._mark_dirty()

    def update_check_time(
        self, login: str, timestamp: Optional[datetime] = None
    ) -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            ts = (timestamp or datetime.now()).isoformat()
            account = self._state.accounts.setdefault(key, AccountState())
            account.last_check_time = ts
            self._mark_dirty()

    def get_last_check_time(self, login: str) -> Optional[datetime]:
        key = normalize_login(login)
        with self._lock:
            time_str = self._state.accounts.get(key, AccountState()).last_check_time
        if not time_str:
            return None
        return datetime.fromisoformat(time_str)

    def set_imap_status(self, login: str, status: str, error: str = "") -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            account = self._state.accounts.setdefault(key, AccountState())
            account.imap_status = status
            account.last_error = error
            self._mark_dirty()

    def get_uid_attempts(self, login: str) -> Dict[int, int]:
        """UID -> failed fetch/persist attempts for UIDs above the cursor."""
        key = normalize_login(login)
        with self._lock:
            return dict(self._state.uid_attempts.get(key, {}))

    def set_uid_attempts(self, login: str, attempts: Mapping[int, int]) -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            cleaned = {int(uid): int(count) for uid, count in attempts.items()}
            if cleaned:
                self._state.uid_attempts[key] = cleaned
            else:
                self._state.uid_attempts.pop(key, None)
            self._mark_dirty()

    def get_dead_letter_alert_at(self, login: str) -> Optional[datetime]:
        key = normalize_login(login)
        with self._lock:
            raw = self._state.dead_letter_alert_at.get(key)
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            log.warning("state_dead_letter_alert_time_invalid file=%s", self.state_file)
            return None

    def set_dead_letter_alert_at(self, login: str, timestamp: datetime) -> None:
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            self._state.dead_letter_alert_at[key] = timestamp.isoformat()
            self._mark_dirty()

    def get_resync_epoch(self, login: str) -> Optional[int]:
        """UIDVALIDITY of a Message-ID resync still in progress, else None."""
        key = normalize_login(login)
        with self._lock:
            return self._state.resync_epoch.get(key)

    def set_resync_epoch(self, login: str, uidvalidity: Optional[int]) -> None:
        """Record the epoch of a resync in progress; None ends it."""
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            if uidvalidity is None:
                self._state.resync_epoch.pop(key, None)
            else:
                self._state.resync_epoch[key] = int(uidvalidity)
            self._mark_dirty()

    def add_context_dead_letter(self, login: str) -> None:
        """Count one email dead-lettered at CONTEXT for the account's next
        poll alert (R3)."""
        key = normalize_login(login)
        if not key:
            return
        with self._lock:
            counts = self._state.context_dead_letters
            counts[key] = counts.get(key, 0) + 1
            self._mark_dirty()

    def pop_context_dead_letters(self, login: str) -> int:
        """Take the account's CONTEXT dead-letter count; 0 when none."""
        key = normalize_login(login)
        with self._lock:
            count = self._state.context_dead_letters.pop(key, 0)
            if count:
                self._mark_dirty()
        return count

    def get_accounts_snapshot(self) -> Dict[str, AccountState]:
        with self._lock:
            return {
                login: AccountState(**account.__dict__)
                for login, account in self._state.accounts.items()
            }

    def add_tokens(self, count: int) -> None:
        with self._lock:
            today = datetime.now().strftime("%Y-%m-%d")
            if self._state.llm.date != today:
                self._state.llm.tokens_used_today = 0
                self._state.llm.date = today
            self._state.llm.tokens_used_today += count
            self._mark_dirty()

    def set_llm_unavailable(self, unavailable: bool, error: str = "") -> None:
        with self._lock:
            self._state.llm.unavailable = unavailable
            self._state.llm.last_error = error
            self._mark_dirty()

    def save(self, force: bool = False) -> None:
        with self._lock:
            now = datetime.now()
            should_save = (
                force
                or self._dirty
                or (now - self._last_save_ts) > timedelta(seconds=60)
            )
            if not should_save:
                return

            payload = {
                "accounts": {
                    login: account.__dict__
                    for login, account in self._state.accounts.items()
                },
                "llm": self._state.llm.__dict__,
                "meta": {**self._state.meta.__dict__, "last_save": now.isoformat()},
                "uid_attempts": {
                    login: {str(uid): count for uid, count in sorted(entries.items())}
                    for login, entries in self._state.uid_attempts.items()
                },
                "dead_letter_alert_at": dict(self._state.dead_letter_alert_at),
                "resync_epoch": dict(self._state.resync_epoch),
                "context_dead_letters": dict(self._state.context_dead_letters),
            }

            with _SAVE_LOCK:
                self._write_state_file(payload)
            self._dirty = False
            self._last_save_ts = now

    def _write_state_file(self, payload: Mapping[str, Any]) -> None:
        """Write payload to a temp file of its own beside the state file
        (mkstemp: unique, private), fsync it, then rename it over the state
        file. On failure the temp file is removed and the error propagates,
        so the state stays dirty and the next save writes it."""
        tmp_name: Optional[str] = None
        try:
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{self.state_file.name}.",
                suffix=".tmp",
                dir=self.state_file.parent,
            )
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                # The cursor must be on disk before the rename makes it live.
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, self.state_file)
        except BaseException as exc:
            log.warning(
                "state_save_failed file=%s error=%s: %s",
                self.state_file,
                type(exc).__name__,
                exc,
            )
            if tmp_name is not None:
                try:
                    Path(tmp_name).unlink(missing_ok=True)
                except OSError as cleanup_exc:
                    log.warning(
                        "state_temp_cleanup_failed file=%s error=%s",
                        tmp_name,
                        cleanup_exc,
                    )
            raise

    def _mark_dirty(self) -> None:
        self._dirty = True


__all__ = ["StateManager", "BotState", "AccountState", "LLMState", "MetaState"]
