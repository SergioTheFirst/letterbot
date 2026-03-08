"""Thread-safe persistence for Letterbot v26.

The state file keeps track of processed UIDs and service health so the
pipeline can resume after restarts without duplicating work. The design
follows the Constitution's Guaranteed Mode: operations fail gracefully
and prefer safe defaults over crashes.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from mailbot_v26.account_identity import normalize_login

DEFAULT_STATE_PATH = Path(__file__).resolve().parent / "state.json"


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
        )

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
            }

            tmp_file = self.state_file.with_suffix(".tmp")
            with open(tmp_file, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
            tmp_file.replace(self.state_file)
            self._dirty = False
            self._last_save_ts = now

    def _mark_dirty(self) -> None:
        self._dirty = True


__all__ = ["StateManager", "BotState", "AccountState", "LLMState", "MetaState"]
