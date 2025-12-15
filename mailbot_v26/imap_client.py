"""IMAP helper implementing the UID+SINCE hybrid search mandated by the
Constitution. The class is intentionally small so it can be tested with
mocks without opening real network connections.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable, List, Sequence

try:  # pragma: no cover - import guard
    from imapclient import IMAPClient
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    IMAPClient = None  # type: ignore

from config_loader import AccountConfig
from state_manager import StateManager


class ResilientIMAP:
    """IMAP client that combines UID and SINCE queries to avoid duplicates."""

    def __init__(self, account: AccountConfig, state: StateManager) -> None:
        self.account = account
        self.state = state
        self.logger = logging.getLogger(__name__)

    def _build_search(self, now: datetime | None = None) -> List[Sequence[str]]:
        last_uid = self.state.get_last_uid(self.account.login)
        last_check = self.state.get_last_check_time(self.account.login)
        if not last_check:
            last_check = (now or datetime.now()) - timedelta(days=1)
        since_date = last_check.strftime("%d-%b-%Y")

        if last_uid <= 0:
            return [["SINCE", since_date]]
        return [["OR", ["UID", f"{last_uid + 1}:*"], ["SINCE", since_date]]]

    def fetch_new_messages(self) -> List[tuple[int, bytes]]:
        criteria = self._build_search()
        if IMAPClient is None:
            self.state.set_imap_status(self.account.login, "error", "imapclient missing")
            self.logger.error("IMAP client dependency is not available; skipping fetch")
            return []
        try:
            client = IMAPClient(self.account.host, port=self.account.port, ssl=self.account.use_ssl)
            client.login(self.account.login, self.account.password)
            client.select_folder("INBOX")
            uids: Iterable[int] = client.search(criteria[0])
            last_uid = self.state.get_last_uid(self.account.login)
            new_uids = [uid for uid in uids if uid > last_uid]
            messages: List[tuple[int, bytes]] = []
            for uid in sorted(new_uids):
                data = client.fetch([uid], ["RFC822"])
                raw: bytes = data[uid][b"RFC822"]
                messages.append((uid, raw))
            if messages:
                self.state.update_last_uid(self.account.login, messages[-1][0])
            self.state.update_check_time(self.account.login)
            self.state.set_imap_status(self.account.login, "ok")
            return messages
        except Exception as exc:  # network/imap errors should not crash pipeline
            self.state.set_imap_status(self.account.login, "error", str(exc))
            self.logger.exception("IMAP fetch failed for %s", self.account.login)
            return []


__all__ = ["ResilientIMAP"]
