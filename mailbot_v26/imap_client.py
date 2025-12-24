"""IMAP helper implementing the UID+SINCE hybrid search mandated by the
Constitution. The class is intentionally small so it can be tested with
mocks without opening real network connections.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List, Sequence

try:  # pragma: no cover - import guard
    from imapclient import IMAPClient
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    IMAPClient = None  # type: ignore

from config_loader import AccountConfig
from state_manager import StateManager


class ResilientIMAP:
    """IMAP client that combines UID and SINCE queries to avoid duplicates."""

    def __init__(self, account: AccountConfig, state: StateManager, start_time: datetime | None = None) -> None:
        self.account = account
        self.state = state
        self.logger = logging.getLogger(__name__)
        base_time = start_time or datetime.now()
        self.start_time = base_time.replace(tzinfo=None)

    def _build_search(self) -> List[Sequence[str]]:
        last_uid = self.state.get_last_uid(self.account.login)
        last_check = self.state.get_last_check_time(self.account.login)
        baseline = last_check or self.start_time
        if baseline < self.start_time:
            baseline = self.start_time
        since_date = baseline.strftime("%d-%b-%Y")

        if last_uid <= 0:
            return [["SINCE", since_date]]
        return [["OR", ["UID", f"{last_uid + 1}:*"], ["SINCE", since_date]]]

    def fetch_new_messages(self) -> List[tuple[int, bytes]]:
        criteria = self._build_search()
        if IMAPClient is None:
            self.state.set_imap_status(self.account.login, "error", "imapclient missing")
            self.logger.error("IMAP client dependency is not available; skipping fetch")
            return []
        client = None
        try:
            client = IMAPClient(
                self.account.host,
                port=self.account.port,
                ssl=self.account.use_ssl,
                timeout=30,
            )
            client.login(self.account.login, self.account.password)
            client.select_folder("INBOX")
            uids: Iterable[int] = client.search(criteria[0])
            last_uid = self.state.get_last_uid(self.account.login)
            uid_list = list(uids)
            latest_seen_uid = max(uid_list) if uid_list else last_uid
            new_uids = [uid for uid in uid_list if uid > last_uid]
            messages: List[tuple[int, bytes]] = []
            for uid in sorted(new_uids):
                data = client.fetch([uid], ["RFC822", "INTERNALDATE"])
                envelope = data.get(uid, {})
                internaldate = envelope.get(b"INTERNALDATE")
                if isinstance(internaldate, datetime) and internaldate.tzinfo is not None:
                    internaldate = internaldate.replace(tzinfo=None)
                if isinstance(internaldate, datetime) and internaldate < self.start_time:
                    latest_seen_uid = max(latest_seen_uid, uid)
                    continue

                raw: bytes = envelope[b"RFC822"]
                messages.append((uid, raw))
                latest_seen_uid = max(latest_seen_uid, uid)

            if latest_seen_uid > last_uid:
                self.state.update_last_uid(self.account.login, latest_seen_uid)
            self.state.update_check_time(self.account.login)
            self.state.set_imap_status(self.account.login, "ok")
            return messages
        except Exception as exc:  # network/imap errors should not crash pipeline
            self.state.set_imap_status(self.account.login, "error", str(exc))
            self.logger.exception("IMAP fetch failed for %s", self.account.login)
            return []
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    self.logger.warning("IMAP logout failed for %s", self.account.login)


__all__ = ["ResilientIMAP"]
