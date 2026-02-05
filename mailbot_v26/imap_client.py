"""IMAP helper implementing the UID+SINCE hybrid search mandated by the
Constitution. The class is intentionally small so it can be tested with
mocks without opening real network connections.
"""

from __future__ import annotations

import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence

try:  # pragma: no cover - import guard
    from imapclient import IMAPClient
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    IMAPClient = None  # type: ignore

from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.state_manager import StateManager


class ResilientIMAP:
    """IMAP client that combines UID and SINCE queries to avoid duplicates."""

    def __init__(
        self,
        account: AccountConfig,
        state: StateManager,
        start_time: datetime | None = None,
        allow_prestart_emails: bool = False,
        max_email_mb: int = 15,
    ) -> None:
        self.account = account
        self.state = state
        self.logger = logging.getLogger(__name__)
        base_time = start_time or datetime.now(timezone.utc)
        self.run_start_utc = self._normalize_to_utc(base_time)
        self.allow_prestart_emails = allow_prestart_emails
        self.max_email_bytes = max_email_mb * 1024 * 1024
        self._skip_log_path = Path(__file__).resolve().parent / "logs" / "ingest_skips.ndjson"

    @staticmethod
    def _normalize_to_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _resolve_since_datetime(self, last_check: datetime | None) -> datetime:
        if self.allow_prestart_emails:
            if last_check is not None:
                return self._normalize_to_utc(last_check)
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        baseline = self._normalize_to_utc(last_check) if last_check else self.run_start_utc
        if baseline < self.run_start_utc:
            baseline = self.run_start_utc
        return baseline

    def _log_prestart_skip(self, uid: int, internaldate_utc: datetime) -> None:
        self.logger.warning(
            "imap_prestart_skip account_id=%s uid=%s internaldate_utc=%s run_start_utc=%s",
            self.account.account_id,
            uid,
            internaldate_utc.isoformat(),
            self.run_start_utc.isoformat(),
        )
        payload = {
            "account_id": self.account.account_id,
            "uid": uid,
            "internaldate_utc": internaldate_utc.isoformat(),
            "run_start_utc": self.run_start_utc.isoformat(),
        }
        try:
            self._skip_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._skip_log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            self.logger.warning(
                "imap_prestart_skip_log_failed account_id=%s uid=%s error=%s",
                self.account.account_id,
                uid,
                exc,
            )

    def _build_oversize_warning(
        self,
        *,
        headers: bytes,
        message_size: int | None,
    ) -> bytes:
        size_mb = (message_size or 0) / (1024 * 1024)
        limit_mb = self.max_email_bytes / (1024 * 1024)
        warning_text = (
            "Письмо слишком большое для загрузки.\n"
            f"Размер: {size_mb:.1f} MB (лимит {limit_mb:.1f} MB).\n"
            "Тело и вложения пропущены."
        )
        warning_body = warning_text.encode("utf-8")
        if headers.endswith(b"\r\n"):
            return headers + b"\r\n" + warning_body
        return headers + b"\r\n\r\n" + warning_body

    @staticmethod
    def _extract_header_payload(envelope: dict[bytes, object]) -> bytes:
        for key in (b"BODY[HEADER]", b"BODY.PEEK[HEADER]", b"RFC822.HEADER"):
            payload = envelope.get(key)
            if isinstance(payload, bytes):
                return payload
        return b""

    def _build_search(self) -> List[Sequence[str]]:
        last_uid = self.state.get_last_uid(self.account.login)
        last_check = self.state.get_last_check_time(self.account.login)
        baseline = self._resolve_since_datetime(last_check)
        since_date = baseline.strftime("%d-%b-%Y")

        if last_uid <= 0:
            return [["UID", "1:*", "SINCE", since_date]]
        return [["UID", f"{last_uid + 1}:*", "SINCE", since_date]]

    def fetch_new_messages(self) -> List[tuple[int, bytes]]:
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
            client.login(
                self.account.username or self.account.login,
                self.account.password,
            )
            client.select_folder("INBOX")
            last_uid = self.state.get_last_uid(self.account.login)
            last_check = self.state.get_last_check_time(self.account.login)
            baseline = self._resolve_since_datetime(last_check)
            since_date = baseline.strftime("%d-%b-%Y")

            is_bootstrap = last_uid <= 0
            if is_bootstrap:
                all_uids = list(client.search(["UID", "1:*"]))
                max_uid = max(all_uids) if all_uids else 0
                uid_list = list(client.search(["UID", "1:*", "SINCE", since_date]))
                latest_seen_uid = max_uid if max_uid > last_uid else last_uid
                new_uids = uid_list
            else:
                uids: Iterable[int] = client.search(["UID", f"{last_uid + 1}:*"])
                uid_list = list(uids)
                latest_seen_uid = max(uid_list) if uid_list else last_uid
                new_uids = list(
                    client.search(["UID", f"{last_uid + 1}:*", "SINCE", since_date])
                )
            messages: List[tuple[int, bytes]] = []
            for uid in sorted(new_uids):
                data = client.fetch([uid], ["RFC822.SIZE", "INTERNALDATE"])
                envelope = data.get(uid, {})
                internaldate = envelope.get(b"INTERNALDATE")
                if isinstance(internaldate, datetime):
                    internaldate_utc = self._normalize_to_utc(internaldate)
                    if (
                        not self.allow_prestart_emails
                        and internaldate_utc < self.run_start_utc
                    ):
                        self._log_prestart_skip(uid, internaldate_utc)
                        continue

                message_size = envelope.get(b"RFC822.SIZE")
                if isinstance(message_size, bytes):
                    try:
                        message_size = int(message_size.decode("utf-8"))
                    except (TypeError, ValueError):
                        message_size = None
                raw: bytes
                if isinstance(message_size, int) and message_size > self.max_email_bytes:
                    header_data = client.fetch([uid], ["BODY.PEEK[HEADER]"])
                    header_envelope = header_data.get(uid, {})
                    headers = self._extract_header_payload(header_envelope)
                    raw = self._build_oversize_warning(
                        headers=headers,
                        message_size=message_size,
                    )
                    self.logger.warning(
                        "imap_message_oversize uid=%s size_bytes=%s max_bytes=%s",
                        uid,
                        message_size,
                        self.max_email_bytes,
                    )
                else:
                    data = client.fetch([uid], ["RFC822"])
                    envelope = data.get(uid, {})
                    raw = envelope[b"RFC822"]
                messages.append((uid, raw))

            if latest_seen_uid > last_uid:
                self.state.update_last_uid(self.account.login, latest_seen_uid)
            self.state.update_check_time(self.account.login)
            self.state.set_imap_status(self.account.login, "ok")
            return messages
        except Exception as exc:  # network/imap errors should not crash pipeline
            self.state.set_imap_status(self.account.login, "error", str(exc))
            self.logger.exception("IMAP fetch failed for %s", self.account.login)
            raise
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    self.logger.warning("IMAP logout failed for %s", self.account.login)


__all__ = ["ResilientIMAP"]
