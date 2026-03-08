from __future__ import annotations

import email
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
import logging
import socket
import ssl
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from mailbot_v26.account_identity import normalize_login
from mailbot_v26.state_manager import StateManager

try:
    from imapclient import IMAPClient  # type: ignore
except Exception:  # pragma: no cover
    IMAPClient = None  # type: ignore


log = logging.getLogger(__name__)


class IMAPError(RuntimeError):
    pass


@dataclass
class IMAPAccount:
    host: str
    port: int = 993
    ssl: bool = True
    user: str = ""
    password: str = ""
    mailbox: str = "INBOX"
    max_email_mb: int = 15  # защита от больших писем


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _decode_header_value(value: str) -> str:
    try:
        parts = decode_header(value)
    except Exception:
        return value
    out = []
    for chunk, enc in parts:
        if isinstance(chunk, bytes):
            try:
                out.append(chunk.decode(enc or "utf-8", errors="replace"))
            except Exception:
                out.append(chunk.decode("utf-8", errors="replace"))
        else:
            out.append(chunk)
    return "".join(out)


def _get_msg_date_utc(msg: Message) -> Optional[datetime]:
    raw = msg.get("Date")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return None
    if dt is None:
        return None
    return _safe_dt(dt)


def _parse_rfc822_size(fetch_data: Dict[bytes, Any]) -> Optional[int]:
    # imapclient возвращает {b'RFC822.SIZE': int, ...} либо ключи str
    for k in (b"RFC822.SIZE", "RFC822.SIZE"):
        if k in fetch_data:
            try:
                return int(fetch_data[k])
            except Exception:
                return None
    return None


def _imapclient_available() -> bool:
    return (_imap_client_cls is not None) or (IMAPClient is not None)


# для тестов/моков можно подменять фабрику на уровне модуля
_imap_client_cls = None


class ResilientIMAP:
    """
    IMAP-клиент с "живучестью":
    - переподключение при сетевых ошибках
    - ограничение на размер письма (max_email_mb)
    - выборка писем порциями, без зависания
    """

    # для тестов/моков можно подменять класс клиента:
    _imap_client_cls = None

    def __init__(
        self,
        account: Any,
        state: Optional[StateManager] = None,
        start_time: Optional[datetime] = None,
        allow_prestart_emails: bool = False,
        first_run_bootstrap: bool = False,
        first_run_bootstrap_hours: int = 24,
        first_run_bootstrap_max_messages: int = 20,
        max_email_mb: Optional[int] = None,
        *,
        connect_timeout: float = 15.0,
        io_timeout: float = 30.0,
        retries: int = 3,
        backoff_base: float = 0.7,
    ) -> None:
        if not _imapclient_available():
            raise IMAPError(
                "Пакет imapclient не установлен. Установите: pip install imapclient"
            )

        normalized_account = IMAPAccount(
            host=getattr(account, "host", ""),
            port=int(getattr(account, "port", 993) or 993),
            ssl=bool(getattr(account, "ssl", getattr(account, "use_ssl", True))),
            user=getattr(account, "user", getattr(account, "login", "")),
            password=getattr(account, "password", ""),
            mailbox=getattr(account, "mailbox", "INBOX"),
            max_email_mb=int(
                max_email_mb or getattr(account, "max_email_mb", 15) or 15
            ),
        )
        self.account = normalized_account
        self._state = state
        self._start_time_utc = _safe_dt(start_time)
        self._allow_prestart_emails = bool(allow_prestart_emails)
        self._first_run_bootstrap = bool(first_run_bootstrap)
        self._bootstrap_hours = max(0, int(first_run_bootstrap_hours))
        self._bootstrap_max_messages = max(0, int(first_run_bootstrap_max_messages))
        self._last_fetch_included_prestart = False
        self.connect_timeout = float(connect_timeout)
        self.io_timeout = float(io_timeout)
        self.retries = int(retries)
        self.backoff_base = float(backoff_base)

        self._client = None

    def _connect(self) -> Any:
        cls = _imap_client_cls or IMAPClient or self._imap_client_cls
        if callable(cls) and cls is not IMAPClient:
            try:
                produced = cls()
                if produced is not None:
                    cls = produced
            except TypeError:
                pass
        if cls is None:
            raise IMAPError("IMAPClient недоступен.")

        # socket timeout влияет на connect/recv
        socket.setdefaulttimeout(self.io_timeout)

        try:
            try:
                client = cls(
                    self.account.host,
                    port=self.account.port,
                    ssl=self.account.ssl,
                    timeout=self.connect_timeout,
                )
            except TypeError:
                try:
                    client = cls(self.account.host, self.account.port, self.account.ssl)
                except TypeError:
                    client = cls(
                        self.account.host,
                        port=self.account.port,
                        ssl=self.account.ssl,
                    )
            client.login(self.account.user, self.account.password)
            client.select_folder(self.account.mailbox)
            return client
        except Exception as e:
            raise IMAPError(f"Не удалось подключиться к IMAP: {e}") from e

    def _ensure(self) -> Any:
        if self._client is None:
            self._client = self._connect()
        return self._client

    def close(self) -> None:
        try:
            if self._client is not None:
                try:
                    self._client.logout()
                except Exception:
                    pass
        finally:
            self._client = None

    @property
    def last_fetch_included_prestart(self) -> bool:
        return self._last_fetch_included_prestart

    def _resolve_search_scope(
        self, start_time_utc: datetime
    ) -> tuple[list[str], Optional[datetime], bool]:
        if self._allow_prestart_emails:
            return [], None, False
        if (
            self._first_run_bootstrap
            and self._bootstrap_hours > 0
            and self._bootstrap_max_messages > 0
        ):
            cutoff = start_time_utc - timedelta(hours=self._bootstrap_hours)
            return ["SINCE", cutoff.strftime("%d-%b-%Y")], cutoff, True
        return ["SINCE", start_time_utc.strftime("%d-%b-%Y")], start_time_utc, False

    def _with_retries(self, fn, *args, **kwargs):
        last_exc: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                client = self._ensure()
                return fn(client, *args, **kwargs)
            except (socket.timeout, ssl.SSLError, OSError) as e:
                last_exc = e
                log.warning(
                    "IMAP сеть/SSL ошибка, переподключение (%s/%s): %s",
                    attempt + 1,
                    self.retries + 1,
                    e,
                )
                self.close()
            except Exception as e:
                # любые прочие ошибки тоже попробуем один раз переподключением (часто IMAP рвёт сессию)
                last_exc = e
                log.warning(
                    "IMAP ошибка, переподключение (%s/%s): %s",
                    attempt + 1,
                    self.retries + 1,
                    e,
                )
                self.close()

            if attempt < self.retries:
                time.sleep(self.backoff_base * (2**attempt))

        raise IMAPError(
            f"IMAP операция не удалась после ретраев: {last_exc}"
        ) from last_exc

    # ---------- API ----------

    def search_uids_since(self, since_utc: datetime) -> List[int]:
        """
        Возвращает UID писем, у которых INTERNALDATE >= since (по дням),
        а точную фильтрацию по времени делаем уже по заголовку Date при парсинге.
        """
        since_utc = _safe_dt(since_utc) or _utcnow()
        # IMAP SINCE работает по дате (день), формат: DD-Mon-YYYY
        since_str = since_utc.strftime("%d-%b-%Y")

        def _search(client):
            return client.search(["SINCE", since_str])

        uids = self._with_retries(_search)
        # imapclient может вернуть list[int] или tuple
        return [int(x) for x in (uids or [])]

    def fetch_headers_and_size(
        self, uids: Sequence[int]
    ) -> Dict[int, Dict[bytes, Any]]:
        """
        Забираем заголовки (RFC822.HEADER) + размер (RFC822.SIZE) без тела.
        """
        if not uids:
            return {}

        def _fetch(client):
            return client.fetch(
                list(uids), ["RFC822.HEADER", "RFC822.SIZE", "INTERNALDATE"]
            )

        data = self._with_retries(_fetch) or {}
        # imapclient ключи uids -> dict
        out: Dict[int, Dict[bytes, Any]] = {}
        for k, v in data.items():
            out[int(k)] = v
        return out

    def fetch_full_rfc822(self, uid: int) -> bytes:
        """
        Забираем полное письмо RFC822 (только если размер <= лимита).
        """
        max_bytes = int(self.account.max_email_mb) * 1024 * 1024

        # сначала узнаем размер
        meta = self.fetch_headers_and_size([uid]).get(int(uid), {})
        size = _parse_rfc822_size(meta)
        if size is not None and size > max_bytes:
            raise IMAPError(
                f"Письмо UID={uid} слишком большое: {size} bytes > limit {max_bytes} bytes"
            )

        def _fetch(client):
            data = client.fetch([int(uid)], [b"RFC822"])
            # ожидаем {uid: {b'RFC822': bytes}}
            d = data.get(int(uid)) or {}
            payload = d.get(b"RFC822") or d.get("RFC822")
            if not isinstance(payload, (bytes, bytearray)):
                raise IMAPError(f"Не удалось получить RFC822 для UID={uid}")
            return bytes(payload)

        return self._with_retries(_fetch)

    def iter_new_messages(
        self,
        *,
        start_time_utc: datetime,
        limit: int = 50,
        uid_batch: int = 200,
    ) -> Iterable[Tuple[int, Message]]:
        """
        Итератор новых писем:
        1) ищем UID по SINCE (по дням)
        2) тянем заголовки пачками, режем по max_email_mb
        3) тянем полные письма только для подходящих
        4) финальная фильтрация по заголовку Date >= start_time_utc (UTC)
        """
        start_time_utc = _safe_dt(start_time_utc) or _utcnow()
        uids = self.search_uids_since(start_time_utc)
        if not uids:
            return iter(())

        # UID лучше сортировать, чтобы было стабильно
        uids = sorted(set(int(x) for x in uids))

        emitted = 0
        max_bytes = int(self.account.max_email_mb) * 1024 * 1024

        for i in range(0, len(uids), uid_batch):
            if emitted >= limit:
                break

            chunk = uids[i : i + uid_batch]
            headers_map = self.fetch_headers_and_size(chunk)

            # сначала отфильтруем крупные, затем по Date
            candidates: List[int] = []
            for uid in chunk:
                meta = headers_map.get(int(uid), {})
                size = _parse_rfc822_size(meta)
                if size is not None and size > max_bytes:
                    log.info("Skip oversize email UID=%s size=%s", uid, size)
                    continue
                candidates.append(int(uid))

            for uid in candidates:
                if emitted >= limit:
                    break

                try:
                    raw = self.fetch_full_rfc822(uid)
                except IMAPError as e:
                    log.warning("Skip email UID=%s due to error: %s", uid, e)
                    continue

                try:
                    msg = email.message_from_bytes(raw)
                except Exception as e:
                    log.warning("Skip broken MIME UID=%s: %s", uid, e)
                    continue

                msg_dt = _get_msg_date_utc(msg)
                if msg_dt is None:
                    # если Date нет — считаем старым, чтобы не спамить
                    continue
                if msg_dt < start_time_utc:
                    continue

                emitted += 1
                yield uid, msg

    def fetch_new_messages(self, *, limit: int = 50) -> List[Tuple[int, bytes]]:
        self._last_fetch_included_prestart = False
        start_time_utc = self._start_time_utc or _utcnow()
        state_login = normalize_login(self.account.user)
        range_start = 1
        if self._state is not None and state_login:
            range_start = max(1, self._state.get_last_uid(state_login) + 1)

        def _search(client, criteria):
            return client.search(criteria)

        base_criteria = ["UID", f"{range_start}:*"]
        all_uids = sorted(
            int(uid) for uid in (self._with_retries(_search, base_criteria) or [])
        )

        search_scope, internaldate_cutoff, bootstrap_active = (
            self._resolve_search_scope(start_time_utc)
        )
        scoped_criteria = [*base_criteria, *search_scope]
        scoped_uids = sorted(
            int(uid) for uid in (self._with_retries(_search, scoped_criteria) or [])
        )
        highest_seen = max(all_uids) if all_uids else None

        eligible: list[tuple[int, dict[bytes, Any], Optional[datetime]]] = []
        for uid in scoped_uids:
            try:
                meta = self.fetch_headers_and_size([uid]).get(uid, {})
            except Exception:
                continue
            internal_date = _safe_dt(
                meta.get(b"INTERNALDATE") or meta.get("INTERNALDATE")
            )
            if (
                internaldate_cutoff
                and internal_date
                and internal_date < internaldate_cutoff
            ):
                continue
            eligible.append((uid, meta, internal_date))

        fetch_cap = max(0, int(limit))
        if bootstrap_active:
            fetch_cap = min(fetch_cap, self._bootstrap_max_messages)
            if len(eligible) > fetch_cap:
                eligible = eligible[-fetch_cap:]

        messages: List[Tuple[int, bytes]] = []
        max_bytes = int(self.account.max_email_mb) * 1024 * 1024
        for uid, meta, internal_date in eligible:
            if len(messages) >= fetch_cap:
                break
            if internal_date and internal_date < start_time_utc:
                self._last_fetch_included_prestart = True
            size = _parse_rfc822_size(meta)
            if size is not None and size > max_bytes:
                header_map = (
                    self._with_retries(
                        lambda client: client.fetch([uid], ["BODY.PEEK[HEADER]"])
                    )
                    or {}
                )
                header_bytes = (header_map.get(uid) or {}).get(b"BODY[HEADER]", b"")
                header_msg = email.message_from_bytes(header_bytes or b"")
                synthetic = EmailMessage()
                synthetic["From"] = header_msg.get("From", "")
                synthetic["Subject"] = header_msg.get("Subject", "")
                synthetic.set_content(
                    "Письмо слишком большое и не было загружено полностью."
                )
                messages.append((uid, synthetic.as_bytes()))
                continue
            try:
                raw = self.fetch_full_rfc822(uid)
            except Exception:
                continue
            messages.append((uid, raw))

        if self._state is not None and state_login:
            if highest_seen is not None:
                self._state.update_last_uid(state_login, highest_seen)
            self._state.update_check_time(state_login)
        return messages


# ---- удобные helper-ы, если где-то в проекте нужно ----


def extract_basic_headers(msg: Message) -> Tuple[str, str, str]:
    subject = _decode_header_value(msg.get("Subject", "") or "")
    from_ = _decode_header_value(msg.get("From", "") or "")
    to = _decode_header_value(msg.get("To", "") or "")
    return subject, from_, to
