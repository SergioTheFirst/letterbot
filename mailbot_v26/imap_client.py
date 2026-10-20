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
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

from mailbot_v26.account_identity import normalize_login
from mailbot_v26.state_manager import StateManager

try:
    from imapclient import IMAPClient  # type: ignore
except Exception:  # pragma: no cover
    IMAPClient = None  # type: ignore

try:
    from imapclient import SocketTimeout  # type: ignore
except Exception:  # pragma: no cover
    SocketTimeout = None  # type: ignore


log = logging.getLogger(__name__)

# UIDs per header FETCH: one round trip per chunk instead of one per message.
HEADER_FETCH_CHUNK = 200
_HEADER_FETCH_FIELDS = [
    "RFC822.SIZE",
    "INTERNALDATE",
    "BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)]",
]
_HEADER_FIELDS_RESPONSE_PREFIX = b"BODY[HEADER.FIELDS"

# Polls in which a UID failed to be fetched or stored before it is
# dead-lettered and the cursor may pass it (poison message).
MAX_UID_FETCH_ATTEMPTS = 3
# Body-fetch failures in a row that end fetching for the poll; the rest is
# carried, so a flapping server does not burn an attempt on every UID.
MAX_CONSECUTIVE_BODY_FAILURES = 2


class IMAPError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchPlan:
    """What one fetch_new_messages() call saw; commit_cursor() resolves it."""

    login: str
    epoch_changed: bool
    uidvalidity: Optional[int]
    base_cursor: int
    uids: Tuple[int, ...]
    policy_skipped: FrozenSet[int]
    known: FrozenSet[int]
    dead: FrozenSet[int]
    returned: FrozenSet[int]
    fetch_failed: FrozenSet[int]
    carried: Tuple[int, ...]


@dataclass(frozen=True)
class CursorCommit:
    cursor: int
    dead_uids: Tuple[int, ...]
    failed_uids: Tuple[int, ...]
    carried: Tuple[int, ...]


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


def _parse_message_id(fetch_data: Dict[Any, Any]) -> Optional[str]:
    # Response key is b"BODY[HEADER.FIELDS (MESSAGE-ID)]"; some servers quote
    # the field name, so match on the prefix.
    raw: Any = None
    for key, value in fetch_data.items():
        key_bytes = key.encode("ascii", "replace") if isinstance(key, str) else key
        if isinstance(key_bytes, bytes) and key_bytes.upper().startswith(
            _HEADER_FIELDS_RESPONSE_PREFIX
        ):
            raw = value
            break
    if isinstance(raw, str):
        raw = raw.encode("utf-8", "replace")
    if not isinstance(raw, (bytes, bytearray)):
        return None
    value = email.message_from_bytes(bytes(raw)).get("Message-ID")
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _since_criteria(cutoff_utc: datetime) -> list[Any]:
    # RFC 3501 SINCE compares the date of INTERNALDATE in the server's own
    # timezone, so a server west of UTC files mail just after cutoff under the
    # previous day. Search one day earlier; the exact cutoff is applied to the
    # timezone-aware INTERNALDATE afterwards (A9).
    return ["SINCE", (cutoff_utc - timedelta(days=1)).date()]


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
        self._last_uidvalidity_changed = False
        self._last_resync_reason = "normal_poll"
        self._last_bootstrap_active = False
        self._last_message_ids: Dict[int, Optional[str]] = {}
        self._current_uidvalidity: Optional[int] = None
        self._plan: Optional[FetchPlan] = None
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

        # Per-client connect/read timeouts; never the process-wide socket default.
        timeout: Any = self.connect_timeout
        if SocketTimeout is not None:
            timeout = SocketTimeout(connect=self.connect_timeout, read=self.io_timeout)

        try:
            try:
                client = cls(
                    self.account.host,
                    port=self.account.port,
                    ssl=self.account.ssl,
                    timeout=timeout,
                )
            except TypeError:
                log.warning(
                    "imap_client_timeout_unsupported host=%s: connecting without a socket timeout",
                    self.account.host,
                )
                try:
                    client = cls(self.account.host, self.account.port, self.account.ssl)
                except TypeError:
                    client = cls(
                        self.account.host,
                        port=self.account.port,
                        ssl=self.account.ssl,
                    )
            try:
                # Aware INTERNALDATE: naive local times were later read as UTC (A9).
                client.normalise_times = False
            except Exception as exc:
                log.warning(
                    "imap_normalise_times_unsupported host=%s error=%s",
                    self.account.host,
                    type(exc).__name__,
                )
            client.login(self.account.user, self.account.password)
            # readonly (EXAMINE): fetching RFC822 must not set \Seen in the user's mailbox
            client.select_folder(self.account.mailbox, readonly=True)
            return client
        except Exception as e:
            raise IMAPError(f"Не удалось подключиться к IMAP: {e}") from e

    def _ensure(self) -> Any:
        if self._client is None:
            self._client = self._connect()
        return self._client

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.logout()
        except Exception as exc:
            # imaplib closes the socket even when LOGOUT fails; report, don't raise.
            log.warning(
                "imap_logout_failed host=%s error=%s: %s",
                self.account.host,
                type(exc).__name__,
                exc,
            )

    @property
    def last_fetch_included_prestart(self) -> bool:
        return self._last_fetch_included_prestart

    @property
    def last_uidvalidity_changed(self) -> bool:
        return self._last_uidvalidity_changed

    @property
    def last_resync_reason(self) -> str:
        return self._last_resync_reason

    @property
    def last_bootstrap_active(self) -> bool:
        return self._last_bootstrap_active

    @property
    def last_message_ids(self) -> Dict[int, Optional[str]]:
        """UID -> Message-ID (None when absent) for every UID whose headers the
        last fetch_new_messages() call fetched."""
        return dict(self._last_message_ids)

    @property
    def current_uidvalidity(self) -> Optional[int]:
        """UIDVALIDITY the last fetch_new_messages() call saw (None: unknown);
        the epoch of the UIDs it returned."""
        return self._current_uidvalidity

    def _fetch_uidvalidity(self) -> Optional[int]:
        def _status(client) -> Optional[int]:
            folder_status = None
            if not hasattr(client, "folder_status"):
                return None
            try:
                folder_status = client.folder_status(self.account.mailbox, ["UIDVALIDITY"])
            except TypeError:
                folder_status = client.folder_status(
                    self.account.mailbox, [b"UIDVALIDITY"]
                )
            except Exception:
                return None
            if not isinstance(folder_status, dict):
                return None
            raw = folder_status.get("UIDVALIDITY")
            if raw is None:
                raw = folder_status.get(b"UIDVALIDITY")
            if raw is None:
                return None
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        try:
            return self._with_retries(_status)
        except Exception:
            return None

    def _resolve_search_scope(
        self, start_time_utc: datetime
    ) -> tuple[list[Any], Optional[datetime], bool]:
        if self._allow_prestart_emails:
            return [], None, False
        if (
            self._first_run_bootstrap
            and self._bootstrap_hours > 0
            and self._bootstrap_max_messages > 0
        ):
            cutoff = start_time_utc - timedelta(hours=self._bootstrap_hours)
            return _since_criteria(cutoff), cutoff, True
        return _since_criteria(start_time_utc), start_time_utc, False

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
        Одним FETCH на все uids: размер (RFC822.SIZE), INTERNALDATE и
        заголовок Message-ID, без тела. Ошибка после ретраев -> IMAPError.
        """
        if not uids:
            return {}

        def _fetch(client):
            return client.fetch(list(uids), list(_HEADER_FETCH_FIELDS))

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
        return self._fetch_rfc822_body(uid)

    def _fetch_rfc822_body(self, uid: int) -> bytes:
        """RFC822 body of a UID whose size is already known to be within limit."""

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

    def fetch_new_messages(
        self,
        *,
        limit: int = 50,
        is_known_uid: Optional[Callable[[int], bool]] = None,
    ) -> List[Tuple[int, bytes]]:
        """New messages above the cursor; writes no state.

        The call records a plan; commit_cursor() moves the cursor once the
        returned messages are stored. is_known_uid(uid) -> True skips a UID
        that is already stored (not consulted after a UIDVALIDITY change).
        """
        self._plan = None
        # One IMAP session per poll cycle: always LOGOUT, even when the fetch fails.
        try:
            return self._fetch_new_messages(limit=limit, is_known_uid=is_known_uid)
        finally:
            self.close()

    def _fetch_new_messages(
        self, *, limit: int, is_known_uid: Optional[Callable[[int], bool]]
    ) -> List[Tuple[int, bytes]]:
        self._last_fetch_included_prestart = False
        self._last_uidvalidity_changed = False
        self._last_resync_reason = "normal_poll"
        self._last_bootstrap_active = False
        self._last_message_ids = {}
        start_time_utc = self._start_time_utc or _utcnow()
        state_login = normalize_login(self.account.user)
        base_cursor = 0
        attempts: Dict[int, int] = {}
        previous_uidvalidity: Optional[int] = None
        if self._state is not None and state_login:
            get_uidvalidity = getattr(self._state, "get_uidvalidity", None)
            if callable(get_uidvalidity):
                try:
                    previous_uidvalidity = get_uidvalidity(state_login)
                except Exception:
                    previous_uidvalidity = None
        current_uidvalidity = self._fetch_uidvalidity()
        self._current_uidvalidity = current_uidvalidity
        uidvalidity_changed = (
            previous_uidvalidity is not None
            and current_uidvalidity is not None
            and previous_uidvalidity != current_uidvalidity
        )
        if self._state is not None and state_login:
            if uidvalidity_changed:
                # New UID epoch: restart from UID 1; the old cursor and attempt
                # counts are replaced by commit_cursor().
                log.warning(
                    "imap_uidvalidity_changed account=%s mailbox=%s previous=%s current=%s",
                    state_login,
                    self.account.mailbox,
                    previous_uidvalidity,
                    current_uidvalidity,
                )
            else:
                base_cursor = max(0, int(self._state.get_last_uid(state_login)))
                attempts = self._state.get_uid_attempts(state_login)
        range_start = base_cursor + 1

        def _search(client, criteria):
            return client.search(criteria)

        base_criteria = ["UID", f"{range_start}:*"]
        # "UID n:*" also matches the highest UID when n is above it (RFC 3501).
        all_uids = sorted(
            uid
            for uid in (
                int(raw) for raw in (self._with_retries(_search, base_criteria) or [])
            )
            if uid >= range_start
        )

        search_scope, internaldate_cutoff, bootstrap_active = (
            self._resolve_search_scope(start_time_utc)
        )
        if (
            uidvalidity_changed
            and self._bootstrap_hours > 0
            and self._bootstrap_max_messages > 0
        ):
            internaldate_cutoff = start_time_utc - timedelta(hours=self._bootstrap_hours)
            search_scope = _since_criteria(internaldate_cutoff)
            bootstrap_active = True
        scoped_criteria = [*base_criteria, *search_scope]
        scoped_uids = sorted(
            int(uid) for uid in (self._with_retries(_search, scoped_criteria) or [])
        )
        scoped_uids = [uid for uid in scoped_uids if uid >= range_start]
        uids = sorted(set(all_uids) | set(scoped_uids))
        scoped_set = set(scoped_uids)
        policy_skipped = {uid for uid in uids if uid not in scoped_set}
        self._last_uidvalidity_changed = uidvalidity_changed
        self._last_bootstrap_active = bootstrap_active
        if uidvalidity_changed:
            self._last_resync_reason = "uidvalidity_change"
        elif bootstrap_active:
            self._last_resync_reason = "first_run"
        else:
            self._last_resync_reason = "normal_poll"

        # meta None: the server answered the header batch without this UID.
        eligible: list[tuple[int, Optional[dict[bytes, Any]], Optional[datetime]]] = []
        message_ids: Dict[int, Optional[str]] = {}
        # Batched header FETCH; a chunk that still fails after retries raises
        # IMAPError and leaves no plan, so the cursor does not move.
        for chunk_start in range(0, len(scoped_uids), HEADER_FETCH_CHUNK):
            chunk = scoped_uids[chunk_start : chunk_start + HEADER_FETCH_CHUNK]
            headers_map = self.fetch_headers_and_size(chunk)
            for uid in chunk:
                meta = headers_map.get(uid)
                internal_date: Optional[datetime] = None
                if meta is not None:
                    message_ids[uid] = _parse_message_id(meta)
                    internal_date = _safe_dt(
                        meta.get(b"INTERNALDATE") or meta.get("INTERNALDATE")
                    )
                if (
                    internaldate_cutoff
                    and internal_date
                    and internal_date < internaldate_cutoff
                ):
                    policy_skipped.add(uid)
                    continue
                eligible.append((uid, meta, internal_date))
        self._last_message_ids = message_ids
        log.debug(
            "imap_header_fetch mailbox=%s uids=%d batches=%d eligible=%d",
            self.account.mailbox,
            len(scoped_uids),
            (len(scoped_uids) + HEADER_FETCH_CHUNK - 1) // HEADER_FETCH_CHUNK,
            len(eligible),
        )

        fetch_cap = max(0, int(limit))
        if bootstrap_active:
            fetch_cap = min(fetch_cap, self._bootstrap_max_messages)
            if len(eligible) > fetch_cap:
                cut = len(eligible) - fetch_cap
                policy_skipped.update(uid for uid, _, _ in eligible[:cut])
                eligible = eligible[cut:]

        messages: List[Tuple[int, bytes]] = []
        known: set[int] = set()
        dead: set[int] = set()
        fetch_failed: set[int] = set()
        carried: List[int] = []
        attempted = 0
        failures_in_a_row = 0
        # Old-epoch rows reuse UIDs of the new epoch: no known-UID filter then.
        probe_known = None if uidvalidity_changed else is_known_uid
        max_bytes = int(self.account.max_email_mb) * 1024 * 1024
        for uid, meta, internal_date in eligible:
            if attempts.get(uid, 0) >= MAX_UID_FETCH_ATTEMPTS:
                dead.add(uid)  # dead-lettered by an earlier commit
                continue
            if probe_known is not None:
                try:
                    is_known = bool(probe_known(uid))
                except Exception as exc:
                    log.warning(
                        "imap_known_uid_probe_failed mailbox=%s uid=%s error=%s: "
                        "fetching without the known-UID filter",
                        self.account.mailbox,
                        uid,
                        type(exc).__name__,
                    )
                    probe_known = None
                    is_known = False
                if is_known:
                    known.add(uid)
                    continue
            if (
                attempted >= fetch_cap
                or failures_in_a_row >= MAX_CONSECUTIVE_BODY_FAILURES
            ):
                carried.append(uid)
                continue
            attempted += 1
            if meta is None:
                fetch_failed.add(uid)
                log.warning(
                    "imap_header_missing mailbox=%s uid=%s: retried next poll",
                    self.account.mailbox,
                    uid,
                )
                continue
            if internal_date and internal_date < start_time_utc:
                self._last_fetch_included_prestart = True
            try:
                size = _parse_rfc822_size(meta)
                if size is not None and size > max_bytes:
                    raw = self._oversize_stand_in(uid)
                else:
                    raw = self._fetch_rfc822_body(uid)
            except Exception as exc:
                failures_in_a_row += 1
                fetch_failed.add(uid)
                log.warning(
                    "imap_body_fetch_failed mailbox=%s uid=%s error=%s",
                    self.account.mailbox,
                    uid,
                    type(exc).__name__,
                )
                continue
            failures_in_a_row = 0
            messages.append((uid, raw))

        self._plan = FetchPlan(
            login=state_login,
            epoch_changed=uidvalidity_changed,
            uidvalidity=current_uidvalidity,
            base_cursor=base_cursor,
            uids=tuple(uids),
            policy_skipped=frozenset(policy_skipped),
            known=frozenset(known),
            dead=frozenset(dead),
            returned=frozenset(uid for uid, _ in messages),
            fetch_failed=frozenset(fetch_failed),
            carried=tuple(carried),
        )
        return messages

    def _oversize_stand_in(self, uid: int) -> bytes:
        """A small message with the real From/Subject for mail over max_email_mb."""
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
        synthetic.set_content("Письмо слишком большое и не было загружено полностью.")
        return synthetic.as_bytes()

    def commit_cursor(self, persisted_uids: Iterable[int]) -> Optional[CursorCommit]:
        """Move the cursor past what the last fetch_new_messages() resolved.

        Call once per successful fetch, after the returned messages were
        stored; persisted_uids are the returned UIDs now in storage (new or
        duplicate). A returned UID that was not stored, or failed to fetch,
        gets an attempt; at MAX_UID_FETCH_ATTEMPTS it is dead-lettered and
        resolved. The cursor stops before the first unresolved UID (failed or
        carried), never moves back within a UIDVALIDITY epoch, and the stored
        UIDVALIDITY is replaced only by a known value. Returns None when there
        is no fetch to commit.
        """
        plan, self._plan = self._plan, None
        state = self._state
        if plan is None or state is None or not plan.login:
            return None
        persisted = plan.returned & {int(uid) for uid in persisted_uids}
        failed = sorted(plan.fetch_failed | (plan.returned - persisted))
        attempts = {} if plan.epoch_changed else state.get_uid_attempts(plan.login)
        newly_dead: List[int] = []
        for uid in failed:
            attempts[uid] = attempts.get(uid, 0) + 1
            if attempts[uid] >= MAX_UID_FETCH_ATTEMPTS:
                newly_dead.append(uid)
        resolved = (
            plan.policy_skipped | plan.known | plan.dead | persisted | set(newly_dead)
        )
        cursor = plan.base_cursor
        for uid in plan.uids:
            if uid not in resolved:
                break
            cursor = uid
        if not plan.epoch_changed:
            cursor = max(cursor, state.get_last_uid(plan.login))

        state.update_last_uid(plan.login, cursor)
        if plan.uidvalidity is not None:
            state.update_uidvalidity(
                plan.login, plan.uidvalidity, mailbox=self.account.mailbox
            )
        state.update_check_time(plan.login)
        state.set_uid_attempts(
            plan.login, {uid: n for uid, n in attempts.items() if uid > cursor}
        )
        log.info(
            "imap_fetch_plan account=%s mailbox=%s epoch_changed=%s uidvalidity=%s "
            "base_cursor=%s cursor=%s uids=%d policy_skipped=%d known=%d "
            "returned=%d persisted=%d failed=%d dead=%d carried=%d",
            plan.login,
            self.account.mailbox,
            plan.epoch_changed,
            plan.uidvalidity,
            plan.base_cursor,
            cursor,
            len(plan.uids),
            len(plan.policy_skipped),
            len(plan.known),
            len(plan.returned),
            len(persisted),
            len(failed),
            len(newly_dead),
            len(plan.carried),
        )
        return CursorCommit(
            cursor=cursor,
            dead_uids=tuple(newly_dead),
            failed_uids=tuple(failed),
            carried=plan.carried,
        )


# ---- удобные helper-ы, если где-то в проекте нужно ----


def extract_basic_headers(msg: Message) -> Tuple[str, str, str]:
    subject = _decode_header_value(msg.get("Subject", "") or "")
    from_ = _decode_header_value(msg.get("From", "") or "")
    to = _decode_header_value(msg.get("To", "") or "")
    return subject, from_, to
