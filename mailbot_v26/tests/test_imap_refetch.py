"""WP2b: refetch one stored message by UID after a restart.

The pipeline caches live in memory, so a restart loses the raw email of every
queued item. ResilientIMAP.fetch_message_by_uid() brings it back: by UID while
UIDVALIDITY is unchanged, otherwise by a SEARCH on its Message-ID (exactly one
hit), and always verifies the Message-ID before handing content back.
"""

from __future__ import annotations

import email
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import pytest

from mailbot_v26 import imap_client
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.imap_client import IMAPError, ResilientIMAP
from mailbot_v26.tests.test_imap_client import (
    _FakeIMAPClient,
    _body_fetches,
    _install_fake,
    _is_header_fetch,
)

START = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
LOGIN = "user@example.com"
MESSAGE_ID = "<m1@example.com>"
_HEADER_FIELDS_KEY = b"BODY[HEADER.FIELDS (MESSAGE-ID)]"


def raw_email(
    message_id: str | None = MESSAGE_ID,
    *,
    subject: str = "Invoice 42",
    body: str = "Please pay invoice 42.",
) -> bytes:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = "Sender <sender@example.com>"
    message["To"] = LOGIN
    message["Date"] = "Tue, 02 Jan 2024 12:05:00 +0000"
    if message_id is not None:
        message["Message-ID"] = message_id
    message.set_content(body)
    return message.as_bytes()


def server_item(
    raw: bytes,
    *,
    size: int | None = None,
    internaldate: datetime = START + timedelta(minutes=5),
) -> dict[bytes, object]:
    message_id = email.message_from_bytes(raw).get("Message-ID")
    header = f"Message-ID: {message_id}\r\n\r\n" if message_id else "\r\n"
    return {
        b"RFC822": raw,
        b"RFC822.SIZE": len(raw) if size is None else size,
        b"INTERNALDATE": internaldate,
        _HEADER_FIELDS_KEY: header.encode("ascii"),
    }


class RefetchIMAPClient(_FakeIMAPClient):
    """Fake server that answers SEARCH HEADER (case-insensitive substring, as
    RFC 3501 says), leaves UIDs it does not hold out of FETCH responses and
    can refuse logins (network down)."""

    def __init__(self) -> None:
        super().__init__(host="imap.example.com", port=993, ssl=True)
        self.login_error: Exception | None = None
        self.login_calls = 0

    def login(self, login: str, password: str) -> None:
        self.login_calls += 1
        if self.login_error is not None:
            raise self.login_error
        super().login(login, password)

    def search(self, criteria):  # type: ignore[override]
        if criteria and criteria[0] == "HEADER":
            self.search_calls.append(list(criteria))
            field, needle = str(criteria[1]), str(criteria[2]).lower()
            hits = []
            for uid in self._uids:
                value = email.message_from_bytes(self._data[uid][b"RFC822"]).get(field)
                if value is not None and needle in str(value).lower():
                    hits.append(uid)
            return hits
        return super().search(criteria)

    def fetch(self, uids, fields):  # type: ignore[override]
        self.fetch_calls.append((list(uids), list(fields)))
        if self.header_fetch_error is not None and _is_header_fetch(fields):
            raise self.header_fetch_error
        result: dict[int, dict[bytes, object]] = {}
        for uid in uids:
            item = self._data.get(uid)
            if item is None:
                continue  # a real server answers only for UIDs it holds
            if "BODY.PEEK[HEADER]" in fields:
                raw = bytes(item[b"RFC822"])  # type: ignore[arg-type]
                result[uid] = {b"BODY[HEADER]": raw.split(b"\n\n", 1)[0] + b"\n\n"}
            else:
                result[uid] = self._present(item)
        return result


def header_searches(fake: RefetchIMAPClient) -> list[list[object]]:
    return [call for call in fake.search_calls if call and call[0] == "HEADER"]


def fetched_uids(fake: RefetchIMAPClient) -> set[int]:
    return {uid for uids, _fields in fake.fetch_calls for uid in uids}


@pytest.fixture()
def account() -> AccountConfig:
    return AccountConfig(
        account_id="test",
        login=LOGIN,
        password="secret",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )


@pytest.fixture()
def fake(monkeypatch) -> RefetchIMAPClient:
    server = RefetchIMAPClient()
    server.uidvalidity = 100
    _install_fake(monkeypatch, server)
    monkeypatch.setattr(imap_client.time, "sleep", lambda *_a, **_k: None)
    return server


def test_same_epoch_refetches_by_uid_and_verifies_message_id(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    raw = raw_email()
    fake.set_messages({7: server_item(raw)})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=f"\r\n {MESSAGE_ID}\t"
    )

    assert result.found
    assert result.raw == raw
    assert result.uid == 7
    assert result.reason == "uid"
    assert header_searches(fake) == []
    # EXAMINE (read-only): refetching must not mark the mail as read
    assert fake.select_calls == [("INBOX", True)]
    assert fake.logout_calls == 1


def test_changed_uidvalidity_finds_message_by_message_id(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    raw = raw_email()
    fake.uidvalidity = 200
    fake.set_messages({3: server_item(raw), 7: server_item(raw_email("<other@x>"))})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=MESSAGE_ID
    )

    assert result.found
    assert result.raw == raw
    assert result.uid == 3
    assert result.reason == "message_id"
    assert header_searches(fake) == [["HEADER", "Message-ID", MESSAGE_ID]]
    # the old UID means nothing in the new epoch: it is never fetched
    assert 7 not in fetched_uids(fake)
    assert fake.logout_calls == 1


def test_missing_message_is_reported_gone(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    fake.set_messages({8: server_item(raw_email("<unrelated@example.com>"))})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=MESSAGE_ID
    )

    assert not result.found
    assert result.raw is None
    assert result.reason == "gone"
    assert fake.logout_calls == 1


def test_uid_holding_another_message_is_a_mismatch(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    fake.set_messages({7: server_item(raw_email("<someone-else@example.com>"))})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=MESSAGE_ID
    )

    assert not result.found
    assert result.reason == "message_id_mismatch"
    assert _body_fetches(fake) == []


def test_legacy_row_without_uidvalidity_falls_back_to_message_id(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    raw = raw_email()
    fake.set_messages({4: server_item(raw), 7: server_item(raw_email("<old@x>"))})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=None, expected_message_id=MESSAGE_ID
    )

    assert result.found
    assert result.uid == 4
    assert result.raw == raw


def test_changed_uidvalidity_without_message_id_is_not_trusted(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    fake.uidvalidity = 200
    fake.set_messages({7: server_item(raw_email(None))})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=None
    )

    assert not result.found
    assert result.reason == "uidvalidity_changed"
    assert fetched_uids(fake) == set()


def test_message_id_matching_several_messages_is_ambiguous(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    fake.uidvalidity = 200
    fake.set_messages({2: server_item(raw_email()), 5: server_item(raw_email())})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=MESSAGE_ID
    )

    assert not result.found
    assert result.reason == "ambiguous_message_id"
    assert _body_fetches(fake) == []


@pytest.mark.parametrize(
    "message_id",
    ['<a"b@example.com>', "<a{5}@example.com>", "<почта@example.com>"],
    ids=["quote", "literal", "non-ascii"],
)
def test_message_id_unsafe_for_search_is_never_sent_to_the_server(
    fake: RefetchIMAPClient, account: AccountConfig, message_id: str
) -> None:
    fake.uidvalidity = 200
    fake.set_messages({3: server_item(raw_email())})

    result = ResilientIMAP(account, state=None).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=message_id
    )

    assert not result.found
    assert result.reason == "message_id_unsearchable"
    assert header_searches(fake) == []


def test_oversize_message_is_replaced_by_the_stand_in(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    fake.set_messages({7: server_item(raw_email(), size=3 * 1024 * 1024)})

    result = ResilientIMAP(account, state=None, max_email_mb=1).fetch_message_by_uid(
        7, expected_uidvalidity=100, expected_message_id=MESSAGE_ID
    )

    assert result.found
    stand_in = email.message_from_bytes(result.raw or b"")
    assert stand_in["Subject"] == "Invoice 42"
    assert "Please pay" not in (result.raw or b"").decode("utf-8", "replace")
    assert _body_fetches(fake) == []


def test_network_failure_raises_imap_error(
    fake: RefetchIMAPClient, account: AccountConfig
) -> None:
    fake.set_messages({7: server_item(raw_email())})
    fake.login_error = OSError("connection refused")

    with pytest.raises(IMAPError):
        ResilientIMAP(account, state=None, retries=1).fetch_message_by_uid(
            7, expected_uidvalidity=100, expected_message_id=MESSAGE_ID
        )
    assert fake.login_calls == 2
