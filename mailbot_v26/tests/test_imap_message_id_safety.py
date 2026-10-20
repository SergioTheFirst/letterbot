"""P2SEC: a Message-ID comes from the email (and the server answering the
header FETCH), so it is bounded and never turned into IMAP protocol.

fetch_message_by_uid() SEARCHes a stored Message-ID after a UIDVALIDITY
change. imapclient sends a criteria string that needs no quoting as a bare
atom, so the allowlist (_SEARCHABLE_MESSAGE_ID) may only pass characters that
are valid in an IMAP astring atom: a CR/LF would end the command line and a
trailing {N} would announce a literal. A Message-ID longer than an RFC 5322
line (998 characters) is treated as absent.
"""

from __future__ import annotations

import logging

import pytest
from imapclient import IMAPClient
from imapclient import imapclient as imapclient_impl

from mailbot_v26 import imap_client
from mailbot_v26.imap_client import _SEARCHABLE_MESSAGE_ID

# RFC 5322 line limit: a longer Message-ID counts as absent.
MAX_MESSAGE_ID_LENGTH = 998
_HEADER_FIELDS_KEY = b"BODY[HEADER.FIELDS (MESSAGE-ID)]"

# RFC 3501: ASTRING-CHAR = ATOM-CHAR / "]", and ATOM-CHAR is any CHAR except
# atom-specials: "(" ")" "{" SP CTL "%" "*" DQUOTE "\" "]".
_ATOM_SPECIALS = frozenset('(){ %*"\\]')
_ASTRING_CHARS = frozenset(
    chr(code) for code in range(0x21, 0x7F) if chr(code) not in _ATOM_SPECIALS
) | {"]"}


def _id_of_length(length: int) -> str:
    return "<" + "a" * (length - len("<@x>")) + "@x>"


SEARCHABLE_IDS = [
    "<m1@example.com>",
    "<CAJ+x=Y_z-1@mail.gmail.com>",
    "<20261020.1234.ABC@[127.0.0.1]>",
    "<a!#$&'+/=?^_`|~.b@example.com>",
    _id_of_length(MAX_MESSAGE_ID_LENGTH),
]

REJECTED_IDS = {
    "cr": "<a\rb@example.com>",
    "lf": "<a\nb@example.com>",
    "crlf_command": "<a@x>\r\nA002 DELETE INBOX",
    "quote": '<a"b@example.com>',
    "backslash": "<a\\b@example.com>",
    "literal": "<a{5}@example.com>",
    "trailing_literal": "x{5}",
    "close_brace": "<a}b@example.com>",
    "parens": "<a(b)@example.com>",
    "space": "<a b@example.com>",
    "percent_wildcard": "<a%b@example.com>",
    "star_wildcard": "<a*b@example.com>",
    "nul": "<a\x00b@example.com>",
    "del": "<a\x7fb@example.com>",
    "non_ascii": "<почта@example.com>",
    "empty": "",
    "too_long": _id_of_length(MAX_MESSAGE_ID_LENGTH + 1),
}


# ---- allowlist ----


@pytest.mark.parametrize("message_id", SEARCHABLE_IDS)
def test_allowlist_accepts_plain_message_ids(message_id: str) -> None:
    assert _SEARCHABLE_MESSAGE_ID.fullmatch(message_id)


@pytest.mark.parametrize(
    "message_id", list(REJECTED_IDS.values()), ids=list(REJECTED_IDS)
)
def test_allowlist_rejects_protocol_characters_and_over_long_ids(
    message_id: str,
) -> None:
    assert _SEARCHABLE_MESSAGE_ID.fullmatch(message_id) is None


def test_allowlist_passes_only_imap_astring_atom_characters() -> None:
    allowed = {
        chr(code) for code in range(0x100) if _SEARCHABLE_MESSAGE_ID.fullmatch(chr(code))
    }

    assert allowed
    assert allowed <= _ASTRING_CHARS, sorted(allowed - _ASTRING_CHARS)


# ---- how imapclient puts the criteria on the wire (imapclient==2.2.0) ----


def _criteria_bytes(message_id: str) -> list[bytes]:
    return imapclient_impl._normalise_search_criteria(
        ["HEADER", "Message-ID", message_id]
    )


@pytest.mark.parametrize("message_id", SEARCHABLE_IDS)
def test_imapclient_sends_an_allowlisted_message_id_as_a_bare_atom(
    message_id: str,
) -> None:
    criteria = _criteria_bytes(message_id)

    assert criteria == [b"HEADER", b"Message-ID", message_id.encode("ascii")]
    assert not isinstance(criteria[-1], imapclient_impl._quoted)
    assert not imapclient_impl._is8bit(criteria[-1])


def test_imapclient_does_not_protect_rejected_message_ids() -> None:
    # Why the allowlist exists: imapclient quotes only for '"', '\' and space;
    # CR/LF (even inside quotes), a trailing {N} and wildcards go out verbatim.
    assert _criteria_bytes("<a@x>\r\nA002")[-1] == b"<a@x>\r\nA002"
    assert _criteria_bytes("<a@x>\r\nA002 LOGOUT")[-1] == b'"<a@x>\r\nA002 LOGOUT"'
    assert _criteria_bytes("x{5}")[-1] == b"x{5}"
    assert _criteria_bytes("<a%b@x>")[-1] == b"<a%b@x>"
    assert _criteria_bytes('<a"b@x>')[-1] == b'"<a\\"b@x>"'
    assert _criteria_bytes("<a\\b@x>")[-1] == b'"<a\\\\b@x>"'
    with pytest.raises(UnicodeEncodeError):
        _criteria_bytes("<почта@x>")


class _WireRecorder:
    """Stands in for imaplib under a real IMAPClient and records the bytes."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def _new_tag(self) -> bytes:
        return b"A001"

    def send(self, data: bytes) -> None:
        self.sent.append(bytes(data))

    def _command_complete(self, _name: str, _tag: bytes) -> tuple[str, list[bytes]]:
        return "OK", [b"SEARCH completed"]

    def _untagged_response(
        self, _typ: str, _data: list[bytes], _name: str
    ) -> tuple[str, list[bytes]]:
        return "OK", [b"3"]


def _wire_client() -> tuple[IMAPClient, _WireRecorder]:
    recorder = _WireRecorder()
    client = IMAPClient.__new__(IMAPClient)  # no connection
    client._imap = recorder
    client.use_uid = True
    client._cached_capabilities = ()
    return client, recorder


@pytest.mark.parametrize("message_id", SEARCHABLE_IDS)
def test_search_for_an_allowlisted_message_id_is_one_well_formed_line(
    message_id: str,
) -> None:
    client, recorder = _wire_client()

    assert client.search(["HEADER", "Message-ID", message_id]) == [3]
    assert b"".join(recorder.sent) == (
        b"A001 UID SEARCH HEADER Message-ID " + message_id.encode("ascii") + b"\r\n"
    )


# ---- length cap ----


def test_message_id_cap_is_the_rfc_5322_line_limit() -> None:
    assert imap_client.MAX_MESSAGE_ID_LENGTH == MAX_MESSAGE_ID_LENGTH


def _header_meta(field: str) -> dict[bytes, bytes]:
    return {_HEADER_FIELDS_KEY: field.encode("ascii")}


def test_header_message_id_over_998_chars_is_treated_as_absent_and_logged(
    caplog,
) -> None:
    too_long = _id_of_length(MAX_MESSAGE_ID_LENGTH + 1)

    with caplog.at_level(logging.WARNING, logger=imap_client.__name__):
        assert imap_client._parse_message_id(
            _header_meta(f"Message-ID: {too_long}\r\n\r\n")
        ) is None

    [record] = [r for r in caplog.records if "message_id_too_long" in r.getMessage()]
    assert f"length={MAX_MESSAGE_ID_LENGTH + 1}" in record.getMessage()
    assert "aaaa" not in record.getMessage()  # the value itself is never logged


def test_folded_header_message_id_of_998_chars_is_kept() -> None:
    exact = _id_of_length(MAX_MESSAGE_ID_LENGTH)

    assert imap_client._parse_message_id(
        _header_meta(f"Message-ID:\r\n {exact}\r\n\r\n")
    ) == exact


def test_huge_message_id_literal_from_the_server_is_treated_as_absent() -> None:
    meta = {_HEADER_FIELDS_KEY: b"Message-ID: <" + b"x" * 100_000 + b"@x>\r\n\r\n"}

    assert imap_client._parse_message_id(meta) is None


def test_expected_message_id_over_998_chars_normalizes_to_absent() -> None:
    exact = _id_of_length(MAX_MESSAGE_ID_LENGTH)

    assert imap_client._normalize_message_id(
        _id_of_length(MAX_MESSAGE_ID_LENGTH + 1)
    ) is None
    assert imap_client._normalize_message_id(f" {exact}\r\n") == exact
    assert imap_client._normalize_message_id(None) is None
