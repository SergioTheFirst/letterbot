"""P2SEC: a bot token is masked in every form exception text puts it in.

Besides the raw ``/bot<id>:<secret>/`` URL path, exception and log text can
carry the token URL-encoded (":" as %3A, e.g. a URL quoted into a query
string, or %253A when encoded twice), in quotes, or inside a repr(), where an
escape such as ``\\n`` puts a letter right before the bot id.
"""

from __future__ import annotations

import io
import json
import logging
import urllib.parse

import pytest
import requests
import urllib3

from mailbot_v26.observability.redact import RedactingFormatter, redact_secrets

TOKEN = "987654321:AAHfakeTokenValueForTestsOnly_1234567"
BOT_ID, SECRET = TOKEN.split(":", 1)
URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"


def _connection_error(url_path: str) -> requests.ConnectionError:
    # requests' "connection refused": the exception text carries the URL path.
    return requests.ConnectionError(
        urllib3.exceptions.MaxRetryError(
            "HTTPSConnectionPool(host='api.telegram.org', port=443)",
            url_path,
            reason=urllib3.exceptions.NewConnectionError(None, "boom"),
        )
    )


def _assert_masked(text: str) -> str:
    redacted = redact_secrets(text)
    assert SECRET not in redacted
    assert SECRET[:8] not in redacted
    assert "<redacted" in redacted
    return redacted


URL_ENCODED_FORMS = {
    "path_pct3A": f"Max retries exceeded with url: /bot{BOT_ID}%3A{SECRET}/sendMessage",
    "path_pct3a_lowercase": f"url: /bot{BOT_ID}%3a{SECRET}/sendMessage",
    "quoted_url": urllib.parse.quote(URL, safe=""),
    "url_in_query": "proxy error for " + urllib.parse.urlencode({"next": URL}),
    "bare_in_query": urllib.parse.urlencode({"chat_id": "42", "token": TOKEN}),
    "double_encoded_url": urllib.parse.quote(urllib.parse.quote(URL, safe=""), safe=""),
    "bare_after_encoded_slash": f"GET %2F{TOKEN}%2FgetMe",
    "bare_after_encoded_equals": f"token%3D{BOT_ID}%3A{SECRET}",
    "exception_str": str(_connection_error(f"/bot{BOT_ID}%3A{SECRET}/sendMessage")),
    "exception_repr": repr(_connection_error(f"/bot{BOT_ID}%3A{SECRET}/sendMessage")),
}

QUOTED_FORMS = {
    "single_quoted": f"invalid token '{TOKEN}'",
    "double_quoted": f'invalid token "{TOKEN}"',
    "quoted_url": f"request to '{URL}' failed",
    "json": json.dumps({"bot_token": TOKEN, "url": URL}),
    "json_escaped_slashes": json.dumps(URL).replace("/", "\\/"),
}

REPR_FORMS = {
    "repr_str": repr(TOKEN),
    "repr_bytes": repr(TOKEN.encode("ascii")),
    "repr_dict": repr({"bot_token": TOKEN}),
    "repr_url": repr(URL),
    "repr_connection_error": repr(_connection_error(f"/bot{TOKEN}/sendMessage")),
    # The repr escape "\n" (or "\t", "\r") leaves a letter right before the id.
    "repr_after_newline": repr(f"Unauthorized\n{TOKEN}"),
    "repr_after_tab": repr(f"token:\t{TOKEN}"),
    "repr_exception_after_newline": repr(RuntimeError(f"bad token\r\n{TOKEN}")),
}


@pytest.mark.parametrize(
    "text", list(URL_ENCODED_FORMS.values()), ids=list(URL_ENCODED_FORMS)
)
def test_url_encoded_token_is_masked(text: str) -> None:
    _assert_masked(text)


@pytest.mark.parametrize("text", list(QUOTED_FORMS.values()), ids=list(QUOTED_FORMS))
def test_quoted_token_is_masked(text: str) -> None:
    _assert_masked(text)


@pytest.mark.parametrize("text", list(REPR_FORMS.values()), ids=list(REPR_FORMS))
def test_repr_token_is_masked(text: str) -> None:
    _assert_masked(text)


def test_url_encoded_bot_path_keeps_its_shape() -> None:
    assert (
        redact_secrets(f"/bot{BOT_ID}%3A{SECRET}/sendMessage")
        == "/bot<redacted>/sendMessage"
    )
    assert (
        redact_secrets(f"%2Fbot{BOT_ID}%3A{SECRET}%2FsendMessage")
        == "%2Fbot<redacted>%2FsendMessage"
    )


def test_bare_url_encoded_token_keeps_its_surroundings() -> None:
    assert (
        redact_secrets(f"chat_id=42&token={BOT_ID}%3A{SECRET}&x=1")
        == "chat_id=42&token=<redacted-token>&x=1"
    )


@pytest.mark.parametrize(
    "text",
    [
        "ratio 12:34 at 10:15",
        "2026-10-20T12:34:56+03:00",
        "https%3A%2F%2Fexample.com%2Fpath%3Fq%3D1",
        "uid set 1234567:1234999",
        "1234567%3Ashort",
        "id 1234567:" + "a" * 29,
        "sha256:" + "ab12" * 16,
        "port=443 host=api.telegram.org",
    ],
)
def test_text_without_a_token_is_unchanged(text: str) -> None:
    assert redact_secrets(text) == text


def test_redacting_formatter_masks_a_repr_logged_exception() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(RedactingFormatter("%(levelname)s %(message)s"))
    test_logger = logging.getLogger("mailbot.tests.redaction_forms")
    test_logger.addHandler(handler)
    test_logger.propagate = False
    try:
        test_logger.error(
            "send failed: %r / %r",
            _connection_error(f"/bot{BOT_ID}%3A{SECRET}/sendMessage"),
            f"Unauthorized\n{TOKEN}",
        )
    finally:
        test_logger.removeHandler(handler)
        test_logger.propagate = True

    output = stream.getvalue()
    assert "send failed" in output
    assert SECRET not in output
    assert SECRET[:8] not in output
