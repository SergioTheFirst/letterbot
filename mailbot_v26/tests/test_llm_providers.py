"""A17: the Cloudflare provider reports failures instead of returning "".

Before, an HTTP error came back as "" and the router logged ok=1. Now the
provider raises LLMProviderError with a secret-free message, and the router
logs the failure (ok=0) and returns "" to its caller.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

import pytest

from mailbot_v26.llm.providers import (
    CloudflareProvider,
    CloudflareProviderConfig,
    LLMProviderError,
)
from mailbot_v26.llm.router import LLMRouter, LLMRouterConfig

ACCOUNT_ID = "acct-5a-0123456789"
API_TOKEN = "cf-token-SECRET-5A-abcdef"


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


def _provider() -> CloudflareProvider:
    return CloudflareProvider(
        CloudflareProviderConfig(account_id=ACCOUNT_ID, api_token=API_TOKEN)
    )


def _respond_with(monkeypatch: pytest.MonkeyPatch, payload: object) -> None:
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda request, timeout: _FakeResponse(body)
    )


def _raise_on_open(monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
    def _urlopen(request, timeout):
        raise exc

    monkeypatch.setattr(urllib.request, "urlopen", _urlopen)


def _http_error(code: int) -> urllib.error.HTTPError:
    url = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/ai/run/m"
    return urllib.error.HTTPError(url, code, "Unauthorized", None, None)


def _messages() -> list[dict]:
    return [{"role": "user", "content": "Hello"}]


def test_documented_text_response_is_returned(monkeypatch) -> None:
    # Workers AI REST /ai/run returns {"result": {"response": "<text>"}}.
    _respond_with(
        monkeypatch,
        {"result": {"response": " Hello "}, "success": True, "errors": []},
    )

    assert _provider().complete(_messages()) == "Hello"


def test_message_shaped_response_is_still_supported(monkeypatch) -> None:
    _respond_with(
        monkeypatch, {"result": {"response": {"message": {"content": "Hi"}}}}
    )

    assert _provider().complete(_messages()) == "Hi"


def test_http_error_raises_provider_error_without_secrets(monkeypatch) -> None:
    _raise_on_open(monkeypatch, _http_error(401))

    with pytest.raises(LLMProviderError) as raised:
        _provider().complete(_messages())

    message = str(raised.value)
    assert "HTTP 401" in message
    assert API_TOKEN not in message
    assert ACCOUNT_ID not in message


def test_network_error_raises_provider_error(monkeypatch) -> None:
    _raise_on_open(monkeypatch, urllib.error.URLError("getaddrinfo failed"))

    with pytest.raises(LLMProviderError, match="URLError") as raised:
        _provider().complete(_messages())

    message = str(raised.value)
    assert API_TOKEN not in message
    assert ACCOUNT_ID not in message


def test_invalid_json_raises_provider_error(monkeypatch) -> None:
    _respond_with(monkeypatch, b"<html>bad gateway</html>")

    with pytest.raises(LLMProviderError, match="invalid"):
        _provider().complete(_messages())


def test_response_without_result_raises_provider_error(monkeypatch) -> None:
    _respond_with(monkeypatch, {"result": None, "success": False, "errors": []})

    with pytest.raises(LLMProviderError, match="no result"):
        _provider().complete(_messages())


def test_missing_credentials_raise_provider_error() -> None:
    provider = CloudflareProvider(CloudflareProviderConfig(account_id="", api_token=""))

    with pytest.raises(LLMProviderError, match="credentials missing"):
        provider.complete(_messages())


def test_http_error_through_router_is_a_logged_failure(
    monkeypatch, tmp_path, caplog
) -> None:
    _raise_on_open(monkeypatch, _http_error(401))
    router = LLMRouter(
        LLMRouterConfig(
            primary="cloudflare",
            fallback="cloudflare",
            cloudflare_enabled=True,
            cloudflare_account_id=ACCOUNT_ID,
            cloudflare_api_key=API_TOKEN,
            runtime_flags_path=tmp_path / "runtime_flags.json",
        )
    )

    with caplog.at_level(logging.INFO):
        result = router.complete(_messages())

    assert result == ""
    text = caplog.text
    assert "[LLM] provider=cloudflare" in text and "ok=0" in text
    assert "ok=1" not in text
    assert "[LLM-ERROR] provider=cloudflare" in text and "HTTP 401" in text
    assert API_TOKEN not in text
