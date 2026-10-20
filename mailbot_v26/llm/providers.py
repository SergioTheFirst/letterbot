from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from mailbot_v26.llm.global_lock import gigachat_lock

logger = logging.getLogger(__name__)
_GIGACHAT_LOCK_WAIT_LOG_THRESHOLD_MS = 200


class LLMProviderError(RuntimeError):
    """Provider-level exception raised on LLM failures."""


class LLMProvider(ABC):
    @abstractmethod
    def complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def healthcheck(self) -> bool:
        raise NotImplementedError


@dataclass(frozen=True)
class CloudflareProviderConfig:
    account_id: str
    api_token: str
    model: str = "@cf/meta/llama-3-8b-instruct"


class CloudflareProvider(LLMProvider):
    """Cloudflare Workers AI REST provider; failures raise LLMProviderError."""

    def __init__(self, config: CloudflareProviderConfig) -> None:
        self.config = config

    def _build_request(self, payload: dict[str, Any]) -> urllib.request.Request:
        url = (
            f"https://api.cloudflare.com/client/v4/accounts/"
            f"{self.config.account_id}/ai/run/{self.config.model}"
        )
        encoded = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(url, data=encoded, method="POST")
        request.add_header("Authorization", f"Bearer {self.config.api_token}")
        request.add_header("Content-Type", "application/json")
        return request

    def complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        """Return the model text; raise LLMProviderError on any failure.

        Error messages carry the HTTP status or error class only: never the
        token, the account id or the response body (it may echo the email).
        """
        if not self.config.account_id or not self.config.api_token:
            raise LLMProviderError("Cloudflare credentials missing")

        payload: dict[str, Any] = {"messages": messages}
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature

        try:
            request = self._build_request(payload)
            with urllib.request.urlopen(request, timeout=15) as response:
                body = response.read().decode("utf-8")
            parsed = json.loads(body)
        except urllib.error.HTTPError as exc:
            raise LLMProviderError(f"Cloudflare HTTP {exc.code}") from exc
        except OSError as exc:  # URLError, timeouts, connection resets
            reason = getattr(exc, "reason", None) or exc
            raise LLMProviderError(
                f"Cloudflare request failed: {type(exc).__name__}: {reason}"
            ) from exc
        except ValueError as exc:  # invalid JSON / encoding
            raise LLMProviderError(
                f"Cloudflare response invalid ({type(exc).__name__})"
            ) from exc
        return _cloudflare_response_text(parsed)

    def healthcheck(self) -> bool:
        return bool(self.config.account_id and self.config.api_token)


def _cloudflare_response_text(parsed: object) -> str:
    result = parsed.get("result") if isinstance(parsed, dict) else None
    if not isinstance(result, dict):
        raise LLMProviderError("Cloudflare response invalid (no result)")
    response = result.get("response")
    content: object
    if isinstance(response, str):
        # Documented Workers AI /ai/run shape: {"result": {"response": "..."}}.
        content = response
    elif isinstance(response, dict) and "message" in response:
        message = response.get("message")
        content = message.get("content", "") if isinstance(message, dict) else ""
    else:
        content = result.get("output", "")
    if isinstance(content, list):
        content = "".join(str(part) for part in content)
    return str(content or "").strip()


@dataclass(frozen=True)
class GigaChatProviderConfig:
    api_key: str
    base_url: str = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    model: str | None = "GigaChat"
    timeout_s: int = 20


class GigaChatProvider(LLMProvider):
    """GigaChat provider with strict serialization (internal-only; use LLMRouter)."""

    def __init__(self, config: GigaChatProviderConfig) -> None:
        self.config = config

    def _request(self, payload: dict[str, Any]) -> str:
        """POST one chat request; raise LLMProviderError on any failure.

        The router logs these messages, so they carry the HTTP status or error
        class only: never the API key (http.client echoes an invalid header
        value in its ValueError) or the response body.
        """
        try:
            request = urllib.request.Request(
                self.config.base_url,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
            )
            request.add_header("Authorization", f"Bearer {self.config.api_key}")
            request.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(
                request, timeout=self.config.timeout_s
            ) as response:
                body = response.read().decode("utf-8")
            data = json.loads(body)
        except urllib.error.HTTPError as exc:
            raise LLMProviderError(f"GigaChat HTTP {exc.code}") from exc
        except OSError as exc:  # URLError, timeouts, connection resets
            reason = getattr(exc, "reason", None) or exc
            raise LLMProviderError(
                f"GigaChat request failed: {type(exc).__name__}: {reason}"
            ) from exc
        except ValueError as exc:  # bad URL or header value, invalid JSON
            raise LLMProviderError(
                f"GigaChat request or response invalid ({type(exc).__name__})"
            ) from exc
        choices = data.get("choices", [])
        if choices and isinstance(choices[0], dict):
            message = choices[0].get("message", {})
            return str(message.get("content", "")).strip()
        return str(data.get("result", "")).strip()

    def complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        if not self.config.api_key:
            raise LLMProviderError("GigaChat API key missing")

        payload: dict[str, Any] = {"messages": messages}
        if self.config.model:
            payload["model"] = self.config.model
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature

        lock = gigachat_lock()
        start_wait = time.monotonic()
        lock.acquire()
        wait_ms = int((time.monotonic() - start_wait) * 1000)
        if wait_ms >= _GIGACHAT_LOCK_WAIT_LOG_THRESHOLD_MS:
            logger.info("llm_gigachat_lock_wait_ms=%d", wait_ms)
        else:
            logger.debug("llm_gigachat_lock_acquired wait_ms=%d", wait_ms)
        try:
            return self._request(payload)
        finally:
            lock.release()

    def healthcheck(self) -> bool:
        return bool(self.config.api_key and self.config.base_url)
