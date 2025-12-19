from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from mailbot_v26.llm.providers import (
    GigaChatProvider,
    GigaChatProviderConfig,
    LLMProvider,
    LLMProviderError,
)
from mailbot_v26.llm.router import LLMRouter, LLMRouterConfig


@dataclass
class StubProvider(LLMProvider):
    name: str
    response: str = ""
    healthy: bool = True
    calls: int = 0

    def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
        self.calls += 1
        return self.response

    def healthcheck(self) -> bool:
        return self.healthy


def _messages() -> list[dict]:
    return [{"role": "user", "content": "Hello"}]


def test_gigachat_disabled_uses_cloudflare() -> None:
    gigachat = StubProvider(name="gigachat", response="giga")
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=False,
            cloudflare_enabled=True,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    result = router.complete(_messages())

    assert result == "cf"
    assert gigachat.calls == 0
    assert cloudflare.calls == 1


def test_gigachat_healthcheck_failure_falls_back() -> None:
    gigachat = StubProvider(name="gigachat", response="giga", healthy=False)
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    result = router.complete(_messages())

    assert result == "cf"
    assert gigachat.calls == 0
    assert cloudflare.calls == 1


def test_gigachat_serializes_requests() -> None:
    class SerialGigaChat(GigaChatProvider):
        def __init__(self) -> None:
            super().__init__(GigaChatProviderConfig(api_key="token"))
            self.active = 0
            self.max_active = 0
            self._tracker = threading.Lock()

        def _request(self, payload):  # type: ignore[override]
            with self._tracker:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            time.sleep(0.05)
            with self._tracker:
                self.active -= 1
            return "ok"

    provider = SerialGigaChat()
    barrier = threading.Barrier(2)

    def _run() -> None:
        barrier.wait()
        provider.complete(_messages())

    t1 = threading.Thread(target=_run)
    t2 = threading.Thread(target=_run)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert provider.max_active == 1


def test_runtime_error_trips_circuit(caplog) -> None:
    class FailingProvider(LLMProvider):
        def __init__(self) -> None:
            self.calls = 0

        def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
            self.calls += 1
            raise LLMProviderError("boom")

        def healthcheck(self) -> bool:
            return True

    gigachat = FailingProvider()
    cloudflare = StubProvider(name="cloudflare", response="fallback")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    with caplog.at_level("INFO"):
        result = router.complete(_messages())

    assert result == "fallback"
    assert gigachat.calls == 2
    assert cloudflare.calls == 1
    assert any(
        "[LLM-FALLBACK]" in record.message and "runtime_error" in record.message
        for record in caplog.records
    )

    result = router.complete(_messages())
    assert result == "fallback"
    assert gigachat.calls == 2
    assert cloudflare.calls == 2
