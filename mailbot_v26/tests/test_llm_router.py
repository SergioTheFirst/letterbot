from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path

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
            runtime_flags_path=Path("missing_runtime_flags.json"),
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    result = router.complete(_messages())

    assert result == "cf"
    assert gigachat.calls == 0
    assert cloudflare.calls == 1


def test_gigachat_healthcheck_failure_falls_back(tmp_path) -> None:
    gigachat = StubProvider(name="gigachat", response="giga", healthy=False)
    cloudflare = StubProvider(name="cloudflare", response="cf")
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    result = router.complete(_messages())

    assert result == "cf"
    assert gigachat.calls == 0
    assert cloudflare.calls == 1
    assert runtime_flags.read_text(encoding="utf-8") == "{\"enable_gigachat\": false}"


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


def test_runtime_override_missing_disables_gigachat(tmp_path) -> None:
    gigachat = StubProvider(name="gigachat", response="giga")
    cloudflare = StubProvider(name="cloudflare", response="cf")
    runtime_flags = tmp_path / "runtime_flags.json"
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    result = router.complete(_messages())

    assert result == "cf"
    assert gigachat.calls == 0
    assert cloudflare.calls == 1


def test_runtime_override_enables_gigachat(tmp_path) -> None:
    gigachat = StubProvider(name="gigachat", response="giga")
    cloudflare = StubProvider(name="cloudflare", response="cf")
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=False,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    result = router.complete(_messages())

    assert result == "giga"
    assert gigachat.calls == 1
    assert cloudflare.calls == 0


def test_auto_disable_on_consecutive_errors(tmp_path, caplog) -> None:
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
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
            gigachat_max_consecutive_errors=3,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    router.complete(_messages())
    router._circuit_open_until = None
    router.complete(_messages())
    router._circuit_open_until = None
    with caplog.at_level("INFO"):
        result = router.complete(_messages())

    assert result == "fallback"
    assert gigachat.calls == 6
    assert cloudflare.calls == 3
    assert runtime_flags.read_text(encoding="utf-8") == "{\"enable_gigachat\": false}"
    assert router._circuit_open_until is not None
    assert any(
        "[LLM-SAFETY]" in record.message and "consecutive_errors" in record.message
        for record in caplog.records
    )
    assert any(
        "[LLM-FALLBACK]" in record.message and "consecutive_errors" in record.message
        for record in caplog.records
    )


def test_auto_disable_on_latency(tmp_path, caplog) -> None:
    class SlowProvider(LLMProvider):
        def __init__(self) -> None:
            self.calls = 0

        def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
            self.calls += 1
            time.sleep(0.02)
            return "slow"

        def healthcheck(self) -> bool:
            return True

    gigachat = SlowProvider()
    cloudflare = StubProvider(name="cloudflare", response="fallback")
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
            gigachat_max_latency_sec=0.01,
        ),
        providers={"gigachat": gigachat, "cloudflare": cloudflare},
    )

    with caplog.at_level("INFO"):
        result = router.complete(_messages())

    assert result == "fallback"
    assert gigachat.calls == 1
    assert cloudflare.calls == 1
    assert runtime_flags.read_text(encoding="utf-8") == "{\"enable_gigachat\": false}"
    assert any(
        "[LLM-SAFETY]" in record.message and "latency_exceeded" in record.message
        for record in caplog.records
    )
    assert any(
        "[LLM-FALLBACK]" in record.message and "latency_exceeded" in record.message
        for record in caplog.records
    )


def test_router_serializes_gigachat_calls(tmp_path) -> None:
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

    gigachat = SerialGigaChat()
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=False,
            runtime_flags_path=runtime_flags,
        ),
        providers={"gigachat": gigachat},
    )

    barrier = threading.Barrier(2)

    def _run() -> None:
        barrier.wait()
        router.complete(_messages())

    t1 = threading.Thread(target=_run)
    t2 = threading.Thread(target=_run)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert gigachat.max_active == 1


def test_runtime_error_trips_circuit(tmp_path, caplog) -> None:
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
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text("{\"enable_gigachat\": true}", encoding="utf-8")
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="cloudflare",
            gigachat_enabled=True,
            cloudflare_enabled=True,
            runtime_flags_path=runtime_flags,
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
