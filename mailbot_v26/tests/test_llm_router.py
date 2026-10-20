from __future__ import annotations

import logging
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
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
    # A17: the breaker lives in memory; the operator's flag file is untouched.
    assert runtime_flags.read_text(encoding="utf-8") == '{"enable_gigachat": true}'
    assert router._circuit_open_until is not None


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
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
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


def test_consecutive_errors_open_breaker_in_memory_only(tmp_path, caplog) -> None:
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
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
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
    # A17: no auto-disable written to disk, the breaker is in memory.
    assert runtime_flags.read_text(encoding="utf-8") == '{"enable_gigachat": true}'
    assert router._circuit_open_until is not None
    assert any(
        "[LLM-CIRCUIT]" in record.message
        and "state=open" in record.message
        and "consecutive_errors" in record.message
        for record in caplog.records
    )
    assert any(
        "[LLM-FALLBACK]" in record.message and "consecutive_errors" in record.message
        for record in caplog.records
    )


class SlowProvider(LLMProvider):
    def __init__(self, response: str = "slow") -> None:
        self.calls = 0
        self.response = response

    def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
        self.calls += 1
        time.sleep(0.02)
        return self.response

    def healthcheck(self) -> bool:
        return True


def test_slow_successful_gigachat_answer_is_kept(tmp_path, caplog) -> None:
    gigachat = SlowProvider()
    cloudflare = StubProvider(name="cloudflare", response="fallback")
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
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

    # Success is checked before latency: the slow answer is not thrown away...
    assert result == "slow"
    assert router.last_provider == "gigachat"
    assert gigachat.calls == 1
    assert cloudflare.calls == 0
    assert runtime_flags.read_text(encoding="utf-8") == '{"enable_gigachat": true}'
    assert any(
        "[LLM-CIRCUIT]" in record.message
        and "state=open" in record.message
        and "latency_exceeded" in record.message
        for record in caplog.records
    )

    # ...but the next calls go to the fallback while the breaker cools down.
    assert router.complete(_messages()) == "fallback"
    assert gigachat.calls == 1
    assert cloudflare.calls == 1


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
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
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
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
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


# --- A17: provider errors are logged, empty answers are failures ---------------


class RaisingProvider(LLMProvider):
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.calls = 0

    def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
        self.calls += 1
        raise self.exc

    def healthcheck(self) -> bool:
        return True


def _cloudflare_router(tmp_path, provider: LLMProvider) -> LLMRouter:
    return LLMRouter(
        LLMRouterConfig(
            primary="cloudflare",
            fallback="cloudflare",
            cloudflare_enabled=True,
            runtime_flags_path=tmp_path / "runtime_flags.json",
        ),
        providers={"cloudflare": provider},
    )


def _gigachat_router(tmp_path, providers: dict, **overrides) -> LLMRouter:
    runtime_flags = tmp_path / "runtime_flags.json"
    runtime_flags.write_text('{"enable_gigachat": true}', encoding="utf-8")
    config = {
        "primary": "gigachat",
        "fallback": "cloudflare",
        "gigachat_enabled": True,
        "cloudflare_enabled": True,
        "runtime_flags_path": runtime_flags,
        **overrides,
    }
    return LLMRouter(LLMRouterConfig(**config), providers=providers)


def _messages_with(caplog, *needles: str) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if all(needle in record.getMessage() for needle in needles)
    ]


def test_empty_cloudflare_answer_is_logged_as_failure(tmp_path, caplog) -> None:
    router = _cloudflare_router(tmp_path, StubProvider(name="cloudflare", response=""))

    with caplog.at_level(logging.INFO):
        result = router.complete(_messages())

    assert result == ""
    assert _messages_with(caplog, "[LLM] provider=cloudflare", "ok=0")
    assert not _messages_with(caplog, "[LLM] provider=cloudflare", "ok=1")
    assert _messages_with(
        caplog, "[LLM-ERROR] provider=cloudflare", "error_class=EmptyResponse"
    )


def test_provider_error_is_logged_with_class_and_detail(tmp_path, caplog) -> None:
    provider = RaisingProvider(LLMProviderError("Cloudflare HTTP 401"))
    router = _cloudflare_router(tmp_path, provider)

    with caplog.at_level(logging.INFO):
        result = router.complete(_messages())

    assert result == ""
    errors = _messages_with(
        caplog,
        "[LLM-ERROR] provider=cloudflare",
        "error_class=LLMProviderError",
        "Cloudflare HTTP 401",
    )
    assert errors and errors[0].levelno == logging.WARNING
    assert _messages_with(caplog, "[LLM] provider=cloudflare", "ok=0")


def test_unexpected_provider_exception_is_logged_with_traceback(
    tmp_path, caplog
) -> None:
    router = _cloudflare_router(tmp_path, RaisingProvider(KeyError("choices")))

    with caplog.at_level(logging.INFO):
        result = router.complete(_messages())

    assert result == ""
    errors = _messages_with(caplog, "[LLM-ERROR] provider=cloudflare", "KeyError")
    assert errors and errors[0].exc_info is not None


def test_empty_gigachat_answer_falls_back(tmp_path) -> None:
    gigachat = StubProvider(name="gigachat", response="   ")
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = _gigachat_router(
        tmp_path, {"gigachat": gigachat, "cloudflare": cloudflare}
    )

    assert router.complete(_messages()) == "cf"
    assert gigachat.calls == 2
    assert cloudflare.calls == 1
    assert router.last_provider == "cloudflare"
    assert router._circuit_open_until is not None


# --- A17: in-memory circuit breaker with cooldown ------------------------------


class FlakyProvider(LLMProvider):
    """Fails the first ``failures`` calls, then answers."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def complete(self, messages, *, max_tokens=None, temperature=None) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise LLMProviderError("busy")
        return "giga"

    def healthcheck(self) -> bool:
        return True


def test_breaker_half_open_probe_closes_on_success(tmp_path, caplog) -> None:
    gigachat = FlakyProvider(failures=2)
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = _gigachat_router(
        tmp_path, {"gigachat": gigachat, "cloudflare": cloudflare}
    )

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == "cf"
        assert router.complete(_messages()) == "cf"  # cooling down
        assert gigachat.calls == 2
        # Cooldown elapsed: the next call probes GigaChat again.
        router._circuit_open_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert router.complete(_messages()) == "giga"

    assert gigachat.calls == 3
    assert router._circuit_open_until is None
    assert router._gigachat_consecutive_errors == 0
    states = [
        record.getMessage()
        for record in caplog.records
        if "[LLM-CIRCUIT]" in record.getMessage()
    ]
    assert len(states) == 3
    assert "state=open" in states[0] and "reason=runtime_error" in states[0]
    assert "state=half_open" in states[1]
    assert "state=closed" in states[2]


def test_open_breaker_is_logged_once_while_cooling_down(tmp_path, caplog) -> None:
    gigachat = RaisingProvider(LLMProviderError("boom"))
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = _gigachat_router(
        tmp_path, {"gigachat": gigachat, "cloudflare": cloudflare}
    )

    with caplog.at_level(logging.INFO):
        for _ in range(4):
            assert router.complete(_messages()) == "cf"

    assert gigachat.calls == 2
    assert len(_messages_with(caplog, "[LLM-CIRCUIT]", "state=open")) == 1


def test_gigachat_only_setup_degrades_instead_of_bypassing_breaker(
    tmp_path, caplog
) -> None:
    gigachat = RaisingProvider(LLMProviderError("boom"))
    router = _gigachat_router(tmp_path, {"gigachat": gigachat}, fallback="gigachat")

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == ""
        assert router.complete(_messages()) == ""

    # Two attempts on the first call; never again while the breaker is open
    # (the fallback must not route back into GigaChat and block the pipeline).
    assert gigachat.calls == 2
    assert _messages_with(caplog, "[LLM-FALLBACK]", "provider=gigachat", "skipped")


def test_missing_fallback_provider_is_logged(tmp_path, caplog) -> None:
    router = LLMRouter(
        LLMRouterConfig(
            primary="cloudflare",
            fallback="gigachat",
            cloudflare_enabled=True,
            runtime_flags_path=tmp_path / "runtime_flags.json",
        ),
        providers={},
    )

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == ""

    assert _messages_with(caplog, "[LLM-ERROR] provider=gigachat", "not configured")


def test_failed_half_open_probe_reopens_after_one_attempt(tmp_path, caplog) -> None:
    gigachat = RaisingProvider(LLMProviderError("busy"))
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = _gigachat_router(
        tmp_path, {"gigachat": gigachat, "cloudflare": cloudflare}
    )

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == "cf"
        router._circuit_open_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert router.complete(_messages()) == "cf"

    # Two attempts while closed, a single probe once the cooldown is over.
    assert gigachat.calls == 3
    assert router._is_circuit_open()
    states = [
        record.getMessage()
        for record in caplog.records
        if "[LLM-CIRCUIT]" in record.getMessage()
    ]
    assert len(states) == 3
    assert "state=open" in states[0]
    assert "state=half_open" in states[1]
    assert "state=open" in states[2] and "failures=2" in states[2]


def test_reaching_max_consecutive_errors_is_logged_as_warning(
    tmp_path, caplog
) -> None:
    gigachat = RaisingProvider(LLMProviderError("busy"))
    cloudflare = StubProvider(name="cloudflare", response="cf")
    router = _gigachat_router(
        tmp_path,
        {"gigachat": gigachat, "cloudflare": cloudflare},
        gigachat_max_consecutive_errors=2,
    )

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == "cf"
        router._circuit_open_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert router.complete(_messages()) == "cf"

    opened = _messages_with(caplog, "[LLM-CIRCUIT]", "state=open")
    assert [record.levelno for record in opened] == [logging.INFO, logging.WARNING]
    assert "reason=runtime_error" in opened[0].getMessage()
    assert "reason=consecutive_errors" in opened[1].getMessage()
    # Still only in memory: GigaChat comes back after the cooldown.
    assert router._circuit_open_until is not None
    assert (tmp_path / "runtime_flags.json").read_text(
        encoding="utf-8"
    ) == '{"enable_gigachat": true}'


def test_cloudflare_failure_falls_back_to_gigachat(tmp_path, caplog) -> None:
    cloudflare = RaisingProvider(LLMProviderError("Cloudflare HTTP 500"))
    gigachat = StubProvider(name="gigachat", response="giga")
    router = LLMRouter(
        LLMRouterConfig(
            primary="cloudflare",
            fallback="gigachat",
            cloudflare_enabled=True,
            gigachat_enabled=True,
            runtime_flags_path=tmp_path / "runtime_flags.json",
        ),
        providers={"cloudflare": cloudflare, "gigachat": gigachat},
    )

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == "giga"

    assert cloudflare.calls == 1
    assert gigachat.calls == 1
    assert router.last_provider == "gigachat"
    assert _messages_with(
        caplog, "[LLM-FALLBACK]", "from=cloudflare", "to=gigachat", "runtime_error"
    )


def test_gigachat_fallback_after_cloudflare_failure_respects_breaker(
    tmp_path,
) -> None:
    cloudflare = RaisingProvider(LLMProviderError("Cloudflare HTTP 500"))
    gigachat = RaisingProvider(LLMProviderError("busy"))
    router = LLMRouter(
        LLMRouterConfig(
            primary="cloudflare",
            fallback="gigachat",
            cloudflare_enabled=True,
            gigachat_enabled=True,
            runtime_flags_path=tmp_path / "runtime_flags.json",
        ),
        providers={"cloudflare": cloudflare, "gigachat": gigachat},
    )

    assert router.complete(_messages()) == ""
    assert router.complete(_messages()) == ""

    assert cloudflare.calls == 2
    assert gigachat.calls == 1  # the breaker opened after the first failure


def test_gigachat_reached_as_fallback_is_guarded_and_never_persisted(
    tmp_path, caplog
) -> None:
    # Default settings: primary = gigachat, fallback defaults to the primary and
    # runtime_flags.json does not exist (enable_gigachat off): GigaChat is then
    # reached through the fallback, which must not bypass the breaker.
    gigachat = RaisingProvider(LLMProviderError("busy"))
    runtime_flags = tmp_path / "runtime_flags.json"
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="gigachat",
            gigachat_enabled=True,
            cloudflare_enabled=False,
            runtime_flags_path=runtime_flags,
        ),
        providers={"gigachat": gigachat},
    )

    with caplog.at_level(logging.INFO):
        assert router.complete(_messages()) == ""
        assert router.complete(_messages()) == ""

    assert gigachat.calls == 1
    assert _messages_with(caplog, "[LLM-FALLBACK]", "reason=runtime_disabled")
    assert _messages_with(caplog, "skipped", "reason=circuit_open")
    assert not runtime_flags.exists()


def test_router_logs_never_contain_the_prompt(tmp_path, caplog) -> None:
    secret = "SECRET-PROMPT-5A subject and body"
    messages = [{"role": "user", "content": secret}]
    gigachat = RaisingProvider(RuntimeError("unexpected"))
    cloudflare = StubProvider(name="cloudflare", response="")
    router = _gigachat_router(
        tmp_path, {"gigachat": gigachat, "cloudflare": cloudflare}
    )

    with caplog.at_level(logging.DEBUG):
        assert router.complete(messages) == ""

    assert _messages_with(caplog, "[LLM-ERROR] provider=gigachat", "RuntimeError")
    assert _messages_with(caplog, "[LLM-ERROR] provider=cloudflare", "EmptyResponse")
    assert secret not in caplog.text


# --- A17: GigaChat errors reach the log without the API key --------------------

GIGACHAT_KEY = "giga-key-SECRET-5A-0123456789"
# http.client rejects a header value with a control character and quotes the
# whole value ("Bearer <key>") in its ValueError.
_INVALID_HEADER_ERROR = f"Invalid header value b'Bearer {GIGACHAT_KEY}\\n'"


def _gigachat_provider() -> GigaChatProvider:
    return GigaChatProvider(
        GigaChatProviderConfig(
            api_key=GIGACHAT_KEY, base_url="https://gigachat.test/chat"
        )
    )


def _urlopen_raises(monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
    def _urlopen(request, timeout):
        raise exc

    monkeypatch.setattr(urllib.request, "urlopen", _urlopen)


def test_gigachat_invalid_header_error_does_not_echo_the_key(monkeypatch) -> None:
    _urlopen_raises(monkeypatch, ValueError(_INVALID_HEADER_ERROR))

    with pytest.raises(LLMProviderError) as raised:
        _gigachat_provider().complete(_messages())

    assert GIGACHAT_KEY not in str(raised.value)
    assert "ValueError" in str(raised.value)


def test_gigachat_http_error_reports_the_status_only(monkeypatch) -> None:
    _urlopen_raises(
        monkeypatch,
        urllib.error.HTTPError(
            "https://gigachat.test/chat", 401, "Unauthorized", None, None
        ),
    )

    with pytest.raises(LLMProviderError, match="GigaChat HTTP 401"):
        _gigachat_provider().complete(_messages())


def test_gigachat_error_is_logged_by_the_router_without_the_key(
    tmp_path, monkeypatch, caplog
) -> None:
    _urlopen_raises(monkeypatch, ValueError(_INVALID_HEADER_ERROR))
    router = _gigachat_router(
        tmp_path, {"gigachat": _gigachat_provider()}, fallback="gigachat"
    )

    with caplog.at_level(logging.DEBUG):
        assert router.complete(_messages()) == ""

    assert _messages_with(caplog, "[LLM-ERROR] provider=gigachat", "LLMProviderError")
    assert GIGACHAT_KEY not in caplog.text
