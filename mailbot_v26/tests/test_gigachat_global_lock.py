from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from mailbot_v26.llm.providers import GigaChatProvider, GigaChatProviderConfig


def test_gigachat_global_lock_serializes_across_instances(monkeypatch) -> None:
    config = GigaChatProviderConfig(api_key="token")
    provider_a = GigaChatProvider(config)
    provider_b = GigaChatProvider(config)
    start_event = threading.Event()
    release_event = threading.Event()
    start_times: list[float] = []
    record_lock = threading.Lock()

    def fake_request(self, payload):  # type: ignore[no-untyped-def]
        with record_lock:
            start_times.append(time.monotonic())
            index = len(start_times)
        if index == 1:
            start_event.set()
            if not release_event.wait(timeout=1):
                raise AssertionError("release event timeout")
        return "ok"

    monkeypatch.setattr(GigaChatProvider, "_request", fake_request, raising=True)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(
            provider_a.complete, [{"role": "user", "content": "a"}]
        )
        future_b = executor.submit(
            provider_b.complete, [{"role": "user", "content": "b"}]
        )
        assert start_event.wait(timeout=1)
        release_time = time.monotonic()
        release_event.set()
        future_a.result(timeout=1)
        future_b.result(timeout=1)

    assert len(start_times) == 2
    assert start_times[1] >= release_time
