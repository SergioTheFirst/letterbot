import sqlite3
import time
from pathlib import Path
from threading import Thread

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter
from mailbot_v26.llm.router import LLMRouter, LLMRouterConfig
from mailbot_v26.llm.providers import GigaChatProvider
from mailbot_v26.pipeline import processor
from mailbot_v26.system.orchestrator import SystemMode, SystemOrchestrator


class _FakeGigaChat(GigaChatProvider):
    def __init__(self) -> None:
        # bypass parent init
        self.calls: list[float] = []

    def complete(self, messages, *, max_tokens=None, temperature=None):  # type: ignore[override]
        self.calls.append(time.time())
        time.sleep(0.2)
        return "ok"

    def healthcheck(self) -> bool:  # type: ignore[override]
        return True


def test_event_contract_emission_idempotent(tmp_path):
    emitter = EventEmitter(tmp_path / "events.sqlite")
    event = EventV1(
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=time.time(),
        account_id="acc",
        entity_id=None,
        email_id=1,
        payload={"p": 1},
    )
    assert emitter.emit(event) is True
    assert emitter.emit(event) is False
    with sqlite3.connect(tmp_path / "events.sqlite") as conn:
        count = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]
        assert count == 1


def test_event_emitter_fail_does_not_break_pipeline(monkeypatch):
    calls = {}

    def _raise(self, event):
        calls["called"] = True
        raise RuntimeError("db down")

    monkeypatch.setattr(processor, "contract_event_emitter", type("E", (), {"emit": _raise})())
    processor._emit_contract_event(  # noqa: SLF001
        EventType.EMAIL_RECEIVED,
        ts_utc=time.time(),
        account_id="acc",
        entity_id=None,
        email_id=1,
        payload={},
    )
    assert calls["called"] is True


def test_gigachat_global_serialization():
    provider = _FakeGigaChat()
    router = LLMRouter(
        LLMRouterConfig(
            primary="gigachat",
            fallback="gigachat",
            gigachat_enabled=True,
            gigachat_api_key="x",
            cloudflare_enabled=False,
        ),
        providers={"gigachat": provider},
    )
    router._runtime_gigachat_enabled = True  # noqa: SLF001

    outputs: list[str] = []

    def _run():
        outputs.append(router.complete([{"role": "user", "content": "hi"}]))

    t1 = Thread(target=_run)
    t2 = Thread(target=_run)
    start = time.time()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    duration = time.time() - start
    assert len(outputs) == 2
    assert duration >= 0.35
    assert provider.calls[0] <= provider.calls[1]


def test_no_direct_gigachat_usage(tmp_path):
    restricted = []
    repo_root = Path(__file__).resolve().parents[1]
    for path in (repo_root / "mailbot_v26").rglob("*.py"):
        if "llm/router.py" in str(path) or "llm/providers.py" in str(path):
            continue
        if "tests" in path.parts:
            continue
        content = path.read_text(encoding="utf-8")
        if "GigaChatProvider" in content:
            restricted.append(str(path))
    assert not restricted, f"Direct GigaChat usage found: {restricted}"


def test_orchestrator_policy_does_not_change_pipeline():
    orchestrator = SystemOrchestrator()
    orchestrator.update_component("llm", healthy=False, reason="timeout")
    allowed, reason = orchestrator.decide_llm_allowed()
    assert allowed is False
    assert reason == "llm_disabled_by_mode"
    digest_allowed, _ = orchestrator.decide_digest_send_allowed()
    assert digest_allowed is True
    snap = orchestrator.snapshot()
    assert snap["mode"] == orchestrator.mode.value
    orchestrator.update_component("llm", healthy=True, reason=None)
    assert orchestrator.mode in {
        SystemMode.FULL,
        SystemMode.DEGRADED_NO_TELEGRAM,
        SystemMode.DEGRADED_NO_LLM,
        SystemMode.EMERGENCY_READ_ONLY,
    }
