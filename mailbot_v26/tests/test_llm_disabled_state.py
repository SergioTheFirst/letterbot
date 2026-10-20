"""WP5a: an intentionally unwired or unconfigured LLM is "off", not degraded.

The per-email LLM stage is not wired yet (owner decision D1): every LLM
candidate email used to log processing_error stage=llm and flip the system to
DEGRADED_NO_LLM for good. Now "off" is its own state: no error noise, no mode
flip; DEGRADED_NO_LLM only when an enabled LLM actually fails, and the next
success recovers it.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest

from mailbot_v26.config.llm_queue import LLMQueueConfig
from mailbot_v26.llm.request_queue import LLMRequest
from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.pipeline import processor, stage_llm
from mailbot_v26.pipeline.stage_llm import LLMStageDisabled
from mailbot_v26.system.startup_health import HealthStatus, StartupHealthChecker
from mailbot_v26.system_health import OperationalMode, system_health
from mailbot_v26.worker.telegram_sender import DeliveryResult

SECRET_SUBJECT = "Invoice 42 SECRET-SUBJ-5A"
SECRET_SENDER = "secret.sender.5a@example.com"


@pytest.fixture(autouse=True)
def _fresh_system_health() -> Iterator[None]:
    system_health.reset()
    yield
    system_health.reset()


def _json_records(caplog: pytest.LogCaptureFixture, event: str) -> list[dict]:
    records = []
    for record in caplog.records:
        try:
            payload = json.loads(record.getMessage())
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("event") == event:
            records.append(payload)
    return records


# --- the stage ---------------------------------------------------------------


def test_unwired_stage_raises_disabled_instead_of_logging_an_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    ctx = SimpleNamespace(email_id=7, llm_result=None)

    with pytest.raises(LLMStageDisabled) as raised:
        stage_llm.run_llm_stage(
            subject=SECRET_SUBJECT,
            from_email=SECRET_SENDER,
            body_text="body",
            attachments=[{"filename": "a.pdf"}],
            ctx=ctx,
        )

    assert raised.value.reason == "llm_stage_not_wired"
    assert not _json_records(caplog, "processing_error")
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert SECRET_SUBJECT not in caplog.text
    assert SECRET_SENDER not in caplog.text


def test_unwired_stage_reports_its_disabled_reason() -> None:
    assert stage_llm.llm_stage_disabled_reason() == "llm_stage_not_wired"


# --- system health -------------------------------------------------------------


def test_disabled_llm_is_off_and_keeps_full_mode() -> None:
    change = system_health.disable_component("LLM", reason="llm_stage_not_wired")

    assert change is None
    assert system_health.mode == OperationalMode.FULL
    assert system_health.component_state("LLM") == "off"


def test_disabling_a_failed_llm_clears_degraded_mode() -> None:
    system_health.update_component("LLM", False, reason="provider down")
    assert system_health.mode == OperationalMode.DEGRADED_NO_LLM

    change = system_health.disable_component("LLM", reason="llm_not_configured")

    assert change is not None
    assert change.current == OperationalMode.FULL
    assert system_health.component_state("LLM") == "off"


def test_enabled_llm_failure_degrades_and_next_success_recovers() -> None:
    assert system_health.component_state("LLM") == "unknown"

    system_health.update_component("LLM", False, reason="provider down")
    assert system_health.mode == OperationalMode.DEGRADED_NO_LLM
    assert system_health.component_state("LLM") == "degraded"

    system_health.update_component("LLM", True)
    assert system_health.mode == OperationalMode.FULL
    assert system_health.component_state("LLM") == "ok"


def test_entering_off_state_is_logged_once(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    system_health.disable_component("LLM", reason="llm_stage_not_wired")
    system_health.disable_component("LLM", reason="llm_stage_not_wired")

    records = _json_records(caplog, "system_component_state_changed")
    assert len(records) == 1
    assert records[0]["component"] == "LLM"
    assert records[0]["from"] == "unknown"
    assert records[0]["to"] == "off"
    assert records[0]["reason"] == "llm_stage_not_wired"


# --- the per-email pipeline ----------------------------------------------------


def _setup_pipeline(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    calls: dict[str, list] = {
        "sent": [],
        "notices": [],
        "queued": [],
        "worker": [],
        "traces": [],
    }
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, path=":memory:"),
    )
    monkeypatch.setattr(
        processor, "decision_trace_writer", SimpleNamespace(write=lambda **kwargs: None)
    )
    monkeypatch.setattr(
        processor,
        "context_store",
        SimpleNamespace(
            resolve_sender_entity=lambda **kwargs: None,
            record_interaction_event=lambda **kwargs: (None, None),
            recompute_email_frequency=lambda **kwargs: (0.0, 0),
        ),
    )
    monkeypatch.setattr(
        processor,
        "shadow_priority_engine",
        SimpleNamespace(compute=lambda **kwargs: ("🔵", None)),
    )
    monkeypatch.setattr(
        processor, "shadow_action_engine", SimpleNamespace(compute=lambda **kwargs: [])
    )
    monkeypatch.setattr(
        processor,
        "runtime_flag_store",
        SimpleNamespace(
            get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=False), False)
        ),
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.8,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_PRIORITY_V2=False,
        ),
    )
    # Every email is an LLM candidate within budget: the LLM stage always runs.
    monkeypatch.setattr(
        processor,
        "is_top_percentile",
        lambda **kwargs: SimpleNamespace(is_top=True, anchored=True),
    )
    monkeypatch.setattr(
        processor, "_resolve_llm_budget_access", lambda **kwargs: (True, True, False)
    )
    # The background queue is enabled, so a "failure" would queue a retry.
    monkeypatch.setattr(
        processor,
        "get_llm_queue_config",
        lambda: LLMQueueConfig(llm_request_queue_enabled=True, max_concurrent_llm_calls=1),
    )
    monkeypatch.setattr(
        processor,
        "_enqueue_llm_request_with_retry",
        lambda request, *, email_id: calls["queued"].append(email_id) or True,
    )
    monkeypatch.setattr(
        processor, "_ensure_llm_worker", lambda: calls["worker"].append(True)
    )

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        calls["sent"].append(email_id)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    monkeypatch.setattr(
        processor,
        "send_system_notice",
        lambda **kwargs: calls["notices"].append(kwargs) or True,
    )
    original_emit_trace = processor._emit_decision_trace

    def _capture_trace(trace, **kwargs) -> None:
        calls["traces"].append(trace)
        original_emit_trace(trace, **kwargs)

    monkeypatch.setattr(processor, "_emit_decision_trace", _capture_trace)
    return calls


def _process(message_id: int) -> None:
    processor.process_message(
        account_email="wp5a@example.com",
        message_id=message_id,
        from_email=SECRET_SENDER,
        subject=SECRET_SUBJECT,
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Please review the attached invoice.",
        attachments=[],
        telegram_chat_id="chat",
    )


def test_unwired_llm_stage_keeps_full_mode_without_error_noise(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    calls = _setup_pipeline(monkeypatch)
    # The real, unwired stage (other tests replace it).
    monkeypatch.setattr(processor, "run_llm_stage", stage_llm.run_llm_stage)
    caplog.set_level(logging.INFO)

    _process(5101)
    _process(5102)

    assert system_health.mode == OperationalMode.FULL
    assert system_health.component_state("LLM") == "off"
    llm_errors = [
        r for r in _json_records(caplog, "processing_error") if r.get("stage") == "llm"
    ]
    assert llm_errors == []
    assert calls["sent"] == [5101, 5102]
    assert calls["notices"] == []
    assert calls["queued"] == []
    assert calls["worker"] == []
    gate_traces = [t for t in calls["traces"] if t.decision_kind == "LLM_GATE"]
    assert gate_traces
    assert all("LLM_STAGE_DISABLED" in t.signals_fired for t in gate_traces)
    assert all("LLM_CALLED_DIRECT" not in t.signals_fired for t in gate_traces)
    assert SECRET_SUBJECT not in caplog.text


def test_enabled_llm_failure_still_degrades_then_recovers(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    calls = _setup_pipeline(monkeypatch)
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(processor, "run_llm_stage", stage_llm.run_llm_stage)
    _process(5201)
    assert system_health.component_state("LLM") == "off"

    # An enabled LLM that fails is a real degradation...
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: None)
    _process(5202)
    assert system_health.mode == OperationalMode.DEGRADED_NO_LLM
    assert system_health.component_state("LLM") == "degraded"
    assert [
        r for r in _json_records(caplog, "processing_error") if r.get("stage") == "llm"
    ]

    # ...and the next success recovers it.
    monkeypatch.setattr(
        processor,
        "run_llm_stage",
        lambda **kwargs: SimpleNamespace(
            priority="🔵",
            action_line="Ответить",
            body_summary="Краткое описание письма.",
            attachment_summaries=[],
            llm_provider="cloudflare",
        ),
    )
    _process(5203)
    assert system_health.mode == OperationalMode.FULL
    assert system_health.component_state("LLM") == "ok"
    assert calls["sent"] == [5201, 5202, 5203]


def test_queued_request_with_unwired_stage_is_skipped_not_failed(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(processor, "run_llm_stage", stage_llm.run_llm_stage)
    caplog.set_level(logging.INFO)
    request = LLMRequest(
        account_email="wp5a@example.com",
        email_id=5301,
        subject=SECRET_SUBJECT,
        from_email=SECRET_SENDER,
        body_text="body",
        attachments=[],
        received_at=datetime(2024, 1, 1, 12, 0),
        input_chars=4,
    )

    processor._process_llm_queue_request(request)

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    skipped = _json_records(caplog, "llm_queue_request_skipped")
    assert skipped and skipped[-1]["reason"] == "llm_stage_not_wired"
    assert SECRET_SUBJECT not in caplog.text


def test_health_snapshot_payload_reports_ai_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processor, "metrics_aggregator", SimpleNamespace(snapshot=lambda: {})
    )
    monkeypatch.setattr(
        processor, "system_gates", SimpleNamespace(evaluate=lambda _metrics: None)
    )

    system_health.disable_component("LLM", reason="llm_stage_not_wired")
    payload = processor._build_health_snapshot_payload()
    assert payload["llm_state"] == "off"
    assert payload["system_mode"] == OperationalMode.FULL.value

    system_health.update_component("LLM", False, reason="provider down")
    payload = processor._build_health_snapshot_payload()
    assert payload["llm_state"] == "degraded"
    assert payload["system_mode"] == OperationalMode.DEGRADED_NO_LLM.value


# --- startup health ------------------------------------------------------------


def _llm_results(*, cloudflare: str, gigachat: str) -> list[dict[str, str]]:
    return [
        {"component": "DB", "status": HealthStatus.OK, "details": "ok"},
        {"component": "Telegram", "status": HealthStatus.OK, "details": "ok"},
        {"component": "Cloudflare", "status": cloudflare, "details": "x"},
        {"component": "GigaChat", "status": gigachat, "details": "x"},
    ]


def _checker(tmp_path: Path) -> StartupHealthChecker:
    return StartupHealthChecker(tmp_path, SimpleNamespace())


def test_startup_reports_unwired_llm_off_not_degraded(tmp_path: Path) -> None:
    mode = _checker(tmp_path).evaluate_mode(
        _llm_results(cloudflare=HealthStatus.DEGRADED, gigachat=HealthStatus.DEGRADED)
    )

    assert mode == OperationalMode.FULL
    assert system_health.component_state("LLM") == "off"


def test_startup_reports_unwired_llm_off_even_with_configured_provider(
    tmp_path: Path,
) -> None:
    mode = _checker(tmp_path).evaluate_mode(
        _llm_results(cloudflare=HealthStatus.OK, gigachat=HealthStatus.DEGRADED)
    )

    assert mode == OperationalMode.FULL
    assert system_health.component_state("LLM") == "off"


@pytest.mark.parametrize(
    ("cloudflare", "gigachat", "expected_mode", "expected_state"),
    [
        (HealthStatus.DEGRADED, HealthStatus.DEGRADED, OperationalMode.FULL, "off"),
        (HealthStatus.OK, HealthStatus.DEGRADED, OperationalMode.FULL, "ok"),
        (HealthStatus.OK, HealthStatus.FAILED, OperationalMode.FULL, "ok"),
        (
            HealthStatus.FAILED,
            HealthStatus.DEGRADED,
            OperationalMode.DEGRADED_NO_LLM,
            "degraded",
        ),
    ],
)
def test_startup_with_wired_stage_degrades_only_on_provider_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cloudflare: str,
    gigachat: str,
    expected_mode: OperationalMode,
    expected_state: str,
) -> None:
    monkeypatch.setattr(stage_llm, "llm_stage_disabled_reason", lambda: None)

    mode = _checker(tmp_path).evaluate_mode(
        _llm_results(cloudflare=cloudflare, gigachat=gigachat)
    )

    assert mode == expected_mode
    assert system_health.component_state("LLM") == expected_state
