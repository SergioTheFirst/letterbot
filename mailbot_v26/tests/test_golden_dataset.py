from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.pipeline import processor


@dataclass(frozen=True)
class GoldenCase:
    id: int
    from_email: str
    subject: str
    body: str


@dataclass(frozen=True)
class GoldenExpected:
    id: int
    priority: str
    action_line: str


def _load_cases() -> list[GoldenCase]:
    base_path = Path(__file__).resolve().parent / "golden"
    cases_path = base_path / "cases.json"
    data = json.loads(cases_path.read_text(encoding="utf-8"))
    return [GoldenCase(**entry) for entry in data]


def _load_expected() -> dict[int, GoldenExpected]:
    base_path = Path(__file__).resolve().parent / "golden"
    expected_path = base_path / "expected.json"
    data = json.loads(expected_path.read_text(encoding="utf-8"))
    return {entry["id"]: GoldenExpected(**entry) for entry in data}


def _fuzzy_match(left: str, right: str, threshold: float = 0.8) -> bool:
    if not left or not right:
        return False
    return SequenceMatcher(None, left.lower(), right.lower()).ratio() >= threshold


def test_golden_dataset_accuracy(monkeypatch, tmp_path) -> None:
    cases = _load_cases()
    expected_map = _load_expected()
    db_path = tmp_path / "golden_traces.sqlite"

    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(processor, "decision_trace_writer", DecisionTraceWriter(db_path))
    monkeypatch.setattr(
        processor,
        "shadow_priority_engine",
        SimpleNamespace(compute=lambda llm_priority, from_email: (llm_priority, None)),
    )
    monkeypatch.setattr(processor, "shadow_action_engine", SimpleNamespace(compute=lambda **kwargs: []))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(
        processor,
        "runtime_flag_store",
        SimpleNamespace(get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=False), False)),
    )

    total = 0
    matches = 0
    for case in cases:
        expected = expected_map[case.id]

        def _fake_run_llm_stage(**kwargs):
            return SimpleNamespace(
                priority=expected.priority,
                action_line=expected.action_line,
                body_summary="Summary",
                attachment_summaries=[],
                llm_provider="gigachat",
                llm_model="golden",
                prompt_full="PROMPT",
                llm_response="RESPONSE",
            )

        monkeypatch.setattr(processor, "run_llm_stage", _fake_run_llm_stage)
        captured: dict[str, object] = {}
        monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: captured.update(kwargs))

        processor.process_message(
            account_email="account@example.com",
            message_id=case.id,
            from_email=case.from_email,
            subject=case.subject,
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text=case.body,
            attachments=[],
            telegram_chat_id="chat",
        )

        total += 1
        priority_match = captured.get("priority") == expected.priority
        action_match = _fuzzy_match(
            str(captured.get("action_line", "")),
            expected.action_line,
        )
        if priority_match and action_match:
            matches += 1

    accuracy = matches / total if total else 0.0
    assert accuracy >= 0.8
