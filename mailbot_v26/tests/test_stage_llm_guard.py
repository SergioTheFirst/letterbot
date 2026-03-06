from __future__ import annotations

import sys
import types
from types import SimpleNamespace

from mailbot_v26.pipeline import stage_llm


def test_stage_llm_does_not_call_full_processor(monkeypatch) -> None:
    called = {"value": False}

    fake_module = types.ModuleType("mailbot_v26.bot_core.pipeline")

    def _unexpected(_ctx):
        called["value"] = True
        raise AssertionError("must not call full processor")

    fake_module.stage_llm = _unexpected
    monkeypatch.setitem(sys.modules, "mailbot_v26.bot_core.pipeline", fake_module)

    ctx = SimpleNamespace(email_id=123, llm_result={"text": "ok"})
    result = stage_llm.run_llm_stage(ctx=ctx)

    assert result == {"text": "ok"}
    assert called["value"] is False
