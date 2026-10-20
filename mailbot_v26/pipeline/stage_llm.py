from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mailbot_v26.bot_core.pipeline import PipelineContext

LLM_STAGE_NOT_WIRED = "llm_stage_not_wired"


class LLMStageDisabled(RuntimeError):
    """The per-email LLM stage is intentionally off (not wired / not configured).

    Not an LLM failure: callers keep the deterministic (heuristic) result and
    report AI as "off" instead of degrading the system mode.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def llm_stage_disabled_reason() -> str | None:
    """Why the per-email LLM stage is off, or None when it can run.

    Per-email LLM wiring is in the backlog (owner decision D1, 2026-10-19), so
    the stage is off. Whoever wires it returns None here (or a reason such as
    "no provider configured") and replaces the raise in run_llm_stage.
    """
    return LLM_STAGE_NOT_WIRED


def run_llm_stage(
    *,
    subject: str | None = None,
    from_email: str | None = None,
    body_text: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
    ctx: "PipelineContext | None" = None,
) -> Any:
    """
    Authoritative LLM stage entrypoint for the legacy pipeline.

    A result already present on ``ctx`` is returned as is. Otherwise the stage
    raises LLMStageDisabled: it logs nothing itself, so no email data (subject,
    sender) can reach the log, and callers decide how to report "AI off".
    """
    if ctx is not None:
        existing = getattr(ctx, "llm_result", None)
        if existing:
            return existing
    raise LLMStageDisabled(llm_stage_disabled_reason() or LLM_STAGE_NOT_WIRED)
