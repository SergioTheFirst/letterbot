from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mailbot_v26.bot_core.pipeline import PipelineContext

logger = logging.getLogger(__name__)


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

    The implementation is expected to be provided by the production LLM
    stack. This shim keeps the import graph stable and avoids startup
    errors when the stage is monkeypatched in tests.
    """
    if ctx is not None:
        from mailbot_v26.bot_core.pipeline import stage_llm as core_stage_llm

        core_stage_llm(ctx)
        return ctx.llm_result
    attachments = attachments or []
    logger.error(
        "LLM stage is not configured; subject=%s from=%s attachments=%d",
        subject or "",
        from_email or "",
        len(attachments),
    )
    return None
