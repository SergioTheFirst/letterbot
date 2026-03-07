from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mailbot_v26.observability import get_logger

if TYPE_CHECKING:
    from mailbot_v26.bot_core.pipeline import PipelineContext

logger = get_logger("mailbot")


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
        existing = getattr(ctx, "llm_result", None)
        if existing:
            return existing
        logger.warning(
            "llm_stage_ctx_unconfigured",
            email_id=getattr(ctx, "email_id", None),
        )
        return None

    attachments = attachments or []
    logger.error(
        "processing_error",
        stage="llm",
        email_id=None,
        error=(
            "LLM stage is not configured; subject=%s from=%s attachments=%d"
            % (subject or "", from_email or "", len(attachments))
        ),
    )
    return None
