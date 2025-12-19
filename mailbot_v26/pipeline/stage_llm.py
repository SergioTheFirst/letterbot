from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def run_llm_stage(
    *,
    subject: str,
    from_email: str,
    body_text: str,
    attachments: list[dict[str, Any]],
) -> Any:
    """
    Authoritative LLM stage entrypoint for the legacy pipeline.

    The implementation is expected to be provided by the production LLM
    stack. This shim keeps the import graph stable and avoids startup
    errors when the stage is monkeypatched in tests.
    """
    logger.error(
        "LLM stage is not configured; subject=%s from=%s attachments=%d",
        subject,
        from_email,
        len(attachments),
    )
    return None
