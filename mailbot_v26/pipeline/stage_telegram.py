from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def send_to_telegram(
    *,
    chat_id: str,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str,
    body_summary: str,
    attachment_summaries: list[dict[str, Any]],
    account_email: str,
) -> None:
    """
    Telegram stage entrypoint for the legacy pipeline.

    The formatting/sending implementation is intentionally deferred to the
    production pipeline. This shim preserves the import graph and keeps
    side-effect behavior unchanged unless it is overridden.
    """
    logger.info(
        "Telegram stage not configured for legacy pipeline: chat_id=%s account=%s",
        chat_id,
        account_email,
    )
