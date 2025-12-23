from __future__ import annotations

from .telegram_payload import TelegramPayload

from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")


def enqueue_tg(
    *,
    email_id: int,
    payload: TelegramPayload,
) -> None:
    """
    Telegram stage entrypoint for the legacy pipeline.

    The formatting/sending implementation is intentionally deferred to the
    production pipeline. This shim preserves the import graph and keeps
    side-effect behavior unchanged unless it is overridden.
    """
    logger.info(
        "telegram_stage_unconfigured",
        email_id=email_id,
        chat_id=payload.metadata.get("chat_id"),
        account_email=payload.metadata.get("account_email"),
        telegram_text=payload.html_text,
    )


def send_preview_to_telegram(
    *,
    chat_id: str,
    preview_text: str,
    account_email: str,
) -> None:
    """
    Preview actions Telegram stage entrypoint for the legacy pipeline.

    Uses plain text only; formatting/sending is deferred to production.
    """
    logger.info(
        "telegram_preview_unconfigured",
        chat_id=chat_id,
        account_email=account_email,
    )


def send_system_notice(
    *,
    chat_id: str,
    notice_text: str,
    account_email: str,
) -> None:
    """
    Optional system-mode notice for Telegram.

    Uses plain text only; formatting/sending is deferred to production.
    """
    logger.info(
        "telegram_system_notice_unconfigured",
        chat_id=chat_id,
        account_email=account_email,
        notice_text=notice_text,
    )
