"""Formatter entrypoint for driver-first Telegram output."""

from __future__ import annotations

from mailbot_v26.pipeline.processor import MessageProcessor, InboundMessage, Attachment

__all__ = ["MessageProcessor", "InboundMessage", "Attachment"]
