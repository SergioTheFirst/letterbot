"""Formatter entrypoint for driver-first Telegram output."""

from __future__ import annotations

from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor

__all__ = [
    "MessageProcessor",
    "InboundMessage",
    "Attachment",
    "MailTypeClassifier",
]
