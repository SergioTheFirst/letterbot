"""Formatter entrypoint for driver-first Telegram output with domain intelligence."""

from __future__ import annotations

from mailbot_v26.domain.domain_classifier import DomainClassifier, MailTypeClassifier
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor

__all__ = [
    "MessageProcessor",
    "InboundMessage",
    "Attachment",
    "DomainClassifier",
    "MailTypeClassifier",
]
