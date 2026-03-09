from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from mailbot_v26.domain.issuer_identity import normalize_sender_identity

if TYPE_CHECKING:
    from mailbot_v26.pipeline.processor import MessageInterpretation


@dataclass(frozen=True, slots=True)
class TelegramRenderRequest:
    email_id: int
    received_at: datetime
    sender_email: str
    subject: str
    interpretation: MessageInterpretation | None = None
    sender_name: str | None = None
    action_line: str = ""
    mail_type: str = ""
    body_summary: str = ""
    body_text: str = ""
    attachments: list[dict[str, Any]] = field(default_factory=list)
    attachment_summaries: list[dict[str, Any]] = field(default_factory=list)
    insights: list[Any] = field(default_factory=list)
    commitments: list[Any] = field(default_factory=list)
    relationship_profile: dict[str, Any] | None = None
    insight_digest: Any = None
    preview_hint: str | None = None
    llm_failed: bool = False
    signal_invalid: bool = False
    enable_premium_clarity: bool = False
    telegram_chat_id: str = "render-contract-chat"
    telegram_bot_token: str = "render-contract-token"
    account_email: str = "render-contract@example.com"


@dataclass(frozen=True, slots=True)
class TelegramRenderResult:
    text: str
    parse_mode: str
    reply_markup: dict[str, Any] | None
    render_mode: str
    message_ref: str
    timestamp_iso: str
    sender_identity_key: str
    sender_identity_label: str
    payload_invalid: bool


def _build_sender_identity(request: TelegramRenderRequest) -> dict[str, str]:
    interpretation = request.interpretation
    display_name = request.sender_name or (
        str(interpretation.issuer_label).strip()
        if interpretation is not None and interpretation.issuer_label
        else ""
    )
    doc_marker = (
        str(interpretation.issuer_tax_id).strip()
        if interpretation is not None and interpretation.issuer_tax_id
        else ""
    )
    identity = normalize_sender_identity(
        sender_email=request.sender_email,
        display_name=display_name,
        subject_hint=request.subject,
        doc_marker=doc_marker,
    )
    label = str(display_name or identity.get("display") or request.sender_email or "неизвестно").strip()
    return {
        "key": str(identity.get("key") or request.sender_email or "unknown").strip(),
        "label": label or "неизвестно",
    }


def render_email_notification(request: TelegramRenderRequest) -> TelegramRenderResult:
    from mailbot_v26.pipeline import processor

    if request.enable_premium_clarity:
        render_result = processor._render_notification(
            message_id=request.email_id,
            received_at=request.received_at,
            priority=(
                request.interpretation.priority
                if request.interpretation is not None
                else "🔵"
            ),
            from_email=request.sender_email,
            from_name=request.sender_name,
            subject=request.subject,
            action_line=request.action_line,
            mail_type=request.mail_type,
            body_summary=request.body_summary,
            body_text=request.body_text,
            attachments=list(request.attachments),
            llm_result=SimpleNamespace(failed=request.llm_failed, error=False),
            signal_quality=SimpleNamespace(is_usable=not request.signal_invalid),
            aggregated_insights=list(request.insights),
            insight_digest=request.insight_digest,
            telegram_chat_id=request.telegram_chat_id,
            telegram_bot_token=request.telegram_bot_token,
            account_email=request.account_email,
            attachment_summaries=list(request.attachment_summaries),
            commitments=list(request.commitments),
            relationship_profile=request.relationship_profile,
            interpretation=request.interpretation,
            enable_premium_clarity=True,
            preview_hint=request.preview_hint,
        )
    else:
        build_context = processor.TelegramBuildContext(
            email_id=request.email_id,
            received_at=request.received_at,
            priority=(
                request.interpretation.priority
                if request.interpretation is not None
                else "🔵"
            ),
            from_email=request.sender_email,
            subject=request.subject,
            action_line=request.action_line,
            mail_type=request.mail_type,
            body_summary=request.body_summary,
            body_text=request.body_text,
            attachment_summary="",
            attachment_details=[],
            attachment_files=list(request.attachments),
            attachments_count=len(request.attachments),
            extracted_text_len=len(request.body_text or ""),
            llm_failed=request.llm_failed,
            signal_invalid=request.signal_invalid,
            insights=list(request.insights),
            insight_digest=request.insight_digest,
            commitments_present=bool(request.commitments),
            relationship_profile=request.relationship_profile,
            interpretation=request.interpretation,
            preview_hint=request.preview_hint,
        )
        payload, render_mode, payload_invalid = processor.build_telegram_payload(
            build_context
        )
        render_result = SimpleNamespace(
            payload=payload,
            render_mode=render_mode,
            payload_invalid=payload_invalid,
        )
    identity = _build_sender_identity(request)
    return TelegramRenderResult(
        text=render_result.payload.html_text,
        parse_mode="HTML",
        reply_markup=render_result.payload.reply_markup,
        render_mode=render_result.render_mode.value,
        message_ref=str(request.email_id),
        timestamp_iso=request.received_at.isoformat(),
        sender_identity_key=identity["key"],
        sender_identity_label=identity["label"],
        payload_invalid=render_result.payload_invalid,
    )


__all__ = [
    "TelegramRenderRequest",
    "TelegramRenderResult",
    "render_email_notification",
]
