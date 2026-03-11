from __future__ import annotations

import html
import io
import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from email import message_from_bytes
from email.message import Message as EmailMessage
from pathlib import Path
from typing import Dict, List
import zipfile

from mailbot_v26.bot_core.extractors.doc import extract_docx_text
from mailbot_v26.bot_core.extractors.excel import extract_excel_text
from mailbot_v26.bot_core.extractors.pdf import extract_pdf
from mailbot_v26.constants import (
    MAX_ATTACHMENT_BYTES,
    MAX_CHARS_PER_ATTACHMENT,
    MAX_TOTAL_EXTRACTED_CHARS,
    MAX_TOTAL_MAIL_BYTES,
)
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor
from mailbot_v26.pipeline.tg_renderer import build_attachment_insight
from mailbot_v26.pipeline.tg_renderer import render_telegram_message
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.telegram.decision_trace_ui import build_email_actions_keyboard
from mailbot_v26.text.mime_utils import decode_bytes, decode_mime_header
from mailbot_v26.text.sanitize import sanitize_text
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.worker.telegram_sender import DeliveryResult, send_telegram

try:  # local import to avoid circular typing issues
    from mailbot_v26.config_loader import (
        AccountConfig,
        BotConfig,
        load_telegram_ui_config,
    )
    from mailbot_v26.account_identity import normalize_login
except Exception:  # pragma: no cover - defensive import for early boot
    AccountConfig = None  # type: ignore
    BotConfig = None  # type: ignore

    def load_telegram_ui_config():  # type: ignore
        return type("_TgUi", (), {"show_decision_trace": False})()


logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    email_id: int
    account_email: str
    uid: int

    # PARSE
    body_text: str | None = None
    attachments_text: list[str] | None = None

    # LLM
    llm_result: dict | None = None

    # TG
    telegram_text: str | None = None


PIPELINE_CACHE: Dict[int, PipelineContext] = {}
PIPELINE_RAW_CACHE: Dict[int, bytes] = {}
PIPELINE_INBOUND_CACHE: Dict[int, InboundMessage] = {}

_PIPELINE_CONFIG: BotConfig | None = None
_PIPELINE_PROCESSOR: MessageProcessor | None = None
_ACCOUNT_MAP: Dict[str, AccountConfig] = {}
_ENABLE_PREMIUM_PROCESSOR: bool = False


def configure_pipeline(
    config: BotConfig,
    processor: MessageProcessor,
    *,
    enable_premium_processor: bool = False,
) -> None:
    global _PIPELINE_CONFIG, _PIPELINE_PROCESSOR, _ACCOUNT_MAP, _ENABLE_PREMIUM_PROCESSOR
    _PIPELINE_CONFIG = config
    _PIPELINE_PROCESSOR = processor
    _ACCOUNT_MAP = {normalize_login(acc.login): acc for acc in (config.accounts or [])}
    _ENABLE_PREMIUM_PROCESSOR = bool(enable_premium_processor)


def remember_raw_email(email_id: int, raw_email: bytes) -> None:
    PIPELINE_RAW_CACHE[email_id] = raw_email


def store_inbound(email_id: int, inbound: InboundMessage) -> None:
    PIPELINE_INBOUND_CACHE[email_id] = inbound


def _strip_html_content(text: str) -> str:
    working = re.sub(
        r"<(head|script|style)[^>]*>.*?</\1>",
        " ",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    working = re.sub(r"<!--.*?-->", " ", working, flags=re.DOTALL)
    working = re.sub(
        r"<(br|p|div|tr|td|th|li|ul|ol|h[1-6])[^>]*>",
        "\n",
        working,
        flags=re.IGNORECASE,
    )
    working = re.sub(
        r"</(p|div|tr|td|th|li|ul|ol|h[1-6])>", "\n", working, flags=re.IGNORECASE
    )
    working = re.sub(r"<[^>]+>", " ", working)
    working = html.unescape(working)
    working = re.sub(r"[ \t]+", " ", working)
    working = re.sub(r"(\n\s*){2,}", "\n\n", working)
    return working.strip()


def _normalize_text_content(text: str) -> str:
    normalized = unicodedata.normalize("NFC", text or "")
    cleaned = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]", "", normalized)
    return cleaned.strip()


def _decode_subject(email_obj: EmailMessage) -> str:
    raw_subject = email_obj.get("Subject", "")
    return decode_mime_header(raw_subject)


def _decode_from(email_obj: EmailMessage) -> str:
    raw_from = email_obj.get("From", "")
    return decode_mime_header(raw_from)


def _decode_part_payload(part: EmailMessage) -> str:
    payload = part.get_payload(decode=True) or b""
    charset = part.get_content_charset()
    text = decode_bytes(payload, charset)
    return _normalize_text_content(text)


def _is_real_attachment(part: EmailMessage, filename: str, payload_size: int) -> bool:
    if part.is_multipart():
        return False

    disposition = (part.get_content_disposition() or "").lower()
    content_type = (part.get_content_type() or "").lower()
    lower_name = (filename or "").strip().lower()
    extension = Path(filename or "").suffix.lower()

    if disposition == "inline":
        return False

    if not filename:
        small_payload = payload_size <= 2048
        if disposition != "attachment" or small_payload:
            return False
        if content_type in {"text/html", "text/css"}:
            return False

    if lower_name in {"attachment.bin", "noname", "unnamed", "part.bin"}:
        return False

    if content_type in {"text/html", "text/css", "application/xhtml+xml"}:
        return False

    if extension in {".css", ".woff", ".woff2", ".ttf", ".otf", ".svg"}:
        return False

    return True


def _extract_body(email_obj: EmailMessage) -> str:
    plain_text: str | None = None
    html_text: str | None = None

    if email_obj.is_multipart():
        for part in email_obj.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get_content_disposition() == "attachment":
                continue

            content_type = (part.get_content_type() or "").lower()
            if content_type.startswith("text/plain") and plain_text is None:
                try:
                    decoded = _decode_part_payload(part)
                    if decoded:
                        plain_text = decoded
                except Exception:
                    continue
            elif content_type.startswith("text/html") and html_text is None:
                try:
                    decoded = _decode_part_payload(part)
                    if decoded:
                        html_text = _strip_html_content(decoded)
                        html_text = _normalize_text_content(html_text)
                except Exception:
                    continue

        if plain_text:
            return plain_text
        if html_text:
            return html_text
        return ""

    try:
        content_type = (email_obj.get_content_type() or "").lower()
        decoded = _decode_part_payload(email_obj)
        if content_type.startswith("text/html"):
            decoded = _strip_html_content(decoded)
        return _normalize_text_content(decoded)
    except Exception:
        return ""


def _zip_uncompressed_size(file_bytes: bytes) -> int | None:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            return sum(info.file_size for info in zf.infolist())
    except Exception:
        return None


def _estimate_payload_size(part: EmailMessage) -> int:
    header_size = part.get("Content-Length")
    if header_size:
        try:
            return int(str(header_size).strip())
        except ValueError:
            pass

    payload = part.get_payload(decode=False)
    if payload is None:
        return 0
    if isinstance(payload, list):
        return 0

    payload_text = payload if isinstance(payload, str) else str(payload)
    transfer_encoding = (part.get("Content-Transfer-Encoding") or "").lower()
    if transfer_encoding == "base64":
        compact = re.sub(r"\s+", "", payload_text)
        padding = compact.count("=")
        return max(0, (len(compact) * 3) // 4 - padding)
    return len(payload_text)


def _hard_truncate_extracted_text(
    text: str,
    *,
    filename: str,
    max_chars: int,
) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    logger.info(
        "extraction_truncated",
        extra={
            "event": "extraction_truncated",
            "attachment_filename": filename,
            "original_chars": len(text),
            "max_chars": max_chars,
            "reason": "per_attachment",
        },
    )
    return text[:max_chars]


def _extract_attachment_text(
    att: Attachment,
    *,
    max_chars: int | None = None,
    max_zip_uncompressed_bytes: int | None = None,
) -> str:
    """
    Extract text from a single attachment.

    Contract: returns a string (possibly empty) and never raises.
    """
    if max_chars is None:
        max_chars = MAX_CHARS_PER_ATTACHMENT
    if max_zip_uncompressed_bytes is None:
        max_zip_uncompressed_bytes = 80 * 1024 * 1024
    name_lower = (att.filename or "").lower()
    content_type = (att.content_type or "").lower()

    logger.debug(
        "Extracting from %s (type: %s, size: %d bytes)",
        att.filename,
        content_type,
        att.size_bytes or len(att.content),
    )

    try:
        if (
            name_lower.endswith((".zip", ".docx", ".xlsx"))
            and max_zip_uncompressed_bytes > 0
        ):
            uncompressed_size = _zip_uncompressed_size(att.content)
            if (
                uncompressed_size is not None
                and uncompressed_size > max_zip_uncompressed_bytes
            ):
                logger.warning(
                    "zip_bomb_guard_triggered filename=%s uncompressed_bytes=%s max_bytes=%s",
                    att.filename,
                    uncompressed_size,
                    max_zip_uncompressed_bytes,
                )
                return "[ERROR: Compressed content too large/untrusted]"

        sanitize_limit = max(max_chars * 5, max_chars) if max_chars > 0 else 8000
        if name_lower.endswith(".pdf"):
            extracted_text, zero_reason = extract_pdf(att.content, att.filename)
            text = sanitize_text(
                extracted_text,
                max_len=sanitize_limit,
            )
            text = _hard_truncate_extracted_text(
                text, filename=att.filename, max_chars=max_chars
            )
            if text:
                logger.info("PDF extraction: %d chars from %s", len(text), att.filename)
            else:
                logger.warning(
                    "pdf_zero_text filename=%s reason=%s",
                    att.filename,
                    zero_reason,
                )
            return text

        if name_lower.endswith((".doc", ".docx")):
            text = sanitize_text(
                extract_docx_text(att.content, att.filename),
                max_len=sanitize_limit,
            )
            text = _hard_truncate_extracted_text(
                text, filename=att.filename, max_chars=max_chars
            )
            logger.info("DOC extraction: %d chars from %s", len(text), att.filename)
            return text

        if name_lower.endswith((".xls", ".xlsx")):
            text = sanitize_text(
                extract_excel_text(att.content, att.filename),
                max_len=sanitize_limit,
            )
            text = _hard_truncate_extracted_text(
                text, filename=att.filename, max_chars=max_chars
            )
            logger.info("Excel extraction: %d chars from %s", len(text), att.filename)
            return text

        if content_type.startswith("text") or name_lower.endswith(
            (".txt", ".csv", ".log", ".md", ".json")
        ):
            decoded = att.content.decode("utf-8", errors="ignore")
            text = sanitize_text(decoded, max_len=sanitize_limit)
            text = _hard_truncate_extracted_text(
                text, filename=att.filename, max_chars=max_chars
            )
            logger.info("Text extraction: %d chars from %s", len(text), att.filename)
            return text

    except Exception as e:
        logger.error("Extraction failed for %s: %s", att.filename, e, exc_info=True)

    return ""


def _extract_attachments(
    email_obj: EmailMessage,
    max_attachment_mb: int | None = None,
    *,
    max_mb: int | None = None,
    max_zip_uncompressed_mb: int | None = None,
    max_extracted_chars: int | None = None,
    max_extracted_total_chars: int | None = None,
) -> List[Attachment]:
    """
    Extract attachments from an email message.

    Contract: returns Attachment objects with text (possibly empty), content cleared,
    and per-attachment limits applied.
    """
    if max_attachment_mb is None:
        max_attachment_mb = max_mb
    if max_attachment_mb is None:
        max_attachment_mb = max(1, MAX_ATTACHMENT_BYTES // (1024 * 1024))
    if max_zip_uncompressed_mb is None:
        max_zip_uncompressed_mb = 80
    if max_extracted_chars is None:
        max_extracted_chars = MAX_CHARS_PER_ATTACHMENT
    if max_extracted_total_chars is None:
        max_extracted_total_chars = MAX_TOTAL_EXTRACTED_CHARS
    byte_limit = min(max_attachment_mb * 1024 * 1024, MAX_ATTACHMENT_BYTES)
    total_mail_limit = MAX_TOTAL_MAIL_BYTES
    max_extracted_chars = min(max_extracted_chars, MAX_CHARS_PER_ATTACHMENT)
    max_extracted_total_chars = min(
        max_extracted_total_chars, MAX_TOTAL_EXTRACTED_CHARS
    )
    max_zip_uncompressed_bytes = max_zip_uncompressed_mb * 1024 * 1024
    entries: list[tuple[str, str, bytes, int, str | None]] = []
    candidates: list[tuple[int, str, str, bytes, int]] = []
    total_bytes = 0
    total_limit_reached = False
    for part in email_obj.walk():
        raw_filename = part.get_filename()
        filename = decode_mime_header(raw_filename or "")
        if not filename:
            alt_name = part.get_param("name") or ""
            filename = decode_mime_header(alt_name)

        if filename.lower().startswith("attachment.bin"):
            fallback_name = decode_mime_header(part.get_param("name") or "")
            filename = fallback_name or ""

        try:
            payload_size = _estimate_payload_size(part)
            if not _is_real_attachment(part, filename, payload_size):
                continue
            content_type = part.get_content_type() or ""
            too_large = byte_limit > 0 and payload_size > byte_limit
            total_limit_triggered = total_limit_reached or (
                total_mail_limit > 0 and total_bytes + payload_size > total_mail_limit
            )
            if too_large or total_limit_triggered:
                skipped_reason = "too_large" if too_large else "total_limit"
                total_bytes += payload_size
                if total_mail_limit > 0 and total_bytes > total_mail_limit:
                    total_limit_reached = True
                logger.info(
                    "attachment_skipped",
                    extra={
                        "event": "attachment_skipped",
                        "attachment_filename": filename,
                        "size_bytes": payload_size,
                        "skipped_reason": skipped_reason,
                    },
                )
                entries.append(
                    (
                        filename,
                        content_type,
                        b"",
                        payload_size,
                        skipped_reason,
                    )
                )
                continue
            payload = part.get_payload(decode=True) or b""
            if not filename or filename.lower().startswith("attachment.bin"):
                continue
            total_bytes += payload_size
            entry = (
                filename,
                content_type,
                payload,
                payload_size,
                None,
            )
            entries.append(entry)
            candidates.append((len(entries) - 1, *entry[:-1]))
        except Exception:
            continue
    if not candidates and not entries:
        return []

    def _extract(candidate: tuple[int, str, str, bytes, int]) -> tuple[int, str]:
        index, filename, content_type, payload, payload_size = candidate
        temp_attachment = Attachment(
            filename=filename,
            content=payload,
            content_type=content_type,
            text="",
            size_bytes=payload_size,
        )
        extracted_text = _extract_attachment_text(
            temp_attachment,
            max_chars=max_extracted_chars,
            max_zip_uncompressed_bytes=max_zip_uncompressed_bytes,
        )
        temp_attachment.content = b""
        del temp_attachment
        return index, extracted_text

    extracted_map: dict[int, str] = {}
    if candidates:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(_extract, candidate) for candidate in candidates]
            for future in futures:
                index, extracted_text = future.result()
                extracted_map[index] = extracted_text

    attachments: List[Attachment] = []
    remaining_chars = (
        max_extracted_total_chars if max_extracted_total_chars > 0 else None
    )
    for index, (
        filename,
        content_type,
        _payload,
        payload_size,
        skipped_reason,
    ) in enumerate(entries):
        extracted_text = "" if skipped_reason else extracted_map.get(index, "")
        if remaining_chars is not None:
            if remaining_chars <= 0:
                if extracted_text:
                    logger.info(
                        "extraction_truncated",
                        extra={
                            "event": "extraction_truncated",
                            "attachment_filename": filename,
                            "original_chars": len(extracted_text),
                            "max_chars": 0,
                            "reason": "total_limit",
                        },
                    )
                extracted_text = ""
            elif len(extracted_text) > remaining_chars:
                logger.info(
                    "extraction_truncated",
                    extra={
                        "event": "extraction_truncated",
                        "attachment_filename": filename,
                        "original_chars": len(extracted_text),
                        "max_chars": remaining_chars,
                        "reason": "total_limit",
                    },
                )
                extracted_text = extracted_text[:remaining_chars]
            remaining_chars -= len(extracted_text)
        attachment = Attachment(
            filename=filename,
            content=b"",
            content_type=content_type,
            text=extracted_text,
            size_bytes=payload_size,
            metadata={"skipped_reason": skipped_reason} if skipped_reason else {},
        )
        attachments.append(attachment)
    return attachments


def parse_raw_email(raw_bytes: bytes, config: BotConfig) -> InboundMessage:
    email_obj = message_from_bytes(raw_bytes)
    subject = _decode_subject(email_obj)
    sender = _decode_from(email_obj)
    message_id = decode_mime_header(email_obj.get("Message-ID", ""))
    in_reply_to = decode_mime_header(email_obj.get("In-Reply-To", ""))
    references = decode_mime_header(email_obj.get("References", ""))
    body = _extract_body(email_obj)
    attachments = _extract_attachments(
        email_obj,
        max_attachment_mb=config.general.max_attachment_mb,
        max_zip_uncompressed_mb=config.general.max_zip_uncompressed_mb,
        max_extracted_chars=config.general.max_extracted_chars,
        max_extracted_total_chars=config.general.max_extracted_total_chars,
    )
    return InboundMessage(
        subject=subject,
        body=body,
        sender=sender,
        attachments=attachments,
        rfc_message_id=message_id or None,
        in_reply_to=in_reply_to or None,
        references=references or None,
    )


def stage_parse(ctx: PipelineContext) -> None:
    logger.info("[PIPELINE] email_id=%s stage=PARSE start", ctx.email_id)
    inbound = PIPELINE_INBOUND_CACHE.get(ctx.email_id)
    if inbound is None:
        raw = PIPELINE_RAW_CACHE.get(ctx.email_id)
        if raw is None or _PIPELINE_CONFIG is None:
            raise RuntimeError("No data available for PARSE stage")
        inbound = parse_raw_email(raw, _PIPELINE_CONFIG)
        PIPELINE_INBOUND_CACHE[ctx.email_id] = inbound
        PIPELINE_RAW_CACHE.pop(ctx.email_id, None)

    ctx.body_text = inbound.body
    ctx.attachments_text = [att.text or "" for att in inbound.attachments or []]
    logger.info("[PIPELINE] email_id=%s stage=PARSE done", ctx.email_id)


def stage_llm(ctx: PipelineContext) -> None:
    attempts = getattr(ctx, "attempts", 1)
    if attempts > 1:
        logger.info("[PIPELINE] email_id=%s stage=LLM retry=%s", ctx.email_id, attempts)
    inbound = PIPELINE_INBOUND_CACHE.get(ctx.email_id)
    if inbound is None:
        raise RuntimeError("No parsed email for LLM stage")
    if _PIPELINE_PROCESSOR is None:
        raise RuntimeError("Processor unavailable for LLM stage")

    body_text = ctx.body_text if ctx.body_text is not None else inbound.body
    attachments_text = (
        ctx.attachments_text
        if ctx.attachments_text is not None
        else [att.text or "" for att in inbound.attachments or []]
    )

    result_text = _PIPELINE_PROCESSOR.process(ctx.account_email, inbound)
    ordinary_result = _PIPELINE_PROCESSOR.get_last_ordinary_result()
    ctx.llm_result = {
        "text": result_text or "",
        "body_text": body_text,
        "attachments_text": attachments_text,
        "priority": ordinary_result.get("priority"),
        "sender": ordinary_result.get("sender") or inbound.sender,
        "subject": ordinary_result.get("subject") or inbound.subject,
        "action_line": ordinary_result.get("action_line") or "",
        "summary": ordinary_result.get("summary") or "",
        "attachments": ordinary_result.get("attachments")
        or [
            {
                "filename": att.filename,
                "text": att.text,
                "content_type": att.content_type,
                "size_bytes": att.size_bytes,
                "metadata": att.metadata,
            }
            for att in (inbound.attachments or [])
        ],
    }


def stage_tg(ctx: PipelineContext) -> DeliveryResult:
    inbound = PIPELINE_INBOUND_CACHE.get(ctx.email_id)
    if inbound is None:
        raise RuntimeError("No parsed email for TG stage")
    if _PIPELINE_CONFIG is None:
        raise RuntimeError("Config unavailable for TG stage")

    account = _ACCOUNT_MAP.get(normalize_login(ctx.account_email))
    if not account:
        raise RuntimeError(f"Account config missing for {ctx.account_email}")

    telegram_text = ""
    if ctx.llm_result:
        telegram_text = (
            ctx.llm_result.get("text") or ctx.llm_result.get("telegram_text") or ""
        )
    ctx.telegram_text = telegram_text

    if not telegram_text or not telegram_text.strip():
        raise RuntimeError("telegram payload empty")

    llm_priority = ""
    attachments: list[dict[str, object]] = []
    if ctx.llm_result:
        llm_priority = str(ctx.llm_result.get("priority") or "").strip()
        attachments = list(ctx.llm_result.get("attachments") or [])
    priority = llm_priority if llm_priority in {"🔴", "🟡", "🔵"} else "🔵"

    pretty_sender = (
        str(ctx.llm_result.get("sender") or inbound.sender or "").strip()
        if ctx.llm_result
        else str(inbound.sender or "").strip()
    )
    pretty_subject = (
        str(ctx.llm_result.get("subject") or inbound.subject or "").strip()
        if ctx.llm_result
        else str(inbound.subject or "").strip()
    )
    pretty_action = (
        str(ctx.llm_result.get("action_line") or "").strip()
        if ctx.llm_result
        else ""
    )
    pretty_summary = (
        str(ctx.llm_result.get("summary") or "").strip() if ctx.llm_result else ""
    )
    if pretty_sender and pretty_subject:
        try:
            telegram_text = render_telegram_message(
                priority=priority,
                from_email=pretty_sender,
                subject=pretty_subject,
                action_line=pretty_action,
                summary=pretty_summary,
                attachments=attachments,
                mail_type=inbound.mail_type,
            )
        except Exception:
            telegram_text = telegram_text or ""

    attachment_insight = build_attachment_insight(
        mail_type=inbound.mail_type,
        attachments=attachments,
        subject=inbound.subject,
        summary=ctx.llm_result.get("body_text", "") if ctx.llm_result else "",
    )
    if attachment_insight and attachment_insight not in telegram_text:
        telegram_text = f"{telegram_text}\n{attachment_insight}"

    payload = TelegramPayload(
        html_text=telegram_safe(telegram_text.strip()),
        priority=priority,
        metadata={
            "bot_token": _PIPELINE_CONFIG.keys.telegram_bot_token,
            "chat_id": account.telegram_chat_id,
        },
        reply_markup=build_email_actions_keyboard(
            email_id=ctx.email_id,
            expanded=False,
            initial_prio=True,
            show_decision_trace=load_telegram_ui_config().show_decision_trace,
        ),
    )
    result = send_telegram(payload)
    if not result.delivered:
        return result
    logger.info("[PIPELINE] email_id=%s stage=TG done", ctx.email_id)

    # cleanup to avoid leaks
    PIPELINE_INBOUND_CACHE.pop(ctx.email_id, None)
    PIPELINE_CACHE.pop(ctx.email_id, None)
    PIPELINE_RAW_CACHE.pop(ctx.email_id, None)
    return result


__all__ = [
    "PipelineContext",
    "PIPELINE_CACHE",
    "PIPELINE_INBOUND_CACHE",
    "PIPELINE_RAW_CACHE",
    "configure_pipeline",
    "remember_raw_email",
    "store_inbound",
    "parse_raw_email",
    "stage_parse",
    "stage_llm",
    "stage_tg",
]
