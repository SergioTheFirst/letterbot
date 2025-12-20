from __future__ import annotations

import html
import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from email import message_from_bytes
from email.message import Message as EmailMessage
from email.utils import parseaddr
from pathlib import Path
from typing import Dict, List

from mailbot_v26.bot_core.extractors.doc import extract_docx_text
from mailbot_v26.bot_core.extractors.excel import extract_excel_text
from mailbot_v26.bot_core.extractors.pdf import extract_pdf_text
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor
from mailbot_v26.text.mime_utils import decode_bytes, decode_mime_header
from mailbot_v26.text.sanitize import sanitize_text
from mailbot_v26.worker.telegram_sender import send_telegram

try:  # local import to avoid circular typing issues
    from mailbot_v26.config_loader import AccountConfig, BotConfig
except Exception:  # pragma: no cover - defensive import for early boot
    AccountConfig = None  # type: ignore
    BotConfig = None  # type: ignore

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


def configure_pipeline(config: BotConfig, processor: MessageProcessor) -> None:
    global _PIPELINE_CONFIG, _PIPELINE_PROCESSOR, _ACCOUNT_MAP
    _PIPELINE_CONFIG = config
    _PIPELINE_PROCESSOR = processor
    _ACCOUNT_MAP = {acc.login: acc for acc in (config.accounts or [])}


def remember_raw_email(email_id: int, raw_email: bytes) -> None:
    PIPELINE_RAW_CACHE[email_id] = raw_email


def store_inbound(email_id: int, inbound: InboundMessage) -> None:
    PIPELINE_INBOUND_CACHE[email_id] = inbound


def _strip_html_content(text: str) -> str:
    working = re.sub(
        r"<(head|script|style)[^>]*>.*?</\1>", " ", text, flags=re.IGNORECASE | re.DOTALL
    )
    working = re.sub(r"<!--.*?-->", " ", working, flags=re.DOTALL)
    working = re.sub(
        r"<(br|p|div|tr|td|th|li|ul|ol|h[1-6])[^>]*>",
        "\n",
        working,
        flags=re.IGNORECASE,
    )
    working = re.sub(r"</(p|div|tr|td|th|li|ul|ol|h[1-6])>", "\n", working, flags=re.IGNORECASE)
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


def _is_real_attachment(part: EmailMessage, filename: str, payload: bytes) -> bool:
    if part.is_multipart():
        return False

    disposition = (part.get_content_disposition() or "").lower()
    content_type = (part.get_content_type() or "").lower()
    lower_name = (filename or "").strip().lower()
    extension = Path(filename or "").suffix.lower()

    if disposition == "inline":
        return False

    if not filename:
        small_payload = len(payload or b"") <= 2048
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


def _extract_attachment_text(att: Attachment) -> str:
    name_lower = (att.filename or "").lower()
    content_type = (att.content_type or "").lower()

    logger.debug(
        "Extracting from %s (type: %s, size: %d bytes)",
        att.filename,
        content_type,
        att.size_bytes or len(att.content),
    )

    try:
        if name_lower.endswith(".pdf"):
            text = sanitize_text(
                extract_pdf_text(att.content, att.filename), max_len=5000
            )
            logger.info("PDF extraction: %d chars from %s", len(text), att.filename)
            return text

        if name_lower.endswith((".doc", ".docx")):
            text = sanitize_text(
                extract_docx_text(att.content, att.filename), max_len=5000
            )
            logger.info("DOC extraction: %d chars from %s", len(text), att.filename)
            return text

        if name_lower.endswith((".xls", ".xlsx")):
            text = sanitize_text(
                extract_excel_text(att.content, att.filename), max_len=5000
            )
            logger.info("Excel extraction: %d chars from %s", len(text), att.filename)
            return text

        if content_type.startswith("text") or name_lower.endswith(
            (".txt", ".csv", ".log", ".md", ".json")
        ):
            decoded = att.content.decode("utf-8", errors="ignore")
            text = sanitize_text(decoded, max_len=4000)
            logger.info("Text extraction: %d chars from %s", len(text), att.filename)
            return text

    except Exception as e:
        logger.error("Extraction failed for %s: %s", att.filename, e, exc_info=True)

    return ""


def _extract_attachments(email_obj: EmailMessage, max_mb: int) -> List[Attachment]:
    byte_limit = max_mb * 1024 * 1024
    candidates: list[tuple[str, str, bytes, int]] = []
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
            payload = part.get_payload(decode=True) or b""
            payload_size = len(payload)
            if not _is_real_attachment(part, filename, payload):
                payload = b""
                continue
            if byte_limit > 0 and payload_size > byte_limit:
                payload = b""
                continue
            if not filename or filename.lower().startswith("attachment.bin"):
                payload = b""
                continue
            candidates.append(
                (
                    filename,
                    part.get_content_type() or "",
                    payload,
                    payload_size,
                )
            )
        except Exception:
            continue
    if not candidates:
        return []

    def _extract(candidate: tuple[str, str, bytes, int]) -> str:
        filename, content_type, payload, payload_size = candidate
        temp_attachment = Attachment(
            filename=filename,
            content=payload,
            content_type=content_type,
            text="",
            size_bytes=payload_size,
        )
        extracted_text = _extract_attachment_text(temp_attachment)
        temp_attachment.content = b""
        del temp_attachment
        return extracted_text

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(_extract, candidate) for candidate in candidates]
        extracted_texts = [future.result() for future in futures]

    attachments: List[Attachment] = []
    for (filename, content_type, _payload, payload_size), extracted_text in zip(
        candidates, extracted_texts
    ):
        attachment = Attachment(
            filename=filename,
            content=b"",
            content_type=content_type,
            text=extracted_text,
            size_bytes=payload_size,
        )
        attachments.append(attachment)
    return attachments


def parse_raw_email(raw_bytes: bytes, config: BotConfig) -> InboundMessage:
    email_obj = message_from_bytes(raw_bytes)
    subject = _decode_subject(email_obj)
    sender = _decode_from(email_obj)
    body = _extract_body(email_obj)
    attachments = _extract_attachments(email_obj, config.general.max_attachment_mb)
    return InboundMessage(
        subject=subject, body=body, sender=sender, attachments=attachments
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
        logger.info(
            "[PIPELINE] email_id=%s stage=LLM retry=%s", ctx.email_id, attempts
        )
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
    ctx.llm_result = {
        "text": result_text or "",
        "body_text": body_text,
        "attachments_text": attachments_text,
    }


def stage_tg(ctx: PipelineContext) -> None:
    inbound = PIPELINE_INBOUND_CACHE.get(ctx.email_id)
    if inbound is None:
        raise RuntimeError("No parsed email for TG stage")
    if _PIPELINE_CONFIG is None:
        raise RuntimeError("Config unavailable for TG stage")

    account = _ACCOUNT_MAP.get(ctx.account_email)
    if not account:
        raise RuntimeError(f"Account config missing for {ctx.account_email}")

    telegram_text = ""
    if ctx.llm_result:
        telegram_text = ctx.llm_result.get("text") or ctx.llm_result.get("telegram_text") or ""
    ctx.telegram_text = telegram_text

    if telegram_text and telegram_text.strip():
        send_telegram(
            _PIPELINE_CONFIG.keys.telegram_bot_token,
            account.telegram_chat_id,
            telegram_text.strip(),
        )
    logger.info("[PIPELINE] email_id=%s stage=TG done", ctx.email_id)

    # cleanup to avoid leaks
    PIPELINE_INBOUND_CACHE.pop(ctx.email_id, None)
    PIPELINE_CACHE.pop(ctx.email_id, None)
    PIPELINE_RAW_CACHE.pop(ctx.email_id, None)


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
