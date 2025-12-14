"""MailBot Premium v26 - Runtime orchestrator"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from email import message_from_bytes
from email.header import decode_header, make_header
from email.message import Message as EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List

CURRENT_DIR = Path(__file__).resolve().parent
LOG_PATH = CURRENT_DIR / "mailbot.log"


def _configure_logging() -> None:
    handlers: List[logging.Handler] = []
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        handlers.append(file_handler)
    except OSError as exc:
        print(f"File logging unavailable: {exc}")

    handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
        force=True
    )


_configure_logging()
logger = logging.getLogger("mailbot")

sys.path.insert(0, str(CURRENT_DIR.parent))

from mailbot_v26.config_loader import BotConfig, load_config
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.state_manager import StateManager
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor
from mailbot_v26.worker.telegram_sender import send_telegram
from mailbot_v26.bot_core.extractors.doc import extract_docx_text
from mailbot_v26.bot_core.extractors.excel import extract_excel_text
from mailbot_v26.bot_core.extractors.pdf import extract_pdf_text
from mailbot_v26.text import sanitize_text


def _decode_subject(email_obj: EmailMessage) -> str:
    raw_subject = email_obj.get("Subject", "")
    try:
        return str(make_header(decode_header(raw_subject)))
    except Exception:
        return raw_subject or ""


def _decode_sender(email_obj: EmailMessage) -> str:
    raw_from = email_obj.get("From", "")
    try:
        decoded = str(make_header(decode_header(raw_from)))
        if "<" in decoded:
            name = decoded.split("<")[0].strip()
            return name if name else decoded
        return decoded
    except Exception:
        return raw_from or ""


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _decode_date(email_obj: EmailMessage) -> datetime:
    raw_date = email_obj.get("Date", "")
    try:
        parsed = parsedate_to_datetime(raw_date)
        if parsed is None:
            return datetime.now()
        if parsed.tzinfo:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return datetime.now()


def _decode_part(part: EmailMessage) -> str:
    """Extract text from a single MIME part.

    Some emails use 7bit/8bit encoding without transfer-encoding. In those
    cases ``get_payload(decode=True)`` returns ``None`` even though the
    payload is present. We fall back to the raw payload to avoid dropping the
    message body entirely.
    """

    raw_payload = part.get_payload(decode=True)
    if raw_payload is None:
        raw_payload = part.get_payload(decode=False)

    if isinstance(raw_payload, str):
        text = raw_payload.strip()
    else:
        payload_bytes = raw_payload or b""
        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload_bytes.decode(charset, errors="ignore").strip()
        except Exception:
            text = ""

    return text


def _extract_body(email_obj: EmailMessage) -> str:
    if email_obj.is_multipart():
        parts = []
        for part in email_obj.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get_content_disposition() == "attachment":
                continue

            text = _decode_part(part)
            if text:
                parts.append(text)
        return "\n".join(parts)

    return _decode_part(email_obj)


def _extract_attachment_text(att: Attachment) -> str:
    name_lower = (att.filename or "").lower()
    content_type = (att.content_type or "").lower()
    try:
        if name_lower.endswith(".pdf"):
            return sanitize_text(extract_pdf_text(att.content, att.filename), max_len=5000)
        if name_lower.endswith((".doc", ".docx")):
            return sanitize_text(extract_docx_text(att.content, att.filename), max_len=5000)
        if name_lower.endswith((".xls", ".xlsx")):
            return sanitize_text(extract_excel_text(att.content, att.filename), max_len=5000)
        if content_type.startswith("text") or name_lower.endswith((".txt", ".csv", ".log", ".md", ".json")):
            decoded = att.content.decode("utf-8", errors="ignore")
            return sanitize_text(decoded, max_len=4000)
        return ""
    except Exception:
        return ""


def _extract_attachments(email_obj: EmailMessage, max_mb: int) -> List[Attachment]:
    attachments: List[Attachment] = []
    byte_limit = max_mb * 1024 * 1024
    for part in email_obj.walk():
        disposition = part.get_content_disposition()
        filename = _decode_header_value(part.get_filename())
        if disposition != "attachment" and not filename:
            continue
        try:
            payload = part.get_payload(decode=True) or b""
            if byte_limit > 0 and len(payload) > byte_limit:
                continue
            attachment = Attachment(
                filename=filename or "attachment.bin",
                content=payload,
                content_type=part.get_content_type() or "",
            )
            attachment.text = _extract_attachment_text(attachment)
            attachments.append(attachment)
        except Exception:
            continue
    return attachments


def _parse_raw_email(raw_bytes: bytes, config: BotConfig) -> InboundMessage:
    email_obj = message_from_bytes(raw_bytes)
    subject = _decode_subject(email_obj)
    sender = _decode_sender(email_obj)
    received_at = _decode_date(email_obj)
    body = _extract_body(email_obj)
    attachments = _extract_attachments(email_obj, config.general.max_attachment_mb)
    return InboundMessage(
        subject=subject,
        sender=sender,
        body=body,
        attachments=attachments,
        received_at=received_at,
    )


def main(config_dir: Path | None = None) -> None:
    print("MailBot Premium v26 starting...")
    print(f"Log file: {LOG_PATH}\n")

    logger.info("MailBot v26 start")

    try:
        base_config_dir = config_dir or CURRENT_DIR / "config"
        config = load_config(base_config_dir)
        logger.info("Configuration loaded: %d accounts", len(config.accounts))
        print(f"Loaded {len(config.accounts)} accounts")
    except Exception as exc:
        logger.exception("Failed to load configuration")
        print(f"Configuration error: {exc}")
        time.sleep(10)
        return

    state = StateManager(CURRENT_DIR / "state.json")
    processor = MessageProcessor(config=config, state=state)
    print("Ready to work\n")

    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"Cycle {cycle} {time.strftime('%H:%M:%S')}")
            logger.info("Cycle %d started", cycle)

            for account in config.accounts:
                login = account.login or "no_login"
                print(f"Checking {login}")

                try:
                    imap = ResilientIMAP(account, state)
                    new_messages = imap.fetch_new_messages()

                    if not new_messages:
                        print("No new messages")
                        continue

                    print(f"Received {len(new_messages)} messages")

                    for uid, raw in new_messages:
                        print(f"Processing UID {uid}")
                        try:
                            inbound = _parse_raw_email(raw, config)
                            final_text = processor.process(login, inbound)

                            if final_text and final_text.strip():
                                ok = send_telegram(
                                    config.keys.telegram_bot_token,
                                    account.telegram_chat_id,
                                    final_text.strip()
                                )
                                if not ok:
                                    print("Telegram send failed (see log)")
                                else:
                                    print("Telegram send ok")
                                logger.info("UID %s: Telegram %s", uid, "OK" if ok else "FAIL")
                            else:
                                print("Empty result")

                        except Exception as e:
                            print(f"Processing error: {e}")
                            logger.exception("Processing error for UID %s", uid)

                    state.save()

                except Exception as e:
                    print(f"IMAP error: {e}")
                    logger.exception("IMAP error for %s", login)

            state.save()
            delay = max(120, config.general.check_interval)
            print(f"Sleeping {delay} seconds...")
            time.sleep(delay)

    except KeyboardInterrupt:
        print("Stopped by user")
        logger.info("Stopped by user")
    except Exception as e:
        print(f"Critical error: {e}")
        logger.exception("Fatal error")
        time.sleep(10)


if __name__ == "__main__":
    main()
