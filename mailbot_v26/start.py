"""MailBot Premium v26 - Runtime orchestrator"""
from __future__ import annotations

import html
import logging
import re
import sys
import time
import unicodedata
from email import message_from_bytes
from email.message import Message as EmailMessage
from pathlib import Path
from typing import List

CURRENT_DIR = Path(__file__).resolve().parent
# Добавляем родительскую папку в путь для импортов
sys.path.insert(0, str(CURRENT_DIR))
sys.path.insert(0, str(CURRENT_DIR.parent))

from bot_core.extractors.doc import extract_docx_text
from bot_core.extractors.excel import extract_excel_text
from bot_core.extractors.pdf import extract_pdf_text
from config_loader import BotConfig, load_config
from imap_client import ResilientIMAP
from pipeline.processor import Attachment, InboundMessage, MessageProcessor
from state_manager import StateManager
from text.mime_utils import decode_bytes, decode_mime_header
from text.sanitize import sanitize_text
from worker.telegram_sender import send_telegram

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
        force=True,
    )


_configure_logging()
logger = logging.getLogger("mailbot")


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
        len(att.content),
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
    attachments: List[Attachment] = []
    byte_limit = max_mb * 1024 * 1024
    for part in email_obj.walk():
        disposition = part.get_content_disposition()
        raw_filename = part.get_filename()
        filename = decode_mime_header(raw_filename or "") or "attachment.bin"
        if disposition != "attachment" and not filename:
            continue
        try:
            payload = part.get_payload(decode=True) or b""
            if byte_limit > 0 and len(payload) > byte_limit:
                continue
            attachment = Attachment(
                filename=filename,
                content=payload,
                content_type=part.get_content_type() or "",
                text="",
            )
            attachment.text = _extract_attachment_text(attachment)
            attachments.append(attachment)
        except Exception:
            continue
    return attachments


def _parse_raw_email(raw_bytes: bytes, config: BotConfig) -> InboundMessage:
    email_obj = message_from_bytes(raw_bytes)
    subject = _decode_subject(email_obj)
    sender = _decode_from(email_obj)
    body = _extract_body(email_obj)
    attachments = _extract_attachments(email_obj, config.general.max_attachment_mb)
    return InboundMessage(
        subject=subject, body=body, sender=sender, attachments=attachments
    )


def main(config_dir: Path | None = None) -> None:
    print("\n" + "=" * 60)
    print("MAILBOT PREMIUM v26 - STARTING")
    print("=" * 60)
    print(f"Log file: {LOG_PATH}\n")

    logger.info("=== MailBot v26 started ===")

    try:
        base_config_dir = config_dir or CURRENT_DIR / "config"
        config = load_config(base_config_dir)
        logger.info("Configuration loaded: %d accounts", len(config.accounts))
        print(f"[OK] Loaded {len(config.accounts)} accounts")
    except Exception as exc:
        logger.exception("Failed to load configuration")
        print(f"[ERROR] Configuration error: {exc}")
        time.sleep(10)
        return

    state = StateManager(CURRENT_DIR / "state.json")
    processor = MessageProcessor(config=config, state=state)
    print("[OK] Ready to work\n")

    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n{'=' * 60}")
            print(f"CYCLE #{cycle} - {time.strftime('%H:%M:%S')}")
            print(f"{'=' * 60}")
            logger.info("Cycle %d started", cycle)

            for account in config.accounts:
                login = account.login or "no_login"
                print(f"\n[MAIL] Checking: {login}")

                try:
                    imap = ResilientIMAP(account, state)
                    new_messages = imap.fetch_new_messages()

                    if not new_messages:
                        print("   └─ no new messages")
                        continue

                    print(f"   └─ received {len(new_messages)} new messages")

                    for uid, raw in new_messages:
                        print(f"      ├─ UID {uid}")
                        try:
                            inbound = _parse_raw_email(raw, config)
                            subject = inbound.subject[:60] if inbound.subject else "(no subject)"
                            print(f"      │  Subject: {subject}")

                            final_text = processor.process(login, inbound)

                            if final_text and final_text.strip():
                                ok = send_telegram(
                                    config.keys.telegram_bot_token,
                                    account.telegram_chat_id,
                                    final_text.strip(),
                                )
                                status = "[OK] sent" if ok else "[FAIL] failed"
                                print(f"      │  Telegram: {status}")
                                logger.info("UID %s: Telegram %s", uid, "OK" if ok else "FAIL")
                            else:
                                print(f"      │  Result: empty")

                        except Exception as e:
                            print(f"      └─ [ERROR] {e}")
                            logger.exception("Processing error for UID %s", uid)

                    state.save()

                except Exception as e:
                    print(f"   └─ [IMAP ERROR] {e}")
                    logger.exception("IMAP error for %s", login)

            state.save()
            delay = max(120, config.general.check_interval)
            print(f"\n[WAIT] Sleeping {delay} seconds...")
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n\n[STOP] Stopped by user")
        logger.info("Stopped by user")
    except Exception as e:
        print(f"\n\n[CRITICAL] {e}")
        logger.exception("Fatal error")
        time.sleep(10)


if __name__ == "__main__":
    main()
