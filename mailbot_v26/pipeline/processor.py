from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from mailbot_v26.llm.summarizer import LLMSummarizer

from mailbot_v26.text import clean_email_body, sanitize_text


@dataclass
class Attachment:
    filename: str
    content: bytes
    content_type: str = ""
    text: str | None = None


@dataclass
class InboundMessage:
    subject: str
    body: str
    sender: str = ""
    received_at: datetime | None = None
    attachments: List[Attachment] | None = None

    def __post_init__(self) -> None:
        if self.attachments is None:
            self.attachments = []


class MessageProcessor:
    """Single premium pipeline entry point."""

    def __init__(self, config, state) -> None:
        self.config = config
        self.state = state
        self.llm = LLMSummarizer(config.llm_call)

    def process(self, account_login: str, message: InboundMessage) -> Optional[str]:
        try:
            return self._build(account_login, message)
        except Exception:
            return None

    def _build(self, account_login: str, message: InboundMessage) -> Optional[str]:
        print("USING NEW PIPELINE")
        timestamp_line = self._format_timestamp(message.received_at)
        sender_line = sanitize_text((message.sender or "").strip() or account_login, max_len=200)
        subject_line = sanitize_text((message.subject or "Без темы").strip(), max_len=300) or "Без темы"

        body_clean = clean_email_body(message.body or "")
        body_clean = sanitize_text(body_clean, max_len=6000)

        body_summary = self._build_body_summary(body_clean, subject_line, sender_line)

        attachment_blocks: List[tuple[str, str]] = []
        for att in message.attachments or []:
            block = self._summarize_attachment(att, body_clean, subject_line)
            attachment_blocks.append(block)

        lines: List[str] = [timestamp_line, sender_line, subject_line, "", body_summary]

        for filename, block in attachment_blocks:
            lines.append("")
            lines.append(filename)
            lines.append(block)

        result = "\n".join(lines).strip()
        if len(result) > 3500:
            result = result[:3497] + "..."

        baseline = len("\n".join(lines[:3]))
        if not self._is_valid_output(result, baseline):
            fallback_body = self._fallback_summary(body_clean, subject=subject_line, sender=sender_line)
            safe_blocks = [self._infer_attachment_block(att, body_clean, subject_line) for att in message.attachments or []]
            safe_lines: List[str] = [timestamp_line, sender_line, subject_line, "", fallback_body]
            for filename, block in safe_blocks:
                safe_lines.append("")
                safe_lines.append(filename)
                safe_lines.append(block)
            result = "\n".join(safe_lines).strip()

        return result

    def _build_body_summary(self, body: str, subject: str, sender: str) -> str:
        fallback = self._fallback_summary(body, subject=subject, sender=sender)
        if len(body) < 50:
            return fallback

        body_summary_raw = self.llm.summarize_email(body)
        body_summary = sanitize_text(body_summary_raw, max_len=1200)
        if not self._is_meaningful(body_summary, min_len=30) or not self._has_two_sentences(body_summary):
            body_summary = fallback
        return body_summary

    def _summarize_attachment(self, att: Attachment, body_context: str, subject: str) -> tuple[str, str]:
        filename = att.filename or "Вложение"
        att_text = sanitize_text(att.text or "", max_len=4000)
        kind = self._detect_attachment_kind(att.filename, att.content_type)

        summary = ""
        if att_text:
            if len(att_text) >= 50:
                summary_raw = self.llm.summarize_attachment(att_text, kind=kind)
                summary = sanitize_text(summary_raw, max_len=1200)
                if not self._is_meaningful(summary, min_len=25):
                    summary = self._fallback_summary(att_text, limit=600, subject=subject)
            else:
                summary = self._infer_attachment_purpose(filename, kind, body_context, att_text)

        if not summary:
            summary = self._infer_attachment_purpose(filename, kind, body_context, att_text)

        summary = self._ensure_sentence(summary)
        return filename, summary

    def _infer_attachment_block(self, att: Attachment, body_context: str, subject: str) -> tuple[str, str]:
        filename = att.filename or "Вложение"
        return filename, self._infer_attachment_purpose(filename, self._detect_attachment_kind(att.filename, att.content_type), body_context, sanitize_text(att.text or "", max_len=200))

    @staticmethod
    def _detect_attachment_kind(filename: str | None, content_type: str = "") -> str:
        lower_ct = (content_type or "").lower()
        lower = (filename or "").lower()
        if lower.endswith((".xls", ".xlsx")) or "excel" in lower_ct:
            return "EXCEL"
        if lower.endswith((".doc", ".docx")) or "word" in lower_ct:
            return "CONTRACT"
        if lower.endswith(".pdf") or "pdf" in lower_ct:
            return "PDF"
        if lower.endswith((".png", ".jpg", ".jpeg", ".gif")) or "image" in lower_ct:
            return "IMAGE"
        return "GENERIC"

    @staticmethod
    def _format_timestamp(received_at: datetime | None) -> str:
        dt = received_at or datetime.now()
        return dt.strftime("%H:%M %d.%m.%Y")

    @staticmethod
    def _is_meaningful(text: str, min_len: int = 15) -> bool:
        return bool(text and text.strip() and len(text.strip()) >= min_len)

    @staticmethod
    def _fallback_summary(text: str, limit: int = 700, subject: str = "", sender: str = "") -> str:
        sanitized = sanitize_text(text or "", max_len=limit + 400)
        stripped = MessageProcessor._strip_greetings_and_signatures(sanitized)
        working = stripped or sanitized

        sentences = re.split(r"(?<=[.!?])\s+", working)
        meaningful: list[str] = []
        for sentence in sentences:
            sentence_clean = sentence.strip()
            if len(sentence_clean.split()) < 3:
                continue
            meaningful.append(sentence_clean)
            if len(" ".join(meaningful)) >= limit:
                break

        base = " ".join(meaningful[:3]) if meaningful else ""
        base = base[: limit - 3] + "..." if base and len(base) > limit else base

        if not base:
            context_intro = f"Письмо от {sender or 'отправителя'} касается темы \"{subject or 'без темы'}\".".strip()
            snippet = working[: max(120, min(limit, 260))].strip()
            detail = f"Основной текст: {snippet}." if snippet else "Автор прислал краткое сообщение без подробностей."
            base = f"{context_intro} {detail}".strip()

        if not MessageProcessor._has_two_sentences(base):
            base = MessageProcessor._ensure_two_sentences(base, subject, sender)

        return base

    @staticmethod
    def _ensure_two_sentences(text: str, subject: str, sender: str) -> str:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        while len(sentences) < 2:
            hint = f"Письмо связано с темой \"{subject or 'обсуждаемая тема'}\" от {sender or 'неизвестного отправителя'}."
            sentences.append(hint)
        return " ".join(sentences[:3])

    @staticmethod
    def _has_two_sentences(text: str) -> bool:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        return len(sentences) >= 2

    @staticmethod
    def _ensure_sentence(text: str) -> str:
        cleaned = text.strip()
        if not cleaned.endswith(('.', '!', '?')):
            cleaned += "."
        return cleaned

    def _infer_attachment_purpose(
        self, filename: str, kind: str, body_context: str, att_text: str | None
    ) -> str:
        base = filename or "Вложение"
        hints = {
            "EXCEL": "таблица (Excel)",
            "CONTRACT": "документ (Word)",
            "PDF": "PDF-документ",
            "IMAGE": "изображение",
        }
        label = hints.get(kind, "файл")

        context = body_context.lower()
        guess = ""
        for keyword, meaning in {
            "счет": "счёт на оплату",
            "invoice": "инвойс или счёт",
            "договор": "проект договора",
            "соглашение": "дополнительное соглашение",
            "прайс": "прайс-лист",
            "коммерчес": "коммерческое предложение",
            "заявк": "заявка или форма",
        }.items():
            if keyword in context or (att_text and keyword in att_text.lower()):
                guess = meaning
                break

        description = guess or "дополнительные материалы по теме письма"
        if att_text:
            snippet = att_text[:180] + "..." if len(att_text) > 200 else att_text
            detail = f"Содержит отрывок: {snippet}."
        else:
            detail = "Содержимое не извлечено, но файл соответствует обсуждаемой теме."
        return f"{base}: {label}, вероятно {description}. {detail}"

    def _is_valid_output(self, message: str, baseline_len: int) -> bool:
        if len(message or "") <= baseline_len:
            return False

        semantic = [ln for ln in (message or "").split("\n")[3:] if ln.strip()]
        if len(semantic) < 1:
            return False

        banned = ("=?", "PK", "IHDR", "IDAT", "Содержание письма отсутствует")
        if any(token in message for token in banned):
            return False

        return True

    @staticmethod
    def _strip_greetings_and_signatures(text: str) -> str:
        greetings = (
            "hello",
            "hi",
            "добрый день",
            "здравствуйте",
            "привет",
            "уважаемый",
            "dear",
        )
        signatures = (
            "с уважением",
            "best regards",
            "regards",
            "cheers",
            "thanks",
            "thank you",
        )

        lines = text.split("\n")
        filtered_start: list[str] = []
        skip_prefix = True
        for line in lines:
            lower = line.strip().lower()
            if skip_prefix and lower and any(lower.startswith(g) for g in greetings):
                continue
            skip_prefix = False
            filtered_start.append(line)

        filtered_end: list[str] = []
        for line in reversed(filtered_start):
            lower = line.strip().lower()
            if lower and any(lower.startswith(s) for s in signatures):
                continue
            filtered_end.append(line)
        filtered_end.reverse()

        return "\n".join(filtered_end).strip()
