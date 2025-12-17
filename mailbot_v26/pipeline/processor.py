from __future__ import annotations

import html
import logging
import re
from pathlib import Path
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from email.utils import parseaddr

from mailbot_v26.bot_core.action_engine import analyze_action
from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier
from mailbot_v26.domain.fact_snippets import (
    normalize_text,
    pick_attachment_fact,
    pick_email_body_fact,
)
from mailbot_v26.domain.signal_compressor import (
    compress_attachment_fact,
    compress_body_fact,
)
from mailbot_v26.text import clean_email_body, sanitize_text


logger = logging.getLogger(__name__)

IGNORED_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".webp",
    ".tiff",
    ".svg",
}


@dataclass
class Attachment:
    filename: str
    content: bytes
    content_type: str = ""
    text: str | None = None


@dataclass
class AttachmentSummary:
    filename: str
    description: str
    kind: str
    priority: int
    text_length: int = 0
    size_bytes: int = 0
    doc_type: str = "OTHER"


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

    _FORBIDDEN_PHRASES = {
        "касается",
        "по теме",
        "можно просмотреть",
        "содержит информацию",
        "без подробностей",
    }

    _PRIORITY_EMOJI = {"RED": "🔴", "YELLOW": "🟡", "BLUE": "🔵"}
    _ATTACHMENT_ORDER = {"INVOICE": 0, "CONTRACT": 1, "PDF": 2, "EXCEL": 3, "GENERIC": 4}
    _MAX_ATTACHMENTS = 10
    _ATTACHMENT_SNIPPET_LIMIT = 120
    _ATTACHMENT_LINE_LENGTH = 160
    _ATTACHMENT_TOKEN_LIMIT = 12
    _DOC_TYPE_PRIORITY = {
        "CONTRACT": 0,
        "AGREEMENT": 0,
        "INVOICE": 1,
        "PRICE": 2,
        "PRICE_LIST": 2,
        "TABLE": 3,
        "REPORT": 3,
        "OTHER": 4,
    }
    _STOPWORDS = {
        "и",
        "в",
        "во",
        "на",
        "по",
        "за",
        "для",
        "как",
        "что",
        "это",
        "the",
        "and",
        "with",
        "from",
    }
    _FORBIDDEN_ATTACHMENT_TOKENS = {
        "старый",
        "формат",
        "кодировка",
        "утилита",
        "поддерживается",
        "данным",
        "файла",
    }
    _VERB_ORDER = [
        "Проверить",
        "Оплатить",
        "Согласовать",
        "Ответить",
        "Ознакомиться",
    ]
    _MAIL_TYPE_DEFAULTS = {
        "PAYMENT_REQUEST": {"priority": "RED", "verb": "Оплатить"},
        "PAYMENT_REMINDER": {"priority": "RED", "verb": "Оплатить"},
        "CONTRACT_APPROVAL": {"priority": "YELLOW", "verb": "Согласовать"},
        "CONTRACT_UPDATE": {"priority": "YELLOW", "verb": "Согласовать"},
        "INVOICE": {"priority": "YELLOW", "verb": "Оплатить"},
        "PRICE_LIST": {"priority": "BLUE", "verb": "Ознакомиться"},
        "DELIVERY_NOTICE": {"priority": "YELLOW", "verb": "Проверить"},
        "DEADLINE_REMINDER": {"priority": "YELLOW", "verb": "Проверить"},
        "ACCOUNT_CHANGE": {"priority": "RED", "verb": "Проверить"},
        "SECURITY_ALERT": {"priority": "RED", "verb": "Проверить"},
        "POLICY_UPDATE": {"priority": "BLUE", "verb": "Ознакомиться"},
        "MEETING_CHANGE": {"priority": "YELLOW", "verb": "Ответить"},
        "INFORMATION_ONLY": {"priority": "BLUE", "verb": "Ознакомиться"},
        "UNKNOWN": {"priority": "BLUE", "verb": "Ознакомиться"},
    }

    def __init__(self, config, state) -> None:
        self.config = config
        self.state = state

    def _filter_attachments(self, attachments: List[Attachment]) -> List[Attachment]:
        filtered: List[Attachment] = []
        for att in attachments:
            ext = Path(att.filename or "").suffix.lower()
            if ext in IGNORED_EXTENSIONS:
                continue
            filtered.append(att)
        return filtered

    def process(self, account_login: str, message: InboundMessage) -> Optional[str]:
        try:
            return self._build(account_login, message)
        except Exception:
            return None

    def _build(self, account_login: str, message: InboundMessage) -> Optional[str]:
        body_clean = clean_email_body(message.body or "")
        body_clean = sanitize_text(body_clean, max_len=6000)
        subject_clean = sanitize_text((message.subject or "").strip() or "Без темы", max_len=200)
        sender_clean = self._normalize_source(message.sender or account_login)
        body_summary = self._summarize_email_body(body_clean)
        attachments = self._filter_attachments(message.attachments or [])

        mail_type = MailTypeClassifier.classify(subject_clean, body_clean, attachments)

        action_facts = analyze_action(" ".join([subject_clean, body_clean]))
        priority = self._resolve_priority(
            message, body_clean, subject_clean, action_facts, mail_type, attachments
        )
        verb = self._select_verb(action_facts, body_clean, mail_type)

        line1 = self._build_line1(priority, sender_clean, subject_clean, message.received_at)
        line2 = self._build_line2(verb, subject_clean, body_clean, attachments)

        base_lines = self._enforce_length([line1, line2])
        attachments, extra_attachments = self._build_attachment_summaries(attachments, subject_clean)
        telegram_message = self._compose(
            base_lines, attachments, body_summary, extra_attachments, subject_clean
        )

        combined_preview = "\n".join(
            base_lines
            + ([body_summary] if body_summary else [])
            + [att.description for att in attachments]
            + ([f"ещё {extra_attachments} вложений"] if extra_attachments else [])
        )
        if not self._has_primary_signal(combined_preview):
            priority = "BLUE"
            line1 = self._build_line1(priority, sender_clean, subject_clean, message.received_at)
            base_lines = self._enforce_length([line1, line2])
            telegram_message = self._compose(
                base_lines, attachments, body_summary, extra_attachments, subject_clean
            )

        if not self._passes_quality_gates(base_lines, priority, verb, mail_type):
            fallback_lines = self._fallback_lines(sender_clean, subject_clean, verb)
            base_lines = self._enforce_length(fallback_lines)
            telegram_message = self._compose(
                base_lines, attachments, body_summary, extra_attachments, subject_clean
            )

        if not self._passes_quality_gates(base_lines, priority, verb, mail_type):
            minimal = self._enforce_length(self._fallback_lines(sender_clean, "Сообщение", verb), hard_trim=True)
            telegram_message = self._compose(minimal, [], body_summary, 0, subject_clean)

        return self._append_account_line(telegram_message, account_login)

    def _resolve_priority(
        self,
        message: InboundMessage,
        body: str,
        subject: str,
        facts,
        mail_type: str,
        attachments: List[Attachment],
    ) -> str:
        sender_domain = (message.sender or "").split("@")[-1].lower()
        combined = " ".join([(subject or "").lower(), body.lower()])
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)

        mail_type_default = self._MAIL_TYPE_DEFAULTS.get(mail_type, {})
        priority = mail_type_default.get("priority", "BLUE")

        urgent = self._contains_any(combined, {"срочно", "urgent", "asap"})
        if urgent:
            priority = "RED"

        if self._has_deadline(combined, today, tomorrow):
            priority = "RED"

        if facts.amount and facts.date:
            priority = "RED"

        sensitive_domains = {"bank", "nalog", "fns", "court", "gov"}
        if any(dom in sender_domain for dom in sensitive_domains) and facts.action:
            priority = "RED"

        attachment_kinds = {
            self._detect_attachment_kind(att.filename, att.content_type)
            for att in attachments
        }
        if not facts.action and not urgent:
            if "INVOICE" in attachment_kinds or "CONTRACT" in attachment_kinds:
                priority = self._max_priority(priority, "YELLOW")
            if self._is_management_sender(message.sender):
                priority = self._max_priority(priority, "YELLOW")

        priority = self._max_priority(priority, "BLUE")

        return priority

    def _build_line1(self, priority: str, source: str, subject: str, received_at: datetime | None) -> str:
        return f"{self._PRIORITY_EMOJI[priority]} от {source}"

    def _build_line2(
        self, verb: str, subject: str, body: str, attachments: List[Attachment]
    ) -> str:
        normalized_verb = self._normalize_verb_choice(verb)
        return self._normalize_action_subject(normalized_verb, subject, attachments, body)

    def _summarize_email_body(self, body_text: str) -> str:
        return self._normalize_body_one_liner(body_text)

    def _select_verb(self, facts, body: str, mail_type: str) -> str:
        defaults = self._MAIL_TYPE_DEFAULTS.get(mail_type, {})
        if defaults.get("verb"):
            verb = defaults.get("verb")
        else:
            verb = None

        if not verb:
            lowered_body = body.lower()
            if facts.action and re.search(r"оплат", facts.action):
                verb = "Оплатить"
            elif facts.action and re.search(r"утверд|подпис", facts.action):
                verb = "Согласовать"
            elif "соглас" in lowered_body:
                verb = "Согласовать"
            elif "подтверд" in lowered_body:
                verb = "Ответить"
            elif "ответ" in lowered_body:
                verb = "Ответить"
            elif "провер" in lowered_body:
                verb = "Проверить"
            elif "треб" in lowered_body:
                verb = "Проверить"
            else:
                verb = "Ознакомиться"

        return self._normalize_verb_choice(verb)

    def _extract_essence(self, subject: str, body: str, max_words: int = 5) -> str:
        keywords = self._keywords(subject)
        if len(keywords) < 2:
            keywords.extend(self._keywords(body))
        filtered = [w for w in keywords if not w.isdigit()][: max_words + 1]
        if len(filtered) < 2:
            filtered.extend([w for w in ("сообщение", "детали") if w not in filtered])
        essence_words = filtered[: max(2, min(max_words, len(filtered)))]
        return " ".join(essence_words)

    def _build_attachment_summaries(
        self, attachments: List[Attachment], subject: str
    ) -> tuple[List[AttachmentSummary], int]:
        # Почтовые клиенты вытягивают все файлы, но на этапе агрегации часть
        # вложений исчезала: мы отбрасывали "attachment.bin"/inline-чunks и
        # обрезали список после _MAX_ATTACHMENTS, поэтому файлы с пустым текстом
        # или низким приоритетом не доходили до Telegram.
        attachments_out: list[AttachmentSummary] = []
        for att in attachments:
            filename = att.filename or "Вложение"
            lower_filename = filename.lower()
            content_type = (att.content_type or "").lower()

            if self._is_inline_attachment(lower_filename, content_type):
                logger.debug("Keeping inline-like attachment for rendering: %s", filename)

            kind = self._detect_attachment_kind(att.filename, att.content_type)

            att_text_raw = sanitize_text(att.text or "", max_len=1500)
            att_text_plain = normalize_text(self._strip_markup(att_text_raw))
            text_length = len(att_text_plain)
            doc_type = self._classify_attachment_type(filename, kind, att_text_plain)
            description = self._attachment_description(filename, kind, att_text_plain)
            priority_rank = self._ATTACHMENT_ORDER.get(kind, 5)

            attachments_out.append(
                AttachmentSummary(
                    filename=filename,
                    description=description,
                    kind=kind,
                    priority=priority_rank,
                    text_length=text_length,
                    size_bytes=len(att.content or b""),
                    doc_type=doc_type,
                )
            )

        if len(attachments_out) != len(attachments):
            logger.warning(
                "Attachment coverage mismatch: expected %d, got %d",
                len(attachments),
                len(attachments_out),
            )
            while len(attachments_out) < len(attachments):
                att = attachments[len(attachments_out)]
                filler_name = att.filename or "Вложение"
                doc_type = self._classify_attachment_type(
                    filler_name,
                    self._detect_attachment_kind(att.filename, att.content_type),
                    normalize_text(att.text or ""),
                )
                attachments_out.append(
                    AttachmentSummary(
                        filename=filler_name,
                        description=self._attachment_description(
                            filler_name,
                            self._detect_attachment_kind(att.filename, att.content_type),
                            normalize_text(att.text or ""),
                        ),
                        kind=self._detect_attachment_kind(att.filename, att.content_type),
                        priority=self._ATTACHMENT_ORDER.get(
                            self._detect_attachment_kind(att.filename, att.content_type), 5
                        ),
                        text_length=len(normalize_text(att.text or "")),
                        size_bytes=len(att.content or b""),
                        doc_type=doc_type,
                    )
                )

        extra_attachments = 0

        return attachments_out, extra_attachments

    @staticmethod
    def _is_inline_attachment(lower_filename: str, content_type: str) -> bool:
        inline_types = {"text/html", "text/css", "application/octet-stream"}
        if content_type.startswith("font/"):
            return True

        has_meaningful_extension = lower_filename.endswith(
            (".doc", ".docx", ".xls", ".xlsx", ".pdf")
        )
        return (
            (not lower_filename or lower_filename == "attachment.bin")
            and content_type in inline_types
            and not has_meaningful_extension
        )

    def _safe_attachment_fallback(
        self, filename: str | None, kind: str, text_length: int = 0, summary_failed: bool = False
    ) -> str:
        base_name = filename or "Вложение"
        lowered = base_name.lower()
        ext = ""
        if "." in lowered:
            ext = lowered[lowered.rfind(".") :]

        category = self._fallback_category(lowered, kind)
        return category

    def describe_attachment(
        self, filename: str, ext: str, extracted_text: str | None
    ) -> str:
        clean_text = normalize_text(self._strip_markup(extracted_text or "")).strip()
        kind_hint = "EXCEL" if (ext or "").lower() in {".xls", ".xlsx"} else ""
        fallback_name = filename or (f"Вложение{ext}" if ext else "Вложение")
        return self._attachment_description(fallback_name, kind_hint, clean_text)

    def _fallback_category(self, lowered_filename: str, kind: str) -> str:
        ext = ""
        if "." in lowered_filename:
            ext = lowered_filename[lowered_filename.rfind(".") :]

        if ext in {".doc", ".docx"}:
            if any(token in lowered_filename for token in {"догов", "contract"}):
                return "договор"
            return "текстовый документ"
        if ext in {".xls", ".xlsx"}:
            if any(token in lowered_filename for token in {"прайс", "price"}):
                return "прайс-лист"
            return "таблица"
        if ext == ".pdf":
            return "документ"
        if kind == "EXCEL":
            return "таблица"
        if kind == "CONTRACT":
            return "договор"
        return "файл"

    def _summarize_attachment(self, att: Attachment, subject: str, kind: str) -> tuple[str, int]:
        filename = self._purge_markup_tokens(att.filename or "Вложение")
        att_text_raw = sanitize_text(att.text or "", max_len=1500)
        att_text = normalize_text(self._strip_markup(att_text_raw))
        text_length = len(att_text)

        doc_type = self._classify_attachment_type(filename, kind, att_text)
        summary = pick_attachment_fact(att_text, filename, doc_type)

        summary = compress_attachment_fact(summary or "", doc_type)

        return summary or "", text_length

    def _compose(
        self,
        base_lines: List[str],
        attachments: List[AttachmentSummary],
        body_summary: str = "",
        extra_attachments: int = 0,
        subject: str = "",
    ) -> str:
        rendered_attachments = self._render_attachments(attachments, extra_attachments)
        body_line = f"<i>{self._escape_html(body_summary.strip())}</i>" if body_summary.strip() else ""
        attachment_block = "\n".join(rendered_attachments) if rendered_attachments else ""
        header_block = self._escape_html(base_lines[0])
        if subject:
            header_block = f"{header_block}\n<b>{self._escape_html(subject.strip())}</b>"

        parts = [header_block]

        if len(base_lines) > 1 and base_lines[1].strip():
            parts.append(self._escape_html(base_lines[1].strip()))

        attachment_count = len(attachments) + max(extra_attachments, 0)
        if attachment_count:
            parts.append(f"📎 Вложений: {attachment_count}")

        if body_line:
            parts.append(body_line)

        if attachment_block:
            parts.append(attachment_block)

        return "\n\n".join(part for part in parts if part).strip()

    @staticmethod
    def _append_account_line(message: str, account_login: str) -> str:
        footer = f"<i>to: {html.escape((account_login or '').strip())}</i>"
        return f"{message}\n{footer}" if message else footer

    def _render_attachments(
        self, attachments: List[AttachmentSummary], extra_attachments: int = 0
    ) -> List[str]:
        if not attachments:
            return [f"<i>ещё {extra_attachments} файлов</i>"] if extra_attachments else []

        limited = attachments[:6]
        lines: List[str] = []

        for attachment in limited:
            line = self.format_attachment_line(
                attachment.filename,
                attachment.description,
                attachment.kind,
                max_meaning=self._ATTACHMENT_SNIPPET_LIMIT,
                max_line_length=self._ATTACHMENT_LINE_LENGTH,
                text_length=attachment.text_length,
            )
            if line:
                escaped_line = self._escape_html(line)
                lines.append(f"<i>{escaped_line}</i>")

        remaining = max(extra_attachments, 0) + max(0, len(attachments) - 6)
        if remaining:
            lines.append(f"<i>ещё {remaining} файлов</i>")

        return lines

    @staticmethod
    def format_attachment_line(
        filename: str,
        extracted_text: str,
        kind_hint: str,
        *,
        max_meaning: int | None = None,
        text_length: int = 0,
        max_line_length: int | None = None,
    ) -> str:
        meaning_limit = max_meaning or MessageProcessor._ATTACHMENT_SNIPPET_LIMIT
        line_limit = max_line_length or MessageProcessor._ATTACHMENT_LINE_LENGTH
        clean_name = " ".join((filename or "Вложение").split()) or "Вложение"
        summary = (extracted_text or "").strip()

        if text_length == 0 or not summary:
            return MessageProcessor._trim_text(clean_name, line_limit)

        cleaned_summary = MessageProcessor._trim_attachment_snippet(summary, meaning_limit)
        name_tokens = {token.lower() for token in re.findall(r"[\w-]{2,}", clean_name)}
        summary_tokens: list[str] = []
        for token in cleaned_summary.split():
            plain = re.sub(r"[^\w-]", "", token).lower()
            if plain and plain in name_tokens:
                continue
            summary_tokens.append(token)

        if not summary_tokens:
            return clean_name

        summary_text = " ".join(summary_tokens[: MessageProcessor._ATTACHMENT_TOKEN_LIMIT])
        line = f"{clean_name} — {summary_text}"
        if len(line) <= line_limit:
            return line

        max_summary = max(8, min(meaning_limit, line_limit - len(clean_name) - len(" — ")))
        trimmed_summary = MessageProcessor._trim_text(summary_text, max_summary)
        line = f"{clean_name} — {trimmed_summary}"
        if len(line) <= line_limit:
            return line

        max_name = max(8, line_limit - len(trimmed_summary) - len(" — "))
        trimmed_name = MessageProcessor._trim_text(clean_name, max_name)
        return f"{trimmed_name} — {trimmed_summary}"

    def _attachment_description(self, filename: str, kind_hint: str, att_text: str) -> str:
        stem_tokens = {t.lower() for t in self._keywords(Path(filename or "Вложение").stem)}
        cleaned = normalize_text(self._strip_markup(att_text or "")).strip()
        if not cleaned:
            return ""

        ext = Path((filename or "")).suffix.lower()
        if ext in {".xls", ".xlsx"} or kind_hint == "EXCEL":
            return self._excel_essence(cleaned, stem_tokens)

        return self._document_essence(cleaned, stem_tokens)

    def _excel_essence(self, att_text: str, name_tokens: set[str]) -> str:
        header_stopwords = {
            "номер",
            "наименование",
            "адрес",
            "учреждения",
            "учреждение",
            "адреса",
            "дата",
            "период",
            "телефон",
            "почта",
            "email",
            "количество",
            "итого",
            "сумма",
        }
        words = re.findall(r"[A-Za-zА-Яа-яЁё]{4,}", att_text.lower())
        filtered = [
            w
            for w in words
            if w not in header_stopwords
            and w not in self._STOPWORDS
            and w not in name_tokens
        ]
        if not filtered:
            return ""

        counts = Counter(filtered)
        first_pos: dict[str, int] = {}
        for idx, word in enumerate(filtered):
            first_pos.setdefault(word, idx)

        ranked = sorted(first_pos.keys(), key=lambda w: (-counts[w], first_pos[w]))
        essence_tokens = ranked[:4]
        return " ".join(essence_tokens)

    def _document_essence(self, att_text: str, name_tokens: set[str]) -> str:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n", att_text) if s.strip()]
        picked = ""
        for sentence in sentences:
            if len(sentence.split()) >= 6:
                picked = sentence
                break
        if not picked and sentences:
            picked = sentences[0]

        if not picked:
            return ""

        raw_tokens = [re.sub(r"^[^\w]+|[^\w]+$", "", tok) for tok in picked.split() if tok.strip()]
        filtered = [t for t in raw_tokens if t and t.lower() not in name_tokens]
        nouns = [
            t
            for t in filtered
            if len(t) >= 4 and t.lower() not in self._STOPWORDS and not t.isdigit()
        ]

        if nouns:
            essence_tokens = nouns[:4]
        else:
            essence_tokens = filtered[:10]

        if len(essence_tokens) < 2:
            return ""

        if len(essence_tokens) > 4:
            essence_tokens = essence_tokens[:4]

        return " ".join(essence_tokens)

    def _filter_forbidden_tokens(self, tokens: list[str]) -> list[str]:
        blocked = self._FORBIDDEN_ATTACHMENT_TOKENS
        return [tok for tok in tokens if all(bad not in tok.lower() for bad in blocked)]

    @staticmethod
    def _trim_text(text: str, limit: int) -> str:
        if limit <= 0:
            return ""
        if len(text) <= limit:
            return text
        if limit <= 1:
            return text[:limit]
        return text[: limit - 1].rstrip() + "…"

    def _strip_forbidden_tokens(self, text: str) -> str:
        tokens = text.split()
        return " ".join(self._filter_forbidden_tokens(tokens))

    @classmethod
    def _trim_attachment_snippet(cls, text: str, limit: int | None = None) -> str:
        applied_limit = limit or cls._ATTACHMENT_SNIPPET_LIMIT
        return cls._trim_text(text, applied_limit)

    @staticmethod
    def _empty_attachment_phrase(filename: str, kind_hint: str) -> str:
        lower_ext = Path(filename or "").suffix.lower()
        if lower_ext in {".xls", ".xlsx"} or kind_hint == "EXCEL":
            return "табличный файл"
        return "вложение"

    def _remove_subject_terms(self, summary: str, subject: str) -> str:
        subject_keywords = {kw.lower() for kw in self._keywords(subject)}
        cleaned_tokens: List[str] = []
        for token in summary.split():
            plain = re.sub(r"[^\w-]", "", token).lower()
            if plain and plain in subject_keywords:
                continue
            cleaned_tokens.append(token)
        cleaned = " ".join(cleaned_tokens).strip()
        return cleaned or summary

    def _classify_attachment_type(self, filename: str, kind: str, att_text: str) -> str:
        combined = f"{filename} {att_text}".lower()
        if any(token in combined for token in {"прайс", "прайс-лист", "price"}):
            return "PRICE"
        if any(token in combined for token in {"счет", "счёт", "invoice", "оплат"}):
            return "INVOICE"
        if any(token in combined for token in {"договор", "контракт", "соглашение"}):
            return "CONTRACT"
        if any(token in combined for token in {"отчет", "отчёт", "report"}):
            return "REPORT"
        if kind == "EXCEL":
            return "TABLE"
        if kind == "CONTRACT":
            return "CONTRACT"
        return "OTHER"

    def _attachment_subject(self, att_text: str, filename: str) -> str:
        key_terms = self._key_nouns(att_text)
        if key_terms:
            return " ".join(key_terms[:3])
        sentences = [s.strip() for s in re.split(r"[.!?]\s+", att_text) if s.strip()]
        first_sentence = sentences[0] if sentences else ""
        if not first_sentence:
            keywords = self._keywords(filename)
            return " ".join(keywords[:3]) or "основные данные"
        words = first_sentence.split()
        trimmed = " ".join(words[:8]).strip()
        return trimmed or "основные данные"

    def _key_nouns(self, att_text: str) -> list[str]:
        words = re.findall(r"[\w-]{4,}", att_text.lower())
        filtered = [w for w in words if w not in self._STOPWORDS and not w.isdigit()]
        counts = Counter(filtered)
        ordered: list[str] = []
        for word in filtered:
            if word in ordered:
                continue
            ordered.append(word)
        ordered.sort(key=lambda w: (-counts[w], filtered.index(w)))
        return ordered[:5]

    def _first_fact(self, att_text: str) -> str:
        date_match = re.search(r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b", att_text)
        if date_match:
            return date_match.group(0)
        number_match = re.search(r"\b\d+[\d.,]*\b", att_text)
        if number_match:
            return number_match.group(0)
        return ""

    def _fallback_short_summary(self, filename: str, subject: str, kind: str, att_text: str) -> str:
        base_name = filename or "Вложение"
        combined = " ".join([base_name.lower(), (subject or "").lower(), (att_text or "").lower()])

        def contains(tokens: set[str]) -> bool:
            return any(token in combined for token in tokens)

        if kind == "EXCEL":
            if contains({"прайс", "price"}):
                return "ключевые цены"
            if contains({"invoice", "счет", "счёт", "оплат"}):
                return "суммы и реквизиты"
            if contains({"реестр", "registry", "реест"}):
                return "реестр записей"
            if contains({"отчет", "отчёт", "report"}):
                return "показатели отчета"
            return "ключевые данные"

        if kind == "CONTRACT":
            return "условия договора"

        if kind == "PDF":
            if contains({"счет", "счёт", "invoice", "оплат"}):
                return "счет в pdf"
            if contains({"договор", "contract"}):
                return "договор в pdf"
            return "ключевые детали"

        if kind == "INVOICE":
            return "суммы и реквизиты"

        keywords = self._keywords(base_name)
        if keywords:
            return " ".join(keywords[:3])
        return "основное из названия"

    def _passes_quality_gates(self, base_lines: List[str], priority: str, verb: str, mail_type: str) -> bool:
        if len(base_lines) != 2 or any(not ln.strip() for ln in base_lines):
            return False
        if not base_lines[0].startswith(tuple(self._PRIORITY_EMOJI.values())):
            return False
        if not any(base_lines[1].startswith(v) for v in self._VERB_ORDER):
            return False
        base_message = "\n".join(base_lines)
        if len(base_message) >= 300:
            return False
        lowered = base_message.lower()
        if any(phrase in lowered for phrase in self._FORBIDDEN_PHRASES):
            return False
        expected_verb = self._MAIL_TYPE_DEFAULTS.get(mail_type, {}).get("verb")
        if expected_verb:
            if not base_lines[1].startswith(expected_verb):
                return False
        return True

    def _fallback_lines(self, source: str, subject: str, verb: str) -> List[str]:
        now_line = self._build_line1("BLUE", source, subject, datetime.now())
        essence = self._extract_essence(subject, subject)
        safe_verb = verb if verb in self._VERB_ORDER else "Ознакомиться"
        return [now_line, f"{safe_verb} {essence}"]

    def _enforce_length(self, lines: List[str], hard_trim: bool = False) -> List[str]:
        joined = "\n".join(lines)
        if len(joined) < 280:
            return lines
        line1 = lines[0][:150].rstrip()
        line2 = lines[1][:140].rstrip()
        if hard_trim:
            line1 = line1[:120].rstrip()
            line2 = line2[:120].rstrip()
        return [line1, line2]

    @staticmethod
    def _shorten_subject(subject: str) -> str:
        cleaned = re.sub(r"\s{2,}", " ", subject).strip()
        return cleaned[:80] if len(cleaned) > 80 else cleaned

    @staticmethod
    def _normalize_source(sender: str) -> str:
        if not sender:
            return "Отправитель"

        name, email = parseaddr(sender)
        name = name.strip()
        email = email.strip()

        if not name:
            base = email.split("@")[0] if email else sender
            base = re.sub(r"[._]", " ", base).strip()
            name = base.title() if base else "Отправитель"

        readable_name = re.sub(r"\s{2,}", " ", name).strip() or "Отправитель"
        readable_name = readable_name[:60]

        if email:
            return f"{readable_name} <{email}>"

        return readable_name

    @staticmethod
    def _contains_any(text: str, markers: set[str]) -> bool:
        lowered = text.lower()
        return any(marker in lowered for marker in markers)

    @staticmethod
    def _max_priority(left: str, right: str) -> str:
        order = {"BLUE": 0, "YELLOW": 1, "RED": 2}
        return left if order.get(left, 0) >= order.get(right, 0) else right

    def _normalize_verb_choice(self, verb: str) -> str:
        normalized = {
            "оплатить": "Оплатить",
            "оплата": "Оплатить",
            "согласовать": "Согласовать",
            "подписать": "Согласовать",
            "подтвердить": "Ответить",
            "продлить": "Проверить",
            "требуется": "Проверить",
            "проверить": "Проверить",
            "ответить": "Ответить",
            "ознакомиться": "Ознакомиться",
        }.get(verb.lower())

        if normalized:
            return normalized

        for allowed in self._VERB_ORDER:
            if verb.lower().startswith(allowed.lower()[:5]):
                return allowed

        return "Ознакомиться"

    def _normalize_action_subject(
        self, verb: str, subject: str, attachments: List[Attachment], body: str
    ) -> str:
        essence = self._extract_essence(subject, body)
        normalized_object = self._refine_action_object(subject, attachments, essence)

        subject_tokens = {tok.lower() for tok in self._keywords(subject)}
        cleaned_object_parts: list[str] = []
        for token in normalized_object.split():
            plain = re.sub(r"[^\w-]", "", token).lower()
            if plain and any(
                plain == base or plain.startswith(base) or base.startswith(plain)
                for base in subject_tokens
            ):
                continue
            cleaned_object_parts.append(token)

        cleaned_object = " ".join(cleaned_object_parts).strip()
        if not cleaned_object:
            fallback_tokens = self._keywords(subject) or self._keywords(body)
            cleaned_object = " ".join(fallback_tokens[:2]).strip()

        tokens = [tok for tok in cleaned_object.split() if tok]
        tokens = tokens[:3]
        object_phrase = " ".join(tokens)

        if not object_phrase:
            return verb

        return f"{verb} {object_phrase}".strip()

    def _normalize_body_one_liner(self, body_text: str) -> str:
        raw = self._strip_markup(body_text or "")
        if not raw:
            return ""

        greetings = (
            "здравствуйте",
            "добрый день",
            "доброе утро",
            "добрый вечер",
            "привет",
            "hello",
            "hi",
        )
        signature_markers = (
            "--",
            "с уважением",
            "best regards",
            "kind regards",
            "regards",
        )
        filtered_lines: list[str] = []
        for raw_line in raw.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            lowered = line.lower()
            if lowered.startswith(("from:", "sent:", "to:", "subject:", "от:", "дата:")):
                continue
            if line.startswith(">"):
                continue
            if any(lowered.startswith(marker) for marker in signature_markers):
                break
            if self._looks_like_contact(line):
                continue
            for greet in greetings:
                if lowered.startswith(greet):
                    line = line[len(greet) :].strip(" ,.-")
                    lowered = line.lower()
                    break
            if not line:
                continue
            filtered_lines.append(line)

        compact = " ".join(filtered_lines).strip()
        if not compact:
            return ""

        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n", compact) if s.strip()]
        if not sentences:
            return ""

        verbs = {
            "оплатить",
            "согласовать",
            "подтвердить",
            "проверить",
            "выслать",
            "прислать",
            "нужно",
            "требуется",
        }

        def pick_sentence() -> str:
            for sentence in sentences:
                lowered = sentence.lower()
                if any(v in lowered for v in verbs):
                    return sentence
            for sentence in sentences:
                lowered = sentence.lower()
                if self._find_amount(sentence) or self._find_deadline(sentence) or "номер" in lowered:
                    return sentence
            for sentence in sentences:
                if len(sentence.split()) >= 5:
                    return sentence
            return sentences[0]

        primary = pick_sentence()
        tokens = re.findall(r"[\w-]{2,}", primary)
        filler = {
            "здравствуйте",
            "добрый",
            "день",
            "доброе",
            "утро",
            "вечер",
            "сообщается",
            "сообщаем",
            "на",
            "рассмотрение",
            "пожалуйста",
        }
        cleaned_tokens: list[str] = []
        for token in tokens:
            lowered = token.lower()
            if lowered in filler:
                continue
            cleaned_tokens.append(token)

        if len(cleaned_tokens) < 8:
            extra_tokens = [t for t in re.findall(r"[\w-]{2,}", compact) if t]
            for token in extra_tokens:
                lowered = token.lower()
                if lowered in filler:
                    continue
                cleaned_tokens.append(token)
                if len(cleaned_tokens) >= 8:
                    break

        if len(cleaned_tokens) < 8:
            return ""

        cleaned_tokens = cleaned_tokens[:12]
        summary = " ".join(cleaned_tokens)
        return summary.strip()

    def _normalize_body_text(self, body_text: str) -> str:
        raw = self._strip_markup(body_text or "")
        normalized = normalize_text(raw)
        if not normalized:
            return ""

        greetings = (
            "добрый день",
            "доброе утро",
            "добрый вечер",
            "здравствуйте",
            "привет",
            "hello",
            "hi",
        )
        signatures = (
            "с уважением",
            "best regards",
            "kind regards",
            "regards",
            "спасибо",
        )
        attachment_markers = (
            "в приложении",
            "во вложении",
            "прикреп",
            "см. вложение",
            "см. приложение",
            "attachment",
        )

        filtered: list[str] = []
        for raw_line in normalized.split("\n"):
            line = raw_line.strip()
            lowered = line.lower()
            if not line:
                continue
            for greet in greetings:
                if lowered.startswith(greet):
                    line = line[len(greet) :].strip(" ,.-")
                    lowered = line.lower()
                    break
            if not line:
                continue
            for marker in attachment_markers:
                if marker in lowered:
                    line = re.sub(marker, "", line, flags=re.IGNORECASE)
                    lowered = line.lower()
            line = line.strip()
            if not line:
                continue
            if any(lowered.startswith(sig) for sig in signatures):
                continue
            if self._looks_like_contact(line):
                continue
            filtered.append(line)

        return " ".join(filtered).strip()

    @staticmethod
    def _looks_like_contact(line: str) -> bool:
        if "@" in line or "www." in line:
            return True
        phone_like = re.search(r"\+?\d[\d\s().-]{6,}\d", line)
        return bool(phone_like)

    def _pick_body_sentence(self, text: str) -> str:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n", text) if s.strip()]
        for sentence in sentences:
            if len(sentence.split()) >= 4:
                return sentence
        return sentences[0] if sentences else ""

    def _prepare_body_tokens(self, text: str) -> list[str]:
        tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9-]{2,}", text)
        cleaned: list[str] = []
        for token in tokens:
            plain = re.sub(r"[^\w₽€$.,-]", "", token)
            if not plain:
                continue
            cleaned.append(plain)
        return cleaned

    def _render_body_summary(self, tokens: list[str]) -> str:
        if not tokens:
            return ""

        action = self._find_action(" ".join(tokens)) or "Сообщается"
        summary_tokens: list[str] = [action] + tokens
        summary_tokens = summary_tokens[:12]

        summary_tokens = summary_tokens[:12]

        summary = " ".join(summary_tokens)
        while len(summary) > 120 and len(summary_tokens) > 8:
            summary_tokens.pop()
            summary = " ".join(summary_tokens)

        word_count = len(summary.split())
        if word_count < 8:
            return ""

        return summary.strip()

    def _refine_action_object(self, subject: str, attachments: List[Attachment], fallback: str) -> str:
        lowered = subject.lower()
        tokens = re.findall(r"[\w-]{2,}", subject)
        token_pairs = [(t.lower(), t) for t in tokens]
        attachment_kinds = {
            self._detect_attachment_kind(att.filename, att.content_type)
            for att in attachments
        }

        core_stopwords = {
            "уведомление",
            "сообщение",
            "письмо",
            "тема",
            "по",
            "о",
            "об",
            "from",
            "for",
            "новый",
            "новое",
            "новом",
        } | self._STOPWORDS
        payment_tokens = {"счет", "счёт", "invoice", "оплата", "оплат", "bill", "жку"}
        cooperation_tokens = {"сотрудничество", "договор", "контракт", "соглашение"}

        def pick_company() -> str:
            for low, original in token_pairs:
                if low in core_stopwords or low in payment_tokens or low in cooperation_tokens:
                    continue
                if len(low) < 3:
                    continue
                return original.capitalize()
            return ""

        company = pick_company()
        has_price = "PRICE_LIST" in attachment_kinds or "прайс" in lowered
        has_invoice = "INVOICE" in attachment_kinds or self._contains_any(lowered, payment_tokens)
        cooperation = next((low for low in lowered.split() if low in cooperation_tokens), None)

        if has_price:
            return "цены прайса"

        if has_invoice:
            return "счёт" + (f" {company}" if company else "")

        if cooperation:
            if company:
                return f"документы {company}"
            return "документы"

        if "проверка" in lowered or "document" in lowered:
            if company:
                return f"документы {company}"
            return "документы"

        if "table" in lowered or "таблиц" in lowered or "отчет" in lowered or "отчёт" in lowered:
            return "данные"

        essence_keywords = self._keywords(subject) or self._keywords(fallback)
        if company and company.lower() not in {w.lower() for w in essence_keywords}:
            essence_keywords.insert(0, company)
        if essence_keywords:
            return " ".join(essence_keywords[:3])

        return fallback

    @staticmethod
    def _has_deadline(text: str, today, tomorrow) -> bool:
        date_matches = re.findall(r"\b(\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)\b", text)
        for raw in date_matches:
            try:
                parsed = datetime.strptime(raw.replace("/", "."), "%d.%m.%Y").date()
            except ValueError:
                try:
                    parsed = datetime.strptime(raw.replace("/", "."), "%d.%m").date().replace(year=today.year)
                except ValueError:
                    continue
            if parsed in {today, tomorrow}:
                return True
        return "сегодня" in text or "завтра" in text

    @staticmethod
    def _is_management_sender(sender: str) -> bool:
        lowered = (sender or "").lower()
        return any(token in lowered for token in ("client", "клиент", "director", "директор", "manager", "менедж"))

    def _keywords(self, text: str) -> List[str]:
        words = re.findall(r"[\w-]{3,}", text.lower())
        meaningful = [w for w in words if w not in self._STOPWORDS][:8]
        return meaningful

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
        if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp")) or "image" in lower_ct:
            return "IMAGE"
        if any(token in lower for token in ("invoice", "bill", "счет", "счёт")):
            return "INVOICE"
        return "GENERIC"

    @staticmethod
    def _purge_markup_tokens(text: str) -> str:
        cleaned = (text or "").replace("<", " ").replace(">", " ")
        cleaned = re.sub(r"(?i)<!doctype[^>]*", " ", cleaned)
        cleaned = re.sub(r"(?i)\b(html|style|table|doctype)\b", " ", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        compact = cleaned.strip()
        return compact or "Вложение"

    @staticmethod
    def _strip_markup(text: str) -> str:
        cleaned = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<!--.*?-->", " ", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    @staticmethod
    def _escape_html(text: str) -> str:
        return html.escape(text or "", quote=False)

    def _attachment_fallback_summary(self, att_text: str, subject: str, filename: str, kind: str) -> str:
        lowered = att_text.lower()
        kind = self._refine_attachment_kind(att_text, kind)
        keyword = self._pick_keyword(lowered)
        if kind == "PRICE_LIST":
            focus = keyword or "ассортимент"
            core = f"прайс-лист: цены и ассортимент {focus}."
        elif kind == "INVOICE":
            focus = keyword or "оплату"
            core = f"счёт: сумма за {focus} и реквизиты."
        elif kind == "CONTRACT":
            focus = keyword or "соглашения"
            core = f"договор: условия и предмет {focus}."
        else:
            focus = keyword or "данные"
            core = f"документ: основные данные по {focus}."

        return core

    @staticmethod
    def _limit_sentences(text: str, max_sentences: int) -> str:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        limited = " ".join(sentences[:max_sentences])
        return limited

    @staticmethod
    def _ensure_sentence(text: str) -> str:
        cleaned = text.strip()
        if not cleaned.endswith(('.', '!', '?')):
            cleaned += "."
        return cleaned

    @staticmethod
    def _refine_attachment_kind(att_text: str, kind: str) -> str:
        lowered = (att_text or "").lower()
        if any(token in lowered for token in ("прайс", "цена", "стоимост", "прайслист", "ценник")):
            return "PRICE_LIST"
        if any(token in lowered for token in ("счет", "счёт", "invoice", "оплата")):
            return "INVOICE"
        if any(token in lowered for token in ("договор", "соглашение", "контракт")):
            return "CONTRACT"
        return kind

    @staticmethod
    def _pick_keyword(text: str) -> str | None:
        candidates = [w for w in re.findall(r"[\w-]{4,}", text) if len(w) > 4]
        return candidates[0] if candidates else None

    def _filter_tokens(self, tokens: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        placeholder_pattern = re.compile(r"[_№]+")
        for token in tokens:
            plain = re.sub(r"[^\w₽€$.,-]", "", token)
            if not plain or placeholder_pattern.fullmatch(plain):
                continue
            if len(plain) < 3:
                continue
            lowered = plain.lower()
            if lowered.isdigit():
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            cleaned.append(plain)
        return cleaned

    @staticmethod
    def _find_amount(text: str) -> str | None:
        match = re.search(r"(\d[\d\s.,]*\s?(?:₽|руб|eur|eur\.|€|usd|долл|\$))", text, re.IGNORECASE)
        if match:
            return match.group(1).replace(" ", " ").strip()
        return None

    @staticmethod
    def _find_deadline(text: str) -> str | None:
        date_match = re.search(r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b", text)
        if date_match:
            return date_match.group(0)
        word_match = re.search(r"\b(сегодня|завтра|послезавтра)\b", text, re.IGNORECASE)
        if word_match:
            return word_match.group(1)
        return None

    def _find_action(self, text: str) -> str | None:
        actions = {
            "оплат": "Оплатить",
            "соглас": "Согласовать",
            "подпис": "Подписать",
            "утверд": "Согласовать",
            "подтверд": "Подтвердить",
            "предостав": "Предоставить",
            "отправ": "Отправить",
            "ожидаем": "Ответить",
            "треб": "Требуется",
            "pay": "Оплатить",
            "approve": "Согласовать",
            "sign": "Подписать",
            "confirm": "Подтвердить",
            "reply": "Ответить",
        }
        lowered = text.lower()
        for marker, verb in actions.items():
            if marker in lowered:
                return verb
        return None

    @staticmethod
    def _find_change_marker(text: str) -> str | None:
        lowered = text.lower()
        markers = {
            "измен": "изменены условия",
            "обнов": "обновлены данные",
            "нов": "новый документ",
            "дополн": "добавлены требования",
        }
        for marker, phrase in markers.items():
            if marker in lowered:
                return phrase
        return None

    def _attachment_fact(self, doc_type: str, text: str, filename: str) -> str:
        type_label = {
            "CONTRACT": "договор",
            "PRICE": "прайс-лист",
            "INVOICE": "счет/инвойс",
            "TABLE": "таблица",
            "REPORT": "отчет",
            "OTHER": "документ",
        }.get(doc_type, "документ")

        amount = self._find_amount(text)
        deadline = self._find_deadline(text)
        change = self._find_change_marker(text)
        action = self._find_action(text)

        if doc_type == "INVOICE":
            if amount and deadline:
                return f"{type_label}: {amount} до {deadline}"
            if amount:
                return f"{type_label}: {amount} к оплате"
            if deadline:
                return f"{type_label}: оплатить до {deadline}"
        if doc_type == "CONTRACT":
            if change:
                return f"{type_label}: {change}"
            if action:
                return f"{type_label}: {action.lower()}"
            if "соглас" in text.lower():
                return f"{type_label}: требуется согласование"
        if doc_type == "PRICE":
            if change:
                return f"{type_label}: {change}"
            keywords = self._keywords(text)
            if keywords:
                return f"{type_label}: цены на {' '.join(keywords[:3])}"
        if doc_type == "TABLE":
            keywords = self._keywords(text)
            if keywords:
                return f"{type_label}: {' '.join(keywords[:4])}"
        if doc_type == "REPORT":
            keywords = self._keywords(text)
            if keywords:
                return f"{type_label}: {' '.join(keywords[:4])}"
        if amount and doc_type != "INVOICE":
            return f"{type_label}: сумма {amount}"

        name_keywords = self._keywords(filename)
        if name_keywords:
            return f"{type_label}: {' '.join(name_keywords[:3])}"
        return f"{type_label}: по названию файла"

    def _has_primary_signal(self, text: str) -> bool:
        lowered = text.lower()
        if self._find_amount(text):
            return True
        if self._find_deadline(text):
            return True
        if self._find_change_marker(text):
            return True
        return any(marker in lowered for marker in ("оплат", "соглас", "подпис", "треб", "измен", "нов"))


__all__ = ["Attachment", "AttachmentSummary", "InboundMessage", "MessageProcessor"]
