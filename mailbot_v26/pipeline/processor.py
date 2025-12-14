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
        sender_line = self._purge_markup_tokens(
            sanitize_text((message.sender or "").strip() or account_login, max_len=200)
        )
        subject_line = self._purge_markup_tokens(
            sanitize_text((message.subject or "Без темы").strip(), max_len=300) or "Без темы"
        )

        body_clean = clean_email_body(message.body or "")
        body_clean = sanitize_text(body_clean, max_len=6000)

        body_summary = self._purge_markup_tokens(
            self._build_body_summary(body_clean, subject_line, sender_line)
        )

        attachment_blocks: List[tuple[str, str]] = []
        for att in message.attachments or []:
            kind = self._detect_attachment_kind(att.filename, att.content_type)
            if kind in {"IMAGE", "GENERIC"}:
                continue
            block = self._summarize_attachment(att, body_clean, subject_line, kind)
            if block:
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
            safe_blocks = [
                self._infer_attachment_block(att, body_clean, subject_line)
                for att in message.attachments or []
                if self._detect_attachment_kind(att.filename, att.content_type) in {"PDF", "EXCEL", "CONTRACT"}
            ]
            safe_lines: List[str] = [timestamp_line, sender_line, subject_line, "", fallback_body]
            for filename, block in safe_blocks:
                safe_lines.append("")
                safe_lines.append(filename)
                safe_lines.append(block)
            result = "\n".join(safe_lines).strip()

        if not self._is_valid_output(result, baseline):
            minimal_body = self._ensure_sentence(self._fallback_summary("", subject=subject_line, sender=sender_line))
            result = "\n".join([timestamp_line, sender_line, subject_line, "", minimal_body]).strip()

        return result

    def _build_body_summary(self, body: str, subject: str, sender: str) -> str:
        fallback = self._fallback_summary(body, subject=subject, sender=sender)
        if len(body) < 80:
            return fallback

        body_summary_raw = self.llm.summarize_email(body)
        body_summary = sanitize_text(body_summary_raw, max_len=1200)
        if (
            not self._is_meaningful(body_summary, min_len=30)
            or not self._has_two_sentences(body_summary)
            or not self._has_body_terms(body, body_summary)
            or self._contains_forbidden_templates(body_summary)
        ):
            body_summary = fallback
        return body_summary

    def _summarize_attachment(
        self, att: Attachment, body_context: str, subject: str, kind: str
    ) -> tuple[str, str] | None:
        filename = self._purge_markup_tokens(att.filename or "Вложение") or "Вложение"
        att_text = self._strip_markup(sanitize_text(att.text or "", max_len=4000))

        if not att_text or len(att_text) < 80:
            return None

        kind = self._refine_attachment_kind(att_text, kind)

        summary_raw = self.llm.summarize_attachment(att_text, kind=kind)
        summary = self._strip_markup(sanitize_text(summary_raw, max_len=600))
        summary = self._purge_markup_tokens(
            self._guard_attachment_summary(summary, att_text, subject, filename, kind)
        )
        if att_text and summary:
            normalized_overlap = summary.lower().replace("...", "").strip(" .")
            if normalized_overlap and normalized_overlap in att_text.lower():
                summary = self._attachment_fallback_summary(att_text, subject, filename, kind)
        if not self._is_meaningful(summary, min_len=30) or self._contains_forbidden_templates(summary):
            summary = self._attachment_fallback_summary(att_text, subject, filename, kind)

        summary = self._limit_sentences(self._ensure_sentence(summary), 3)
        if not self._is_meaningful(summary, min_len=20):
            return None
        return filename, summary

    def _infer_attachment_block(self, att: Attachment, body_context: str, subject: str) -> tuple[str, str]:
        filename = self._purge_markup_tokens(att.filename or "Вложение") or "Вложение"
        kind = self._detect_attachment_kind(att.filename, att.content_type)
        if kind in {"IMAGE", "GENERIC"}:
            return filename, "Вложение содержит дополнительную информацию из письма."

        att_text = self._strip_markup(sanitize_text(att.text or "", max_len=4000))
        if not att_text or len(att_text) < 80:
            return filename, "Файл требует отдельного просмотра из-за малого объема текста."

        summary = self._attachment_fallback_summary(att_text, subject, filename, kind)
        summary = self._limit_sentences(self._ensure_sentence(self._purge_markup_tokens(summary)), 3)
        return filename, summary

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

        deterministic = MessageProcessor._build_deterministic_body_summary(
            working, subject=subject, sender=sender, limit=limit
        )

        if not MessageProcessor._has_two_sentences(deterministic):
            deterministic = MessageProcessor._ensure_two_sentences(deterministic, subject, sender)

        return MessageProcessor._purge_markup_tokens(deterministic)

    @staticmethod
    def _ensure_two_sentences(text: str, subject: str, sender: str) -> str:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        while len(sentences) < 2:
            hint = f"Письмо связано с темой \"{subject or 'обсуждаемая тема'}\" от {sender or 'неизвестного отправителя'} и содержит уточнения."
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

    def _is_valid_output(self, message: str, baseline_len: int) -> bool:
        if len(message or "") <= baseline_len:
            return False

        semantic = [ln for ln in (message or "").split("\n")[3:] if ln.strip()]
        if len(semantic) < 1:
            return False

        lower_message = (message or "").lower()
        banned = ("=?", "pk", "ihdr", "idat", "содержание письма отсутствует")
        if any(token in lower_message for token in banned):
            return False

        if "<" in message or ">" in message:
            return False
        if any(marker in lower_message for marker in ("doctype", "<html", "<style", "<table")):
            return False
        if re.search(r"\b(html|style|table|doctype)\b", lower_message):
            return False
        if any(ext in lower_message for ext in (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp")):
            return False

        if self._contains_forbidden_templates(message):
            return False

        sections = [part.strip() for part in message.split("\n\n") if part.strip()]
        if any(len(section) > 500 for section in sections):
            return False

        if not any(len(section.split()) > 3 for section in sections[1:]):
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

    @staticmethod
    def _purge_markup_tokens(text: str) -> str:
        cleaned = (text or "").replace("<", " ").replace(">", " ")
        cleaned = re.sub(r"(?i)<!doctype[^>]*", " ", cleaned)
        cleaned = re.sub(r"(?i)\b(html|style|table|doctype)\b", " ", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        compact = cleaned.strip()
        if len(compact.strip("._-")) < 2:
            return "Вложение"
        return compact

    @staticmethod
    def _strip_markup(text: str) -> str:
        cleaned = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<!--.*?-->", " ", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    def _guard_attachment_summary(
        self, summary: str, att_text: str, subject: str, filename: str, kind: str
    ) -> str:
        normalized_summary = re.sub(r"\s+", " ", (summary or "")).strip().lower()
        normalized_source = re.sub(r"\s+", " ", (att_text or "")).strip().lower()
        if not normalized_summary:
            return self._attachment_fallback_summary(att_text, subject, filename, kind)
        if not normalized_source:
            return summary
        if normalized_summary == normalized_source:
            return self._attachment_fallback_summary(att_text, subject, filename, kind)
        if normalized_summary in normalized_source or normalized_source in normalized_summary:
            return self._attachment_fallback_summary(att_text, subject, filename, kind)
        if not self._attachment_has_keyword(normalized_summary, normalized_source):
            return self._attachment_fallback_summary(att_text, subject, filename, kind)
        return summary

    def _attachment_fallback_summary(self, att_text: str, subject: str, filename: str, kind: str) -> str:
        lowered = att_text.lower()
        kind = self._refine_attachment_kind(att_text, kind)
        keyword = self._pick_keyword(lowered)
        name_for_text = re.sub(r"\.[^.\s]+$", "", filename)

        if kind == "PRICE_LIST":
            focus = keyword or "цены"
            core = f"{name_for_text}: прайс-лист с ценами на {focus}."
            detail = "Указаны позиции и стоимость, пригодны для расчета заказа."
        elif kind == "INVOICE":
            focus = keyword or "счет"
            core = f"{name_for_text}: счет на оплату, упомянут {focus}."
            detail = "Есть реквизиты и сумма к оплате, по тексту видно платежное назначение."
        elif kind == "CONTRACT":
            focus = keyword or "договор"
            core = f"{name_for_text}: договор или соглашение, обсуждается {focus}."
            detail = "Текст описывает условия сторон, порядок исполнения и взаимные обязанности."
        else:
            focus = keyword or "данные"
            core = f"{name_for_text}: документ с данными про {focus}."
            detail = "Выделены ключевые пункты содержания, включая основные термины."

        tail = "Нужно изучить вложение для применения информации."
        return " ".join([core, detail, tail])

    @staticmethod
    def _limit_sentences(text: str, max_sentences: int) -> str:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        limited = " ".join(sentences[:max_sentences])
        return limited

    # --- Semantic enforcement helpers ---

    _VERB_CUES = {
        "прошу",
        "направляю",
        "необходимо",
        "оплатить",
        "согласовать",
        "подписать",
        "отправить",
        "пришлите",
        "требуется",
        "ожидаем",
        "обсудить",
        "предлагаю",
        "готовим",
        "поставить",
        "доставить",
        "подтвердите",
        "выслать",
    }

    _FORBIDDEN_TEMPLATES = (
        "касается темы",
        "по теме письма",
        "автор прислал краткое сообщение",
        "без подробностей",
        "без технических деталей",
        "можно просмотреть при необходимости",
        "файл дополняет информацию",
    )

    @classmethod
    def _contains_forbidden_templates(cls, text: str) -> bool:
        normalized = (text or "").lower()
        return any(pattern in normalized for pattern in cls._FORBIDDEN_TEMPLATES)

    @classmethod
    def _extract_body_terms(cls, text: str) -> tuple[str | None, str | None]:
        words = re.findall(r"[\w-]{4,}", text.lower())
        verb = next((w for w in words if w in cls._VERB_CUES or re.search(r"(ть|йте|ите|уем)$", w)), None)
        noun = next((w for w in words if len(w) >= 5 and w != verb), None)
        return verb, noun

    @classmethod
    def _has_body_terms(cls, body: str, summary: str) -> bool:
        verb, noun = cls._extract_body_terms(body)
        lowered = summary.lower()
        verb_ok = not verb or verb in lowered
        noun_ok = not noun or noun in lowered
        return verb_ok and noun_ok

    @classmethod
    def _build_deterministic_body_summary(cls, text: str, subject: str, sender: str, limit: int) -> str:
        verb, noun = cls._extract_body_terms(text)
        snippet = cls._select_informative_snippet(text, limit)
        parts: list[str] = []
        if verb and noun:
            parts.append(f"Отправитель {verb} {noun} и уточняет детали: {snippet}.")
        elif verb:
            parts.append(f"Сообщение сообщает, что необходимо {verb}: {snippet}.")
        elif noun:
            parts.append(f"Основной вопрос касается {noun}: {snippet}.")
        else:
            parts.append(f"Текст содержит детали: {snippet}.")
        parts.append(
            f"Письмо от {sender or 'отправителя'} связано с темой \"{subject or 'без темы'}\" и включает конкретные сведения."
        )
        combined = " ".join(parts)
        return combined[: limit - 3] + "..." if len(combined) > limit else combined

    @staticmethod
    def _select_informative_snippet(text: str, limit: int) -> str:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        for sentence in sentences:
            clean = sentence.strip()
            if len(clean.split()) >= 3:
                return clean[: max(80, min(limit // 2, 200))]
        fallback = (text or "").strip()
        return fallback[: max(80, min(limit // 2, 200))]

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

    @staticmethod
    def _attachment_has_keyword(summary: str, source: str) -> bool:
        source_words = set(re.findall(r"[\w-]{4,}", source))
        return any(word in summary for word in source_words)

