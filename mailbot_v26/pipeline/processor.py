from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from mailbot_v26.bot_core.action_engine import analyze_action
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

    _FORBIDDEN_PHRASES = {
        "касается",
        "по теме",
        "можно просмотреть",
        "содержит информацию",
        "без подробностей",
    }

    _PRIORITY_EMOJI = {"RED": "🔴", "YELLOW": "🟡", "BLUE": "🔵"}
    _PRIORITY_LABEL = {"RED": "СРОЧНО", "YELLOW": "ВАЖНО", "BLUE": "ИНФО"}
    _ATTACHMENT_ORDER = {"INVOICE": 0, "CONTRACT": 1, "PDF": 2, "EXCEL": 3, "GENERIC": 4}
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
    _VERB_ORDER = [
        "Оплатить",
        "Подписать",
        "Согласовать",
        "Подтвердить",
        "Проверить",
        "Ответить",
        "Требуется",
        "Ознакомиться",
    ]

    def __init__(self, config, state) -> None:
        self.config = config
        self.state = state

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

        action_facts = analyze_action(" ".join([subject_clean, body_clean]))
        priority = self._resolve_priority(message, body_clean, action_facts)

        line1 = self._build_line1(priority, sender_clean, subject_clean, message.received_at)
        line2 = self._build_line2(action_facts, subject_clean, body_clean)

        base_lines = self._enforce_length([line1, line2])
        attachments = self._build_attachments(message.attachments or [], subject_clean)
        telegram_message = self._compose(base_lines, attachments)

        if not self._passes_quality_gates(base_lines):
            fallback_lines = self._fallback_lines(sender_clean, subject_clean)
            base_lines = self._enforce_length(fallback_lines)
            telegram_message = self._compose(base_lines, attachments)

        if not self._passes_quality_gates(base_lines):
            minimal = self._enforce_length(self._fallback_lines(sender_clean, "Сообщение"), hard_trim=True)
            telegram_message = self._compose(minimal, [])

        return telegram_message

    def _resolve_priority(self, message: InboundMessage, body: str, facts) -> str:
        sender_domain = (message.sender or "").split("@")[-1].lower()
        subject = (message.subject or "").lower()
        combined = " ".join([subject, body.lower()])
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)

        if self._contains_any(combined, {"срочно", "urgent", "asap"}):
            return "RED"

        if self._has_deadline(combined, today, tomorrow):
            return "RED"

        if facts.amount and facts.date:
            return "RED"

        sensitive_domains = {"bank", "nalog", "fns", "court", "gov"}
        if any(dom in sender_domain for dom in sensitive_domains) and facts.action:
            return "RED"

        attachment_kinds = {
            self._detect_attachment_kind(att.filename, att.content_type)
            for att in message.attachments or []
        }
        if facts.action:
            return "YELLOW"
        if "INVOICE" in attachment_kinds or "CONTRACT" in attachment_kinds:
            return "YELLOW"
        if self._is_management_sender(message.sender):
            return "YELLOW"

        return "BLUE"

    def _build_line1(self, priority: str, source: str, subject: str, received_at: datetime | None) -> str:
        time_part = (received_at or datetime.now()).strftime("%H:%M")
        short_subject = self._shorten_subject(subject)
        return f"{self._PRIORITY_EMOJI[priority]} {self._PRIORITY_LABEL[priority]} от {source} — {short_subject} ({time_part})"

    def _build_line2(self, facts, subject: str, body: str) -> str:
        verb = self._select_verb(facts, body)
        essence = self._extract_essence(subject, body)
        return f"{verb} {essence}"

    def _select_verb(self, facts, body: str) -> str:
        lowered_body = body.lower()
        if facts.action and re.search(r"оплат", facts.action):
            return "Оплатить"
        if facts.action and re.search(r"утверд|подпис", facts.action):
            return "Подписать"
        if "соглас" in lowered_body:
            return "Согласовать"
        if "подтверд" in lowered_body:
            return "Подтвердить"
        if "ответ" in lowered_body:
            return "Ответить"
        if "провер" in lowered_body:
            return "Проверить"
        if "треб" in lowered_body:
            return "Требуется"
        return "Ознакомиться"

    def _extract_essence(self, subject: str, body: str, max_words: int = 5) -> str:
        keywords = self._keywords(subject)
        if len(keywords) < 2:
            keywords.extend(self._keywords(body))
        filtered = [w for w in keywords if not w.isdigit()][: max_words + 1]
        if len(filtered) < 2:
            filtered.extend([w for w in ("сообщение", "детали") if w not in filtered])
        essence_words = filtered[: max(2, min(max_words, len(filtered)))]
        return " ".join(essence_words)

    def _build_attachments(self, attachments: List[Attachment], subject: str) -> List[str]:
        usable: list[tuple[int, str, str]] = []
        for att in attachments:
            kind = self._detect_attachment_kind(att.filename, att.content_type)
            if kind == "IMAGE":
                continue
            summary = self._summarize_attachment(att, subject, kind)
            if not summary:
                continue
            priority_rank = self._ATTACHMENT_ORDER.get(kind, 5)
            usable.append((priority_rank, att.filename or "Вложение", summary))

        usable.sort(key=lambda item: item[0])
        blocks: List[str] = []
        for _, filename, summary in usable[:3]:
            blocks.append("")
            blocks.append(filename)
            blocks.append(summary)
        return blocks

    def _summarize_attachment(self, att: Attachment, subject: str, kind: str) -> str | None:
        filename = self._purge_markup_tokens(att.filename or "Вложение")
        att_text = self._strip_markup(sanitize_text(att.text or "", max_len=1500))
        if not att_text or len(att_text) < 40:
            return None

        summary = self._attachment_fallback_summary(att_text, subject, filename, kind)
        summary = self._limit_sentences(self._ensure_sentence(summary), 2)
        if subject.lower() in summary.lower():
            return None
        return summary

    def _compose(self, base_lines: List[str], attachments: List[str]) -> str:
        return "\n".join(base_lines + attachments).strip()

    def _passes_quality_gates(self, base_lines: List[str]) -> bool:
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
        return True

    def _fallback_lines(self, source: str, subject: str) -> List[str]:
        now_line = self._build_line1("BLUE", source, subject, datetime.now())
        essence = self._extract_essence(subject, subject)
        return [now_line, f"Ознакомиться {essence}"]

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
        if "@" in sender:
            name_part = sender.split("<")[-1].split("@")[0]
            readable = re.sub(r"[._]", " ", name_part).strip()
            readable = readable or sender.split("@")[0]
            return readable.title()[:60]
        return sender.strip()[:60]

    @staticmethod
    def _contains_any(text: str, markers: set[str]) -> bool:
        lowered = text.lower()
        return any(marker in lowered for marker in markers)

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

    def _attachment_fallback_summary(self, att_text: str, subject: str, filename: str, kind: str) -> str:
        lowered = att_text.lower()
        kind = self._refine_attachment_kind(att_text, kind)
        keyword = self._pick_keyword(lowered)
        name_for_text = re.sub(r"\.[^.\s]+$", "", filename)

        if kind == "PRICE_LIST":
            focus = keyword or "цены"
            core = f"{name_for_text}: прайс-лист с ценами на {focus}."
        elif kind == "INVOICE":
            focus = keyword or "оплату"
            core = f"{name_for_text}: счет на оплату за {focus}."
        elif kind == "CONTRACT":
            focus = keyword or "договор"
            core = f"{name_for_text}: договор, обсуждаются условия {focus}."
        else:
            focus = keyword or "данные"
            core = f"{name_for_text}: документ с данными про {focus}."

        detail = "Нужно изучить вложение для применения информации."
        return " ".join([core, detail])

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


__all__ = ["Attachment", "InboundMessage", "MessageProcessor"]
