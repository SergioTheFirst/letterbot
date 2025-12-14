from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from mailbot_v26.bot_core.action_engine import analyze_action
from mailbot_v26.domain.domain_classifier import DomainClassifier, MailTypeClassifier
from mailbot_v26.domain.domain_priority import DOMAIN_PRIORITY_MAP
from mailbot_v26.domain.domain_policies import DOMAIN_POLICIES
from mailbot_v26.text import clean_email_body, sanitize_text


logger = logging.getLogger(__name__)


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
        "Продлить",
        "Требуется",
        "Ознакомиться",
    ]
    _MAIL_TYPE_DEFAULTS = {
        "PAYMENT_REQUEST": {"priority": "RED", "verb": "Оплатить"},
        "PAYMENT_REMINDER": {"priority": "RED", "verb": "Оплатить"},
        "CONTRACT_APPROVAL": {"priority": "YELLOW", "verb": "Подписать"},
        "CONTRACT_UPDATE": {"priority": "YELLOW", "verb": "Согласовать"},
        "INVOICE": {"priority": "YELLOW", "verb": "Оплатить"},
        "PRICE_LIST": {"priority": "BLUE", "verb": "Ознакомиться"},
        "DELIVERY_NOTICE": {"priority": "YELLOW", "verb": "Проверить"},
        "DEADLINE_REMINDER": {"priority": "YELLOW", "verb": "Требуется"},
        "ACCOUNT_CHANGE": {"priority": "RED", "verb": "Проверить"},
        "SECURITY_ALERT": {"priority": "RED", "verb": "Проверить"},
        "POLICY_UPDATE": {"priority": "BLUE", "verb": "Ознакомиться"},
        "MEETING_CHANGE": {"priority": "YELLOW", "verb": "Подтвердить"},
        "INFORMATION_ONLY": {"priority": "BLUE", "verb": "Ознакомиться"},
        "UNKNOWN": {"priority": "BLUE", "verb": "Ознакомиться"},
    }

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

        domain = DomainClassifier.classify(message.sender, sender_clean, subject_clean)
        logger.info("Domain detected: %s", domain)
        priority_suggestion = DOMAIN_PRIORITY_MAP.get(domain, DOMAIN_PRIORITY_MAP["UNKNOWN"])
        logger.info("Domain priority suggestion: %s", priority_suggestion)
        mail_type = MailTypeClassifier.classify(subject_clean, body_clean, message.attachments or [], domain)

        action_facts = analyze_action(" ".join([subject_clean, body_clean]))
        priority = self._resolve_priority(message, body_clean, subject_clean, action_facts, domain, mail_type)
        verb = self._select_verb(action_facts, body_clean, domain, mail_type)

        line1 = self._build_line1(priority, sender_clean, subject_clean, message.received_at)
        line2 = self._build_line2(verb, subject_clean, body_clean, domain, message.attachments or [])

        base_lines = self._enforce_length([line1, line2])
        attachments = self._build_attachment_summaries(message.attachments or [], subject_clean)
        telegram_message = self._compose(base_lines, attachments)

        if not self._passes_quality_gates(base_lines, priority, verb, domain, mail_type):
            fallback_lines = self._fallback_lines(sender_clean, subject_clean, verb)
            base_lines = self._enforce_length(fallback_lines)
            telegram_message = self._compose(base_lines, attachments)

        if not self._passes_quality_gates(base_lines, priority, verb, domain, mail_type):
            minimal = self._enforce_length(self._fallback_lines(sender_clean, "Сообщение", verb), hard_trim=True)
            telegram_message = self._compose(minimal, [])

        return telegram_message

    def _resolve_priority(self, message: InboundMessage, body: str, subject: str, facts, domain: str, mail_type: str) -> str:
        sender_domain = (message.sender or "").split("@")[-1].lower()
        combined = " ".join([(subject or "").lower(), body.lower()])
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)

        policy_default = DOMAIN_POLICIES.get(domain, {}).get("default_priority", "BLUE")
        mail_type_default = self._MAIL_TYPE_DEFAULTS.get(mail_type, {})
        priority = mail_type_default.get("priority", policy_default)

        urgent = self._contains_any(combined, {"срочно", "urgent", "asap"})
        if urgent:
            priority = "RED"

        if self._has_deadline(combined, today, tomorrow) and domain != "DOMAIN_REGISTRAR":
            priority = "RED"

        if facts.amount and facts.date and domain != "DOMAIN_REGISTRAR":
            priority = "RED"

        sensitive_domains = {"bank", "nalog", "fns", "court", "gov"}
        if any(dom in sender_domain for dom in sensitive_domains) and facts.action:
            priority = "RED"

        attachment_kinds = {
            self._detect_attachment_kind(att.filename, att.content_type)
            for att in message.attachments or []
        }
        if not facts.action and not urgent:
            if "INVOICE" in attachment_kinds or "CONTRACT" in attachment_kinds:
                priority = self._max_priority(priority, "YELLOW")
            if self._is_management_sender(message.sender):
                priority = self._max_priority(priority, "YELLOW")

        priority = self._max_priority(priority, policy_default)

        if domain in {"BANK", "COURT"} and priority == "BLUE":
            priority = self._max_priority(priority, "YELLOW")

        if domain == "FAMILY" and priority == "RED" and not urgent:
            priority = "BLUE"

        return priority

    def _build_line1(self, priority: str, source: str, subject: str, received_at: datetime | None) -> str:
        time_part = (received_at or datetime.now()).strftime("%H:%M")
        short_subject = self._shorten_subject(subject)
        return f"{self._PRIORITY_EMOJI[priority]} {self._PRIORITY_LABEL[priority]} от {source} — {short_subject} ({time_part})"

    def _build_line2(
        self, verb: str, subject: str, body: str, domain: str, attachments: List[Attachment]
    ) -> str:
        return self._normalize_action_subject(verb, subject, domain, attachments, body)

    def _select_verb(self, facts, body: str, domain: str, mail_type: str) -> str:
        defaults = self._MAIL_TYPE_DEFAULTS.get(mail_type, {})
        policy_defaults = DOMAIN_POLICIES.get(domain, {})
        policy_allowed = policy_defaults.get("allowed_types")
        if defaults.get("verb") and (policy_allowed is None or mail_type in policy_allowed):
            verb = defaults.get("verb")
        else:
            verb = policy_defaults.get("default_verb")

        if not verb:
            lowered_body = body.lower()
            if facts.action and re.search(r"оплат", facts.action):
                verb = "Оплатить"
            elif facts.action and re.search(r"утверд|подпис", facts.action):
                verb = "Подписать"
            elif "соглас" in lowered_body:
                verb = "Согласовать"
            elif "подтверд" in lowered_body:
                verb = "Подтвердить"
            elif "ответ" in lowered_body:
                verb = "Ответить"
            elif "провер" in lowered_body:
                verb = "Проверить"
            elif "треб" in lowered_body:
                verb = "Требуется"
            else:
                verb = "Ознакомиться"

        return verb

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
    ) -> List[AttachmentSummary]:
        usable: list[AttachmentSummary] = []
        for att in attachments:
            kind = self._detect_attachment_kind(att.filename, att.content_type)
            if kind == "IMAGE":
                continue
            summary = self._summarize_attachment(att, subject, kind)
            if not summary:
                continue
            priority_rank = self._ATTACHMENT_ORDER.get(kind, 5)
            usable.append(
                AttachmentSummary(
                    filename=att.filename or "Вложение",
                    description=summary,
                    kind=kind,
                    priority=priority_rank,
                )
            )

        usable.sort(key=lambda item: item.priority)
        deduped: list[AttachmentSummary] = []
        seen: set[str] = set()
        for item in usable:
            filename_key = (item.filename or "").strip().lower()
            if filename_key in seen:
                continue
            seen.add(filename_key)
            deduped.append(item)
        return deduped[:3]

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

    def _compose(self, base_lines: List[str], attachments: List[AttachmentSummary]) -> str:
        rendered_attachments = self._render_attachments(attachments)
        return "\n".join(base_lines + rendered_attachments).strip()

    def _render_attachments(self, attachments: List[AttachmentSummary]) -> List[str]:
        if not attachments:
            return []

        lines: List[str] = [""]
        for attachment in attachments:
            clean_description = " ".join((attachment.description or "").split())
            lines.append(f"{attachment.filename} — {clean_description}")
        return lines

    def _passes_quality_gates(self, base_lines: List[str], priority: str, verb: str, domain: str, mail_type: str) -> bool:
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
        policy_priority = DOMAIN_POLICIES.get(domain, {}).get("default_priority", "BLUE")
        if self._max_priority(policy_priority, priority) != priority:
            return False
        if domain in {"BANK", "COURT"} and priority == "BLUE":
            return False
        family_red = domain == "FAMILY" and priority == "RED"
        if family_red and not self._contains_any(lowered, {"срочно", "urgent", "asap"}):
            return False
        policy_allowed = DOMAIN_POLICIES.get(domain, {}).get("allowed_types")
        expected_verb = self._MAIL_TYPE_DEFAULTS.get(mail_type, {}).get("verb")
        if expected_verb and (policy_allowed is None or mail_type in policy_allowed):
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
    def _max_priority(left: str, right: str) -> str:
        order = {"BLUE": 0, "YELLOW": 1, "RED": 2}
        return left if order.get(left, 0) >= order.get(right, 0) else right

    def _normalize_action_subject(
        self, verb: str, subject: str, domain: str, attachments: List[Attachment], body: str
    ) -> str:
        essence = self._extract_essence(subject, body)
        normalized_object = self._refine_action_object(subject, attachments, essence)
        return f"{verb} {normalized_object}".strip()

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
                return original.upper()
            return ""

        def format_word(word: str) -> str:
            clean = re.sub(r"[^\w-]", "", word)
            if clean.lower() in {"с", "к", "в", "по", "об", "от", "за", "для", "без"}:
                return clean.lower()
            if clean.lower() in {"услуги", "сотрудничество", "счёт", "счет", "прайс", "прайсом", "договор", "контракт"}:
                return clean.lower()
            if len(clean) <= 3:
                return clean.upper()
            if clean.isupper():
                return clean
            return clean.capitalize()

        def build_phrase(parts: List[str]) -> str:
            words: List[str] = []
            for part in parts:
                if not part:
                    continue
                for token in part.split():
                    cleaned = token.strip()
                    if cleaned:
                        words.append(format_word(cleaned))
            if not words:
                return fallback
            if len(words) > 5:
                words = words[:5]
            if words and not words[0].isupper() and len(words[0]) > 3:
                words[0] = words[0].lower()
            phrase = " ".join(words).strip()
            return phrase if phrase else fallback

        company = pick_company()
        has_price = "PRICE_LIST" in attachment_kinds or "прайс" in lowered
        has_invoice = "INVOICE" in attachment_kinds or self._contains_any(lowered, payment_tokens)
        cooperation = next((low for low in lowered.split() if low in cooperation_tokens), None)

        if has_price:
            return build_phrase(["с", "прайсом", company or fallback])

        if has_invoice:
            descriptors = [orig for low, orig in token_pairs if low not in payment_tokens and low not in core_stopwords]
            mapped = []
            for desc in descriptors:
                lower_desc = desc.lower()
                if lower_desc.startswith("услуг") or "service" in lower_desc:
                    mapped.append("за услуги")
                else:
                    mapped.append(desc)
            parts: List[str] = ["счёт"]
            parts.extend(mapped[:2])
            if company and company not in parts:
                parts.append(company)
            return build_phrase(parts)

        if cooperation:
            parts = [cooperation]
            if company:
                parts.extend(["с", company])
            return build_phrase(parts)

        essence_keywords = self._keywords(subject)
        if company and company.lower() not in {w.lower() for w in essence_keywords}:
            essence_keywords.append(company)
        if essence_keywords:
            return build_phrase(essence_keywords)

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


__all__ = ["Attachment", "AttachmentSummary", "InboundMessage", "MessageProcessor"]
