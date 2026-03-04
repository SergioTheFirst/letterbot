from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

from mailbot_v26.text.clean_email import clean_email_body
from mailbot_v26.telegram_utils import escape_tg_html
from mailbot_v26.ui.emoji_whitelist import strip_disallowed_emojis
from mailbot_v26.text.sanitize import is_binaryish

_ATTACHMENT_SNIPPET_LIMIT = 240
_BASE64_FRAGMENT = re.compile(r"[A-Za-z0-9+/]{80,}={0,2}")
_TOKEN_RE = re.compile(r"[a-zа-я0-9]+", re.IGNORECASE)
_CONTEXT_KEYWORDS = (
    "срок",
    "дедлайн",
    "сегодня",
    "завтра",
    "вчера",
    "недел",
    "месяц",
    "день",
    "истори",
    "ранее",
    "раньше",
    "пауза",
    "окно",
    "риск",
    "угроз",
    "довер",
    "сниж",
    "ухудш",
    "хуже",
    "эскалац",
    "просроч",
    "задерж",
)
_SUBJECT_PREFIX_RE = re.compile(r"^(?:(?:re|fw|fwd)\s*:\s*)+", re.IGNORECASE)
_RUB_AMOUNT_RE = re.compile(
    r"(\d{1,3}(?:[ \u00a0]\d{3})+|\d{4,})(?:[\.,]\d{1,2})?\s*(?:₽|руб\.?|рублей|rur)?",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b(\d{2})\.(\d{2})(?:\.(\d{2,4}))?\b")
_PAYMENT_DUE_CONTEXT_RE = re.compile(
    r"(?:оплат(?:ить|а|е)?\s*до|срок\s*оплат[ыы]?|до)\s*[:\-]?\s*(\d{2}\.\d{2}(?:\.\d{2,4})?)",
    re.IGNORECASE,
)
_INVOICE_NUMBER_RE = re.compile(
    r"(?:сч[её]т|invoice)\s*(?:№|no\.?|n\s*°)\s*([0-9a-zа-я\-/]{1,32})",
    re.IGNORECASE,
)
_PERIOD_RE = re.compile(
    r"\b(январ(?:ь|я)|феврал(?:ь|я)|март(?:а)?|апрел(?:ь|я)|ма[йя]|июн(?:ь|я)|июл(?:ь|я)|август(?:а)?|сентябр(?:ь|я)|октябр(?:ь|я)|ноябр(?:ь|я)|декабр(?:ь|я))\s+(20\d{2})\b",
    re.IGNORECASE,
)
_COUNTERPARTY_RE = re.compile(r"\b(ООО|АО|ПАО|ИП)\s+[A-Za-zА-Яа-яЁё0-9\-«»\"]{2,}")
_TOKEN_CLEAN_RE = re.compile(r"[^A-Za-zА-Яа-яЁё0-9_\-]+")
_INSIGHT_SUFFIX_LIMIT = 60
_INTERNAL_NOISE_MARKERS = (
    "DecisionTraceV1",
    "ATTENTION_GATE",
    "LLM_GATE",
    "Коды:",
    "Контрфакты",
)
_WATERMARK_LINE = "<i>Powered by LetterBot.ru</i>"


@dataclass(frozen=True)
class TelegramRenderFields:
    action_line: str
    summary: str
    insights: tuple[str, ...]
    commitments: tuple[str, ...]


def _escape_dynamic(text: str | None) -> str:
    cleaned = strip_disallowed_emojis(str(text or ""))
    return escape_tg_html(cleaned)


def _strip_internal_noise(text: str | None) -> str:
    value = str(text or "")
    for marker in _INTERNAL_NOISE_MARKERS:
        value = value.replace(marker, "")
    cleaned_lines = [re.sub(r"[ 	]+", " ", line).strip() for line in value.splitlines()]
    return "\n".join(line for line in cleaned_lines if line).strip()


def _clean_excerpt(text: str | None, max_lines: int = 3) -> str:
    cleaned = clean_email_body(_strip_internal_noise(text))
    if not cleaned:
        return ""
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    if not lines:
        return ""
    if len(lines) == 1 and _is_trivial_sentence(lines[0]):
        return ""
    return "\n".join(lines[: max(1, max_lines)])


def normalize_sentence(text: str | None) -> str:
    if not text:
        return ""
    tokens = _TOKEN_RE.findall(text.lower())
    return " ".join(tokens)


def _token_set(text: str) -> set[str]:
    return set(normalize_sentence(text).split())


def _has_context_signal(text: str) -> bool:
    normalized = normalize_sentence(text)
    return any(keyword in normalized for keyword in _CONTEXT_KEYWORDS)


def is_semantic_duplicate(left: str, right: str) -> bool:
    normalized_left = normalize_sentence(left)
    normalized_right = normalize_sentence(right)
    if not normalized_left or not normalized_right:
        return False
    if normalized_left == normalized_right:
        return True
    shorter, longer = sorted(
        (normalized_left, normalized_right), key=len
    )
    if shorter in longer and len(shorter) / max(len(longer), 1) >= 0.8:
        return True
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens or not right_tokens:
        return False
    intersection = left_tokens & right_tokens
    union = left_tokens | right_tokens
    if not union:
        return False
    jaccard = len(intersection) / len(union)
    min_len = min(len(left_tokens), len(right_tokens))
    overlap = len(intersection) / max(min_len, 1)
    return (jaccard >= 0.85 and min_len >= 3) or (overlap >= 0.9 and min_len >= 2)


def _is_trivial_sentence(text: str | None) -> bool:
    if not text or not text.strip():
        return True
    stripped = text.strip()
    if len(stripped) < 4:
        return True
    tokens = normalize_sentence(stripped).split()
    return len(tokens) < 2


def split_text_into_sentences(text: str | None) -> list[str]:
    if not text:
        return []
    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
    sentences: list[str] = []
    buffer: list[str] = []
    index = 0
    while index < len(normalized):
        char = normalized[index]
        if char in ".!?;\n":
            if char == ".":
                while index + 1 < len(normalized) and normalized[index + 1] == ".":
                    index += 1
            sentence = "".join(buffer).strip()
            if sentence and not _is_trivial_sentence(sentence):
                sentences.append(sentence)
            buffer = []
        else:
            buffer.append(char)
        index += 1
    tail = "".join(buffer).strip()
    if tail and not _is_trivial_sentence(tail):
        sentences.append(tail)
    return sentences


def dedup_text_by_sentence(text: str | None) -> str:
    sentences = split_text_into_sentences(text)
    deduped = dedup_sentences(sentences)
    if not deduped:
        return ""
    if len(deduped) == 1:
        trailing = ""
        if text:
            match = re.search(r"([.!?;]+)\s*$", text.strip())
            if match:
                trailing = match.group(1)
        return f"{deduped[0]}{trailing}"
    return "\n".join(f"• {sentence}" for sentence in deduped)


def dedup_sentences(sentences: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    for sentence in sentences:
        cleaned = (sentence or "").strip()
        if not cleaned:
            continue
        if any(is_semantic_duplicate(cleaned, existing) for existing in deduped):
            continue
        deduped.append(cleaned)
    return deduped


def dedup_rendered_lines(lines: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    for line in lines:
        if not line.strip():
            deduped.append(line)
            continue
        cleaned = line.strip()
        if "<tg-spoiler>" in cleaned or "</tg-spoiler>" in cleaned:
            deduped.append(line)
            continue
        if any(
            is_semantic_duplicate(cleaned, existing.strip())
            for existing in deduped
            if existing.strip()
        ):
            continue
        deduped.append(line)
    return deduped


def dedup_rendered_text(text: str) -> str:
    return "\n".join(dedup_rendered_lines(text.splitlines()))


def _normalize_subject_for_compare(text: str) -> str:
    working = (text or "").strip()
    if not working:
        return ""
    working = re.sub(r"<[^>]+>", " ", working)
    working = working.replace("\\", "/")
    working = re.sub(r"\s+", " ", working)
    while True:
        stripped = _SUBJECT_PREFIX_RE.sub("", working).strip()
        if stripped == working:
            break
        working = stripped
    return working.casefold()


def _maybe_drop_duplicate_subject_line(
    header_subject: str,
    body_lines: list[str],
) -> list[str]:
    if not body_lines:
        return body_lines
    first_line = (body_lines[0] or "").strip()
    if not first_line:
        return body_lines
    normalized_subject = _normalize_subject_for_compare(header_subject)
    normalized_first = _normalize_subject_for_compare(first_line)
    if normalized_subject and normalized_subject == normalized_first:
        return body_lines[1:]
    return body_lines


def apply_semantic_gates(
    *,
    action_line: str | None,
    summary: str | None,
    insights: Iterable[str] | None = None,
    commitments: Iterable[str] | None = None,
) -> TelegramRenderFields:
    resolved_action = _strip_internal_noise(action_line)
    resolved_summary = dedup_text_by_sentence(summary)
    if _is_trivial_sentence(resolved_summary):
        resolved_summary = ""

    if resolved_summary and resolved_action and is_semantic_duplicate(
        resolved_action, resolved_summary
    ):
        resolved_summary = ""

    filtered_insights: list[str] = []
    for insight in insights or []:
        cleaned = dedup_text_by_sentence(insight)
        if not cleaned:
            continue
        if not _has_context_signal(cleaned):
            continue
        if resolved_action and is_semantic_duplicate(resolved_action, cleaned):
            continue
        if resolved_summary and is_semantic_duplicate(resolved_summary, cleaned):
            continue
        filtered_insights.append(cleaned)
    filtered_insights = dedup_sentences(filtered_insights)

    filtered_commitments: list[str] = []
    for commitment in commitments or []:
        cleaned = dedup_text_by_sentence(commitment)
        if not cleaned:
            continue
        if resolved_action and is_semantic_duplicate(resolved_action, cleaned):
            continue
        if resolved_summary and is_semantic_duplicate(resolved_summary, cleaned):
            continue
        filtered_commitments.append(cleaned)
    filtered_commitments = dedup_sentences(filtered_commitments)

    return TelegramRenderFields(
        action_line=resolved_action,
        summary=resolved_summary,
        insights=tuple(filtered_insights),
        commitments=tuple(filtered_commitments),
    )


def _normalize_attachment_text(text: Any) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode(errors="ignore")
    return " ".join(str(text).split())


def _truncate_attachment_text(text: str) -> str:
    if len(text) <= _ATTACHMENT_SNIPPET_LIMIT:
        return text
    truncated = text[: _ATTACHMENT_SNIPPET_LIMIT - 4].rstrip()
    return f"{truncated}...."


def _collect_attachment_source_text(
    attachments: list[dict[str, Any]],
    *,
    subject: str,
    summary: str,
) -> str:
    parts: list[str] = [subject or "", summary or ""]
    for attachment in attachments:
        text = _normalize_attachment_text(attachment.get("text"))
        if text and not _is_binary_leak(text):
            parts.append(text)
        attachment_summary = _normalize_attachment_text(attachment.get("summary"))
        if attachment_summary:
            parts.append(attachment_summary)
    return " ".join(part for part in parts if part).strip()


def _normalize_amount(amount: str) -> str:
    cleaned = " ".join((amount or "").replace("\u00a0", " ").split())
    digits = re.sub(r"[^0-9]", "", cleaned)
    if not digits:
        return ""
    grouped = f"{int(digits):,}".replace(",", " ")
    return f"{grouped} ₽"


def _compact_date(date_value: str) -> str:
    match = _DATE_RE.search(date_value or "")
    if not match:
        return ""
    day, month = match.group(1), match.group(2)
    return f"{day}.{month}"


def _invoice_attachment_insight(
    mail_type: str,
    text: str,
    attachments_count: int,
    message_facts: dict[str, Any] | None = None,
) -> str | None:
    invoice_types = {"INVOICE", "PAYMENT_REMINDER", "INVOICE_OVERDUE", "OVERDUE_INVOICE"}
    lowered = text.lower()
    invoice_text_signal = any(
        marker in lowered
        for marker in ("счет", "счёт", "invoice", "к оплате", "итого", "оплатить до")
    )
    incident_text_signal = any(
        marker in lowered
        for marker in ("offline", "outage", "security alert", "подозрительный вход", "недоступ")
    )
    facts = message_facts or {}
    decision_invoice = str(facts.get("doc_kind") or "") == "invoice"
    if mail_type not in invoice_types and not decision_invoice and (not invoice_text_signal or incident_text_signal):
        return None
    if facts.get("doc_kind") and facts.get("doc_kind") != "invoice":
        return None

    invoice_number = ""
    number_match = _INVOICE_NUMBER_RE.search(text)
    if number_match:
        candidate = (number_match.group(1) or "").strip(" -.:;,")
        if candidate:
            invoice_number = f"Счет №{candidate}"
    amount = _normalize_amount(str(facts.get("amount") or ""))
    for match in _RUB_AMOUNT_RE.finditer(text):
        if (match.start() > 0 and text[match.start() - 1] == ".") or (
            match.end() < len(text) and text[match.end() : match.end() + 1] == "."
        ):
            continue
        chunk = match.group(0)
        start = max(0, match.start() - 28)
        context = lowered[start : match.end() + 6]
        if any(token in context for token in ("итого", "сумм", "к оплат", "оплат", "всего")):
            amount = _normalize_amount(chunk)
            break
    if not amount:
        for match in _RUB_AMOUNT_RE.finditer(text):
            if (match.start() > 0 and text[match.start() - 1] == ".") or (
                match.end() < len(text) and text[match.end() : match.end() + 1] == "."
            ):
                continue
            chunk = match.group(0)
            start = max(0, match.start() - 16)
            context = lowered[start : match.end() + 6]
            if "₽" in chunk or "руб" in context:
                amount = _normalize_amount(chunk)
                break
    due_date = _compact_date(str(facts.get("due_date") or ""))
    due_match = _PAYMENT_DUE_CONTEXT_RE.search(text)
    if not due_date:
        due_date = _compact_date(due_match.group(1)) if due_match else ""
    if not due_date:
        for match in _DATE_RE.finditer(text):
            start = max(0, match.start() - 24)
            context = lowered[start : match.end() + 8]
            if any(token in context for token in ("до", "срок", "оплат")):
                due_date = _compact_date(match.group(0))
                break
    parts: list[str] = []
    if invoice_number:
        parts.append(invoice_number)
    if amount:
        parts.append(amount)
    if due_date:
        parts.append(f"до {due_date}")
    if parts:
        return f"📎 {' · '.join(parts)}"
    suffix = "влож." if attachments_count > 0 else "вложение"
    count = max(attachments_count, 1)
    return f"📎 Счёт · {count} {suffix}"


def _act_attachment_insight(mail_type: str, text: str) -> str | None:
    if not (mail_type.startswith("ACT") or "RECONCILIATION" in mail_type):
        return None
    period_match = _PERIOD_RE.search(text)
    if period_match:
        month = period_match.group(1)
        year = period_match.group(2)
        return f"📎 Акт сверки · {month} {year}"
    counterparty_match = _COUNTERPARTY_RE.search(text)
    if counterparty_match:
        return f"📎 Акт сверки · {counterparty_match.group(0)}"
    return "📎 Акт сверки"


def _table_attachment_insight(attachments: list[dict[str, Any]]) -> str | None:
    for attachment in attachments:
        filename = str(attachment.get("filename") or "вложение").strip()
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in {"xls", "xlsx", "xlsm", "xlsb", "csv"}:
            continue
        text = _normalize_attachment_text(attachment.get("text") or attachment.get("summary") or "")
        if not text:
            continue
        raw_tokens = _TOKEN_CLEAN_RE.sub(" ", text).split()
        tokens: list[str] = []
        for token in raw_tokens:
            if len(token) < 3 or token.isdigit():
                continue
            lowered = token.lower()
            if lowered in {"sheet", "лист", "строка", "колонка", "таблица", "nan"}:
                continue
            if token in tokens:
                continue
            tokens.append(token)
            if len(tokens) == 3:
                break
        if not tokens:
            continue
        suffix = " / ".join(tokens)
        if len(suffix) > _INSIGHT_SUFFIX_LIMIT:
            suffix = suffix[: _INSIGHT_SUFFIX_LIMIT - 1].rstrip() + "…"
        return f"📎 {filename} — {suffix}"
    return None


def build_attachment_insight(
    *,
    mail_type: str | None,
    attachments: list[dict[str, Any]],
    subject: str = "",
    summary: str = "",
    message_facts: dict[str, Any] | None = None,
) -> str | None:
    if not attachments:
        return None
    normalized_mail_type = (mail_type or "").strip().upper()
    source_text = _collect_attachment_source_text(attachments, subject=subject, summary=summary)
    invoice_line = _invoice_attachment_insight(
        normalized_mail_type,
        source_text,
        len(attachments),
        message_facts=message_facts,
    )
    if invoice_line:
        return invoice_line
    act_line = _act_attachment_insight(normalized_mail_type, source_text)
    if act_line:
        return act_line
    table_line = _table_attachment_insight(attachments)
    if table_line:
        return table_line
    if len(attachments) == 1:
        filename = str(attachments[0].get("filename") or "вложение").strip()
        return f"📎 1 вложение: {filename}"
    return f"📎 {len(attachments)} вложения"


def _is_binary_leak(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if "data=b'" in lowered or "data=b\"" in lowered:
        return True
    stripped = text.lstrip()
    if stripped.startswith("b'") or stripped.startswith('b"'):
        return True
    if is_binaryish(text):
        return True
    if _BASE64_FRAGMENT.search(text):
        if any(char in text for char in "+/=") or any(char.isdigit() for char in text):
            return True
    if len(text) >= 60:
        base64ish = sum(1 for char in text if char.isalnum() or char in "+/=")
        if base64ish / len(text) > 0.9 and any(
            char.isdigit() or char in "+/=" for char in text
        ):
            return True
    printable = 0
    total = len(text)
    for char in text:
        if char in ("\n", "\r", "\t"):
            printable += 1
        elif char.isprintable():
            printable += 1
    if total == 0:
        return False
    printable_ratio = printable / total
    return printable_ratio < 0.7


def _attachment_skipped_reason(attachment: dict[str, Any]) -> str | None:
    reason = attachment.get("skipped_reason")
    if reason:
        return str(reason)
    metadata = attachment.get("metadata")
    if isinstance(metadata, dict):
        reason = metadata.get("skipped_reason")
        if reason:
            return str(reason)
    return None


def _attachment_size_bytes(attachment: dict[str, Any]) -> int:
    for key in ("size_bytes", "size"):
        value = attachment.get(key)
        if isinstance(value, int):
            return value
    return 0


def _format_size_mb(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 МБ"
    size_mb = size_bytes / (1024 * 1024)
    formatted = f"{size_mb:.1f}".rstrip("0").rstrip(".")
    return f"{formatted} МБ"


def _attachment_type_label(filename: str) -> str:
    cleaned = (filename or "").strip()
    if "." not in cleaned:
        return "ДРУГОЕ"
    ext = cleaned.rsplit(".", 1)[-1].lower()
    ext = re.sub(r"[^a-z0-9]+", "", ext)
    if not ext:
        return "ДРУГОЕ"
    normalized = {
        "jpeg": "JPG",
        "jpg": "JPG",
    }.get(ext, ext.upper())
    return normalized


def _attachment_type_summary(attachments: list[dict[str, Any]]) -> str:
    counts = Counter(
        _attachment_type_label(str(attachment.get("filename") or ""))
        for attachment in attachments
    )
    summary_parts = [f"{ext}×{count}" for ext, count in sorted(counts.items())]
    summary = ", ".join(summary_parts)
    if summary:
        return f"Вложения: {len(attachments)} ({summary})"
    return f"Вложения: {len(attachments)}"


def format_priority_line(priority: str, from_email: str) -> str:
    if priority not in {"🔴", "🟡", "🔵"}:
        priority = "🔵"
    safe_sender = _escape_dynamic(from_email or "неизвестно")
    return f"{priority} от {safe_sender}:"


def format_subject(subject: str) -> str:
    safe_subject = _escape_dynamic(subject or "(без темы)")
    return f"<b>{safe_subject}</b>"


def format_main_action(action_line: str | None) -> str:
    cleaned = _strip_internal_noise(action_line)
    if not cleaned:
        cleaned = "Действий не требуется"
    safe_action = _escape_dynamic(cleaned)
    return f"<b><i>{safe_action}</i></b>"


def format_narrative_block(
    *,
    fact: str,
    context: str | None,
    action: str | None,
) -> str:
    lines = [f"<b>Факт:</b> <i>{_escape_dynamic(fact)}</i>"]
    if context:
        lines.append(f"<b>Контекст:</b> <i>{_escape_dynamic(context)}</i>")
    if action:
        lines.append(f"<b>Действие:</b> <i>{_escape_dynamic(action)}</i>")
    return "\n".join(lines)


def format_attachments_block(attachments: list[dict[str, Any]]) -> str:
    if not attachments:
        return ""
    lines = [_attachment_type_summary(attachments)]
    for attachment in attachments:
        filename = _escape_dynamic(attachment.get("filename") or "вложение")
        skipped_reason = _attachment_skipped_reason(attachment)
        if skipped_reason == "too_large":
            size_display = _format_size_mb(_attachment_size_bytes(attachment))
            lines.append(
                f"{filename} — <i>слишком большой файл ({size_display}), извлечение отключено</i>"
            )
            continue
        if skipped_reason == "total_limit":
            lines.append(f"{filename} — <i>пропущен из-за ограничения размера письма</i>")
            continue
        extracted_text = _normalize_attachment_text(attachment.get("text"))
        if extracted_text and not _is_binary_leak(extracted_text):
            extracted_text = _truncate_attachment_text(extracted_text)
        else:
            extracted_text = ""
        if extracted_text:
            safe_text = _escape_dynamic(extracted_text)
            lines.append(f"{filename} — <i>{safe_text}</i>")
        else:
            lines.append(f"{filename}")
    return "\n".join(lines)


def build_telegram_text(
    *,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str,
    attachments: list[dict[str, Any]],
    mail_type: str | None = None,
    summary: str = "",
    message_facts: dict[str, Any] | None = None,
    relationship_profile: dict[str, Any] | None = None,
) -> str:
    body_lines = _maybe_drop_duplicate_subject_line(subject, [format_main_action(action_line)])
    lines = [
        format_priority_line(priority, from_email),
        format_subject(subject),
        *body_lines,
    ]
    attachment_line = build_attachment_insight(
        mail_type=mail_type,
        attachments=attachments,
        subject=subject,
        summary=summary,
        message_facts=message_facts,
    )
    if attachment_line:
        lines.append("")
        lines.append(_escape_dynamic(attachment_line))
    if relationship_profile:
        emails_count = int(relationship_profile.get("emails_count") or 0)
        invoice_count = int(relationship_profile.get("invoice_count") or 0)
        overdue_count = int(relationship_profile.get("overdue_count") or 0)
        has_context = emails_count >= 10 or invoice_count >= 3 or overdue_count >= 2
        if has_context:
            lines.extend(
                [
                    "",
                    "💡 Контрагент:",
                    f"{emails_count} писем",
                    f"{invoice_count} счета",
                    f"{overdue_count} задержка",
                ]
            )
    return dedup_rendered_text("\n".join(lines))


def render_telegram_message(
    *,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str | None,
    summary: str | None = None,
    insights: Iterable[str] | None = None,
    commitments: Iterable[str] | None = None,
    attachments: list[dict[str, Any]] | None = None,
    mail_type: str | None = None,
    message_facts: dict[str, Any] | None = None,
    relationship_profile: dict[str, Any] | None = None,
) -> str:
    fields = apply_semantic_gates(
        action_line=action_line,
        summary=summary,
        insights=insights,
        commitments=commitments,
    )
    summary_excerpt = _clean_excerpt(summary, max_lines=3)
    base_text = build_telegram_text(
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=_resolve_action_line(fields.action_line),
        attachments=attachments or [],
        mail_type=mail_type,
        summary=summary_excerpt,
        message_facts=message_facts,
        relationship_profile=relationship_profile,
    )
    if summary_excerpt:
        safe_summary = _escape_dynamic(summary_excerpt)
        return dedup_rendered_text(f"{base_text}\n\n{safe_summary}\n{_WATERMARK_LINE}")
    return dedup_rendered_text(f"{base_text}\n{_WATERMARK_LINE}")


def _resolve_action_line(action_line: str | None) -> str:
    cleaned = (action_line or "").strip()
    if cleaned:
        return cleaned
    return "Действий не требуется"


def build_tg_fallback(
    *,
    priority: str,
    subject: str,
    from_email: str,
    attachments: list[dict[str, Any]],
) -> str:
    lines = [
        format_priority_line(priority, from_email),
        format_subject(subject),
        format_main_action(None),
    ]
    attachments_block = format_attachments_block(attachments)
    if attachments_block:
        lines.append("")
        lines.append(attachments_block)
    return dedup_rendered_text("\n".join(lines))


def build_tg_short_template(*, priority: str, subject: str, from_email: str) -> str:
    return "\n".join(
        [
            format_priority_line(priority, from_email),
            format_subject(subject),
            format_main_action(None),
        ]
    )


def build_minimal_telegram_text(
    *,
    priority: str,
    from_email: str,
    subject: str,
    attachments: list[dict[str, Any]],
    max_attachment_names: int = 3,
) -> str:
    base_text = build_tg_short_template(
        priority=priority, subject=subject, from_email=from_email
    )
    if not attachments:
        return base_text
    minimal_attachments: list[dict[str, Any]] = []
    for attachment in attachments[:max_attachment_names]:
        minimal_attachments.append(
            {
                "filename": attachment.get("filename") or "вложение",
                "content_type": attachment.get("content_type")
                or attachment.get("type")
                or "",
                "text": "",
            }
        )
    attachments_block = format_attachments_block(minimal_attachments)
    if len(attachments) > max_attachment_names:
        attachments_block = f"{attachments_block}\n… и ещё {len(attachments) - len(minimal_attachments)}"
    return dedup_rendered_text("\n\n".join([base_text, attachments_block]))


__all__ = [
    "TelegramRenderFields",
    "apply_semantic_gates",
    "build_telegram_text",
    "build_tg_fallback",
    "build_tg_short_template",
    "build_minimal_telegram_text",
    "dedup_sentences",
    "dedup_rendered_lines",
    "dedup_rendered_text",
    "dedup_text_by_sentence",
    "format_priority_line",
    "format_subject",
    "format_main_action",
    "format_narrative_block",
    "format_attachments_block",
    "is_semantic_duplicate",
    "normalize_sentence",
    "render_telegram_message",
    "split_text_into_sentences",
]
