from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

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


@dataclass(frozen=True)
class TelegramRenderFields:
    action_line: str
    summary: str
    insights: tuple[str, ...]
    commitments: tuple[str, ...]


def _escape_dynamic(text: str | None) -> str:
    cleaned = strip_disallowed_emojis(str(text or ""))
    return escape_tg_html(cleaned)


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


def apply_semantic_gates(
    *,
    action_line: str | None,
    summary: str | None,
    insights: Iterable[str] | None = None,
    commitments: Iterable[str] | None = None,
) -> TelegramRenderFields:
    resolved_action = dedup_text_by_sentence(action_line)
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
    cleaned = (action_line or "").strip()
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
) -> str:
    lines = [
        format_priority_line(priority, from_email),
        format_subject(subject),
        format_main_action(action_line),
    ]
    attachments_block = format_attachments_block(attachments)
    if attachments_block:
        lines.append("")
        lines.append(attachments_block)
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
) -> str:
    fields = apply_semantic_gates(
        action_line=action_line,
        summary=summary,
        insights=insights,
        commitments=commitments,
    )
    base_text = build_telegram_text(
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=_resolve_action_line(fields.action_line),
        attachments=attachments or [],
    )
    if fields.summary:
        safe_summary = _escape_dynamic(fields.summary)
        return dedup_rendered_text(f"{base_text}\n<b><i>{safe_summary}</i></b>")
    return dedup_rendered_text(base_text)


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
