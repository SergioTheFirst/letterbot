$path = 'mailbot_v26/pipeline/processor.py'
$enc = [System.Text.UTF8Encoding]::new($false)
$text = [System.IO.File]::ReadAllText($path, $enc)
$pattern = 'def _fact_type_label\(label: str\) -> str:[\s\S]*?def _enforce_premium_clarity_line_budget\('
$replacement = @"
def _fact_type_label(label: str) -> str:
    return {
        "Сумма": "amount",
        "Дата": "date",
        "Номер": "doc_number",
    }.get(label, "other")


def _fact_source_tag(tag: str) -> str:
    if not tag:
        return ""
    if tag == "тема":
        return "subject"
    if tag == "письмо":
        return "body"
    return "attachment"


def _confidence_bucket(
    *,
    confidence_available: bool,
    confidence_percent: int,
) -> str:
    if not confidence_available:
        return "na"
    if confidence_percent >= 75:
        return "hi"
    if confidence_percent >= 50:
        return "med"
    return "low"


def _build_premium_clarity_attachments(
    attachments: list[dict[str, Any]],
    attachment_summaries: list[dict[str, Any]],
    *,
    suppress_numeric_facts: bool,
) -> list[str]:
    summary_by_name: dict[str, str] = {}
    for summary in attachment_summaries:
        filename = str(summary.get("filename") or "").strip()
        if not filename:
            continue
        summary_text = str(summary.get("summary") or "").strip()
        if not summary_text:
            continue
        summary_by_name[filename.lower()] = summary_text
    lines = [f"📎 Вложения ({len(attachments)}):"]
    for attachment in attachments[:3]:
        raw_filename = attachment.get("filename") or "вложение"
        filename = _escape_dynamic(strip_disallowed_emojis(raw_filename))
        summary_text = summary_by_name.get(str(raw_filename).strip().lower(), "")
        summary_text = _normalize_attachment_text(summary_text)
        if summary_text:
            summary_text = _truncate_attachment_text(summary_text)
        if summary_text and suppress_numeric_facts:
            summary_text = _strip_attachment_numeric_summary(summary_text)
        if summary_text:
            safe_text = _escape_dynamic(strip_disallowed_emojis(summary_text))
            lines.append(f"• {filename} — {safe_text}")
        else:
            lines.append(f"• {filename}")
    remaining = len(attachments) - 3
    if remaining > 0:
        lines.append(f"... и ещё {remaining}")
    return lines


def _format_confidence_dots(confidence_percent: int, scale: int) -> str:
    dots_scale = scale if scale in {5, 10} else 10
    filled = min(dots_scale, (confidence_percent * dots_scale) // 100)
    empty = dots_scale - filled
    return f"{'●' * filled}{'○' * empty}"


def _should_show_confidence_dots(
    *,
    mode: str,
    threshold: int,
    confidence_available: bool,
    confidence_percent: int,
) -> bool:
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"auto", "always", "never"}:
        normalized_mode = "auto"
    if normalized_mode == "never":
        return False
    if not confidence_available:
        return False
    if normalized_mode == "always":
        return True
    return confidence_percent < threshold


def _build_premium_clarity_spoiler_lines(
    lines: list[str],
    *,
    dots: str = "",
) -> list[str]:
    if not lines:
        return []
    sanitized = []
    for line in lines:
        cleaned = (line or "").strip()
        if cleaned:
            sanitized.append(_escape_dynamic(strip_disallowed_emojis(cleaned)))
    if not sanitized:
        return []
    limited = sanitized[:6]
    closing = "</tg-spoiler>"
    if dots:
        closing = f"{closing} {dots}"
    return ["<tg-spoiler>", "Подробнее:", *limited, closing]


def _enforce_premium_clarity_line_budget(
"@
$new = [System.Text.RegularExpressions.Regex]::Replace($text, $pattern, $replacement, [System.Text.RegularExpressions.RegexOptions]::Singleline)
if ($new -eq $text) { throw 'replace_fact_attachment_block_failed' }
[System.IO.File]::WriteAllText($path, $new, $enc)
