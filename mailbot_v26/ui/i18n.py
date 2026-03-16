"""Deterministic RU-first localization helpers for user-facing text."""

from __future__ import annotations

import configparser
import logging
from typing import Iterable

from mailbot_v26.text.mojibake import normalize_mojibake_text

DEFAULT_LOCALE = "en"
_LOGGER = logging.getLogger(__name__)


def _clean_i18n_text(text: str) -> str:
    return normalize_mojibake_text(str(text or ""))


_MAIL_TYPE_LABELS_RU = {
    "invoice": "Счёт",
    "invoice.final": "Счёт — финальный",
    "invoice.overdue": "Счёт — просрочен",
    "payment_reminder": "Напоминание об оплате",
    "reminder": "Напоминание",
    "reminder.first": "Напоминание — первое",
    "reminder.escalation": "Напоминание — эскалация",
    "deadline_reminder": "Напоминание о дедлайне",
    "contract": "Договор",
    "contract.approval": "Договор — на согласовании",
    "contract.update": "Договор — обновление",
    "contract.termination": "Договор — расторжение",
    "contract.amendment": "Договор — изменение",
    "contract.new": "Договор — новый",
    "claim": "Претензия",
    "claim.dispute": "Претензия/спор",
    "claim.complaint": "Жалоба",
    "price_list": "Прайс-лист",
    "delivery_notice": "Уведомление о доставке",
    "security_alert": "Предупреждение безопасности",
    "policy_update": "Обновление политики",
    "meeting_change": "Изменение встречи",
    "account_change": "Изменение аккаунта",
    "information_only": "Информационное письмо",
    "unknown": "Без категории",
    "generic": "Без категории",
}


_MAIL_TYPE_LABELS_EN = {
    "invoice": "Invoice",
    "invoice.final": "Invoice — final",
    "invoice.overdue": "Invoice — overdue",
    "payment_reminder": "Payment reminder",
    "reminder": "Reminder",
    "reminder.first": "Reminder — first",
    "reminder.escalation": "Reminder — escalation",
    "deadline_reminder": "Deadline reminder",
    "contract": "Contract",
    "contract.approval": "Contract — pending approval",
    "contract.update": "Contract — update",
    "contract.termination": "Contract — termination",
    "contract.amendment": "Contract — amendment",
    "contract.new": "Contract — new",
    "claim": "Claim",
    "claim.dispute": "Claim / dispute",
    "claim.complaint": "Complaint",
    "price_list": "Price list",
    "delivery_notice": "Delivery notice",
    "security_alert": "Security alert",
    "policy_update": "Policy update",
    "meeting_change": "Meeting change",
    "account_change": "Account update",
    "information_only": "Informational",
    "unknown": "Uncategorised",
    "generic": "Uncategorised",
}


_DOMAIN_LABELS_RU = {
    "billing": "Биллинг",
    "bank": "Банк/платежи",
    "finance": "Финансы",
    "hr": "HR",
    "legal": "Юр вопросы",
    "it": "ИТ",
    "sales": "Продажи",
    "ops": "Операции",
    "marketing": "Маркетинг",
    "invoice": "Счета",
    "contract": "Договоры",
    "unknown": "Без категории",
}


_DOMAIN_LABELS_EN = {
    "billing": "Billing",
    "bank": "Bank / payments",
    "finance": "Finance",
    "hr": "HR",
    "legal": "Legal",
    "it": "IT",
    "sales": "Sales",
    "ops": "Operations",
    "marketing": "Marketing",
    "invoice": "Invoices",
    "contract": "Contracts",
    "unknown": "Uncategorised",
}


_MODE_LABELS_RU = {
    "full": "Полный режим",
    "degraded_no_llm": "Деградация: без AI",
    "degraded_no_telegram": "Деградация: Telegram недоступен",
    "emergency_read_only": "Авария: только чтение",
}


_MODE_LABELS_EN = {
    "full": "Full mode",
    "degraded_no_llm": "Degraded: no AI",
    "degraded_no_telegram": "Degraded: Telegram unavailable",
    "emergency_read_only": "Emergency: read-only",
}


_ANOMALY_SEVERITY_RU = {
    "info": "инфо",
    "warn": "предупреждение",
    "alert": "тревога",
}


_ANOMALY_SEVERITY_EN = {
    "info": "info",
    "warn": "warning",
    "alert": "alert",
}


_REASON_LABELS_RU = {
    "mt.invoice.final.keyword": "финальный счёт",
    "mt.invoice.overdue.keyword": "просроченный счёт",
    "mt.reminder.escalation.keyword": "эскалация напоминания",
    "mt.reminder.first.keyword": "первое напоминание",
    "mt.reminder.escalation.urgency": "обнаружена срочность",
    "mt.contract.termination.keyword": "расторжение договора",
    "mt.contract.amendment.keyword": "изменение договора",
    "mt.contract.new.keyword": "новый договор",
    "mt.contract.keyword": "договор",
    "mt.contract.approval.keyword": "договор на согласовании",
    "mt.attachment_hint": "подсказка по вложению",
    "mt.reminder.keyword": "маркеры напоминания",
    "mt.reminder.amount": "указана сумма",
    "mt.reminder.date": "указан срок",
    "mt.price.keyword": "прайс/каталог",
    "mt.delivery.keyword": "уведомление о доставке",
    "mt.security.keyword": "предупреждение безопасности",
    "mt.policy.keyword": "обновление политики",
    "mt.meeting.keyword": "изменения встречи",
    "mt.deadline.keyword": "упоминание дедлайна",
    "mt.account.keyword": "обновление аккаунта",
    "mt.info.keyword": "информационное",
    "mt.claim.dispute.keyword": "претензия/спор",
    "mt.claim.complaint.keyword": "жалоба",
    "prio_urgent_keyword": "ключевые слова срочности",
    "prio_urgent_weighted_by_type": "срочность усилена типом",
    "prio_amount_100k": "сумма >100k",
    "prio_amount_50k": "сумма >50k",
    "prio_amount_10k": "сумма >10k",
    "prio_amount_base": "обнаружена сумма",
    "prio_deadline_1d": "дедлайн ≤1д",
    "prio_deadline_3d": "дедлайн ≤3д",
    "prio_deadline_7d": "дедлайн ≤7д",
    "prio_type_invoice_final": "финальный счёт",
    "prio_type_reminder_escalation": "эскалация напоминания",
    "prio_type_contract_termination": "расторжение договора",
    "prio_type_claim": "претензия",
    "prio_freq_spike_3x": "скачок частоты",
    "prio_chain_3plus": "3+ напоминаний подряд",
    "prio_chain_2plus": "2+ напоминаний подряд",
    "prio_vip_base": "VIP отправитель",
    "prio_vip_fyi_dampen": "VIP: FYI",
    "prio_vip_freq_dampen": "VIP: частота",
    "prio_vip_commitment_boost": "VIP: обязательства",
}


_REASON_LABELS_EN = {
    "mt.invoice.final.keyword": "final invoice",
    "mt.invoice.overdue.keyword": "overdue invoice",
    "mt.reminder.escalation.keyword": "escalated reminder",
    "mt.reminder.first.keyword": "first reminder",
    "mt.reminder.escalation.urgency": "urgency detected",
    "mt.contract.termination.keyword": "contract termination",
    "mt.contract.amendment.keyword": "contract amendment",
    "mt.contract.new.keyword": "new contract",
    "mt.contract.keyword": "contract",
    "mt.contract.approval.keyword": "contract pending approval",
    "mt.attachment_hint": "attachment hint",
    "mt.reminder.keyword": "reminder markers",
    "mt.reminder.amount": "amount stated",
    "mt.reminder.date": "due date stated",
    "mt.price.keyword": "price list / catalogue",
    "mt.delivery.keyword": "delivery notice",
    "mt.security.keyword": "security alert",
    "mt.policy.keyword": "policy update",
    "mt.meeting.keyword": "meeting change",
    "mt.deadline.keyword": "deadline mentioned",
    "mt.account.keyword": "account update",
    "mt.info.keyword": "informational",
    "mt.claim.dispute.keyword": "claim / dispute",
    "mt.claim.complaint.keyword": "complaint",
    "prio_urgent_keyword": "urgency keywords",
    "prio_urgent_weighted_by_type": "urgency boosted by type",
    "prio_amount_100k": "amount >100k",
    "prio_amount_50k": "amount >50k",
    "prio_amount_10k": "amount >10k",
    "prio_amount_base": "amount detected",
    "prio_deadline_1d": "deadline ≤1d",
    "prio_deadline_3d": "deadline ≤3d",
    "prio_deadline_7d": "deadline ≤7d",
    "prio_type_invoice_final": "final invoice",
    "prio_type_reminder_escalation": "reminder escalation",
    "prio_type_contract_termination": "contract termination",
    "prio_type_claim": "claim",
    "prio_freq_spike_3x": "frequency spike",
    "prio_chain_3plus": "3+ reminders in chain",
    "prio_chain_2plus": "2+ reminders in chain",
    "prio_vip_base": "VIP sender",
    "prio_vip_fyi_dampen": "VIP: FYI",
    "prio_vip_freq_dampen": "VIP: frequency",
    "prio_vip_commitment_boost": "VIP: commitment",
}


_STRINGS_RU = {
    "preview.title": "AI-превью",
    "preview.action": "Предлагаемое действие:",
    "preview.reason": "Причина:",
    "preview.why": "ПОЧЕМУ ТАК:",
    "preview.confidence": "Уверенность",
    "preview.insights": "Инсайты",
    "preview.narrative": "Нарратив",
    "preview.signals": "Сигналы",
    "preview.digest": "Дайджест инсайтов",
    "preview.commitments": "Обязательства",
    "preview.relationship_context": "Контекст отношений:",
    "preview.counterparty": "Контрагент",
    "preview.commitment_reliability": "Надёжность обязательств",
    "preview.commitment_stats": "(выполнено: {fulfilled_count}, просрочено: {expired_count} за 30 дней)",
    "preview.commitment.pending": "ожидается",
    "preview.commitment.fulfilled": "выполнено",
    "preview.commitment.expired": "просрочено",
    "preview.commitment.unknown": "неизвестно",
    "preview.fact": "Факт:",
    "preview.context": "Контекст:",
    "preview.action_detail": "Действие:",
    "preview.decision_buttons": "[Принять] [Отклонить]",
    "preview.priority_buttons": "[Сделать высокий] [Сделать средний] [Сделать низкий]",
    "preview.signal_unavailable": "Тело письма недоступно (низкое качество извлечения).",
    "preview.subject": "Тема:",
    "preview.sender": "От:",
    "preview.no_subject": "(без темы)",
    "preview.unknown_sender": "неизвестно",
    "digest.daily": "<b>Дайджест дня</b>",
    "digest.weekly": "<b>Дайджест недели (7 дней)</b>",
    "digest.anomalies": "• Аномалии:",
    "digest.attention": "• Внимание:",
    "sla.alert.title": "Внимание: доставка в Telegram деградировала",
    "sla.alert.delivery": "Доставка за 24ч",
    "sla.alert.latency": "p90 задержка",
    "sla.alert.top_error": "Главная ошибка",
    "sla.alert.action": "Действие",
    "sla.alert.no_data": "н/д",
    "sla.alert.action.plain_text": "текст без форматирования",
    "sla.alert.action.retrying": "повторяем",
    "inbound.bad_button": "Некорректная кнопка. Напишите /help.",
    "inbound.ok": "Готово.",
    "inbound.bad_email_id": "Некорректный идентификатор письма.",
    "inbound.priority_ack": "Принято: приоритет исправлен на {priority}.",
    "inbound.priority_help": (
        "Исправления приоритета сохраняются и учитываются в проверке качества автоприоритета.\n"
        "Если исправлений слишком много, автоприоритет остаётся в теневом режиме."
    ),
    "inbound.toggle_unknown": "Неизвестная настройка.",
    "inbound.digest_enabled": "Дайджесты включены.",
    "inbound.digest_disabled": "Дайджесты выключены.",
    "inbound.digest_usage": "Использование: /digest on|off",
    "inbound.autopriority_usage": "Использование: /autopriority on|off",
    "inbound.autopriority_off": "Автоприоритет выключен. Режим: теневой.",
    "inbound.autopriority_on": "Автоприоритет включён.",
    "inbound.autopriority_gate_blocked": "Пока нельзя: качество недостаточно ({reason}).",
    "inbound.autopriority_reason.cooldown": "пауза после отключения",
    "inbound.autopriority_reason.samples": "недостаточно данных",
    "inbound.autopriority_reason.corrections": "слишком много исправлений",
    "inbound.autopriority_reason.analytics": "ошибка аналитики",
    "inbound.command_unknown": "Не понял команду. Напишите /help.",
    "inbound.help.title": "Команды:",
    "inbound.help.status": "/status — краткий статус системы",
    "inbound.help.doctor": "/doctor — диагностика (кратко)",
    "inbound.help.digest": "/digest on|off — включить или выключить дайджесты",
    "inbound.help.autopriority": "/autopriority on|off — включить или выключить автоприоритет",
    "inbound.help.commitments": "/commitments (/tasks) — открытые обязательства",
    "inbound.help.week": "/week — краткая статистика за 7 дней",
    "inbound.help.stats": "/stats — качество автоприоритизации",
    "inbound.help.support": "/support — поддержать проект",
    "inbound.help.lang": "/lang en|ru — переключить язык",
    "inbound.help.help": "/help — эта справка",
    "inbound.status.title": "Статус системы",
    "inbound.status.mode": "Режим: {mode}",
    "inbound.status.sla": (
        "Оперативность уведомлений (24ч): доставка {delivery}, ошибки {errors}"
    ),
    "inbound.status.digest": "Дайджесты: {digest}",
    "inbound.status.autopriority": "Автоприоритет: {mode}",
    "inbound.status.flags": (
        "Флаги: превью={preview}, аномалии={anomalies}, качество={quality}"
    ),
    "inbound.status.last_digests": "Последние отправки дайджестов:",
    "inbound.status.no_data": "нет данных",
    "inbound.status.never_sent": "не отправлялся",
    "inbound.status.digest_line": "{account_email}: день {daily}, неделя {weekly}",
    "inbound.status.enabled": "включены",
    "inbound.status.disabled": "выключены",
    "inbound.status.short_on": "вкл",
    "inbound.status.short_off": "выкл",
    "inbound.status.auto_mode": "авто",
    "inbound.status.shadow_mode": "теневой",
    "inbound.doctor.unavailable": "Доктор недоступен.",
    "inbound.doctor.failed": "Доктор завершился с ошибкой.",
    "inbound.doctor.ok": "Доктор: все проверки ОК.",
    "inbound.doctor.warn_title": "Доктор: есть предупреждения.",
    "inbound.doctor.status_ok": "ОК",
    "inbound.doctor.status_warn": "ПРЕДУПРЕЖДЕНИЕ",
    "inbound.doctor.status_fail": "ОШИБКА",
}


_STRINGS_EN = {
    "preview.title": "AI Preview",
    "preview.action": "Suggested action:",
    "preview.reason": "Reason:",
    "preview.why": "WHY THIS PRIORITY:",
    "preview.confidence": "Confidence",
    "preview.insights": "Insights",
    "preview.narrative": "Narrative",
    "preview.signals": "Signals",
    "preview.digest": "Insights digest",
    "preview.commitments": "Commitments",
    "preview.relationship_context": "Relationship context:",
    "preview.counterparty": "Counterparty",
    "preview.commitment_reliability": "Commitment reliability",
    "preview.commitment_stats": "(fulfilled: {fulfilled_count}, expired: {expired_count} over 30 days)",
    "preview.commitment.pending": "pending",
    "preview.commitment.fulfilled": "fulfilled",
    "preview.commitment.expired": "expired",
    "preview.commitment.unknown": "unknown",
    "preview.fact": "Fact:",
    "preview.context": "Context:",
    "preview.action_detail": "Action:",
    "preview.decision_buttons": "[Accept] [Dismiss]",
    "preview.priority_buttons": "[Set High] [Set Medium] [Set Low]",
    "preview.signal_unavailable": "Email body unavailable (low extraction quality).",
    "preview.subject": "Subject:",
    "preview.sender": "From:",
    "preview.no_subject": "(no subject)",
    "preview.unknown_sender": "unknown",
    "digest.daily": "<b>Daily Digest</b>",
    "digest.weekly": "<b>Weekly Digest (7 days)</b>",
    "digest.anomalies": "• Anomalies:",
    "digest.attention": "• Attention:",
    "sla.alert.title": "Warning: Telegram delivery degraded",
    "sla.alert.delivery": "Delivery (24h)",
    "sla.alert.latency": "p90 latency",
    "sla.alert.top_error": "Top error",
    "sla.alert.action": "Action",
    "sla.alert.no_data": "n/a",
    "sla.alert.action.plain_text": "plain text fallback",
    "sla.alert.action.retrying": "retrying",
    "inbound.bad_button": "Unknown button. Try /help.",
    "inbound.ok": "Done.",
    "inbound.bad_email_id": "Invalid email identifier.",
    "inbound.priority_ack": "Saved: priority updated to {priority}.",
    "inbound.priority_help": (
        "Priority corrections are stored and used in auto-priority "
        "quality checks.\nIf corrections are too frequent, auto-priority "
        "stays in shadow mode."
    ),
    "inbound.toggle_unknown": "Unknown setting.",
    "inbound.digest_enabled": "Digests enabled.",
    "inbound.digest_disabled": "Digests disabled.",
    "inbound.digest_usage": "Usage: /digest on|off",
    "inbound.autopriority_usage": "Usage: /autopriority on|off",
    "inbound.autopriority_off": "Auto-priority disabled. Mode: shadow.",
    "inbound.autopriority_on": "Auto-priority enabled.",
    "inbound.autopriority_gate_blocked": "Not yet: quality insufficient ({reason}).",
    "inbound.autopriority_reason.cooldown": "cooldown after disable",
    "inbound.autopriority_reason.samples": "not enough data",
    "inbound.autopriority_reason.corrections": "too many corrections",
    "inbound.autopriority_reason.analytics": "analytics error",
    "inbound.command_unknown": "Unknown command. Try /help.",
    "inbound.help.title": "Commands:",
    "inbound.help.status": "/status — brief system status",
    "inbound.help.doctor": "/doctor — diagnostics (brief)",
    "inbound.help.digest": "/digest on|off — enable or disable digests",
    "inbound.help.autopriority": "/autopriority on|off — enable or disable auto-priority",
    "inbound.help.commitments": "/commitments (/tasks) — open commitments",
    "inbound.help.week": "/week — 7-day stats summary",
    "inbound.help.stats": "/stats — auto-priority quality",
    "inbound.help.support": "/support — support the project",
    "inbound.help.lang": "/lang en|ru — switch language",
    "inbound.help.help": "/help — this help",
    "inbound.status.title": "System status",
    "inbound.status.mode": "Mode: {mode}",
    "inbound.status.sla": "Notification SLA (24h): delivery {delivery}, errors {errors}",
    "inbound.status.digest": "Digests: {digest}",
    "inbound.status.autopriority": "Auto-priority: {mode}",
    "inbound.status.flags": "Flags: preview={preview}, anomalies={anomalies}, quality={quality}",
    "inbound.status.last_digests": "Last digest sends:",
    "inbound.status.no_data": "no data",
    "inbound.status.never_sent": "never sent",
    "inbound.status.digest_line": "{account_email}: daily {daily}, weekly {weekly}",
    "inbound.status.enabled": "enabled",
    "inbound.status.disabled": "disabled",
    "inbound.status.short_on": "on",
    "inbound.status.short_off": "off",
    "inbound.status.auto_mode": "auto",
    "inbound.status.shadow_mode": "shadow",
    "inbound.doctor.unavailable": "Doctor unavailable.",
    "inbound.doctor.failed": "Doctor finished with error.",
    "inbound.doctor.ok": "Doctor: all checks OK.",
    "inbound.doctor.warn_title": "Doctor: warnings found.",
    "inbound.doctor.status_ok": "OK",
    "inbound.doctor.status_warn": "WARNING",
    "inbound.doctor.status_fail": "ERROR",
}


def get_locale(config: configparser.ConfigParser | dict | None) -> str:
    if config is None:
        return DEFAULT_LOCALE
    try:
        if isinstance(config, configparser.ConfigParser):
            return (
                config.get("ui", "locale", fallback=DEFAULT_LOCALE).strip()
                or DEFAULT_LOCALE
            )
        if isinstance(config, dict):
            ui_section = config.get("ui") or config.get("UI")
            if isinstance(ui_section, dict):
                value = str(ui_section.get("locale") or "").strip()
                return value or DEFAULT_LOCALE
        if hasattr(config, "get"):
            value = config.get("ui", {}).get("locale")  # type: ignore[arg-type]
            if value:
                return str(value).strip() or DEFAULT_LOCALE
    except Exception:
        return DEFAULT_LOCALE
    return DEFAULT_LOCALE


def t(key: str, *, locale: str = DEFAULT_LOCALE, **kwargs) -> str:
    if locale.startswith("ru"):
        catalog = _STRINGS_RU
    elif locale.startswith("en"):
        catalog = _STRINGS_EN
    else:
        catalog = _STRINGS_EN
    template = catalog.get(key)
    if template is None:
        _LOGGER.warning("Missing i18n key: %s (locale=%s)", key, locale)
        return key
    try:
        return _clean_i18n_text(template.format(**kwargs))
    except Exception:
        _LOGGER.warning("Failed to format i18n key: %s (locale=%s)", key, locale)
        return _clean_i18n_text(template)


def _normalize_code(code: str) -> str:
    return code.strip().lower().replace("__", "_")


def humanize_mail_type(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    labels = _MAIL_TYPE_LABELS_EN if locale.startswith("en") else _MAIL_TYPE_LABELS_RU
    normalized = _normalize_code(code).replace("_", ".")
    parts = normalized.split(".")
    while parts:
        candidate = ".".join(parts)
        label = labels.get(candidate)
        if label:
            return label
        parts = parts[:-1]
    fallback = "Type" if locale.startswith("en") else "Тип"
    return f"{fallback}: {code}"


def humanize_domain(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    labels = _DOMAIN_LABELS_EN if locale.startswith("en") else _DOMAIN_LABELS_RU
    normalized = _normalize_code(code).replace("_", "")
    fallback = "Domain" if locale.startswith("en") else "Домен"
    return labels.get(normalized, f"{fallback}: {code}")


def humanize_mode(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    labels = _MODE_LABELS_EN if locale.startswith("en") else _MODE_LABELS_RU
    normalized = _normalize_code(code)
    fallback = "Mode" if locale.startswith("en") else "Режим"
    return labels.get(normalized, f"{fallback}: {code}")


def humanize_severity(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    labels = (
        _ANOMALY_SEVERITY_EN if locale.startswith("en") else _ANOMALY_SEVERITY_RU
    )
    normalized = _normalize_code(code)
    fallback = "Severity" if locale.startswith("en") else "Уровень"
    return labels.get(normalized, f"{fallback}: {code}")


def _humanize_attachment_hint(
    detail: str | None, locale: str = DEFAULT_LOCALE
) -> str:
    fallback = "attachment hint" if locale.startswith("en") else "подсказка по вложению"
    if not detail:
        return fallback
    if "contract" in detail:
        return (
            "attachment looks like contract"
            if locale.startswith("en")
            else "вложение похоже на договор"
        )
    if "invoice" in detail:
        return (
            "attachment looks like invoice"
            if locale.startswith("en")
            else "вложение похоже на счёт"
        )
    return fallback


def humanize_reason_codes(
    reasons: Iterable[str], locale: str = DEFAULT_LOCALE
) -> list[str]:
    labels_map = _REASON_LABELS_EN if locale.startswith("en") else _REASON_LABELS_RU
    mail_type_labels = (
        _MAIL_TYPE_LABELS_EN if locale.startswith("en") else _MAIL_TYPE_LABELS_RU
    )
    labels: list[str] = []
    for reason in reasons:
        if not reason:
            continue
        raw = str(reason)
        key, detail = (raw.split("=", 1) + [None])[:2]
        normalized = _normalize_code(key).replace("_", ".")
        label = labels_map.get(normalized)
        if normalized in {"mt.base", "mt.mail_type"} and detail:
            labels.append(humanize_mail_type(detail, locale))
            continue
        if normalized == "mt.attachment_hint":
            labels.append(_humanize_attachment_hint(detail, locale))
            continue
        if normalized.startswith("mt.") and detail:
            base_label = mail_type_labels.get(normalized.replace("mt.", ""))
            if base_label:
                labels.append(base_label)
                continue
        if label:
            labels.append(label)
            continue
        fallback = (
            "unknown marker" if locale.startswith("en") else "неизвестный маркер"
        )
        labels.append(f"{fallback} ({normalized})")
    return labels


__all__ = [
    "DEFAULT_LOCALE",
    "get_locale",
    "t",
    "humanize_domain",
    "humanize_mail_type",
    "humanize_mode",
    "humanize_reason_codes",
    "humanize_severity",
]
