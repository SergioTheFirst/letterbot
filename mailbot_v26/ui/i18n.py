"""Deterministic RU-first localization helpers for user-facing text."""

from __future__ import annotations

import configparser
from typing import Iterable

DEFAULT_LOCALE = "ru"


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


_MODE_LABELS_RU = {
    "full": "Полный режим",
    "degraded_no_llm": "Деградация: без AI",
    "degraded_no_telegram": "Деградация: Telegram недоступен",
    "emergency_read_only": "Авария: только чтение",
}


_ANOMALY_SEVERITY_RU = {
    "info": "инфо",
    "warn": "предупреждение",
    "alert": "тревога",
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
    "digest.daily": "<b>Дайджест дня</b>",
    "digest.weekly": "<b>Дайджест недели (7 дней)</b>",
    "digest.anomalies": "• Аномалии:",
    "digest.attention": "• Внимание:",
    "sla.alert.title": "Внимание: доставка в Telegram деградировала",
    "sla.alert.delivery": "Доставка за 24ч",
    "sla.alert.latency": "p90 задержка",
    "sla.alert.top_error": "Главная ошибка",
    "sla.alert.action": "Действие",
}


def get_locale(config: configparser.ConfigParser | dict | None) -> str:
    if config is None:
        return DEFAULT_LOCALE
    try:
        if isinstance(config, configparser.ConfigParser):
            return (
                config.get("ui", "locale", fallback=DEFAULT_LOCALE).strip() or DEFAULT_LOCALE
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
    catalog = _STRINGS_RU if locale.startswith("ru") else {}
    template = catalog.get(key, f"(нет перевода: {key})")
    try:
        return template.format(**kwargs)
    except Exception:
        return template


def _normalize_code(code: str) -> str:
    return code.strip().lower().replace("__", "_")


def humanize_mail_type(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    normalized = _normalize_code(code).replace("_", ".")
    parts = normalized.split(".")
    while parts:
        candidate = ".".join(parts)
        label = _MAIL_TYPE_LABELS_RU.get(candidate)
        if label:
            return label
        parts = parts[:-1]
    return f"Тип: {code}"


def humanize_domain(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    normalized = _normalize_code(code).replace("_", "")
    return _DOMAIN_LABELS_RU.get(normalized, f"Домен: {code}")


def humanize_mode(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    normalized = _normalize_code(code)
    return _MODE_LABELS_RU.get(normalized, f"Режим: {code}")


def humanize_severity(code: str | None, locale: str = DEFAULT_LOCALE) -> str:
    if not code:
        return ""
    normalized = _normalize_code(code)
    return _ANOMALY_SEVERITY_RU.get(normalized, f"Уровень: {code}")


def _humanize_attachment_hint(detail: str | None) -> str:
    if not detail:
        return "подсказка по вложению"
    if "contract" in detail:
        return "вложение похоже на договор"
    if "invoice" in detail:
        return "вложение похоже на счёт"
    return "подсказка по вложению"


def humanize_reason_codes(
    reasons: Iterable[str], locale: str = DEFAULT_LOCALE
) -> list[str]:
    labels: list[str] = []
    for reason in reasons:
        if not reason:
            continue
        raw = str(reason)
        key, detail = (raw.split("=", 1) + [None])[:2]
        normalized = _normalize_code(key).replace("_", ".")
        label = _REASON_LABELS_RU.get(normalized)
        if normalized in {"mt.base", "mt.mail_type"} and detail:
            labels.append(humanize_mail_type(detail, locale))
            continue
        if normalized == "mt.attachment_hint":
            labels.append(_humanize_attachment_hint(detail))
            continue
        if normalized.startswith("mt.") and detail:
            base_label = _MAIL_TYPE_LABELS_RU.get(normalized.replace("mt.", ""))
            if base_label:
                labels.append(base_label)
                continue
        if label:
            labels.append(label)
            continue
        labels.append(f"неизвестный маркер ({normalized})")
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
