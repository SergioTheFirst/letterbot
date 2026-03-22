from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

from mailbot_v26.insights.anomaly_engine import compute_anomalies
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.insights.attention_economics import (
    AttentionEconomicsResult,
    compute_attention_economics,
    format_attention_block,
)
from mailbot_v26.insights.quality_metrics import (
    QualityMetricsSnapshot,
    compute_quality_metrics,
)
from mailbot_v26.observability.notification_sla import (
    NotificationSLAResult,
    compute_notification_sla,
)
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline.processor import _resolve_outbound_ui_locale
from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics, WeeklyAccuracyProgress
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe
from mailbot_v26.ui.i18n import DEFAULT_LOCALE, humanize_severity

logger = get_logger("mailbot")

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.ini"
_LOGGER = logging.getLogger(__name__)

_WEEKDAY_ALIASES = {
    "mon": 0,
    "monday": 0,
    "понедельник": 0,
    "tue": 1,
    "tuesday": 1,
    "вторник": 1,
    "wed": 2,
    "wednesday": 2,
    "среда": 2,
    "thu": 3,
    "thursday": 3,
    "четверг": 3,
    "fri": 4,
    "friday": 4,
    "пятница": 4,
    "sat": 5,
    "saturday": 5,
    "суббота": 5,
    "sun": 6,
    "sunday": 6,
    "воскресенье": 6,
}


@dataclass(frozen=True, slots=True)
class WeeklyDigestConfig:
    weekday: int
    hour: int
    minute: int


@dataclass(frozen=True, slots=True)
class WeeklyDigestData:
    week_key: str
    total_emails: int
    deferred_emails: int
    attention_entities: Sequence[dict[str, object]]
    commitment_counts: dict[str, int]
    overdue_commitments: Sequence[dict[str, object]]
    trust_deltas: dict[str, list[dict[str, object]]]
    anomaly_alerts: list[str]
    attention_economics: AttentionEconomicsResult | None = None
    quality_metrics: QualityMetricsSnapshot | None = None
    notification_sla: NotificationSLAResult | None = None
    previous_week_sla: NotificationSLAResult | None = None
    weekly_accuracy_report: dict[str, object] | None = None
    weekly_calibration_report: dict[str, object] | None = None
    weekly_accuracy_progress: WeeklyAccuracyProgress | None = None
    invoice_count: int = 0
    invoice_total_rub: int | None = None
    contract_count: int = 0
    silence_risk: dict[str, object] | None = None
    relationship_top_senders: Sequence[dict[str, object]] = ()
    relationship_trend: str | None = None
    business_summary: dict[str, object] | None = None


def _locale_text(locale: str, *, en: str, ru: str) -> str:
    return en if str(locale or "").strip().casefold().startswith("en") else ru


def _format_attention_top_sinks_localized(
    entities: Sequence[AttentionEntity], locale: str
) -> list[str]:
    if not entities:
        return [_locale_text(locale, en="- not enough data", ru="- недостаточно данных")]
    lines: list[str] = []
    for entity in entities:
        lines.append(
            _locale_text(
                locale,
                en=(
                    f"- {escape_tg_html(entity.label)}: "
                    f"{entity.estimated_read_minutes:.1f} min, emails {entity.message_count}"
                ),
                ru=(
                    f"- {escape_tg_html(entity.label)}: "
                    f"{entity.estimated_read_minutes:.1f} мин, писем {entity.message_count}"
                ),
            )
        )
    return lines


def _format_attention_risks_localized(
    entities: Sequence[AttentionEntity], locale: str
) -> list[str]:
    if not entities:
        return [
            _locale_text(locale, en="- no risks detected", ru="- рисков не зафиксировано")
        ]
    lines: list[str] = []
    for entity in entities:
        reasons: list[str] = []
        if entity.health_delta is not None and entity.health_delta < 0:
            reasons.append(
                _locale_text(
                    locale,
                    en=f"health {entity.health_delta:+.0f}",
                    ru=f"здоровье {entity.health_delta:+.0f}",
                )
            )
        if entity.trust_delta is not None and entity.trust_delta < 0:
            reasons.append(
                _locale_text(
                    locale,
                    en=f"trust {entity.trust_delta * 100:+.1f} pp",
                    ru=f"trust {entity.trust_delta * 100:+.1f} п.п.",
                )
            )
        if entity.anomalies:
            reasons.append(_locale_text(locale, en="anomalies", ru="аномалии"))
        reason_text = ", ".join(reasons) if reasons else _locale_text(
            locale, en="anomalies", ru="аномалии"
        )
        lines.append(f"- {escape_tg_html(entity.label)}: {reason_text}")
    return lines


def _format_attention_best_localized(
    entities: Sequence[AttentionEntity], locale: str
) -> list[str]:
    if not entities:
        return [_locale_text(locale, en="- no growth data", ru="- данных о росте нет")]
    lines: list[str] = []
    for entity in entities:
        details: list[str] = []
        if entity.trust_delta is not None and entity.trust_delta > 0:
            details.append(
                _locale_text(
                    locale,
                    en=f"trust {entity.trust_delta * 100:+.1f} pp",
                    ru=f"trust {entity.trust_delta * 100:+.1f} п.п.",
                )
            )
        if entity.health_delta is not None and entity.health_delta > 0:
            details.append(
                _locale_text(
                    locale,
                    en=f"health {entity.health_delta:+.0f}",
                    ru=f"здоровье {entity.health_delta:+.0f}",
                )
            )
        detail_text = ", ".join(details) if details else _locale_text(
            locale, en="stable", ru="стабильно"
        )
        lines.append(f"- {escape_tg_html(entity.label)}: {detail_text}")
    return lines


def _format_attention_block_localized(
    result: AttentionEconomicsResult, locale: str
) -> list[str]:
    lines = [
        f"<b>{_locale_text(locale, en='⏱ Where attention went', ru='⏱ Куда ушло внимание')}</b>"
    ]
    lines.extend(_format_attention_top_sinks_localized(result.top_sinks, locale))
    lines.append(f"<b>{_locale_text(locale, en='Risks', ru='Риски')}</b>")
    lines.extend(_format_attention_risks_localized(result.at_risk, locale))
    lines.append(
        f"<b>{_locale_text(locale, en='Best counterparties', ru='Лучшие контрагенты')}</b>"
    )
    lines.extend(
        _format_attention_best_localized(result.best_counterparties, locale)
    )
    return lines


def _humanize_relationship_trend(trend: str | None, locale: str) -> str:
    normalized = str(trend or "").strip().casefold()
    if normalized == "improving":
        return _locale_text(locale, en="improving", ru="улучшается")
    if normalized == "declining":
        return _locale_text(locale, en="declining", ru="ухудшается")
    if normalized == "stable":
        return _locale_text(locale, en="stable", ru="стабильный")
    return str(trend or "")


def _parse_weekday(value: str | None) -> int:
    if not value:
        return 0
    cleaned = value.strip().lower()
    if cleaned.isdigit():
        numeric = int(cleaned)
        return max(0, min(6, numeric))
    return _WEEKDAY_ALIASES.get(cleaned, 0)


def _load_ini_parser() -> configparser.ConfigParser:
    return read_user_ini_with_defaults(
        _CONFIG_PATH,
        logger=_LOGGER,
        scope_label="weekly digest settings",
    )


def _load_weekly_digest_config() -> WeeklyDigestConfig:
    weekday = 0
    hour = 9
    minute = 0
    parser = _load_ini_parser()
    section = parser["weekly_digest"] if "weekly_digest" in parser else None
    if section is not None:
        weekday = _parse_weekday(section.get("weekday", fallback="mon"))
        try:
            hour = max(0, min(23, section.getint("hour", fallback=9)))
        except ValueError:
            hour = 9
        try:
            minute = max(0, min(59, section.getint("minute", fallback=0)))
        except ValueError:
            minute = 0
    return WeeklyDigestConfig(weekday=weekday, hour=hour, minute=minute)


def _iso_week_key(now: datetime) -> str:
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _is_due(now: datetime, config: WeeklyDigestConfig) -> bool:
    if now.weekday() != config.weekday:
        return False
    if now.hour < config.hour:
        return False
    if now.hour == config.hour and now.minute < config.minute:
        return False
    return True


def _safe_text(value: str, *, max_len: int = 80) -> str:
    cleaned = value.strip()
    if len(cleaned) > max_len:
        cleaned = f"{cleaned[: max_len - 1]}…"
    return escape_tg_html(cleaned)


def _attention_summary(
    attention: Sequence[dict[str, object]], locale: str = DEFAULT_LOCALE
) -> str:
    if not attention:
        return _locale_text(locale, en="no data", ru="нет данных")
    top = attention[:3]
    parts: list[str] = []
    for item in top:
        name = _safe_text(
            str(
                item.get("entity")
                or _locale_text(locale, en="unknown", ru="неизвестно")
            ),
            max_len=40,
        )
        words = int(item.get("words") or 0)
        minutes = words / 200.0
        minutes_display = 0.1 if minutes > 0 and minutes < 0.1 else minutes
        parts.append(
            _locale_text(
                locale,
                en=f"{name} — {minutes_display:.1f} min",
                ru=f"{name} — {minutes_display:.1f} мин",
            )
        )
    return ", ".join(parts)


def _overdue_summary(
    items: Sequence[dict[str, object]], locale: str = DEFAULT_LOCALE
) -> str:
    if not items:
        return _locale_text(locale, en="none", ru="нет")
    parts: list[str] = []
    for item in items[:5]:
        name = _safe_text(
            str(
                item.get("from_email")
                or _locale_text(locale, en="unknown", ru="неизвестно")
            ),
            max_len=24,
        )
        text = _safe_text(str(item.get("commitment_text") or ""), max_len=48)
        deadline = _safe_text(str(item.get("deadline_iso") or ""), max_len=16)
        parts.append(f"{name} → {text} → {deadline}")
    return "; ".join(parts)


def _trust_summary(
    trust_deltas: dict[str, list[dict[str, object]]], locale: str = DEFAULT_LOCALE
) -> str:
    up = trust_deltas.get("up", [])
    down = trust_deltas.get("down", [])
    if not up and not down:
        return _locale_text(locale, en="not enough history", ru="недостаточно истории")
    parts: list[str] = []
    if up:
        up_items = []
        for item in up:
            name = _safe_text(
                str(item.get("entity_name") or item.get("entity_id") or ""), max_len=24
            )
            delta_pp = float(item.get("delta") or 0.0) * 100.0
            up_items.append(
                _locale_text(
                    locale,
                    en=f"{name} +{delta_pp:.1f} pp",
                    ru=f"{name} +{delta_pp:.1f} п.п.",
                )
            )
        parts.append(
            _locale_text(locale, en=f"growth: {', '.join(up_items)}", ru=f"рост: {', '.join(up_items)}")
        )
    if down:
        down_items = []
        for item in down:
            name = _safe_text(
                str(item.get("entity_name") or item.get("entity_id") or ""), max_len=24
            )
            delta_pp = float(item.get("delta") or 0.0) * 100.0
            down_items.append(
                _locale_text(
                    locale,
                    en=f"{name} {delta_pp:.1f} pp",
                    ru=f"{name} {delta_pp:.1f} п.п.",
                )
            )
        parts.append(
            _locale_text(locale, en=f"decline: {', '.join(down_items)}", ru=f"падение: {', '.join(down_items)}")
        )
    return "; ".join(parts)


def _format_weekly_accuracy_progress(
    progress: WeeklyAccuracyProgress | None,
    locale: str = DEFAULT_LOCALE,
) -> str | None:
    if progress is None:
        return None
    corrections = int(progress.current_corrections)
    if corrections < 3:
        return None
    if float(progress.current_surprise_rate_pp) > 20:
        return None
    accuracy_pct = max(
        0, min(100, int(round(100 - float(progress.current_surprise_rate_pp))))
    )
    delta = int(progress.delta_pp)
    if abs(delta) < 2:
        return None
    if delta > 0:
        return _locale_text(
            locale,
            en=(
                "Your progress: "
                f"bot accuracy increased to {accuracy_pct}% "
                f"thanks to your {corrections} corrections."
            ),
            ru=(
                "Твой прогресс: "
                f"точность бота выросла до {accuracy_pct}% "
                f"благодаря твоим {corrections} коррекциям."
            ),
        )
    return _locale_text(
        locale,
        en=(
            "Your progress: "
            f"bot accuracy fell to {accuracy_pct}% "
            f"with {corrections} corrections over the week."
        ),
        ru=(
            "Твой прогресс: "
            f"точность бота снизилась до {accuracy_pct}% "
            f"при {corrections} коррекциях за неделю."
        ),
    )


def _collect_weekly_human_signals(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
    account_emails: Sequence[str] | None,
) -> tuple[int, int | None, int]:
    try:
        account_ids = analytics._normalize_account_scope(account_email, account_emails)
        if not account_ids:
            return 0, None, 0
        rows = analytics._event_rows_scoped(
            account_ids=account_ids,
            event_type="message_interpretation",
            since_ts=analytics._window_start_ts(7),
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("weekly_human_signals_failed", error=str(exc))
        return 0, None, 0

    invoice_count = 0
    contract_count = 0
    invoice_total_rub = 0
    has_invoice_amount = False
    for row in rows:
        payload = analytics._event_payload(row)
        doc_kind = str(payload.get("doc_kind") or "").strip().lower()
        if doc_kind == "invoice":
            invoice_count += 1
            amount_value = payload.get("amount")
            if amount_value is not None:
                try:
                    invoice_total_rub += int(round(float(amount_value)))
                    has_invoice_amount = True
                except (TypeError, ValueError):
                    logger.warning(
                        "weekly_interpretation_amount_invalid", amount=amount_value
                    )
        if doc_kind == "contract":
            contract_count += 1

    return (
        invoice_count,
        (invoice_total_rub if has_invoice_amount else None),
        contract_count,
    )


def _collect_anomaly_alerts(
    *,
    analytics: KnowledgeAnalytics,
    now: datetime,
    contract_event_emitter: ContractEventEmitter | None = None,
    account_email: str | None = None,
    locale: str = DEFAULT_LOCALE,
) -> list[str]:
    alerts: list[str] = []
    try:
        entities = analytics.recent_entity_activity(days=30, limit=8)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("anomaly_digest_activity_failed", error=str(exc))
        return alerts
    for row in entities:
        entity_id = str(row.get("entity_id") or "")
        if not entity_id:
            continue
        try:
            anomalies = compute_anomalies(
                entity_id=entity_id,
                analytics=analytics,
                now_dt=now,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "anomaly_digest_compute_failed",
                entity_id=entity_id,
                error=str(exc),
            )
            continue
        if not anomalies:
            continue
        label = analytics.entity_label(entity_id=entity_id) or entity_id
        safe_label = escape_tg_html(label)
        for anomaly in anomalies:
            title = escape_tg_html(anomaly.title)
            severity = escape_tg_html(humanize_severity(anomaly.severity, locale=locale))
            alerts.append(f"{safe_label}: {title} ({severity})")
            if contract_event_emitter is not None and account_email:
                try:
                    contract_event_emitter.emit(
                        EventV1(
                            event_type=EventType.ANOMALY_DETECTED,
                            ts_utc=now.timestamp(),
                            account_id=account_email,
                            entity_id=entity_id,
                            email_id=None,
                            payload={
                                "title": anomaly.title,
                                "severity": anomaly.severity,
                                "details": anomaly.details,
                                "type": anomaly.type,
                            },
                        )
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.error(
                        "contract_event_emit_failed",
                        event_type=EventType.ANOMALY_DETECTED.value,
                        error=str(exc),
                    )
    return alerts


def _emit_attention_block_event(
    *,
    event_emitter: EventEmitter | None,
    week_key: str,
    result: AttentionEconomicsResult,
) -> None:
    if event_emitter is None or result is None:
        return
    try:
        event_emitter.emit(
            type="weekly_digest_attention_block_added",
            timestamp=datetime.now(timezone.utc),
            payload={
                "week_key": week_key,
                "window_days": result.window_days,
                "top_sinks": [entity.entity_id for entity in result.top_sinks],
                "at_risk": [entity.entity_id for entity in result.at_risk],
                "best": [entity.entity_id for entity in result.best_counterparties],
            },
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("attention_block_event_failed", error=str(exc))


def _emit_calibration_proposals_event(
    *,
    contract_event_emitter: ContractEventEmitter | None,
    account_email: str,
    account_emails: Sequence[str] | None,
    week_key: str,
    proposals: Sequence[dict[str, object]],
    now: datetime,
) -> None:
    if contract_event_emitter is None or not proposals:
        return
    payload: dict[str, object] = {
        "week_key": week_key,
        "proposals_count": len(proposals),
        "top_labels": [
            str(item.get("label") or "")
            for item in proposals[:3]
            if str(item.get("label") or "").strip()
        ],
    }
    if account_emails:
        payload["account_emails"] = sorted(
            {str(email).strip() for email in account_emails if str(email).strip()}
        )
    try:
        contract_event_emitter.emit(
            EventV1(
                event_type=EventType.CALIBRATION_PROPOSALS_GENERATED,
                ts_utc=now.timestamp(),
                account_id=account_email,
                entity_id=None,
                email_id=None,
                payload=payload,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(
            "contract_event_emit_failed",
            event_type=EventType.CALIBRATION_PROPOSALS_GENERATED.value,
            error=str(exc),
        )


def _collect_weekly_data(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
    account_emails: Sequence[str] | None = None,
    week_key: str,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
    include_notification_sla: bool = False,
    include_weekly_accuracy_report: bool = False,
    weekly_accuracy_window_days: int = 7,
    include_weekly_calibration_report: bool = False,
    weekly_calibration_window_days: int = 7,
    weekly_calibration_top_n: int = 3,
    weekly_calibration_min_corrections: int = 10,
    locale: str = DEFAULT_LOCALE,
    event_emitter: EventEmitter | None = None,
    contract_event_emitter: ContractEventEmitter | None = None,
    now: datetime | None = None,
) -> WeeklyDigestData:
    volume = analytics.weekly_email_volume(
        account_email=account_email,
        account_emails=account_emails,
        days=7,
    )
    attention = analytics.weekly_attention_entities(
        account_email=account_email,
        account_emails=account_emails,
        days=7,
    )
    commitment_counts = analytics.weekly_commitment_counts(
        account_email=account_email,
        account_emails=account_emails,
        days=7,
    )
    overdue = analytics.weekly_overdue_commitments(
        account_email=account_email,
        account_emails=account_emails,
        days=7,
        limit=5,
    )
    trust_deltas = analytics.weekly_trust_score_deltas(days=7)

    attention_economics: AttentionEconomicsResult | None = None
    if include_attention_economics:
        attention_economics = compute_attention_economics(
            analytics=analytics,
            account_email=account_email,
            account_emails=account_emails,
            window_days=7,
            include_anomalies=include_anomalies,
            event_emitter=event_emitter,
            now=now or datetime.now(timezone.utc),
        )
        compute_attention_economics(
            analytics=analytics,
            account_email=account_email,
            account_emails=account_emails,
            window_days=30,
            include_anomalies=include_anomalies,
            event_emitter=event_emitter,
            now=now or datetime.now(timezone.utc),
        )

    anomaly_alerts: list[str] = []
    if include_anomalies:
        anomaly_alerts = _collect_anomaly_alerts(
            analytics=analytics,
            now=now or datetime.now(timezone.utc),
            contract_event_emitter=contract_event_emitter,
            account_email=account_email,
            locale=locale,
        )

    quality_metrics: QualityMetricsSnapshot | None = None
    if include_quality_metrics:
        try:
            quality_metrics = compute_quality_metrics(
                analytics=analytics,
                account_email=account_email,
                account_emails=account_emails,
                window_days=7,
                now=now,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("quality_metrics_weekly_failed", error=str(exc))

    notification_sla: NotificationSLAResult | None = None
    previous_week_sla: NotificationSLAResult | None = None
    if include_notification_sla:
        try:
            now_dt = now or datetime.now(timezone.utc)
            notification_sla = compute_notification_sla(analytics=analytics, now=now_dt)
            previous_week_sla = compute_notification_sla(
                analytics=analytics, now=now_dt - timedelta(days=7)
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("notification_sla_weekly_failed", error=str(exc))

    weekly_accuracy_report: dict[str, object] | None = None
    if include_weekly_accuracy_report:
        try:
            weekly_accuracy_report = analytics.weekly_accuracy_report(
                account_email=account_email,
                days=weekly_accuracy_window_days,
                account_emails=account_emails,
            )
            weekly_accuracy_report["window_days"] = weekly_accuracy_window_days
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("weekly_accuracy_report_failed", error=str(exc))

    weekly_calibration_report: dict[str, object] | None = None
    if include_weekly_calibration_report:
        try:
            anchor = now or datetime.now(timezone.utc)
            since_ts = anchor.timestamp() - (weekly_calibration_window_days * 86400)
            weekly_calibration_report = analytics.weekly_calibration_proposals(
                account_email=account_email,
                since_ts=since_ts,
                top_n=weekly_calibration_top_n,
                min_corrections=weekly_calibration_min_corrections,
                account_emails=account_emails,
            )
            if weekly_calibration_report is not None:
                weekly_calibration_report["window_days"] = (
                    weekly_calibration_window_days
                )
                proposals = weekly_calibration_report.get("proposals") or []
                _emit_calibration_proposals_event(
                    contract_event_emitter=contract_event_emitter,
                    account_email=account_email,
                    account_emails=account_emails,
                    week_key=week_key,
                    proposals=proposals,
                    now=anchor,
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("weekly_calibration_report_failed", error=str(exc))

    weekly_accuracy_progress: WeeklyAccuracyProgress | None = None
    if include_weekly_calibration_report or include_weekly_accuracy_report:
        try:
            anchor = now or datetime.now(timezone.utc)
            progress_window_days = (
                weekly_calibration_window_days
                if include_weekly_calibration_report
                else weekly_accuracy_window_days
            )
            weekly_accuracy_progress = analytics.weekly_accuracy_progress(
                account_email=account_email,
                now_ts=anchor.timestamp(),
                window_days=progress_window_days,
                account_emails=account_emails,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("weekly_accuracy_progress_failed", error=str(exc))

    invoice_count, invoice_total_rub, contract_count = _collect_weekly_human_signals(
        analytics=analytics,
        account_email=account_email,
        account_emails=account_emails,
    )
    silence_risk = None
    try:
        silence_rows = analytics.get_silence_insights(
            account_email=account_email,
            account_emails=account_emails,
            window_days=7,
            limit=1,
        )
        if silence_rows:
            top = silence_rows[0]
            silence_risk = {
                "contact": str(top.get("contact") or "").strip(),
                "days_silent": int(top.get("days_silent") or 0),
            }
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("weekly_silence_insights_failed", error=str(exc))

    relationship_top_senders: Sequence[dict[str, object]] = ()
    relationship_trend: str | None = None
    try:
        relationship_top_senders = analytics.top_sender_relationship_profiles(
            account_email=account_email,
            account_emails=account_emails,
            days=7,
            limit=3,
            now=now,
        )
        if relationship_top_senders:
            avg_trust = sum(
                float(item.get("trust_score") or 0) for item in relationship_top_senders
            ) / len(relationship_top_senders)
            relationship_trend = "stable"
            if avg_trust >= 3.5:
                relationship_trend = "improving"
            elif avg_trust <= 1.5:
                relationship_trend = "declining"
            if silence_risk is None:
                top_risk = max(
                    relationship_top_senders,
                    key=lambda item: int(item.get("last_contact_days") or 0),
                    default=None,
                )
                if top_risk and int(top_risk.get("last_contact_days") or 0) > 0:
                    silence_risk = {
                        "contact": str(top_risk.get("sender_email") or "").strip(),
                        "days_silent": int(top_risk.get("last_contact_days") or 0),
                    }
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("weekly_relationship_digest_failed", error=str(exc))

    try:
        business_summary = analytics.business_summary(
            account_email=account_email,
            account_emails=account_emails,
            window_days=7,
            now=now,
            top_issuer_limit=3,
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("weekly_business_summary_failed", error=str(exc))
        business_summary = None

    return WeeklyDigestData(
        week_key=week_key,
        total_emails=int(volume.get("total") or 0),
        deferred_emails=int(volume.get("deferred") or 0),
        attention_entities=attention,
        commitment_counts=commitment_counts,
        overdue_commitments=overdue,
        trust_deltas=trust_deltas,
        anomaly_alerts=anomaly_alerts,
        attention_economics=attention_economics,
        quality_metrics=quality_metrics,
        notification_sla=notification_sla,
        previous_week_sla=previous_week_sla,
        weekly_accuracy_report=weekly_accuracy_report,
        weekly_calibration_report=weekly_calibration_report,
        weekly_accuracy_progress=weekly_accuracy_progress,
        invoice_count=invoice_count,
        invoice_total_rub=invoice_total_rub,
        contract_count=contract_count,
        silence_risk=silence_risk,
        relationship_top_senders=relationship_top_senders,
        relationship_trend=relationship_trend,
        business_summary=business_summary,
    )


def _build_weekly_digest_text(
    data: WeeklyDigestData, locale: str = DEFAULT_LOCALE
) -> str:
    lines = [
        _locale_text(
            locale,
            en=f"Over the week {data.total_emails} emails. Highlights:",
            ru=f"За неделю {data.total_emails} писем. Главное:",
        )
    ]
    highlights: list[str] = []
    business_summary = data.business_summary or {}
    payable_count = int(
        business_summary.get("payable_invoice_count") or data.invoice_count or 0
    )
    payable_total = int(
        business_summary.get("payable_amount_total") or data.invoice_total_rub or 0
    )
    if payable_count > 0:
        if payable_total > 0:
            total = f"{payable_total:,}".replace(",", " ")
            highlights.append(
                _locale_text(
                    locale,
                    en=f"• Ready to pay now: {payable_count} documents for {total} ₽",
                    ru=f"• К оплате сейчас: {payable_count} документов на {total} ₽",
                )
            )
        else:
            highlights.append(
                _locale_text(
                    locale,
                    en=f"• Ready to pay now: {payable_count} documents",
                    ru=f"• К оплате сейчас: {payable_count} документов",
                )
            )
    attention_parts: list[str] = []
    contract_review_count = int(
        business_summary.get("contract_review_count") or data.contract_count or 0
    )
    reconciliation_attention_count = int(
        business_summary.get("reconciliation_attention_count") or 0
    )
    if contract_review_count > 0:
        attention_parts.append(
            _locale_text(
                locale,
                en=f"{contract_review_count} contracts",
                ru=f"{contract_review_count} договоров",
            )
        )
    if reconciliation_attention_count > 0:
        attention_parts.append(
            _locale_text(
                locale,
                en=f"{reconciliation_attention_count} reconciliations",
                ru=f"{reconciliation_attention_count} актов сверки",
            )
        )
    documents_waiting = int(
        business_summary.get("documents_waiting_attention_count") or 0
    )
    if attention_parts:
        highlights.append(
            _locale_text(
                locale,
                en=f"• Waiting for attention: {', '.join(attention_parts)}",
                ru=f"• Ждут внимания: {', '.join(attention_parts)}",
            )
        )
    elif documents_waiting > 0:
        highlights.append(
            _locale_text(
                locale,
                en=f"• Waiting for attention: {documents_waiting} documents",
                ru=f"• Ждут внимания: {documents_waiting} документов",
            )
        )
    overdue_count = int(data.commitment_counts.get("overdue") or 0)
    if overdue_count > 0:
        highlights.append(
            _locale_text(
                locale,
                en=f"• {overdue_count} commitments are overdue",
                ru=f"• {overdue_count} обязательства просрочены",
            )
        )
    if data.silence_risk:
        contact = _safe_text(
            str(data.silence_risk.get("contact") or "контакт"),
            max_len=40,
        )
        days = int(data.silence_risk.get("days_silent") or 0)
        if days > 0:
            highlights.append(
                _locale_text(
                    locale,
                    en=f"• No reply from {contact} for {days} days (risk)",
                    ru=f"• От {contact} — молчание {days} дней (риск)",
                )
            )
    top_issuers = business_summary.get("top_issuers") or []
    if data.total_emails >= 3 and top_issuers:
        labels = [
            _safe_text(
                str(item.get("issuer_label") or "контрагент"),
                max_len=28,
            )
            for item in top_issuers[:2]
        ]
        highlights.append(
            _locale_text(
                locale,
                en=f"• Most active counterparties: {', '.join(labels)}",
                ru=f"• Самые активные контрагенты: {', '.join(labels)}",
            )
        )
    elif data.total_emails >= 3 and data.relationship_top_senders:
        top = data.relationship_top_senders[0]
        top_sender = _safe_text(
            str(top.get("sender_email") or "контакт"),
            max_len=40,
        )
        top_count = int(top.get("emails_count") or 0)
        if top_count > 0:
            highlights.append(
                _locale_text(
                    locale,
                    en=f"• Top contact: {top_sender} ({top_count} emails)",
                    ru=f"• Топ контакт: {top_sender} ({top_count} писем)",
                )
            )
    if data.total_emails >= 3 and data.relationship_trend:
        highlights.append(
            _locale_text(
                locale,
                en=f"• Relationship trend: {_humanize_relationship_trend(data.relationship_trend, locale)}",
                ru=f"• Тренд отношений: {_humanize_relationship_trend(data.relationship_trend, locale)}",
            )
        )

    if not highlights:
        highlights.append(
            _locale_text(
                locale,
                en="• Quiet week: no critical signals.",
                ru="• Спокойная неделя: критичных сигналов не было.",
            )
        )
    lines.extend(highlights[:4])

    progress_line = _format_weekly_accuracy_progress(
        data.weekly_accuracy_progress, locale=locale
    )
    if progress_line:
        lines.append("")
        lines.append(progress_line)

    if data.attention_economics is not None:
        lines.append("")
        lines.extend(_format_attention_block_localized(data.attention_economics, locale))
    return "\n".join(lines)

def _format_share_accuracy(progress: WeeklyAccuracyProgress | None) -> str:
    if progress is None:
        return "n/a"
    accuracy_pct = max(
        0, min(100, int(round(100 - float(progress.current_surprise_rate_pp))))
    )
    return f"{accuracy_pct}%"


def _build_shareable_weekly_card(
    weekly_data: WeeklyDigestData, locale: str = DEFAULT_LOCALE
) -> str:
    invoice_line = _locale_text(
        locale,
        en=f"{weekly_data.invoice_count} invoices detected",
        ru=f"{weekly_data.invoice_count} счетов обнаружено",
    )
    if weekly_data.invoice_total_rub is not None and weekly_data.invoice_total_rub > 0:
        invoice_line = f"{invoice_line} ({weekly_data.invoice_total_rub} ₽)"
    return "\n".join(
        [
            _locale_text(locale, en="📊 My Mail Week", ru="📊 Моя почтовая неделя"),
            _locale_text(
                locale,
                en=f"{weekly_data.total_emails} emails processed",
                ru=f"{weekly_data.total_emails} писем обработано",
            ),
            invoice_line,
            _locale_text(
                locale,
                en=f"{weekly_data.contract_count} contracts waiting",
                ru=f"{weekly_data.contract_count} договоров ждут",
            ),
            _locale_text(
                locale,
                en=f"accuracy: {_format_share_accuracy(weekly_data.weekly_accuracy_progress)}",
                ru=f"точность: {_format_share_accuracy(weekly_data.weekly_accuracy_progress)}",
            ),
            _locale_text(
                locale,
                en="powered by letterbot · letterbot.ru",
                ru="работает на letterbot · letterbot.ru",
            ),
        ]
    )


def _build_shareable_card_keyboard(
    share_text: str, locale: str = DEFAULT_LOCALE
) -> dict[str, object]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": _locale_text(
                        locale,
                        en="📤 Share report",
                        ru="📤 Поделиться отчётом",
                    ),
                    "switch_inline_query_current_chat": share_text,
                }
            ]
        ]
    }


def maybe_send_weekly_digest(
    *,
    knowledge_db: KnowledgeDB,
    analytics: KnowledgeAnalytics,
    event_emitter: EventEmitter,
    contract_event_emitter: ContractEventEmitter | None = None,
    account_email: str,
    account_emails: Sequence[str] | None = None,
    telegram_chat_id: str,
    email_id: int,
    now: datetime | None = None,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
    include_notification_sla: bool = False,
    include_weekly_accuracy_report: bool = False,
    weekly_accuracy_window_days: int = 7,
) -> None:
    current_time = now or datetime.now(timezone.utc)
    config = _load_weekly_digest_config()
    week_key = _iso_week_key(current_time)
    _locale = _resolve_outbound_ui_locale()

    if not _is_due(current_time, config):
        logger.info(
            "[WEEKLY-DIGEST] decision",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
            week_key=week_key,
        )
        event_emitter.emit(
            type="weekly_digest_skipped",
            timestamp=current_time,
            email_id=email_id,
            payload={
                "reason": "not_due",
                "week_key": week_key,
                "account_email": account_email,
            },
        )
        return

    if analytics.has_weekly_digest_sent(
        account_email=account_email,
        week_key=week_key,
    ):
        logger.info(
            "[WEEKLY-DIGEST] decision",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
            week_key=week_key,
        )
        event_emitter.emit(
            type="weekly_digest_skipped",
            timestamp=current_time,
            email_id=email_id,
            payload={
                "reason": "already_sent",
                "week_key": week_key,
                "account_email": account_email,
            },
        )
        return

    data = _collect_weekly_data(
        analytics=analytics,
        account_email=account_email,
        account_emails=account_emails,
        week_key=week_key,
        include_anomalies=include_anomalies,
        include_attention_economics=include_attention_economics,
        include_quality_metrics=include_quality_metrics,
        include_notification_sla=include_notification_sla,
        include_weekly_accuracy_report=include_weekly_accuracy_report,
        weekly_accuracy_window_days=weekly_accuracy_window_days,
        locale=_locale,
        event_emitter=event_emitter,
        contract_event_emitter=contract_event_emitter,
        now=current_time,
    )

    if data.attention_economics is not None:
        _emit_attention_block_event(
            event_emitter=event_emitter,
            week_key=week_key,
            result=data.attention_economics,
        )

    digest_text = _build_weekly_digest_text(data, locale=_locale)
    share_card = _build_shareable_weekly_card(data, locale=_locale)
    share_card_lines = share_card.splitlines()
    if share_card_lines:
        share_card_lines[-1] = "Powered by LetterBot.ru"
        share_card = "\n".join(share_card_lines)
    digest_text = (
        f"{digest_text}\n\n"
        f"{_locale_text(_locale, en='Share this report', ru='Поделиться отчётом')}\n"
        f"{share_card}"
    )
    payload = TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "chat_id": telegram_chat_id,
            "account_email": account_email,
            "week_key": week_key,
            "shareable_weekly_card": share_card,
            "shareable_weekly_qr_url": "https://letterbot.ru",
        },
        reply_markup=_build_shareable_card_keyboard(share_card, locale=_locale),
    )

    try:
        result = enqueue_tg(email_id=email_id, payload=payload)
        if result is None:
            logger.warning(
                "[WEEKLY-DIGEST] send_unchecked",
                account_email=account_email,
                email_id=email_id,
                week_key=week_key,
            )
            sent = True
        else:
            sent = result.delivered
            if not sent:
                raise RuntimeError(result.error or "Telegram weekly digest send failed")
        if sent:
            knowledge_db.set_last_weekly_digest_state(
                account_email=account_email,
                week_key=week_key,
                sent_at=current_time,
            )
            logger.info(
                "[WEEKLY-DIGEST] decision",
                decision="sent",
                account_email=account_email,
                week_key=week_key,
                total_emails=data.total_emails,
                deferred_emails=data.deferred_emails,
            )
            event_emitter.emit(
                type="weekly_digest_sent",
                timestamp=current_time,
                email_id=email_id,
                payload={
                    "week_key": week_key,
                    "account_email": account_email,
                    "total_emails": data.total_emails,
                    "deferred_emails": data.deferred_emails,
                },
            )
            if contract_event_emitter is not None:
                try:
                    contract_payload = {
                        "week_key": week_key,
                        "account_email": account_email,
                        "total_emails": data.total_emails,
                        "deferred_emails": data.deferred_emails,
                    }
                    if account_emails:
                        scoped_emails = sorted(
                            {
                                str(email).strip()
                                for email in account_emails
                                if str(email).strip()
                            }
                        )
                        if scoped_emails:
                            contract_payload["account_emails"] = scoped_emails
                    contract_event_emitter.emit(
                        EventV1(
                            event_type=EventType.WEEKLY_DIGEST_SENT,
                            ts_utc=current_time.timestamp(),
                            account_id=account_email,
                            entity_id=None,
                            email_id=email_id,
                            payload=contract_payload,
                        )
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.error(
                        "contract_event_emit_failed",
                        event_type=EventType.WEEKLY_DIGEST_SENT.value,
                        error=str(exc),
                    )
    except Exception as exc:
        logger.error(
            "[WEEKLY-DIGEST] failed",
            account_email=account_email,
            email_id=email_id,
            week_key=week_key,
            error=str(exc),
        )
        event_emitter.emit(
            type="weekly_digest_failed",
            timestamp=current_time,
            email_id=email_id,
            payload={
                "week_key": week_key,
                "account_email": account_email,
                "error": str(exc),
            },
        )


__all__ = [
    "WeeklyDigestData",
    "_build_shareable_weekly_card",
    "maybe_send_weekly_digest",
]
