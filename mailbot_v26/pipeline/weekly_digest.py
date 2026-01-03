from __future__ import annotations

import configparser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

from mailbot_v26.insights.anomaly_engine import compute_anomalies
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
from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe
from mailbot_v26.ui.i18n import DEFAULT_LOCALE, humanize_severity, t

logger = get_logger("mailbot")

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.ini"

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


def _parse_weekday(value: str | None) -> int:
    if not value:
        return 0
    cleaned = value.strip().lower()
    if cleaned.isdigit():
        numeric = int(cleaned)
        return max(0, min(6, numeric))
    return _WEEKDAY_ALIASES.get(cleaned, 0)


def _load_weekly_digest_config() -> WeeklyDigestConfig:
    weekday = 0
    hour = 9
    minute = 0
    parser = configparser.ConfigParser()
    if _CONFIG_PATH.exists():
        parser.read(_CONFIG_PATH, encoding="utf-8")
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


def _attention_summary(attention: Sequence[dict[str, object]]) -> str:
    if not attention:
        return "нет данных"
    top = attention[:3]
    parts: list[str] = []
    for item in top:
        name = _safe_text(str(item.get("entity") or "неизвестно"), max_len=40)
        words = int(item.get("words") or 0)
        minutes = words / 200.0
        minutes_display = 0.1 if minutes > 0 and minutes < 0.1 else minutes
        parts.append(f"{name} — {minutes_display:.1f} мин")
    return ", ".join(parts)


def _overdue_summary(items: Sequence[dict[str, object]]) -> str:
    if not items:
        return "нет"
    parts: list[str] = []
    for item in items[:5]:
        name = _safe_text(str(item.get("from_email") or "неизвестно"), max_len=24)
        text = _safe_text(str(item.get("commitment_text") or ""), max_len=48)
        deadline = _safe_text(str(item.get("deadline_iso") or ""), max_len=16)
        parts.append(f"{name} → {text} → {deadline}")
    return "; ".join(parts)


def _trust_summary(trust_deltas: dict[str, list[dict[str, object]]]) -> str:
    up = trust_deltas.get("up", [])
    down = trust_deltas.get("down", [])
    if not up and not down:
        return "недостаточно истории"
    parts: list[str] = []
    if up:
        up_items = []
        for item in up:
            name = _safe_text(str(item.get("entity_name") or item.get("entity_id") or ""), max_len=24)
            delta_pp = float(item.get("delta") or 0.0) * 100.0
            up_items.append(f"{name} +{delta_pp:.1f} п.п.")
        parts.append(f"рост: {', '.join(up_items)}")
    if down:
        down_items = []
        for item in down:
            name = _safe_text(str(item.get("entity_name") or item.get("entity_id") or ""), max_len=24)
            delta_pp = float(item.get("delta") or 0.0) * 100.0
            down_items.append(f"{name} {delta_pp:.1f} п.п.")
        parts.append(f"падение: {', '.join(down_items)}")
    return "; ".join(parts)


def _collect_anomaly_alerts(
    *,
    analytics: KnowledgeAnalytics,
    now: datetime,
    contract_event_emitter: ContractEventEmitter | None = None,
    account_email: str | None = None,
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
            severity = escape_tg_html(
                humanize_severity(anomaly.severity, locale=DEFAULT_LOCALE)
            )
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
    *, event_emitter: EventEmitter | None, week_key: str, result: AttentionEconomicsResult
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


def _collect_weekly_data(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
    week_key: str,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
    include_notification_sla: bool = False,
    include_weekly_accuracy_report: bool = False,
    weekly_accuracy_window_days: int = 7,
    event_emitter: EventEmitter | None = None,
    contract_event_emitter: ContractEventEmitter | None = None,
    now: datetime | None = None,
) -> WeeklyDigestData:
    volume = analytics.weekly_email_volume(account_email=account_email, days=7)
    attention = analytics.weekly_attention_entities(account_email=account_email, days=7)
    commitment_counts = analytics.weekly_commitment_counts(account_email=account_email, days=7)
    overdue = analytics.weekly_overdue_commitments(account_email=account_email, days=7, limit=5)
    trust_deltas = analytics.weekly_trust_score_deltas(days=7)

    attention_economics: AttentionEconomicsResult | None = None
    if include_attention_economics:
        attention_economics = compute_attention_economics(
            analytics=analytics,
            account_email=account_email,
            window_days=7,
            include_anomalies=include_anomalies,
            event_emitter=event_emitter,
            now=now or datetime.now(timezone.utc),
        )
        compute_attention_economics(
            analytics=analytics,
            account_email=account_email,
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
        )

    quality_metrics: QualityMetricsSnapshot | None = None
    if include_quality_metrics:
        try:
            quality_metrics = compute_quality_metrics(
                analytics=analytics,
                account_email=account_email,
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
            notification_sla = compute_notification_sla(
                analytics=analytics, now=now_dt
            )
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
            )
            weekly_accuracy_report["window_days"] = weekly_accuracy_window_days
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("weekly_accuracy_report_failed", error=str(exc))

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
    )


def _build_weekly_digest_text(data: WeeklyDigestData) -> str:
    lines = [t("digest.weekly", locale=DEFAULT_LOCALE)]
    lines.append(
        "• Объём: "
        f"всего {data.total_emails}, "
        f"в дайджест {data.deferred_emails}"
    )
    if data.attention_economics is None:
        lines.append(
            "• Внимание: "
            f"{_attention_summary(data.attention_entities)}"
        )
    commitments = data.commitment_counts
    lines.append(
        "• Обязательства: "
        f"создано {int(commitments.get('created') or 0)}, "
        f"выполнено {int(commitments.get('fulfilled') or 0)}, "
        f"просрочено {int(commitments.get('overdue') or 0)}"
    )
    lines.append(
        "• Просроченные (топ-5): "
        f"{_overdue_summary(data.overdue_commitments)}"
    )
    lines.append(
        "• Уровень доверия: "
        f"{_trust_summary(data.trust_deltas)}"
    )
    if data.quality_metrics is not None:
        qm = data.quality_metrics
        rate_text = (
            f" ({qm.correction_rate * 100:.1f}% писем)"
            if qm.correction_rate is not None
            else ""
        )
        lines.append(
            "• Качество: "
            f"исправлений {qm.corrections_total}{rate_text} (7 дней)"
        )
        if qm.by_new_priority:
            breakdown = ", ".join(
                f"{escape_tg_html(item.key)}: {item.count}" for item in qm.by_new_priority
            )
            lines.append("• Исправления по приоритету: " + breakdown)
        if qm.by_engine:
            breakdown = ", ".join(
                f"{escape_tg_html(item.key)}: {item.count}" for item in qm.by_engine
            )
            lines.append("• Источник оценки: " + breakdown)
    if data.weekly_accuracy_report is not None:
        report = data.weekly_accuracy_report
        corrections = int(report.get("priority_corrections") or 0)
        if corrections > 0:
            emails = int(report.get("emails_received") or 0)
            surprises = int(report.get("surprises") or 0)
            window_days = int(report.get("window_days") or 7)
            accuracy_pct = report.get("accuracy_pct")
            if accuracy_pct is None:
                surprise_rate = report.get("surprise_rate")
                if surprise_rate is not None:
                    accuracy_pct = round((1 - float(surprise_rate)) * 100)
            accuracy_pct = int(accuracy_pct or 0)
            lines.append(f"<b>Отчёт точности ({window_days} дней)</b>")
            lines.append(f"• Писем обработано: {emails}")
            lines.append(f"• Коррекции приоритета: {corrections}")
            lines.append(f"• Сюрпризы: {surprises} (точность: {accuracy_pct}%)")
    if data.notification_sla is not None:
        current = data.notification_sla
        prev = data.previous_week_sla
        p90 = current.p90_latency_7d or 0
        p99 = current.p99_latency_7d or 0
        trend = ""
        if prev and prev.p90_latency_7d is not None:
            delta = p90 - prev.p90_latency_7d
            trend = f" (Δ {delta:+.0f}с)"
        lines.append(
            "• Надёжность уведомлений: "
            f"p90 {p90:.0f}с{trend}, p99 {p99:.0f}с"
        )
    if data.anomaly_alerts:
        lines.append(t("digest.anomalies", locale=DEFAULT_LOCALE))
        lines.extend(f"  - {alert}" for alert in data.anomaly_alerts[:8])
    if data.attention_economics is not None:
        lines.append("")
        lines.extend(format_attention_block(data.attention_economics))
    return "\n".join(lines)


def maybe_send_weekly_digest(
    *,
    knowledge_db: KnowledgeDB,
    analytics: KnowledgeAnalytics,
    event_emitter: EventEmitter,
    contract_event_emitter: ContractEventEmitter | None = None,
    account_email: str,
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
        week_key=week_key,
        include_anomalies=include_anomalies,
        include_attention_economics=include_attention_economics,
        include_quality_metrics=include_quality_metrics,
        include_notification_sla=include_notification_sla,
        include_weekly_accuracy_report=include_weekly_accuracy_report,
        weekly_accuracy_window_days=weekly_accuracy_window_days,
        event_emitter=event_emitter,
        contract_event_emitter=contract_event_emitter,
        now=current_time,
    )

    if data.attention_economics is not None:
        _emit_attention_block_event(
            event_emitter=event_emitter, week_key=week_key, result=data.attention_economics
        )

    digest_text = _build_weekly_digest_text(data)
    payload = TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "chat_id": telegram_chat_id,
            "account_email": account_email,
            "week_key": week_key,
        },
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
                    contract_event_emitter.emit(
                        EventV1(
                            event_type=EventType.WEEKLY_DIGEST_SENT,
                            ts_utc=current_time.timestamp(),
                            account_id=account_email,
                            entity_id=None,
                            email_id=email_id,
                            payload={
                                "week_key": week_key,
                                "account_email": account_email,
                                "total_emails": data.total_emails,
                                "deferred_emails": data.deferred_emails,
                            },
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


__all__ = ["WeeklyDigestData", "maybe_send_weekly_digest"]
