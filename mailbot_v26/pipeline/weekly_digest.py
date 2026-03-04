from __future__ import annotations

import configparser
import logging
import re
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
from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics, WeeklyAccuracyProgress
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe
from mailbot_v26.ui.i18n import DEFAULT_LOCALE, humanize_severity, t

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


_INVOICE_RE = re.compile(r"(сч[её]т|invoice|оплат)", re.IGNORECASE)
_CONTRACT_RE = re.compile(r"(договор|contract|подпис|signature|sign)", re.IGNORECASE)
_RUB_AMOUNT_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:[\s\u00A0]\d{3})+|\d{4,9})\s*(?:₽|руб\.?|рубл[еяи]|rub|rur)",
    re.IGNORECASE,
)


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


def _format_weekly_accuracy_progress(
    progress: WeeklyAccuracyProgress | None,
) -> str | None:
    if progress is None:
        return None
    corrections = int(progress.current_corrections)
    if corrections < 3:
        return None
    if float(progress.current_surprise_rate_pp) > 20:
        return None
    accuracy_pct = max(0, min(100, int(round(100 - float(progress.current_surprise_rate_pp)))))
    delta = int(progress.delta_pp)
    if abs(delta) < 2:
        return None
    if delta > 0:
        return (
            "Твой прогресс: "
            f"точность бота выросла до {accuracy_pct}% "
            f"благодаря твоим {corrections} коррекциям."
        )
    return (
        "Твой прогресс: "
        f"точность бота снизилась до {accuracy_pct}% "
        f"при {corrections} коррекциях за неделю."
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
            event_type="email_received",
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
        text = " ".join(
            str(payload.get(key) or "")
            for key in ("subject", "body_summary")
        )
        if _INVOICE_RE.search(text):
            invoice_count += 1
            for match in _RUB_AMOUNT_RE.finditer(text):
                amount_raw = match.group(1).replace(" ", "").replace("\u00A0", "")
                try:
                    invoice_total_rub += int(amount_raw)
                    has_invoice_amount = True
                except ValueError:
                    continue
        if _CONTRACT_RE.search(text):
            contract_count += 1

    return invoice_count, (invoice_total_rub if has_invoice_amount else None), contract_count


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
        payload["account_emails"] = sorted({str(email).strip() for email in account_emails if str(email).strip()})
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
                weekly_calibration_report["window_days"] = weekly_calibration_window_days
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
    )


def _build_weekly_digest_text(data: WeeklyDigestData) -> str:
    lines = [f"За неделю {data.total_emails} писем. Главное:"]
    highlights: list[str] = []
    if data.invoice_count > 0:
        if data.invoice_total_rub is not None and data.invoice_total_rub > 0:
            total = f"{data.invoice_total_rub:,}".replace(",", " ")
            highlights.append(f"• {data.invoice_count} счёта на оплату (общая сумма {total} ₽)")
        else:
            highlights.append(f"• {data.invoice_count} счёта на оплату")
    if data.contract_count > 0:
        highlights.append(f"• {data.contract_count} договора ждут подписи")
    overdue_count = int(data.commitment_counts.get("overdue") or 0)
    if overdue_count > 0:
        highlights.append(f"• {overdue_count} обязательства просрочены")
    if data.silence_risk:
        contact = _safe_text(str(data.silence_risk.get("contact") or "контакт"), max_len=40)
        days = int(data.silence_risk.get("days_silent") or 0)
        if days > 0:
            highlights.append(f"• От {contact} — молчание {days} дней (риск)")

    if not highlights:
        highlights.append("• Спокойная неделя: критичных сигналов не было.")
    lines.extend(highlights[:4])

    progress_line = _format_weekly_accuracy_progress(data.weekly_accuracy_progress)
    if progress_line:
        lines.append("")
        lines.append(progress_line)

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


__all__ = ["WeeklyDigestData", "maybe_send_weekly_digest"]
