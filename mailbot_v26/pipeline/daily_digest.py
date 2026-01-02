from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

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
from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe
from mailbot_v26.ui.i18n import DEFAULT_LOCALE, humanize_severity, t

logger = get_logger("mailbot")

_TRUST_DELTA_THRESHOLD = 0.0
_RELATIONSHIP_HEALTH_DELTA_THRESHOLD = 5.0
_WARNING_EMOJI = "\u26a0\ufe0f"
_TARGET_EMOJI = "\U0001F3AF"


@dataclass(frozen=True, slots=True)
class DigestData:
    deferred_total: int
    deferred_attachments_only: int
    deferred_informational: int
    deferred_items: list[dict[str, str]]
    commitments_pending: int
    commitments_expired: int
    trust_delta: float | None
    health_delta: float | None
    anomaly_alerts: list[str]
    attention_economics: AttentionEconomicsResult | None
    quality_metrics: QualityMetricsSnapshot | None
    notification_sla: NotificationSLAResult | None
    deadlock_insights: list[dict[str, object]]
    silence_insights: list[dict[str, object]]
    digest_insights_enabled: bool
    digest_insights_max_items: int


def _collect_anomaly_alerts(
    *,
    analytics: KnowledgeAnalytics,
    now: datetime,
    contract_event_emitter: ContractEventEmitter | None = None,
    account_email: str | None = None,
) -> list[str]:
    alerts: list[str] = []
    try:
        entities = analytics.recent_entity_activity(days=30, limit=5)
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


def _collect_digest_data(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
    include_notification_sla: bool = False,
    include_digest_insights: bool = False,
    digest_insights_window_days: int = 7,
    digest_insights_max_items: int = 3,
    now: datetime | None = None,
    contract_event_emitter: ContractEventEmitter | None = None,
) -> DigestData:
    deferred = analytics.deferred_digest_counts(account_email=account_email)
    deferred_items = analytics.deferred_digest_items(account_email=account_email, limit=5)
    commitments = analytics.commitment_status_counts(account_email=account_email)
    trust_delta = analytics.latest_trust_score_delta()
    health_delta = analytics.latest_relationship_health_delta()

    trust_value: float | None = None
    if trust_delta is not None:
        raw_delta = trust_delta.get("delta")
        try:
            trust_value = float(raw_delta)
        except (TypeError, ValueError):
            trust_value = None

    health_value: float | None = None
    if health_delta is not None:
        raw_delta = health_delta.get("delta")
        try:
            health_value = float(raw_delta)
        except (TypeError, ValueError):
            health_value = None

    anomaly_alerts: list[str] = []
    if include_anomalies:
        anomaly_alerts = _collect_anomaly_alerts(
            analytics=analytics,
            now=now or datetime.now(timezone.utc),
            contract_event_emitter=contract_event_emitter,
            account_email=account_email,
        )

    attention_economics: AttentionEconomicsResult | None = None
    if include_attention_economics:
        attention_economics = compute_attention_economics(
            analytics=analytics,
            account_email=account_email,
            window_days=7,
            include_anomalies=include_anomalies,
            now=now or datetime.now(timezone.utc),
        )

        compute_attention_economics(
            analytics=analytics,
            account_email=account_email,
            window_days=30,
            include_anomalies=include_anomalies,
            now=now or datetime.now(timezone.utc),
        )

    quality_metrics: QualityMetricsSnapshot | None = None
    if include_quality_metrics:
        try:
            quality_metrics = compute_quality_metrics(
                analytics=analytics,
                account_email=account_email,
                window_days=1,
                now=now,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("quality_metrics_daily_failed", error=str(exc))

    notification_sla: NotificationSLAResult | None = None
    if include_notification_sla:
        try:
            notification_sla = compute_notification_sla(analytics=analytics, now=now)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("notification_sla_digest_failed", error=str(exc))

    deadlock_insights: list[dict[str, object]] = []
    silence_insights: list[dict[str, object]] = []
    insights_enabled = bool(include_digest_insights)
    insights_max_items = max(0, int(digest_insights_max_items))
    if insights_enabled and insights_max_items > 0:
        deadlock_insights = analytics.deadlock_insights(
            account_email=account_email,
            window_days=max(1, int(digest_insights_window_days)),
            limit=insights_max_items,
        )
        silence_insights = analytics.silence_insights(
            account_email=account_email,
            window_days=max(1, int(digest_insights_window_days)),
            limit=insights_max_items,
        )

    return DigestData(
        deferred_total=int(deferred.get("total", 0)),
        deferred_attachments_only=int(deferred.get("attachments_only", 0)),
        deferred_informational=int(deferred.get("informational", 0)),
        deferred_items=deferred_items,
        commitments_pending=int(commitments.get("pending", 0)),
        commitments_expired=int(commitments.get("expired", 0)),
        trust_delta=trust_value,
        health_delta=health_value,
        anomaly_alerts=anomaly_alerts,
        attention_economics=attention_economics,
        quality_metrics=quality_metrics,
        notification_sla=notification_sla,
        deadlock_insights=deadlock_insights,
        silence_insights=silence_insights,
        digest_insights_enabled=insights_enabled,
        digest_insights_max_items=insights_max_items,
    )


def _build_digest_text(data: DigestData) -> str:
    lines = [t("digest.daily", locale=DEFAULT_LOCALE)]
    if data.deferred_total > 0:
        lines.append(
            "• Отложено писем: "
            f"{data.deferred_total} "
            f"(вложения: {data.deferred_attachments_only}, "
            f"информационные: {data.deferred_informational})"
        )
    if data.deferred_items:
        lines.append("• Отложено для снижения перегрузки:")
        for item in data.deferred_items:
            sender = item.get("sender") or ""
            summary = item.get("summary") or item.get("subject") or ""
            label_parts = [part for part in [sender, summary] if part]
            if label_parts:
                lines.append(f"  - {' — '.join(label_parts)}")
    if data.commitments_pending > 0 or data.commitments_expired > 0:
        lines.append(
            "• Обязательства: "
            f"ожидают {data.commitments_pending}, "
            f"просрочено {data.commitments_expired}"
        )
    if data.trust_delta is not None and abs(data.trust_delta) > _TRUST_DELTA_THRESHOLD:
        delta_pp = data.trust_delta * 100.0
        sign = "+" if delta_pp >= 0 else ""
        lines.append(f"• Уровень доверия: {sign}{delta_pp:.1f} п.п.")
    if data.health_delta is not None and abs(data.health_delta) >= _RELATIONSHIP_HEALTH_DELTA_THRESHOLD:
        sign = "+" if data.health_delta >= 0 else ""
        lines.append(f"• Здоровье отношений: {sign}{data.health_delta:.0f} пунктов")
    if data.anomaly_alerts:
        lines.append(t("digest.anomalies", locale=DEFAULT_LOCALE))
        lines.extend(f"  - {alert}" for alert in data.anomaly_alerts[:5])
    if data.quality_metrics is not None:
        qm = data.quality_metrics
        rate_display = ""
        if qm.correction_rate is not None:
            rate_display = f" ({qm.correction_rate * 100:.1f}% писем)"
        lines.append(
            "• Качество: "
            f"исправлений {qm.corrections_total} за 24ч{rate_display}"
        )
        priority_breakdown = ", ".join(
            f"{escape_tg_html(item.key)}: {item.count}"
            for item in qm.by_new_priority
        )
        if priority_breakdown:
            lines.append(f"  - по приоритету: {priority_breakdown}")
    if data.notification_sla is not None:
        sla = data.notification_sla
        delivered_pct = sla.delivery_rate_24h * 100
        salvage_pct = sla.salvage_rate_24h * 100
        p90 = sla.p90_latency_24h or 0
        lines.append(
            "• Надёжность уведомлений 24ч: "
            f"доставлено {delivered_pct:.1f}%, p90 {p90:.0f}с, резерв {salvage_pct:.1f}%"
        )
        if sla.top_error_reasons_24h:
            top = sla.top_error_reasons_24h[0]
            lines.append(
                f"  - главная ошибка: {escape_tg_html(top.reason)} ({top.share * 100:.1f}%)"
            )
    if data.attention_economics is not None:
        lines.append("")
        lines.extend(format_attention_block(data.attention_economics))
    if data.digest_insights_enabled and data.digest_insights_max_items > 0:
        insights_lines: list[str] = []
        for item in data.deadlock_insights:
            if len(insights_lines) >= data.digest_insights_max_items:
                break
            label = _format_deadlock_label(item)
            if not label:
                continue
            insights_lines.append(
                "• Deadlock: "
                f"{label} "
                f"→ {_TARGET_EMOJI} Предложить созвон (15 мин)"
            )
        for item in data.silence_insights:
            if len(insights_lines) >= data.digest_insights_max_items:
                break
            contact = _format_silence_contact(item)
            if not contact:
                continue
            days = _format_silence_days(item)
            insights_lines.append(
                "• Silence: "
                f"{contact} молчит {days}д "
                f"→ {_TARGET_EMOJI} Пинговать сегодня"
            )
        if insights_lines:
            lines.append(f"{_WARNING_EMOJI} <b>ТРЕБУЕТ ВНИМАНИЯ</b>")
            lines.extend(insights_lines)
    return "\n".join(lines)


def _format_deadlock_label(item: dict[str, object]) -> str:
    sender = str(item.get("from_email") or "").strip()
    subject = str(item.get("subject") or "").strip()
    parts = [part for part in [sender, subject] if part]
    if not parts:
        return ""
    return " — ".join(escape_tg_html(part) for part in parts)


def _format_silence_contact(item: dict[str, object]) -> str:
    contact = str(item.get("contact") or "").strip()
    if not contact:
        return ""
    return escape_tg_html(contact)


def _format_silence_days(item: dict[str, object]) -> int:
    raw = item.get("days_silent")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _has_digest_content(data: DigestData) -> bool:
    if data.deferred_total > 0:
        return True
    if data.commitments_pending > 0 or data.commitments_expired > 0:
        return True
    if data.trust_delta is not None and abs(data.trust_delta) > _TRUST_DELTA_THRESHOLD:
        return True
    if data.health_delta is not None and abs(data.health_delta) >= _RELATIONSHIP_HEALTH_DELTA_THRESHOLD:
        return True
    if data.anomaly_alerts:
        return True
    if data.quality_metrics is not None:
        return True
    if data.attention_economics is not None:
        return True
    if data.digest_insights_enabled and data.digest_insights_max_items > 0:
        if data.deadlock_insights or data.silence_insights:
            return True
    return False


def maybe_send_daily_digest(
    *,
    knowledge_db: KnowledgeDB,
    analytics: KnowledgeAnalytics,
    account_email: str,
    telegram_chat_id: str,
    email_id: int,
    include_anomalies: bool = False,
    include_attention_economics: bool = False,
    include_quality_metrics: bool = False,
    include_notification_sla: bool = False,
    contract_event_emitter: ContractEventEmitter | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    data = _collect_digest_data(
        analytics=analytics,
        account_email=account_email,
        include_anomalies=include_anomalies,
        include_attention_economics=include_attention_economics,
        include_quality_metrics=include_quality_metrics,
        include_notification_sla=include_notification_sla,
        now=now,
        contract_event_emitter=contract_event_emitter,
    )
    already_sent = analytics.has_daily_digest_sent(
        account_email=account_email,
        day=now,
    )

    if already_sent:
        logger.info(
            "[DAILY-DIGEST] decision",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
            deferred_total=data.deferred_total,
            deferred_attachments_only=data.deferred_attachments_only,
            deferred_informational=data.deferred_informational,
            commitments_pending=data.commitments_pending,
            commitments_expired=data.commitments_expired,
            trust_delta=data.trust_delta,
            health_delta=data.health_delta,
        )
        return

    if not _has_digest_content(data):
        logger.info(
            "[DAILY-DIGEST] decision",
            decision="skipped",
            reason="no_content",
            account_email=account_email,
            deferred_total=data.deferred_total,
            deferred_attachments_only=data.deferred_attachments_only,
            deferred_informational=data.deferred_informational,
            commitments_pending=data.commitments_pending,
            commitments_expired=data.commitments_expired,
            trust_delta=data.trust_delta,
            health_delta=data.health_delta,
        )
        return

    digest_text = _build_digest_text(data)
    payload = TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "chat_id": telegram_chat_id,
            "account_email": account_email,
        },
    )

    try:
        result = enqueue_tg(email_id=email_id, payload=payload)
        if result is None:
            logger.warning(
                "[DAILY-DIGEST] send_unchecked",
                account_email=account_email,
                email_id=email_id,
            )
            sent = True
        else:
            sent = result.delivered
            if not sent:
                raise RuntimeError(result.error or "Telegram digest send failed")
        if sent:
            knowledge_db.set_last_digest_sent_at(
                account_email=account_email,
                sent_at=now,
            )
            if contract_event_emitter is not None:
                try:
                    contract_event_emitter.emit(
                        EventV1(
                            event_type=EventType.DAILY_DIGEST_SENT,
                            ts_utc=now.timestamp(),
                            account_id=account_email,
                            entity_id=None,
                            email_id=email_id,
                            payload={
                                "account_email": account_email,
                            },
                        )
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.error(
                        "contract_event_emit_failed",
                        event_type=EventType.DAILY_DIGEST_SENT.value,
                        error=str(exc),
                    )
            logger.info(
                "[DAILY-DIGEST] decision",
                decision="sent",
                account_email=account_email,
                deferred_total=data.deferred_total,
                deferred_attachments_only=data.deferred_attachments_only,
                deferred_informational=data.deferred_informational,
                commitments_pending=data.commitments_pending,
                commitments_expired=data.commitments_expired,
                trust_delta=data.trust_delta,
                health_delta=data.health_delta,
            )
    except Exception as exc:
        logger.error(
            "[DAILY-DIGEST] failed",
            account_email=account_email,
            email_id=email_id,
            error=str(exc),
        )


__all__ = ["DigestData", "maybe_send_daily_digest"]
