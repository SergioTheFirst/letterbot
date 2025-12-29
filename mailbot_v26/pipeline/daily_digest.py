from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from mailbot_v26.insights.anomaly_engine import compute_anomalies
from mailbot_v26.insights.attention_economics import (
    AttentionEconomicsResult,
    compute_attention_economics,
    format_attention_block,
)
from mailbot_v26.observability import get_logger
from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe

logger = get_logger("mailbot")

_TRUST_DELTA_THRESHOLD = 0.0
_RELATIONSHIP_HEALTH_DELTA_THRESHOLD = 5.0


@dataclass(frozen=True, slots=True)
class DigestData:
    deferred_total: int
    deferred_attachments_only: int
    deferred_informational: int
    commitments_pending: int
    commitments_expired: int
    trust_delta: float | None
    health_delta: float | None
    anomaly_alerts: list[str]
    attention_economics: AttentionEconomicsResult | None


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
            severity = escape_tg_html(anomaly.severity)
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
    now: datetime | None = None,
    contract_event_emitter: ContractEventEmitter | None = None,
) -> DigestData:
    deferred = analytics.deferred_digest_counts(account_email=account_email)
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

    return DigestData(
        deferred_total=int(deferred.get("total", 0)),
        deferred_attachments_only=int(deferred.get("attachments_only", 0)),
        deferred_informational=int(deferred.get("informational", 0)),
        commitments_pending=int(commitments.get("pending", 0)),
        commitments_expired=int(commitments.get("expired", 0)),
        trust_delta=trust_value,
        health_delta=health_value,
        anomaly_alerts=anomaly_alerts,
        attention_economics=attention_economics,
    )


def _build_digest_text(data: DigestData) -> str:
    lines = ["<b>Daily Digest</b>"]
    if data.deferred_total > 0:
        lines.append(
            "• Отложено писем: "
            f"{data.deferred_total} "
            f"(вложения: {data.deferred_attachments_only}, "
            f"информационные: {data.deferred_informational})"
        )
    if data.commitments_pending > 0 or data.commitments_expired > 0:
        lines.append(
            "• Обязательства: "
            f"ожидают {data.commitments_pending}, "
            f"просрочено {data.commitments_expired}"
        )
    if data.trust_delta is not None and abs(data.trust_delta) > _TRUST_DELTA_THRESHOLD:
        delta_pp = data.trust_delta * 100.0
        sign = "+" if delta_pp >= 0 else ""
        lines.append(f"• Trust score: {sign}{delta_pp:.1f} п.п.")
    if data.health_delta is not None and abs(data.health_delta) >= _RELATIONSHIP_HEALTH_DELTA_THRESHOLD:
        sign = "+" if data.health_delta >= 0 else ""
        lines.append(f"• Здоровье отношений: {sign}{data.health_delta:.0f} пунктов")
    if data.anomaly_alerts:
        lines.append("• Anomaly Alerts:")
        lines.extend(f"  - {alert}" for alert in data.anomaly_alerts[:5])
    if data.attention_economics is not None:
        lines.append("")
        lines.extend(format_attention_block(data.attention_economics))
    return "\n".join(lines)


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
    if data.attention_economics is not None:
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
    contract_event_emitter: ContractEventEmitter | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    data = _collect_digest_data(
        analytics=analytics,
        account_email=account_email,
        include_anomalies=include_anomalies,
        include_attention_economics=include_attention_economics,
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
