from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from mailbot_v26.insights.anomaly_engine import compute_anomalies
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.telegram_utils import escape_tg_html

logger = get_logger("mailbot")

_MIN_SAMPLE_SIZE = 5
_TOP_LIMIT = 3
_ANOMALY_CHECK_LIMIT = 6


@dataclass(frozen=True, slots=True)
class AttentionEntity:
    entity_id: str
    label: str
    message_count: int
    attachment_count: int
    estimated_read_minutes: float
    deferred_count: int
    trust_delta: float | None = None
    health_delta: float | None = None
    anomalies: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AttentionEconomicsResult:
    window_days: int
    sample_size: int
    entities: Sequence[AttentionEntity]
    top_sinks: Sequence[AttentionEntity]
    at_risk: Sequence[AttentionEntity]
    best_counterparties: Sequence[AttentionEntity]


def _safe_emit(event_emitter: EventEmitter | None, **kwargs) -> None:
    if event_emitter is None:
        return
    try:
        event_emitter.emit(**kwargs)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("attention_economics_emit_failed", error=str(exc))


def _entity_label(*, analytics: KnowledgeAnalytics, entity_id: str) -> str:
    try:
        label = analytics.entity_label(entity_id=entity_id)
    except Exception:  # pragma: no cover - defensive logging
        label = None
    return label or entity_id


def _with_anomalies(
    *,
    analytics: KnowledgeAnalytics,
    entities: list[AttentionEntity],
    include_anomalies: bool,
    now: datetime,
) -> dict[str, tuple[str, ...]]:
    if not include_anomalies:
        return {}
    anomalies_by_entity: dict[str, tuple[str, ...]] = {}
    for entity in entities[:_ANOMALY_CHECK_LIMIT]:
        try:
            anomalies = compute_anomalies(
                entity_id=entity.entity_id,
                analytics=analytics,
                now_dt=now,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "attention_economics_anomaly_failed",
                entity_id=entity.entity_id,
                error=str(exc),
            )
            continue
        if not anomalies:
            continue
        anomalies_by_entity[entity.entity_id] = tuple(
            escape_tg_html(anomaly.title) for anomaly in anomalies[:_TOP_LIMIT]
        )
    return anomalies_by_entity


def compute_attention_economics(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
    window_days: int = 7,
    include_anomalies: bool = False,
    event_emitter: EventEmitter | None = None,
    now: datetime | None = None,
    sample_threshold: int = _MIN_SAMPLE_SIZE,
) -> AttentionEconomicsResult | None:
    try:
        raw_entities = analytics.attention_entity_metrics(
            account_email=account_email, days=window_days
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("attention_economics_compute_failed", error=str(exc))
        _safe_emit(
            event_emitter,
            type="attention_economics_skipped",
            timestamp=now or datetime.utcnow(),
            payload={
                "reason": "analytics_error",
                "window_days": window_days,
            },
        )
        return None

    sample_size = sum(int(item.get("message_count") or 0) for item in raw_entities)
    if sample_size < sample_threshold:
        logger.info(
            "attention_economics_skipped",
            reason="sample_size",
            sample_size=sample_size,
            window_days=window_days,
        )
        _safe_emit(
            event_emitter,
            type="attention_economics_skipped",
            timestamp=now or datetime.utcnow(),
            payload={
                "reason": "sample_size",
                "sample_size": sample_size,
                "window_days": window_days,
            },
        )
        return None

    deltas = analytics.trust_and_health_deltas(days=window_days)
    entities: list[AttentionEntity] = []
    for item in raw_entities:
        entity_id = str(item.get("entity_id") or "").strip()
        if not entity_id:
            continue
        label = _entity_label(analytics=analytics, entity_id=entity_id)
        delta_entry = deltas.get(entity_id, {})
        entities.append(
            AttentionEntity(
                entity_id=entity_id,
                label=label,
                message_count=int(item.get("message_count") or 0),
                attachment_count=int(item.get("attachment_count") or 0),
                estimated_read_minutes=float(item.get("estimated_read_minutes") or 0.0),
                deferred_count=int(item.get("deferred_count") or 0),
                trust_delta=float(delta_entry.get("trust_delta"))
                if "trust_delta" in delta_entry
                else None,
                health_delta=float(delta_entry.get("health_delta"))
                if "health_delta" in delta_entry
                else None,
            )
        )

    anomalies_by_entity = _with_anomalies(
        analytics=analytics,
        entities=entities,
        include_anomalies=include_anomalies,
        now=now or datetime.utcnow(),
    )
    enriched_entities: list[AttentionEntity] = []
    for entity in entities:
        enriched_entities.append(
            AttentionEntity(
                entity_id=entity.entity_id,
                label=entity.label,
                message_count=entity.message_count,
                attachment_count=entity.attachment_count,
                estimated_read_minutes=entity.estimated_read_minutes,
                deferred_count=entity.deferred_count,
                trust_delta=entity.trust_delta,
                health_delta=entity.health_delta,
                anomalies=anomalies_by_entity.get(entity.entity_id, ()),
            )
        )
    entities = enriched_entities

    entities.sort(
        key=lambda item: (
            -float(item.estimated_read_minutes or 0.0),
            item.label.lower(),
        )
    )

    top_sinks = entities[:_TOP_LIMIT]

    at_risk = [
        entity
        for entity in entities
        if (
            (entity.health_delta is not None and entity.health_delta < 0)
            or entity.anomalies
        )
    ]
    at_risk.sort(
        key=lambda item: (
            item.health_delta if item.health_delta is not None else 0.0,
            0 if item.anomalies else 1,
            -float(item.estimated_read_minutes or 0.0),
            item.label.lower(),
        )
    )
    at_risk = at_risk[:_TOP_LIMIT]

    best_counterparties = [
        entity
        for entity in entities
        if (
            (entity.trust_delta is not None and entity.trust_delta > 0)
            or (entity.health_delta is not None and entity.health_delta > 0)
        )
    ]
    best_counterparties.sort(
        key=lambda item: (
            -float(item.trust_delta or item.health_delta or 0.0),
            item.label.lower(),
        )
    )
    best_counterparties = best_counterparties[:_TOP_LIMIT]

    result = AttentionEconomicsResult(
        window_days=window_days,
        sample_size=sample_size,
        entities=tuple(entities),
        top_sinks=tuple(top_sinks),
        at_risk=tuple(at_risk),
        best_counterparties=tuple(best_counterparties),
    )

    _safe_emit(
        event_emitter,
        type="attention_economics_computed",
        timestamp=now or datetime.utcnow(),
        payload={
            "window_days": window_days,
            "entity_count": len(result.entities),
            "sample_size": result.sample_size,
        },
    )
    return result


def _format_top_sinks(entities: Sequence[AttentionEntity]) -> list[str]:
    if not entities:
        return ["- недостаточно данных"]
    lines: list[str] = []
    for entity in entities:
        lines.append(
            f"- {escape_tg_html(entity.label)}: "
            f"{entity.estimated_read_minutes:.1f} мин, писем {entity.message_count}"
        )
    return lines


def _format_risks(entities: Sequence[AttentionEntity]) -> list[str]:
    if not entities:
        return ["- рисков не зафиксировано"]
    lines: list[str] = []
    for entity in entities:
        reasons: list[str] = []
        if entity.health_delta is not None and entity.health_delta < 0:
            reasons.append(f"здоровье {entity.health_delta:+.0f}")
        if entity.trust_delta is not None and entity.trust_delta < 0:
            reasons.append(f"trust {entity.trust_delta * 100:+.1f} п.п.")
        if entity.anomalies:
            reasons.append("аномалии")
        reason_text = ", ".join(reasons) if reasons else "аномалии"
        lines.append(f"- {escape_tg_html(entity.label)}: {reason_text}")
    return lines


def _format_best(entities: Sequence[AttentionEntity]) -> list[str]:
    if not entities:
        return ["- данных о росте нет"]
    lines: list[str] = []
    for entity in entities:
        details: list[str] = []
        if entity.trust_delta is not None and entity.trust_delta > 0:
            details.append(f"trust {entity.trust_delta * 100:+.1f} п.п.")
        if entity.health_delta is not None and entity.health_delta > 0:
            details.append(f"здоровье {entity.health_delta:+.0f}")
        detail_text = ", ".join(details) if details else "стабильно"
        lines.append(f"- {escape_tg_html(entity.label)}: {detail_text}")
    return lines


def format_attention_block(result: AttentionEconomicsResult) -> list[str]:
    lines = ["<b>⏱ Куда ушло внимание</b>"]
    lines.extend(_format_top_sinks(result.top_sinks))
    lines.append("<b>Риски</b>")
    lines.extend(_format_risks(result.at_risk))
    lines.append("<b>Лучшие контрагенты</b>")
    lines.extend(_format_best(result.best_counterparties))
    return lines


__all__ = [
    "AttentionEconomicsResult",
    "AttentionEntity",
    "compute_attention_economics",
    "format_attention_block",
]
