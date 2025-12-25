from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from mailbot_v26.observability import get_logger
from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import telegram_safe

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


def _collect_digest_data(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
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

    return DigestData(
        deferred_total=int(deferred.get("total", 0)),
        deferred_attachments_only=int(deferred.get("attachments_only", 0)),
        deferred_informational=int(deferred.get("informational", 0)),
        commitments_pending=int(commitments.get("pending", 0)),
        commitments_expired=int(commitments.get("expired", 0)),
        trust_delta=trust_value,
        health_delta=health_value,
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
    return False


def maybe_send_daily_digest(
    *,
    knowledge_db: KnowledgeDB,
    analytics: KnowledgeAnalytics,
    account_email: str,
    telegram_chat_id: str,
    email_id: int,
) -> None:
    now = datetime.now(timezone.utc)
    data = _collect_digest_data(analytics=analytics, account_email=account_email)
    last_sent_at = knowledge_db.get_last_digest_sent_at(account_email=account_email)
    already_sent = bool(last_sent_at and last_sent_at.date() == now.date())

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
