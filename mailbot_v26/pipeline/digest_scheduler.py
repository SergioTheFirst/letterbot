from __future__ import annotations

import configparser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.observability.logger import LoggerLike
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import daily_digest, weekly_digest
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.worker.telegram_sender import DeliveryResult

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.ini"


@dataclass(frozen=True, slots=True)
class DigestStorage:
    knowledge_db: KnowledgeDB
    analytics: KnowledgeAnalytics
    event_emitter: EventEmitter | None = None


@dataclass(frozen=True, slots=True)
class DailyDigestConfig:
    hour: int
    minute: int


@dataclass(frozen=True, slots=True)
class WeeklyDigestConfig:
    weekday: int
    hour: int
    minute: int


def _load_daily_digest_config() -> DailyDigestConfig:
    hour = 9
    minute = 0
    parser = configparser.ConfigParser()
    if _CONFIG_PATH.exists():
        parser.read(_CONFIG_PATH, encoding="utf-8")
    section = parser["daily_digest"] if "daily_digest" in parser else None
    if section is not None:
        try:
            hour = max(0, min(23, section.getint("hour", fallback=9)))
        except ValueError:
            hour = 9
        try:
            minute = max(0, min(59, section.getint("minute", fallback=0)))
        except ValueError:
            minute = 0
    return DailyDigestConfig(hour=hour, minute=minute)


def _load_weekly_digest_config() -> WeeklyDigestConfig:
    weekday = 0
    hour = 9
    minute = 0
    parser = configparser.ConfigParser()
    if _CONFIG_PATH.exists():
        parser.read(_CONFIG_PATH, encoding="utf-8")
    section = parser["weekly_digest"] if "weekly_digest" in parser else None
    if section is not None:
        weekday = weekly_digest._parse_weekday(section.get("weekday", fallback="mon"))
        try:
            hour = max(0, min(23, section.getint("hour", fallback=9)))
        except ValueError:
            hour = 9
        try:
            minute = max(0, min(59, section.getint("minute", fallback=0)))
        except ValueError:
            minute = 0
    return WeeklyDigestConfig(weekday=weekday, hour=hour, minute=minute)


def _is_daily_due(now: datetime, config: DailyDigestConfig) -> bool:
    if now.hour < config.hour:
        return False
    if now.hour == config.hour and now.minute < config.minute:
        return False
    return True


def _is_weekly_due(now: datetime, config: WeeklyDigestConfig) -> bool:
    if now.weekday() != config.weekday:
        return False
    if now.hour < config.hour:
        return False
    if now.hour == config.hour and now.minute < config.minute:
        return False
    return True


def _build_daily_payload(
    *,
    account_email: str,
    chat_id: str,
    bot_token: str,
    data: daily_digest.DigestData,
) -> TelegramPayload:
    digest_text = daily_digest._build_digest_text(data)
    return TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "bot_token": bot_token,
            "chat_id": chat_id,
            "account_email": account_email,
        },
    )


def _build_weekly_payload(
    *,
    account_email: str,
    chat_id: str,
    bot_token: str,
    week_key: str,
    data: weekly_digest.WeeklyDigestData,
) -> TelegramPayload:
    digest_text = weekly_digest._build_weekly_digest_text(data)
    return TelegramPayload(
        html_text=telegram_safe(digest_text),
        priority="🔵",
        metadata={
            "bot_token": bot_token,
            "chat_id": chat_id,
            "account_email": account_email,
            "week_key": week_key,
        },
    )


def _send_payload(
    *,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    payload: TelegramPayload,
) -> DeliveryResult:
    return telegram_sender(payload)


def run_digest_tick(
    *,
    now: datetime,
    config,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
) -> None:
    try:
        flags = FeatureFlags(base_dir=_CONFIG_PATH.parent)
        daily_config = _load_daily_digest_config()
        weekly_config = _load_weekly_digest_config()

        for account in config.accounts:
            account_email = account.login
            chat_id = account.telegram_chat_id
            bot_token = config.keys.telegram_bot_token

            if not chat_id:
                logger.warning(
                    "digest_tick_checked",
                    digest_type="daily",
                    decision="skipped",
                    reason="missing_chat_id",
                    account_email=account_email,
                )
                logger.warning(
                    "digest_tick_checked",
                    digest_type="weekly",
                    decision="skipped",
                    reason="missing_chat_id",
                    account_email=account_email,
                )
                continue

            if flags.ENABLE_DAILY_DIGEST:
                _run_daily_digest(
                    now=now,
                    config=daily_config,
                    account_email=account_email,
                    chat_id=chat_id,
                    bot_token=bot_token,
                    storage=storage,
                    telegram_sender=telegram_sender,
                    logger=logger,
                    include_anomalies=flags.ENABLE_ANOMALY_ALERTS,
                )
            else:
                logger.info(
                    "digest_tick_checked",
                    digest_type="daily",
                    decision="skipped",
                    reason="flag_disabled",
                    account_email=account_email,
                )

            if flags.ENABLE_WEEKLY_DIGEST:
                _run_weekly_digest(
                    now=now,
                    config=weekly_config,
                    account_email=account_email,
                    chat_id=chat_id,
                    bot_token=bot_token,
                    storage=storage,
                    telegram_sender=telegram_sender,
                    logger=logger,
                    include_anomalies=flags.ENABLE_ANOMALY_ALERTS,
                )
            else:
                logger.info(
                    "digest_tick_checked",
                    digest_type="weekly",
                    decision="skipped",
                    reason="flag_disabled",
                    account_email=account_email,
                )
    except Exception as exc:
        logger.error("digest_tick_failed", error=str(exc))


def _run_daily_digest(
    *,
    now: datetime,
    config: DailyDigestConfig,
    account_email: str,
    chat_id: str,
    bot_token: str,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
    include_anomalies: bool = False,
) -> None:
    if not _is_daily_due(now, config):
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
        )
        return

    last_sent = storage.knowledge_db.get_last_digest_sent_at(account_email=account_email)
    if last_sent and last_sent.date() == now.date():
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
        )
        return

    data = daily_digest._collect_digest_data(
        analytics=storage.analytics,
        account_email=account_email,
        include_anomalies=include_anomalies,
        now=now,
    )
    if not daily_digest._has_digest_content(data):
        logger.info(
            "digest_tick_checked",
            digest_type="daily",
            decision="skipped",
            reason="no_content",
            account_email=account_email,
        )
        return

    logger.info(
        "digest_tick_checked",
        digest_type="daily",
        decision="due",
        account_email=account_email,
    )

    payload = _build_daily_payload(
        account_email=account_email,
        chat_id=chat_id,
        bot_token=bot_token,
        data=data,
    )

    try:
        result = _send_payload(telegram_sender=telegram_sender, payload=payload)
    except Exception as exc:
        logger.error(
            "digest_failed",
            digest_type="daily",
            account_email=account_email,
            error=str(exc),
        )
        return

    if result.delivered:
        storage.knowledge_db.set_last_digest_sent_at(
            account_email=account_email,
            sent_at=now,
        )
        logger.info(
            "digest_sent",
            digest_type="daily",
            account_email=account_email,
        )
        return

    logger.error(
        "digest_failed",
        digest_type="daily",
        account_email=account_email,
        error=result.error or "telegram delivery failed",
        retryable=result.retryable,
    )


def _run_weekly_digest(
    *,
    now: datetime,
    config: WeeklyDigestConfig,
    account_email: str,
    chat_id: str,
    bot_token: str,
    storage: DigestStorage,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
    logger: LoggerLike,
    include_anomalies: bool = False,
) -> None:
    week_key = weekly_digest._iso_week_key(now)

    if not _is_weekly_due(now, config):
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="not_due",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_skipped",
                timestamp=now,
                email_id=0,
                payload={
                    "reason": "not_due",
                    "week_key": week_key,
                    "account_email": account_email,
                },
            )
        return

    last_week_key = storage.knowledge_db.get_last_weekly_digest_key(
        account_email=account_email
    )
    if last_week_key == week_key:
        logger.info(
            "digest_tick_checked",
            digest_type="weekly",
            decision="skipped",
            reason="already_sent",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_skipped",
                timestamp=now,
                email_id=0,
                payload={
                    "reason": "already_sent",
                    "week_key": week_key,
                    "account_email": account_email,
                },
            )
        return

    logger.info(
        "digest_tick_checked",
        digest_type="weekly",
        decision="due",
        account_email=account_email,
        week_key=week_key,
    )

    data = weekly_digest._collect_weekly_data(
        analytics=storage.analytics,
        account_email=account_email,
        week_key=week_key,
        include_anomalies=include_anomalies,
        now=now,
    )
    payload = _build_weekly_payload(
        account_email=account_email,
        chat_id=chat_id,
        bot_token=bot_token,
        week_key=week_key,
        data=data,
    )

    try:
        result = _send_payload(telegram_sender=telegram_sender, payload=payload)
    except Exception as exc:
        logger.error(
            "digest_failed",
            digest_type="weekly",
            account_email=account_email,
            week_key=week_key,
            error=str(exc),
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_failed",
                timestamp=now,
                email_id=0,
                payload={
                    "week_key": week_key,
                    "account_email": account_email,
                    "error": str(exc),
                },
            )
        return

    if result.delivered:
        storage.knowledge_db.set_last_weekly_digest_state(
            account_email=account_email,
            week_key=week_key,
            sent_at=now,
        )
        logger.info(
            "digest_sent",
            digest_type="weekly",
            account_email=account_email,
            week_key=week_key,
        )
        if storage.event_emitter:
            storage.event_emitter.emit(
                type="weekly_digest_sent",
                timestamp=now,
                email_id=0,
                payload={
                    "week_key": week_key,
                    "account_email": account_email,
                    "total_emails": data.total_emails,
                    "deferred_emails": data.deferred_emails,
                },
            )
        return

    logger.error(
        "digest_failed",
        digest_type="weekly",
        account_email=account_email,
        week_key=week_key,
        error=result.error or "telegram delivery failed",
        retryable=result.retryable,
    )
    if storage.event_emitter:
        storage.event_emitter.emit(
            type="weekly_digest_failed",
            timestamp=now,
            email_id=0,
            payload={
                "week_key": week_key,
                "account_email": account_email,
                "error": result.error or "telegram delivery failed",
            },
        )


__all__ = ["DigestStorage", "DailyDigestConfig", "WeeklyDigestConfig", "run_digest_tick"]
