from __future__ import annotations

import importlib.util
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.feedback import record_priority_correction
from mailbot_v26.insights.auto_priority_quality_gate import AutoPriorityQualityGate
from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.observability.notification_sla import compute_notification_sla
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.system_health import system_health
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.ui.i18n import humanize_mode
from mailbot_v26.worker.telegram_sender import DeliveryResult, send_telegram
from mailbot_v26.features.flags import FeatureFlags

logger = get_logger("mailbot")

_CALLBACK_PREFIXES = ("mb:prio:", "prio:")
_TOGGLE_PREFIXES = ("mb:toggle:", "toggle:")

_PRIORITY_MAP = {
    "R": "🔴",
    "Y": "🟡",
    "B": "🔵",
    "RED": "🔴",
    "YELLOW": "🟡",
    "BLUE": "🔵",
    "🔴": "🔴",
    "🟡": "🟡",
    "🔵": "🔵",
}


def _clean_text(text: str | None) -> str:
    return str(text or "").strip()


def _format_ts(value: datetime | None) -> str:
    if value is None:
        return "не отправлялся"
    return value.astimezone(timezone.utc).strftime("%d.%m %H:%M")


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _safe_chat_id(chat_id: object) -> str:
    return str(chat_id or "").strip()


def _load_email_snapshot(db_path: Path, email_id: int) -> dict[str, object] | None:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT account_email, from_email, priority
                FROM emails
                WHERE id = ?
                """,
                (email_id,),
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("telegram_inbound_email_lookup_failed", error=str(exc))
        return None
    if not row:
        return None
    return dict(row)


def parse_callback_data(data: str) -> tuple[str, dict[str, str]] | None:
    raw = _clean_text(data)
    for prefix in _CALLBACK_PREFIXES:
        if raw.startswith(prefix):
            remainder = raw[len(prefix) :]
            parts = remainder.split(":")
            if len(parts) != 2:
                return None
            email_id, priority_raw = parts
            email_id = email_id.strip()
            priority = _PRIORITY_MAP.get(priority_raw.strip().upper())
            if not email_id or not priority:
                return None
            return "priority", {"email_id": email_id, "priority": priority}

    for prefix in _TOGGLE_PREFIXES:
        if raw.startswith(prefix):
            remainder = raw[len(prefix) :]
            parts = remainder.split(":")
            if len(parts) != 2:
                return None
            flag = parts[0].strip().lower()
            value = parts[1].strip().lower()
            if flag not in {"digest", "autopriority"}:
                return None
            if value not in {"on", "off"}:
                return None
            return "toggle", {"flag": flag, "value": value}

    return None


def parse_command(text: str) -> tuple[str, list[str]]:
    cleaned = _clean_text(text)
    if not cleaned:
        return "", []
    parts = cleaned.split()
    command = parts[0].strip().lower()
    args = [part.strip().lower() for part in parts[1:]]
    return command, args


@dataclass(frozen=True, slots=True)
class TelegramInboundClient:
    bot_token: str
    timeout_s: int = 5
    _requests: object | None = None

    def __post_init__(self) -> None:
        if self._requests is not None:
            return
        spec = importlib.util.find_spec("requests")
        if spec is None:
            object.__setattr__(self, "_requests", None)
        else:
            import requests

            object.__setattr__(self, "_requests", requests)

    def get_updates(self, *, offset: int | None, limit: int = 20) -> list[dict[str, object]]:
        if self._requests is None:
            logger.error("telegram_inbound_requests_missing")
            return []
        params: dict[str, object] = {
            "timeout": self.timeout_s,
            "limit": limit,
            "allowed_updates": ["message", "callback_query"],
        }
        if offset is not None:
            params["offset"] = offset
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        try:
            response = self._requests.get(url, params=params, timeout=self.timeout_s + 5)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.error("telegram_inbound_poll_failed", error=str(exc))
            return []
        if not payload or not payload.get("ok"):
            logger.error("telegram_inbound_poll_error", payload=str(payload))
            return []
        result = payload.get("result", [])
        if isinstance(result, list):
            return [item for item in result if isinstance(item, dict)]
        return []

    def send_message(self, *, chat_id: str, text: str) -> DeliveryResult:
        payload = TelegramPayload(
            html_text=telegram_safe(text),
            priority="🔵",
            metadata={"bot_token": self.bot_token, "chat_id": chat_id},
        )
        return send_telegram(payload)


@dataclass(slots=True)
class InboundStateStore:
    db_path: Path
    state_key: str = "global"

    def __post_init__(self) -> None:
        self.db_path = Path(self.db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS telegram_inbound_state (
                        state_key TEXT PRIMARY KEY,
                        last_update_id INTEGER,
                        updated_at TEXT
                    );
                    """
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_state_init_failed", error=str(exc))

    def get_last_update_id(self) -> int | None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    """
                    SELECT last_update_id
                    FROM telegram_inbound_state
                    WHERE state_key = ?
                    """,
                    (self.state_key,),
                ).fetchone()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_state_read_failed", error=str(exc))
            return None
        if not row or row[0] is None:
            return None
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None

    def set_last_update_id(self, update_id: int) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO telegram_inbound_state (state_key, last_update_id, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(state_key) DO UPDATE SET
                        last_update_id = excluded.last_update_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        self.state_key,
                        int(update_id),
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                conn.commit()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_state_write_failed", error=str(exc))


@dataclass(slots=True)
class TelegramInboundProcessor:
    knowledge_db: KnowledgeDB
    analytics: KnowledgeAnalytics
    event_emitter: EventEmitter
    contract_event_emitter: ContractEventEmitter
    runtime_flag_store: RuntimeFlagStore
    auto_priority_gate: AutoPriorityQualityGate
    auto_priority_gate_config: AutoPriorityGateConfig
    override_store: RuntimeOverrideStore
    send_reply: Callable[[str, str], DeliveryResult | None]
    feature_flags: FeatureFlags
    allowed_chat_ids: frozenset[str]

    def handle_update(self, update: dict[str, object]) -> None:
        if "callback_query" in update:
            callback = update.get("callback_query")
            if isinstance(callback, dict):
                self.handle_callback_query(callback)
            return
        if "message" in update:
            message = update.get("message")
            if isinstance(message, dict):
                self.handle_message(message)
            return

    def handle_callback_query(self, callback: dict[str, object]) -> None:
        data = _clean_text(callback.get("data"))
        message = callback.get("message")
        chat_id = ""
        if isinstance(message, dict):
            chat = message.get("chat")
            if isinstance(chat, dict):
                chat_id = _safe_chat_id(chat.get("id"))
        if not chat_id or not self._is_allowed_chat(chat_id):
            logger.warning("telegram_inbound_unauthorized_callback", chat_id=chat_id)
            return

        parsed = parse_callback_data(data)
        if not parsed:
            self._reply(chat_id, "Некорректная кнопка. Напишите /help.")
            return

        action, payload = parsed
        if action == "priority":
            self._apply_priority(chat_id, payload)
        elif action == "toggle":
            self._apply_toggle(chat_id, payload)

    def handle_message(self, message: dict[str, object]) -> None:
        chat_id = ""
        chat = message.get("chat")
        if isinstance(chat, dict):
            chat_id = _safe_chat_id(chat.get("id"))
        if not chat_id or not self._is_allowed_chat(chat_id):
            logger.warning("telegram_inbound_unauthorized_message", chat_id=chat_id)
            return

        text = _clean_text(message.get("text"))
        command, args = parse_command(text)

        if not command:
            self._reply(chat_id, "Не понял команду. Напишите /help.")
            return

        if command in {"/help", "help"}:
            self._reply(chat_id, self._help_text())
            return
        if command in {"/status", "status"}:
            self._reply(chat_id, self._status_text())
            return
        if command in {"/doctor", "doctor"}:
            self._reply(chat_id, self._doctor_text())
            return
        if command in {"/digest", "digest"}:
            self._handle_digest_toggle(chat_id, args)
            return
        if command in {"/autopriority", "autopriority"}:
            self._handle_auto_priority_toggle(chat_id, args)
            return

        self._reply(chat_id, "Не понял команду. Напишите /help.")

    def _reply(self, chat_id: str, text: str) -> None:
        try:
            self.send_reply(chat_id, text)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_reply_failed", error=str(exc))

    def _is_allowed_chat(self, chat_id: str) -> bool:
        return chat_id in self.allowed_chat_ids

    def _apply_priority(self, chat_id: str, payload: dict[str, str]) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, "Некорректный идентификатор письма.")
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_snapshot(self.knowledge_db.path, email_id)
        account_email = str(snapshot.get("account_email") or "") if snapshot else ""
        sender_email = str(snapshot.get("from_email") or "") if snapshot else ""
        old_priority = str(snapshot.get("priority") or "") if snapshot else ""
        record_priority_correction(
            knowledge_db=self.knowledge_db,
            email_id=email_id,
            correction=payload.get("priority") or "🔵",
            entity_id=None,
            sender_email=sender_email or None,
            account_email=account_email or None,
            system_mode=system_health.mode,
            event_emitter=self.event_emitter,
            contract_event_emitter=self.contract_event_emitter,
            old_priority=old_priority or None,
            engine="priority_v2_auto",
            source="telegram_inbound",
        )
        self._reply(chat_id, "Приоритет обновлён. Спасибо за обратную связь.")

    def _apply_toggle(self, chat_id: str, payload: dict[str, str]) -> None:
        flag = payload.get("flag")
        value = payload.get("value")
        if flag == "digest":
            enabled = value == "on"
            self.override_store.set_digest_enabled(enabled)
            status = "включены" if enabled else "выключены"
            self._reply(chat_id, f"Дайджесты {status}.")
            return
        if flag == "autopriority":
            self._handle_auto_priority_toggle(chat_id, [value or ""])
            return
        self._reply(chat_id, "Неизвестная настройка.")

    def _handle_digest_toggle(self, chat_id: str, args: list[str]) -> None:
        if not args or args[0] not in {"on", "off"}:
            self._reply(chat_id, "Использование: /digest on|off")
            return
        enabled = args[0] == "on"
        self.override_store.set_digest_enabled(enabled)
        status = "включены" if enabled else "выключены"
        self._reply(chat_id, f"Дайджесты {status}.")

    def _handle_auto_priority_toggle(self, chat_id: str, args: list[str]) -> None:
        if not args or args[0] not in {"on", "off"}:
            self._reply(chat_id, "Использование: /autopriority on|off")
            return
        if args[0] == "off":
            self.runtime_flag_store.set_enable_auto_priority(False)
            self._reply(chat_id, "Автоприоритет выключен. Режим: теневой.")
            return
        if not self.auto_priority_gate_config.enabled:
            self.runtime_flag_store.set_enable_auto_priority(True)
            self._reply(chat_id, "Автоприоритет включён.")
            return
        gate_result = self.auto_priority_gate.evaluate(
            engine="priority_v2_auto",
            window_days=self.auto_priority_gate_config.window_days,
            min_samples=self.auto_priority_gate_config.min_samples,
            max_correction_rate=self.auto_priority_gate_config.max_correction_rate,
            cooldown_hours=self.auto_priority_gate_config.cooldown_hours,
        )
        if not gate_result.passed:
            reason_map = {
                "cooldown_active": "пауза после отключения",
                "insufficient_samples": "недостаточно данных",
                "correction_rate_spike": "слишком много исправлений",
                "analytics_failed": "ошибка аналитики",
            }
            reason = reason_map.get(gate_result.reason, "недостаточно данных")
            self._reply(
                chat_id,
                f"Пока нельзя: качество недостаточно ({reason}).",
            )
            return
        self.runtime_flag_store.set_enable_auto_priority(True)
        self._reply(chat_id, "Автоприоритет включён.")

    def _help_text(self) -> str:
        return (
            "Команды:\n"
            "/status — краткий статус системы\n"
            "/doctor — диагностика (кратко)\n"
            "/digest on|off — включить или выключить дайджесты\n"
            "/autopriority on|off — включить или выключить автоприоритет\n"
            "/help — эта справка"
        )

    def _status_text(self) -> str:
        mode_label = humanize_mode(system_health.mode.value, locale="ru")
        sla = compute_notification_sla(analytics=self.analytics)
        digest_override = self.override_store.get_overrides().digest_enabled

        accounts = []
        for account_email in self._account_emails():
            daily = self.knowledge_db.get_last_digest_sent_at(account_email=account_email)
            weekly = self.knowledge_db.get_last_weekly_digest_sent_at(account_email=account_email)
            accounts.append(
                f"{account_email}: день { _format_ts(daily) }, неделя { _format_ts(weekly) }"
            )
        accounts_block = "\n".join(accounts) if accounts else "нет данных"

        runtime_flags, _ = self.runtime_flag_store.get_flags(force=True)
        auto_mode = (
            "авто"
            if self.feature_flags.ENABLE_AUTO_PRIORITY and runtime_flags.enable_auto_priority
            else "теневой"
        )

        digest_flag = (
            "включены" if digest_override is True else "выключены" if digest_override is False else None
        )
        if digest_flag is None:
            digest_flag = "включены" if self.feature_flags.ENABLE_DAILY_DIGEST else "выключены"
        flags_line = (
            "Флаги: "
            f"превью={ 'вкл' if self.feature_flags.ENABLE_PREVIEW_ACTIONS else 'выкл' }, "
            f"аномалии={ 'вкл' if self.feature_flags.ENABLE_ANOMALY_ALERTS else 'выкл' }, "
            f"качество={ 'вкл' if self.feature_flags.ENABLE_QUALITY_METRICS else 'выкл' }"
        )

        return (
            "Статус системы\n"
            f"Режим: {mode_label}\n"
            "Оперативность уведомлений (24ч): "
            f"доставка {_format_percent(sla.delivery_rate_24h)}, "
            f"ошибки {_format_percent(sla.error_rate_24h)}\n"
            f"Дайджесты: {digest_flag}\n"
            f"Автоприоритет: {auto_mode}\n"
            f"{flags_line}\n"
            "Последние отправки дайджестов:\n"
            f"{accounts_block}"
        )

    def _doctor_text(self) -> str:
        try:
            from mailbot_v26 import doctor
        except Exception:  # pragma: no cover - defensive
            return "Доктор недоступен."
        try:
            entries = doctor.run_doctor_checks()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_doctor_failed", error=str(exc))
            return "Доктор завершился с ошибкой."

        failures = [entry for entry in entries if entry.status != "OK"]
        if not failures:
            return "Доктор: все проверки ОК."
        lines = ["Доктор: есть предупреждения."]
        status_map = {"OK": "ОК", "WARN": "ПРЕДУПРЕЖДЕНИЕ", "FAIL": "ОШИБКА"}
        for entry in failures:
            detail = f" ({entry.details})" if entry.details else ""
            status = status_map.get(entry.status, entry.status)
            lines.append(f"- {entry.component}: {status}{detail}")
        return "\n".join(lines)

    def _account_emails(self) -> Iterable[str]:
        try:
            with sqlite3.connect(self.knowledge_db.path) as conn:
                rows = conn.execute(
                    "SELECT DISTINCT account_email FROM emails ORDER BY account_email"
                ).fetchall()
        except Exception:
            return []
        return [str(row[0]) for row in rows if row and row[0]]


def run_inbound_polling(
    *,
    client: TelegramInboundClient,
    processor: TelegramInboundProcessor,
    state_store: InboundStateStore,
    max_updates: int = 20,
) -> None:
    last_update_id = state_store.get_last_update_id()
    offset = last_update_id + 1 if last_update_id is not None else None
    logger.info("telegram_inbound_polled", offset=offset)
    updates = client.get_updates(offset=offset, limit=max_updates)
    if not updates:
        return
    max_seen = last_update_id or 0
    for update in updates:
        update_id = update.get("update_id")
        try:
            processor.handle_update(update)
            logger.info(
                "telegram_inbound_processed",
                update_id=update_id,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "telegram_inbound_failed",
                update_id=update_id,
                error=str(exc),
            )
        try:
            if update_id is not None:
                update_int = int(update_id)
                max_seen = max(max_seen, update_int)
        except (TypeError, ValueError):
            continue

    if max_seen and max_seen != last_update_id:
        state_store.set_last_update_id(max_seen)
        logger.info("telegram_inbound_offset_updated", last_update_id=max_seen)


__all__ = [
    "InboundStateStore",
    "TelegramInboundClient",
    "TelegramInboundProcessor",
    "parse_callback_data",
    "parse_command",
    "run_inbound_polling",
]
