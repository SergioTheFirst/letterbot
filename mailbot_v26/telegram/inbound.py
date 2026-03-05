from __future__ import annotations

import importlib.util
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.config_loader import SupportSettings, load_support_settings
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.feedback import record_priority_confirmation, record_priority_correction
from mailbot_v26.insights.auto_priority_quality_gate import AutoPriorityQualityGate
from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.calibration_report import compute_priority_calibration_report
from mailbot_v26.observability.decision_trace_store import load_latest_decision_traces
from mailbot_v26.observability.decision_trace_view import build_decision_trace_summary
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.observability.notification_sla import compute_notification_sla
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.pipeline import tg_renderer
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.version import get_version
from mailbot_v26.system_health import system_health
from mailbot_v26.telegram.decision_trace_ui import (
    DETAILS_PREFIX,
    HIDE_PREFIX,
    PRIO_BACK_PREFIX,
    PRIO_MENU_PREFIX,
    PRIO_SET_PREFIX,
    PRIO_OK_PREFIX,
    SNOOZE_BACK_PREFIX,
    SNOOZE_MENU_PREFIX,
    SNOOZE_SET_PREFIX,
    build_email_actions_keyboard,
)
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe
from mailbot_v26.ui.i18n import humanize_mode, t
from mailbot_v26.worker.telegram_sender import (
    DeliveryResult,
    edit_telegram_message,
    send_telegram,
)
from mailbot_v26.features.flags import FeatureFlags

logger = get_logger("mailbot")

_CALLBACK_PREFIXES = ("mb:prio:", "prio:")
_TOGGLE_PREFIXES = ("mb:toggle:", "toggle:")
_HELP_PREFIXES = ("mb:help:", "help:")
_UI_LOCALE = "ru"

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


def _t(key: str, **kwargs: object) -> str:
    return t(key, locale=_UI_LOCALE, **kwargs)


def _format_ts(value: datetime | None) -> str:
    if value is None:
        return _t("inbound.status.never_sent")
    return value.astimezone(timezone.utc).strftime("%d.%m %H:%M")


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _safe_chat_id(chat_id: object) -> str:
    return str(chat_id or "").strip()


def _is_trace_expanded(message: dict[str, object] | None) -> bool:
    if not message or not isinstance(message, dict):
        return False
    text = _clean_text(message.get("text"))
    return "DecisionTraceV1" in text


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




def _top_priority_transitions(*, db_path: Path, days: int, limit: int = 3) -> list[tuple[str, int]]:
    since_ts = datetime.now(timezone.utc).timestamp() - (max(1, int(days)) * 86400)
    totals: dict[str, int] = {}
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT payload
                FROM events_v1
                WHERE event_type = 'priority_correction_recorded'
                  AND ts_utc >= ?
                ORDER BY ts_utc DESC
                LIMIT 2000
                """,
                (since_ts,),
            ).fetchall()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("telegram_stats_transitions_failed", error=str(exc))
        return []

    for row in rows:
        payload_raw = row[0] if isinstance(row, tuple) else None
        payload: dict[str, object]
        try:
            payload = json.loads(str(payload_raw or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        old_priority = _PRIORITY_MAP.get(str(payload.get("old_priority") or "").strip().upper(), "")
        new_priority = _PRIORITY_MAP.get(str(payload.get("new_priority") or "").strip().upper(), "")
        if not old_priority or not new_priority:
            continue
        key = f"{old_priority}→{new_priority}"
        totals[key] = totals.get(key, 0) + 1

    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return ordered[: max(0, int(limit))]

def _load_email_render_snapshot(db_path: Path, email_id: int) -> dict[str, object] | None:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            email_row = conn.execute(
                """
                SELECT account_email, from_email, subject, priority, action_line, body_summary
                FROM emails
                WHERE id = ?
                """,
                (email_id,),
            ).fetchone()
            if not email_row:
                return None
            attachments = conn.execute(
                """
                SELECT filename
                FROM attachments
                WHERE email_id = ?
                ORDER BY id ASC
                """,
                (email_id,),
            ).fetchall()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("telegram_inbound_email_render_failed", error=str(exc))
        return None
    attachment_rows = [
        {"filename": row[0] or "вложение", "text": "", "content_type": ""}
        for row in attachments
        if row and row[0]
    ]
    snapshot = dict(email_row)
    snapshot["attachments"] = attachment_rows
    return snapshot


def _ensure_snooze_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_snooze (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_id INTEGER NOT NULL,
                deliver_at_utc TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                reminder_text TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                delivered_at TEXT,
                UNIQUE(email_id, deliver_at_utc)
            );
            """
        )


def _save_snooze(
    *,
    db_path: Path,
    email_id: int,
    deliver_at_utc: datetime,
    reminder_text: str,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    _ensure_snooze_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO telegram_snooze (
                email_id,
                deliver_at_utc,
                status,
                reminder_text,
                attempts,
                last_error,
                created_at,
                updated_at,
                delivered_at
            ) VALUES (?, ?, 'pending', ?, 0, NULL, ?, ?, NULL)
            ON CONFLICT(email_id, deliver_at_utc) DO UPDATE SET
                status = 'pending',
                reminder_text = excluded.reminder_text,
                attempts = 0,
                last_error = NULL,
                updated_at = excluded.updated_at,
                delivered_at = NULL
            """,
            (email_id, deliver_at_utc.isoformat(), reminder_text.strip(), now, now),
        )
        conn.commit()


def _render_tier1_message(snapshot: dict[str, object]) -> str:
    priority = str(snapshot.get("priority") or "🔵")
    from_email = str(snapshot.get("from_email") or "неизвестно")
    subject = str(snapshot.get("subject") or "(без темы)")
    action_line = str(snapshot.get("action_line") or "")
    body_summary = str(snapshot.get("body_summary") or "")
    attachments = snapshot.get("attachments") or []
    base_text = tg_renderer.render_telegram_message(
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=action_line,
        summary=body_summary,
        attachments=attachments if isinstance(attachments, list) else [],
    )
    return telegram_safe(base_text)


def _render_decision_trace_details(snapshot: list[dict[str, object]]) -> str:
    if not snapshot:
        return "Нет данных explainability для этого решения (trace not available)"
    lines: list[str] = ["DecisionTraceV1"]
    for entry in snapshot:
        decision_kind = str(entry.get("decision_kind") or "")
        decision_label = str(entry.get("decision_label") or "")
        evidence = entry.get("evidence") if isinstance(entry.get("evidence"), dict) else {}
        matched = int(evidence.get("matched") or 0)
        total = int(evidence.get("total") or 0)
        codes = entry.get("explain_codes") or []
        counterfactuals = entry.get("counterfactuals") or []
        lines.append(f"{decision_kind}: {decision_label}")
        lines.append(f"Сигналы: {matched}/{total}")
        if codes:
            codes_line = ", ".join(str(code) for code in codes if code)
            lines.append(f"Коды: {codes_line}")
        if counterfactuals:
            lines.append("Контрфакты:")
            for item in counterfactuals:
                signal = str(item.get("signal") or "")
                decision = str(item.get("decision") or "")
                if signal and decision:
                    lines.append(f"- Без {signal} → {decision}")
        lines.append("")
    return "\n".join(line for line in lines if line.strip())


def parse_callback_data(data: str) -> tuple[str, dict[str, str]] | None:
    raw = _clean_text(data)
    if raw.startswith(DETAILS_PREFIX):
        email_id = raw[len(DETAILS_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "details", {"email_id": email_id}
    if raw.startswith(HIDE_PREFIX):
        email_id = raw[len(HIDE_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "hide", {"email_id": email_id}
    if raw.startswith(PRIO_MENU_PREFIX):
        email_id = raw[len(PRIO_MENU_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "prio_menu", {"email_id": email_id}

    if raw.startswith(PRIO_BACK_PREFIX):
        email_id = raw[len(PRIO_BACK_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "prio_back", {"email_id": email_id}

    if raw.startswith(PRIO_SET_PREFIX):
        remainder = raw[len(PRIO_SET_PREFIX) :]
        parts = remainder.split(":")
        if len(parts) != 2:
            return None
        email_id, priority_raw = parts
        email_id = email_id.strip()
        priority = _PRIORITY_MAP.get(priority_raw.strip().upper())
        if not email_id or not priority:
            return None
        return "prio_set", {"email_id": email_id, "priority": priority}

    if raw.startswith(SNOOZE_MENU_PREFIX):
        email_id = raw[len(SNOOZE_MENU_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "snooze_menu", {"email_id": email_id}

    if raw.startswith(SNOOZE_BACK_PREFIX):
        email_id = raw[len(SNOOZE_BACK_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "snooze_back", {"email_id": email_id}

    if raw.startswith(SNOOZE_SET_PREFIX):
        remainder = raw[len(SNOOZE_SET_PREFIX) :]
        parts = remainder.split(":")
        if len(parts) != 2:
            return None
        email_id = parts[0].strip()
        snooze_code = parts[1].strip().lower()
        if not email_id.isdigit() or snooze_code not in {"2h", "6h", "tom"}:
            return None
        return "snooze_set", {"email_id": email_id, "snooze": snooze_code}

    if raw.startswith(PRIO_OK_PREFIX):
        email_id = raw[len(PRIO_OK_PREFIX) :].strip()
        if not email_id.isdigit():
            return None
        return "priority_ok", {"email_id": email_id}

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

    for prefix in _HELP_PREFIXES:
        if raw.startswith(prefix):
            remainder = raw[len(prefix) :].strip().lower()
            if remainder in {"priority", "prio"}:
                return "help", {"topic": "priority"}

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
    support_settings: SupportSettings | None = None
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
    bot_token: str
    show_decision_trace: bool = False
    support_settings: SupportSettings | None = None

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
        callback_id = _clean_text(callback.get("id"))
        message_id: int | None = None
        from_user_id = ""
        if isinstance(message, dict):
            chat = message.get("chat")
            if isinstance(chat, dict):
                chat_id = _safe_chat_id(chat.get("id"))
            try:
                message_id = int(message.get("message_id"))
            except (TypeError, ValueError):
                message_id = None
        from_user = callback.get("from")
        if isinstance(from_user, dict):
            from_user_id = _clean_text(str(from_user.get("id") or ""))
        if not chat_id or not self._is_allowed_chat(chat_id):
            logger.warning("telegram_inbound_unauthorized_callback", chat_id=chat_id)
            return

        parsed = parse_callback_data(data)
        if not parsed:
            if data.startswith(("mb:prio:", "prio_set:", "mb:setprio:")):
                logger.warning(
                    "tg_priority_callback_missing_email_id",
                    callback_data=data,
                    chat_id=chat_id,
                    message_id=message_id,
                    from_user_id=from_user_id,
                )
                self._answer_callback(callback_id, "Не нашёл письмо для изменения")
                return
            self._reply(chat_id, _t("inbound.bad_button"))
            self._answer_callback(callback_id, _t("inbound.bad_button"))
            return

        action, payload = parsed
        if action == "priority":
            logger.info(
                "tg_priority_callback_received",
                callback_data=data,
                chat_id=chat_id,
                message_id=message_id,
                from_user_id=from_user_id,
            )
            if isinstance(message, dict) and message.get("message_id") is not None:
                self._apply_priority_edit(chat_id, message, payload)
            else:
                self._apply_priority(chat_id, payload, send_ack=False)
            self._answer_callback(callback_id, "Приоритет обновлён")
        elif action == "toggle":
            self._apply_toggle(chat_id, payload)
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action == "help":
            self._reply(chat_id, self._priority_help_text())
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action in {"details", "hide"}:
            self._toggle_decision_trace(chat_id, message, payload, expanded=action == "details")
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action == "prio_menu":
            self._open_priority_menu(chat_id, message, payload)
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action == "prio_back":
            self._close_priority_menu(chat_id, message, payload)
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action == "prio_set":
            logger.info(
                "tg_priority_callback_received",
                callback_data=data,
                chat_id=chat_id,
                message_id=message_id,
                from_user_id=from_user_id,
            )
            self._apply_priority_edit(chat_id, message, payload)
            self._answer_callback(callback_id, "Приоритет обновлён")
        elif action == "snooze_menu":
            self._open_snooze_menu(chat_id, message, payload)
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action == "snooze_back":
            self._close_snooze_menu(chat_id, message, payload)
            self._answer_callback(callback_id, _t("inbound.ok"))
        elif action == "snooze_set":
            ack = self._set_snooze(chat_id, message, payload)
            self._answer_callback(callback_id, ack or _t("inbound.bad_button"))
        elif action == "priority_ok":
            if self._record_priority_confirmation(payload):
                self._answer_callback(callback_id, "✓ Учёл")
            else:
                self._answer_callback(callback_id, "Не удалось учесть")

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
            self._reply(chat_id, _t("inbound.command_unknown"))
            return

        if command in {"/help", "help"}:
            self._reply(chat_id, self._help_text())
            return
        if command in {"/status", "status"}:
            self._reply(chat_id, self._status_text(chat_id=chat_id))
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
        if command in {"/commitments", "/tasks", "commitments", "tasks"}:
            self._handle_commitments(chat_id)
            return
        if command in {"/week", "week"}:
            self._reply(chat_id, self._week_text())
            return
        if command in {"/stats", "stats"}:
            self._reply(chat_id, self._stats_text())
            return
        if command in {"/support", "support"}:
            self._reply(chat_id, self._support_text())
            return

        self._reply(chat_id, _t("inbound.command_unknown"))

    def _reply(self, chat_id: str, text: str) -> None:
        try:
            self.send_reply(chat_id, text)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_reply_failed", error=str(exc))

    def _answer_callback(self, callback_id: str, text: str) -> None:
        if not callback_id:
            return
        try:
            from mailbot_v26.worker.telegram_sender import requests as tg_requests
        except Exception:
            tg_requests = None
        if tg_requests is None:
            return
        url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
        payload = {
            "callback_query_id": callback_id,
            "text": text[:200],
            "show_alert": False,
        }
        try:
            tg_requests.post(url, json=payload, timeout=5)
        except Exception:
            return

    def _is_allowed_chat(self, chat_id: str) -> bool:
        return chat_id in self.allowed_chat_ids

    def _record_priority_confirmation(self, payload: dict[str, str]) -> bool:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            return False
        snapshot = _load_email_snapshot(self.knowledge_db.path, int(email_id_raw))
        if not snapshot:
            return False
        priority = str(snapshot.get("priority") or "").strip()
        if not priority:
            return False
        account_email = str(snapshot.get("account_email") or "")
        sender_email = str(snapshot.get("from_email") or "")
        try:
            record_priority_confirmation(
                knowledge_db=self.knowledge_db,
                email_id=int(email_id_raw),
                priority=priority,
                entity_id=None,
                sender_email=sender_email or None,
                account_email=account_email or None,
                system_mode=system_health.mode,
                event_emitter=self.event_emitter,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_priority_ok_failed", error=str(exc))
            return False
        return True

    def _apply_priority(self, chat_id: str, payload: dict[str, str], *, send_ack: bool = True) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
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
            surprise_mode=self.feature_flags.ENABLE_SURPRISE_BUDGET,
        )
        if send_ack:
            priority = payload.get("priority") or "🔵"
            self._reply(
                chat_id,
                _t("inbound.priority_ack", priority=priority),
            )

    def _apply_priority_edit(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            logger.warning("tg_priority_callback_missing_email_id", callback_data=payload)
            return
        if not message or not isinstance(message, dict):
            return
        message_id = message.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return
        account_email = str(snapshot.get("account_email") or "")
        sender_email = str(snapshot.get("from_email") or "")
        old_priority = str(snapshot.get("priority") or "")
        new_priority = payload.get("priority") or "🔵"
        record_priority_correction(
            knowledge_db=self.knowledge_db,
            email_id=email_id,
            correction=new_priority,
            entity_id=None,
            sender_email=sender_email or None,
            account_email=account_email or None,
            system_mode=system_health.mode,
            event_emitter=self.event_emitter,
            contract_event_emitter=self.contract_event_emitter,
            old_priority=old_priority or None,
            engine="priority_v2_auto",
            source="telegram_inbound",
            surprise_mode=self.feature_flags.ENABLE_SURPRISE_BUDGET,
        )
        snapshot["priority"] = new_priority
        tier1_text = _render_tier1_message(snapshot)
        expanded = _is_trace_expanded(message)
        if expanded:
            try:
                traces = load_latest_decision_traces(
                    db_path=self.knowledge_db.path, email_id=email_id, limit=10
                )
                summaries = [build_decision_trace_summary(trace) for trace in traces]
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("telegram_inbound_trace_failed", error=str(exc))
                summaries = []
            trace_payload = [
                {
                    "decision_kind": summary.decision_kind,
                    "decision_label": summary.decision_label,
                    "evidence": summary.evidence,
                    "explain_codes": summary.explain_codes,
                    "counterfactuals": [
                        {"signal": item.signal, "decision": item.decision}
                        for item in summary.counterfactuals
                    ],
                }
                for summary in summaries
            ]
            details_text = _render_decision_trace_details(trace_payload)
            full_text = f"{tier1_text}\n\n{details_text}"
        else:
            full_text = tier1_text
        reply_markup = build_email_actions_keyboard(
            email_id=email_id,
            expanded=expanded,
            prio_menu=False,
            initial_prio=False,
            show_decision_trace=self.show_decision_trace,
        )
        try:
            edit_telegram_message(
                bot_token=self.bot_token,
                chat_id=chat_id,
                message_id=message_id_int,
                html_text=full_text,
                reply_markup=reply_markup,
            )
            logger.info(
                "tg_priority_edit_ok",
                email_id=email_id,
                chat_id=chat_id,
                message_id=message_id_int,
                priority=new_priority,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("tg_priority_edit_failed", error=str(exc))

    def _open_priority_menu(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
            return
        if not message or not isinstance(message, dict):
            return
        message_id = message.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return
        expanded = _is_trace_expanded(message)
        tier1_text = _render_tier1_message(snapshot)
        if expanded:
            try:
                traces = load_latest_decision_traces(
                    db_path=self.knowledge_db.path, email_id=email_id, limit=10
                )
                summaries = [build_decision_trace_summary(trace) for trace in traces]
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("telegram_inbound_trace_failed", error=str(exc))
                summaries = []
            trace_payload = [
                {
                    "decision_kind": summary.decision_kind,
                    "decision_label": summary.decision_label,
                    "evidence": summary.evidence,
                    "explain_codes": summary.explain_codes,
                    "counterfactuals": [
                        {"signal": item.signal, "decision": item.decision}
                        for item in summary.counterfactuals
                    ],
                }
                for summary in summaries
            ]
            details_text = _render_decision_trace_details(trace_payload)
            full_text = f"{tier1_text}\n\n{details_text}"
        else:
            full_text = tier1_text
        reply_markup = build_email_actions_keyboard(
            email_id=email_id,
            expanded=expanded,
            prio_menu=True,
            show_decision_trace=self.show_decision_trace,
        )
        try:
            edit_telegram_message(
                bot_token=self.bot_token,
                chat_id=chat_id,
                message_id=message_id_int,
                html_text=full_text,
                reply_markup=reply_markup,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_edit_failed", error=str(exc))

    def _close_priority_menu(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
            return
        if not message or not isinstance(message, dict):
            return
        message_id = message.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return
        expanded = _is_trace_expanded(message)
        tier1_text = _render_tier1_message(snapshot)
        if expanded:
            try:
                traces = load_latest_decision_traces(
                    db_path=self.knowledge_db.path, email_id=email_id, limit=10
                )
                summaries = [build_decision_trace_summary(trace) for trace in traces]
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("telegram_inbound_trace_failed", error=str(exc))
                summaries = []
            trace_payload = [
                {
                    "decision_kind": summary.decision_kind,
                    "decision_label": summary.decision_label,
                    "evidence": summary.evidence,
                    "explain_codes": summary.explain_codes,
                    "counterfactuals": [
                        {"signal": item.signal, "decision": item.decision}
                        for item in summary.counterfactuals
                    ],
                }
                for summary in summaries
            ]
            details_text = _render_decision_trace_details(trace_payload)
            full_text = f"{tier1_text}\n\n{details_text}"
        else:
            full_text = tier1_text
        reply_markup = build_email_actions_keyboard(
            email_id=email_id,
            expanded=expanded,
            prio_menu=False,
            show_decision_trace=self.show_decision_trace,
        )
        try:
            edit_telegram_message(
                bot_token=self.bot_token,
                chat_id=chat_id,
                message_id=message_id_int,
                html_text=full_text,
                reply_markup=reply_markup,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_edit_failed", error=str(exc))

    def _open_snooze_menu(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> None:
        self._render_with_keyboard(chat_id, message, payload, snooze_menu=True)

    def _close_snooze_menu(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> None:
        self._render_with_keyboard(chat_id, message, payload, snooze_menu=False)

    def _set_snooze(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> str | None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
            return None
        snooze_code = str(payload.get("snooze") or "").strip().lower()
        now_local = datetime.now().astimezone()
        if snooze_code == "2h":
            deliver_local = now_local + timedelta(hours=2)
            ack = "⏰ Напомню в 2 часа"
        elif snooze_code == "6h":
            deliver_local = now_local + timedelta(hours=6)
            ack = "⏰ Напомню в 6 часов"
        elif snooze_code == "tom":
            tomorrow = now_local.date() + timedelta(days=1)
            deliver_local = datetime.combine(tomorrow, datetime.min.time(), tzinfo=now_local.tzinfo).replace(
                hour=9,
                minute=0,
                second=0,
                microsecond=0,
            )
            ack = "⏰ Напомню завтра в 09:00"
        else:
            return None

        email_id = int(email_id_raw)
        reminder_text = ""
        if message and isinstance(message, dict):
            reminder_text = _clean_text(message.get("text"))
        if not reminder_text:
            snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
            if snapshot:
                reminder_text = _render_tier1_message(snapshot)
        if not reminder_text:
            return None
        _save_snooze(
            db_path=self.knowledge_db.path,
            email_id=email_id,
            deliver_at_utc=deliver_local.astimezone(timezone.utc),
            reminder_text=reminder_text,
        )
        self._render_with_keyboard(chat_id, message, payload, snooze_menu=False)
        return ack

    def _render_with_keyboard(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
        *,
        snooze_menu: bool,
    ) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
            return
        if not message or not isinstance(message, dict):
            return
        message_id = message.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return
        expanded = _is_trace_expanded(message)
        tier1_text = _render_tier1_message(snapshot)
        full_text = tier1_text
        if expanded:
            try:
                traces = load_latest_decision_traces(
                    db_path=self.knowledge_db.path, email_id=email_id, limit=10
                )
                summaries = [build_decision_trace_summary(trace) for trace in traces]
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("telegram_inbound_trace_failed", error=str(exc))
                summaries = []
            trace_payload = [
                {
                    "decision_kind": summary.decision_kind,
                    "decision_label": summary.decision_label,
                    "evidence": summary.evidence,
                    "explain_codes": summary.explain_codes,
                    "counterfactuals": [
                        {"signal": item.signal, "decision": item.decision}
                        for item in summary.counterfactuals
                    ],
                }
                for summary in summaries
            ]
            details_text = _render_decision_trace_details(trace_payload)
            full_text = f"{tier1_text}\n\n{details_text}"

        reply_markup = build_email_actions_keyboard(
            email_id=email_id,
            expanded=expanded,
            snooze_menu=snooze_menu,
            show_decision_trace=self.show_decision_trace,
        )
        try:
            edit_telegram_message(
                bot_token=self.bot_token,
                chat_id=chat_id,
                message_id=message_id_int,
                html_text=full_text,
                reply_markup=reply_markup,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_edit_failed", error=str(exc))

    def _toggle_decision_trace(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
        *,
        expanded: bool,
    ) -> None:
        email_id_raw = payload.get("email_id") or ""
        if not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
            return
        if not message or not isinstance(message, dict):
            return
        message_id = message.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return
        tier1_text = _render_tier1_message(snapshot)
        if expanded:
            try:
                traces = load_latest_decision_traces(
                    db_path=self.knowledge_db.path, email_id=email_id, limit=10
                )
                summaries = [build_decision_trace_summary(trace) for trace in traces]
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("telegram_inbound_trace_failed", error=str(exc))
                summaries = []
            trace_payload = [
                {
                    "decision_kind": summary.decision_kind,
                    "decision_label": summary.decision_label,
                    "evidence": summary.evidence,
                    "explain_codes": summary.explain_codes,
                    "counterfactuals": [
                        {"signal": item.signal, "decision": item.decision}
                        for item in summary.counterfactuals
                    ],
                }
                for summary in summaries
            ]
            details_text = _render_decision_trace_details(trace_payload)
            full_text = f"{tier1_text}\n\n{details_text}"
        else:
            full_text = tier1_text
        reply_markup = build_email_actions_keyboard(
            email_id=email_id,
            expanded=expanded,
            prio_menu=False,
            show_decision_trace=self.show_decision_trace,
        )
        try:
            edit_telegram_message(
                bot_token=self.bot_token,
                chat_id=chat_id,
                message_id=message_id_int,
                html_text=full_text,
                reply_markup=reply_markup,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_edit_failed", error=str(exc))

    def _apply_toggle(self, chat_id: str, payload: dict[str, str]) -> None:
        flag = payload.get("flag")
        value = payload.get("value")
        if flag == "digest":
            enabled = value == "on"
            self.override_store.set_digest_enabled(enabled)
            message_key = (
                "inbound.digest_enabled" if enabled else "inbound.digest_disabled"
            )
            self._reply(chat_id, _t(message_key))
            return
        if flag == "autopriority":
            self._handle_auto_priority_toggle(chat_id, [value or ""])
            return
        self._reply(chat_id, _t("inbound.toggle_unknown"))

    def _handle_digest_toggle(self, chat_id: str, args: list[str]) -> None:
        if not args or args[0] not in {"on", "off"}:
            self._reply(chat_id, _t("inbound.digest_usage"))
            return
        enabled = args[0] == "on"
        self.override_store.set_digest_enabled(enabled)
        message_key = "inbound.digest_enabled" if enabled else "inbound.digest_disabled"
        self._reply(chat_id, _t(message_key))

    def _handle_auto_priority_toggle(self, chat_id: str, args: list[str]) -> None:
        if not args or args[0] not in {"on", "off"}:
            self._reply(chat_id, _t("inbound.autopriority_usage"))
            return
        if args[0] == "off":
            self.runtime_flag_store.set_enable_auto_priority(False)
            self._reply(chat_id, _t("inbound.autopriority_off"))
            return
        if not self.auto_priority_gate_config.enabled:
            self.runtime_flag_store.set_enable_auto_priority(True)
            self._reply(chat_id, _t("inbound.autopriority_on"))
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
                "cooldown_active": _t("inbound.autopriority_reason.cooldown"),
                "insufficient_samples": _t("inbound.autopriority_reason.samples"),
                "correction_rate_spike": _t("inbound.autopriority_reason.corrections"),
                "analytics_failed": _t("inbound.autopriority_reason.analytics"),
            }
            reason = reason_map.get(
                gate_result.reason,
                _t("inbound.autopriority_reason.samples"),
            )
            self._reply(
                chat_id,
                _t("inbound.autopriority_gate_blocked", reason=reason),
            )
            return
        self.runtime_flag_store.set_enable_auto_priority(True)
        self._reply(chat_id, _t("inbound.autopriority_on"))

    def _help_text(self) -> str:
        return "\n".join(
            [
                _t("inbound.help.title"),
                _t("inbound.help.status"),
                _t("inbound.help.doctor"),
                _t("inbound.help.digest"),
                _t("inbound.help.autopriority"),
                _t("inbound.help.commitments"),
                _t("inbound.help.week"),
                _t("inbound.help.stats"),
                _t("inbound.help.support"),
                _t("inbound.help.help"),
            ]
        )

    def _handle_commitments(self, chat_id: str) -> None:
        account_emails = list(self._account_emails())
        if not account_emails:
            self._reply(chat_id, "✅ Нет открытых обязательств")
            return
        placeholders = ",".join("?" for _ in account_emails)
        try:
            with sqlite3.connect(self.knowledge_db.path) as conn:
                rows = conn.execute(
                    f"""
                    SELECT c.commitment_text, c.deadline_iso
                    FROM commitments c
                    JOIN emails e ON e.id = c.email_row_id
                    WHERE lower(c.status) IN ('pending', 'unknown')
                      AND lower(e.account_email) IN ({placeholders})
                    ORDER BY
                      CASE WHEN c.deadline_iso IS NULL OR c.deadline_iso = '' THEN 1 ELSE 0 END ASC,
                      c.deadline_iso ASC,
                      c.id DESC
                    LIMIT 100
                    """,
                    tuple(email.casefold() for email in account_emails),
                ).fetchall()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_commitments_failed", error=str(exc))
            rows = []
        if not rows:
            self._reply(chat_id, "✅ Нет открытых обязательств")
            return

        deduped: list[str] = []
        seen: set[tuple[str, str]] = set()
        for row in rows:
            text = _clean_text(str(row[0] if len(row) > 0 else ""))
            if not text:
                continue
            deadline_raw = _clean_text(str(row[1] if len(row) > 1 else ""))
            deadline = deadline_raw[:10] if len(deadline_raw) >= 10 else ""
            key = (text.casefold(), deadline)
            if key in seen:
                continue
            seen.add(key)
            line = f"• {escape_tg_html(text)}"
            if deadline:
                line = f"{line} · до {escape_tg_html(deadline)}"
            deduped.append(line)
            if len(deduped) >= 7:
                break

        if not deduped:
            self._reply(chat_id, "✅ Нет открытых обязательств")
            return
        self._reply(chat_id, "\n".join(["📋 <b>Обязательства:</b>", *deduped]))

    def _week_text(self) -> str:
        account_emails = list(self._account_emails())
        if not account_emails:
            base = (
                "📊 Letterbot — неделя\n"
                "Писем: 0 · Важных: 0 · Низких: 0\n"
                "Коррекций: 0 · Точность: н/д\n"
                "Обязательств открыто: 0"
            )
            return "\n".join([base, self._stats_text(days=7, include_header=False)])
        primary = account_emails[0]
        summary = self.analytics.weekly_compact_summary(
            account_email=primary,
            account_emails=account_emails,
            days=7,
        )
        accuracy_pct = summary.get("accuracy_pct")
        accuracy_text = f"{int(accuracy_pct)}%" if accuracy_pct is not None else "н/д"
        base = (
            "📊 Letterbot — неделя\n"
            f"Писем: {int(summary.get('emails_total') or 0)} · Важных: {int(summary.get('important') or 0)} · Низких: {int(summary.get('low') or 0)}\n"
            f"Коррекций: {int(summary.get('corrections') or 0)} · Точность: {accuracy_text}\n"
            f"Обязательств открыто: {int(summary.get('open_commitments') or 0)}"
        )
        return "\n".join([base, self._stats_text(days=7, include_header=False)])

    def _stats_text(self, *, days: int = 7, include_header: bool = True) -> str:
        account_emails = list(self._account_emails())
        if not account_emails:
            header = "📈 Качество автоприоритизации" if include_header else ""
            trust = "Пока данных мало — делаем выводы вручную."
            lines = [line for line in [header, "Коррекций: 0", "Surprise rate: н/д", "Переходы: нет данных", trust] if line]
            return "\n".join(lines)

        primary = account_emails[0]
        accuracy: dict[str, object] = {}
        calibration: dict[str, object] | None = None
        calibration_totals: dict[str, object] = {}

        try:
            accuracy = self.analytics.weekly_accuracy_report(
                account_email=primary,
                days=days,
                account_emails=account_emails,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_stats_accuracy_failed", error=str(exc))

        try:
            anchor = datetime.now(timezone.utc)
            calibration = self.analytics.weekly_calibration_proposals(
                account_email=primary,
                since_ts=anchor.timestamp() - (days * 86400),
                top_n=3,
                min_corrections=0,
                account_emails=account_emails,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_stats_calibration_failed", error=str(exc))

        try:
            calibration_report = compute_priority_calibration_report(
                db_path=self.knowledge_db.path,
                days=days,
                max_rows=1000,
            )
            totals = calibration_report.get("totals")
            if isinstance(totals, dict):
                calibration_totals = totals
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_stats_calibration_report_failed", error=str(exc))

        corrections = int(accuracy.get("priority_corrections") or 0)
        surprise_rate = accuracy.get("surprise_rate")
        surprise_text = "н/д"
        if surprise_rate is not None:
            surprise_text = f"{float(surprise_rate) * 100:.0f}%"

        transitions_text = "нет данных"
        transitions = _top_priority_transitions(db_path=self.knowledge_db.path, days=days, limit=3)
        if transitions:
            transitions_text = ", ".join(f"{transition} ×{count}" for transition, count in transitions)

        latency_text = "н/д"
        decisions_total = int(calibration_totals.get("decisions_total") or 0)
        corrected_total = int(calibration_totals.get("decisions_corrected") or 0)
        if decisions_total > 0:
            latency_text = f"{corrected_total}/{decisions_total} решений пересмотрены"

        trust_line = "Можно доверять автоприоритизации: да"
        if corrections < 3:
            trust_line = "Можно доверять автоприоритизации: осторожно, мало данных"
        elif surprise_rate is not None and float(surprise_rate) > 0.35:
            trust_line = "Можно доверять автоприоритизации: пока осторожно"

        lines = []
        if include_header:
            lines.append("📈 Качество автоприоритизации")
        lines.extend(
            [
                f"Коррекций: {corrections}",
                f"Surprise rate: {surprise_text}",
                f"Скорость коррекций: {latency_text}",
                f"Переходы: {transitions_text}",
                trust_line,
            ]
        )
        return "\n".join(lines)

    def _priority_help_text(self) -> str:
        return _t("inbound.priority_help")

    def _support_text(self) -> str:
        support = self.support_settings or load_support_settings()
        if not support.enabled:
            return "Поддержка проекта сейчас не настроена."
        if not support.url or support.url == "CHANGE_ME":
            return "Поддержка включена, но ссылка ещё не настроена."
        return "\n".join([
            support.label or "Поддержать Letterbot",
            support.text,
            support.url,
        ])

    def _status_text(self, *, chat_id: str) -> str:
        mode_label = humanize_mode(system_health.mode.value, locale="ru")
        sla = compute_notification_sla(analytics=self.analytics)
        digest_override = self.override_store.get_overrides().digest_enabled

        accounts = []
        for account_email in self._account_emails():
            daily = self.knowledge_db.get_last_digest_sent_at(account_email=account_email)
            weekly = self.knowledge_db.get_last_weekly_digest_sent_at(account_email=account_email)
            accounts.append(
                _t(
                    "inbound.status.digest_line",
                    account_email=account_email,
                    daily=_format_ts(daily),
                    weekly=_format_ts(weekly),
                )
            )
        accounts_block = "\n".join(accounts) if accounts else _t("inbound.status.no_data")

        runtime_flags, _ = self.runtime_flag_store.get_flags(force=True)
        auto_mode = (
            _t("inbound.status.auto_mode")
            if self.feature_flags.ENABLE_AUTO_PRIORITY and runtime_flags.enable_auto_priority
            else _t("inbound.status.shadow_mode")
        )

        digest_flag = (
            _t("inbound.status.enabled")
            if digest_override is True
            else _t("inbound.status.disabled")
            if digest_override is False
            else None
        )
        if digest_flag is None:
            digest_flag = (
                _t("inbound.status.enabled")
                if self.feature_flags.ENABLE_DAILY_DIGEST
                else _t("inbound.status.disabled")
            )
        flags_line = _t(
            "inbound.status.flags",
            preview=_t("inbound.status.short_on")
            if self.feature_flags.ENABLE_PREVIEW_ACTIONS
            else _t("inbound.status.short_off"),
            anomalies=_t("inbound.status.short_on")
            if self.feature_flags.ENABLE_ANOMALY_ALERTS
            else _t("inbound.status.short_off"),
            quality=_t("inbound.status.short_on")
            if self.feature_flags.ENABLE_QUALITY_METRICS
            else _t("inbound.status.short_off"),
        )

        status_lines = [
            _t("inbound.status.title"),
            _t("inbound.status.mode", mode=mode_label),
            _t("inbound.status.sla", delivery=_format_percent(sla.delivery_rate_24h), errors=_format_percent(sla.error_rate_24h)),
            _t("inbound.status.digest", digest=digest_flag),
            _t("inbound.status.autopriority", mode=auto_mode),
            flags_line,
            _t("inbound.status.last_digests"),
            accounts_block,
        ]
        insider_since = self.override_store.get_insider_since(chat_id=chat_id)
        if insider_since:
            status_lines.append(f"⭐ Letterbot Insider since: {insider_since}")
        status_lines.append(f"Version: {get_version()}")
        return "\n".join(status_lines)

    def _doctor_text(self) -> str:
        try:
            from mailbot_v26 import doctor
        except Exception:  # pragma: no cover - defensive
            return _t("inbound.doctor.unavailable")
        try:
            entries = doctor.run_doctor_checks()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("telegram_inbound_doctor_failed", error=str(exc))
            return _t("inbound.doctor.failed")

        failures = [entry for entry in entries if entry.status != "OK"]
        if not failures:
            return _t("inbound.doctor.ok")
        lines = [_t("inbound.doctor.warn_title")]
        status_map = {
            "OK": _t("inbound.doctor.status_ok"),
            "WARN": _t("inbound.doctor.status_warn"),
            "FAIL": _t("inbound.doctor.status_fail"),
        }
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
