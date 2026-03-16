from __future__ import annotations

import importlib.util
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.config_loader import SupportSettings, load_support_settings
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.feedback import (
    record_action_feedback,
    record_priority_confirmation,
    record_priority_correction,
)
from mailbot_v26.insights.auto_priority_quality_gate import AutoPriorityQualityGate
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability import get_logger
from mailbot_v26.observability.calibration_report import (
    compute_priority_calibration_report,
)
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
from mailbot_v26.system_health import OperationalMode, system_health
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
from mailbot_v26.telegram.callback_data import (
    FEEDBACK_PREFIX,
    PRIORITY_PREFIX,
    decode as decode_callback_data_contract,
)
from mailbot_v26.telegram.keyboard_builder import build_notification_keyboard
from mailbot_v26.telegram_utils import escape_tg_html, telegram_safe
from mailbot_v26.ui.i18n import DEFAULT_LOCALE, get_locale, humanize_mode, t
from mailbot_v26.text.mojibake import normalize_mojibake_text
from mailbot_v26.config.llm_queue import load_llm_queue_config
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
_TELEGRAM_TOKEN_RE = re.compile(r"bot\d+:[A-Za-z0-9_-]+")
_SENSITIVE_KV_RE = re.compile(
    r"(?i)\b(token|password|secret|api[_-]?key)\b\s*[:=]\s*([^\s,;]+)"
)

_PRIORITY_MAP = {
    "R": "\U0001f534",
    "Y": "\U0001f7e1",
    "B": "\U0001f535",
    "RED": "\U0001f534",
    "YELLOW": "\U0001f7e1",
    "BLUE": "\U0001f535",
    "\U0001f534": "\U0001f534",
    "\U0001f7e1": "\U0001f7e1",
    "\U0001f535": "\U0001f535",
    "\u0440\u045f\u201d\u0491": "\U0001f534",
    "\u0440\u045f\u201f\u0160": "\U0001f7e1",
    "\u0440\u045f\u201d\u00b5": "\U0001f535",
    "\u0441\u0452\u0441\u045f\u0432\u0402\u045c\u0422\u2018": "\U0001f534",
    "\u0441\u0452\u0441\u045f\u0421\u045f\u045f\u0420\u040b": "\U0001f7e1",
    "\u0441\u0452\u0441\u045f\u0432\u0402\u045c\u0412\u00b5": "\U0001f535",
}
_INLINE_PRIORITY_MAP = {
    "hi": "\U0001f534",
    "med": "\U0001f7e1",
    "lo": "\U0001f535",
}
_FEEDBACK_DECISION_MAP = {
    "paid": "paid",
    "not_invoice": "not_invoice",
    "not_payroll": "not_payroll",
    "not_contract": "not_contract",
    "correct": "accepted",
}
_FEEDBACK_ACK_MAP = {
    "paid": "Отмечено: оплачено",
    "not_invoice": "Отмечено: не счёт",
    "not_payroll": "Отмечено: неверная классификация",
    "not_contract": "Отмечено: не договор",
    "correct": "Принято",
    "snooze": "Выберите время",
}


def _clean_text(text: str | None) -> str:
    return str(text or "").strip()


def _safe_log_text(value: object, *, limit: int = 160) -> str:
    text = _clean_text(str(value or ""))
    if not text:
        return ""
    text = _TELEGRAM_TOKEN_RE.sub("bot<redacted>", text)
    text = _SENSITIVE_KV_RE.sub(r"\1=<redacted>", text)
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def _summarize_poll_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {
            "payload_kind": type(payload).__name__,
            "payload_summary": _safe_log_text(payload),
        }
    summary: dict[str, object] = {
        "payload_ok": bool(payload.get("ok")),
        "payload_keys": sorted(str(key) for key in payload.keys())[:8],
    }
    if "error_code" in payload:
        try:
            summary["error_code"] = int(payload["error_code"])
        except (TypeError, ValueError):
            summary["error_code"] = _safe_log_text(payload.get("error_code"))
    if "description" in payload:
        summary["description"] = _safe_log_text(payload.get("description"))
    result = payload.get("result")
    if isinstance(result, list):
        summary["result_count"] = len(result)
    return summary


def _t(key: str, **kwargs: object) -> str:
    return normalize_mojibake_text(t(key, locale=_UI_LOCALE, **kwargs))


def set_inbound_locale(locale: str) -> None:
    """Set the module-level UI locale for inbound Telegram text."""
    global _UI_LOCALE
    _UI_LOCALE = str(locale).strip() or "ru"


def _normalize_priority_token(value: object) -> str:
    token = normalize_mojibake_text(_clean_text(str(value or "")))
    if not token:
        return ""
    direct = _PRIORITY_MAP.get(token)
    if direct:
        return direct
    return _PRIORITY_MAP.get(token.upper(), "")


def _format_ts(value: datetime | None) -> str:
    if value is None:
        return _t("inbound.status.never_sent")
    return value.astimezone(timezone.utc).strftime("%d.%m %H:%M")


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _status_llm_delivery_mode() -> str:
    if system_health.mode in {
        OperationalMode.DEGRADED_NO_LLM,
        OperationalMode.EMERGENCY_READ_ONLY,
    }:
        return "heuristic"
    try:
        queue = load_llm_queue_config()
    except Exception:
        return "direct"
    if queue.llm_request_queue_enabled and queue.max_concurrent_llm_calls == 1:
        return "queued"
    return "direct"


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
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(emails)").fetchall()
            }
            if not columns:
                return None
            select_fields = ["account_email", "from_email"]
            if "priority" in columns:
                select_fields.append("priority")
            row = conn.execute(
                f"""
                SELECT {", ".join(select_fields)}
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


def _top_priority_transitions(
    *, db_path: Path, days: int, limit: int = 3
) -> list[tuple[str, int]]:
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
        old_priority = _normalize_priority_token(payload.get("old_priority"))
        new_priority = _normalize_priority_token(payload.get("new_priority"))
        if not old_priority or not new_priority:
            continue
        key = f"{old_priority}\u2192{new_priority}"
        totals[key] = totals.get(key, 0) + 1

    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return ordered[: max(0, int(limit))]


def _load_email_render_snapshot(
    db_path: Path, email_id: int
) -> dict[str, object] | None:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(emails)").fetchall()
            }
            if not columns:
                return None
            select_fields = [
                field
                for field in (
                    "account_email",
                    "from_email",
                    "subject",
                    "priority",
                    "priority_source",
                    "action_line",
                    "body_summary",
                )
                if field in columns
            ]
            if not select_fields:
                return None
            email_row = conn.execute(
                f"""
                SELECT {", ".join(select_fields)}
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
    snapshot.setdefault("priority", "")
    snapshot.setdefault("priority_source", "")
    snapshot.setdefault("action_line", "")
    snapshot.setdefault("body_summary", "")
    snapshot["attachments"] = attachment_rows
    return snapshot


def _load_message_interpretation_snapshot(
    db_path: Path, email_id: int
) -> dict[str, object] | None:
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT payload
                FROM events_v1
                WHERE event_type = 'message_interpretation'
                  AND email_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (email_id,),
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("telegram_inbound_interpretation_lookup_failed", error=str(exc))
        return None
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(str(row[0]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _build_default_reply_markup(
    *,
    db_path: Path,
    email_id: int,
    priority: str,
    show_decision_trace: bool,
    expanded: bool,
) -> dict[str, object] | None:
    interpretation = _load_message_interpretation_snapshot(db_path, email_id)
    doc_kind = ""
    if interpretation:
        doc_kind = str(interpretation.get("doc_kind") or "").strip().lower()
    if not doc_kind:
        return build_email_actions_keyboard(
            email_id=email_id,
            expanded=expanded,
            show_decision_trace=show_decision_trace,
        )
    return build_notification_keyboard(
        render_mode="full",
        doc_kind=doc_kind,
        priority=priority,
        message_key=email_id,
        show_decision_trace=show_decision_trace,
        decision_trace_expanded=expanded,
    )


def _action_feedback_exists(
    db_path: Path,
    *,
    email_id: int,
    decision: str,
) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM action_feedback
                WHERE email_id = ?
                  AND decision = ?
                LIMIT 1
                """,
                (str(email_id), decision),
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("telegram_inbound_action_feedback_lookup_failed", error=str(exc))
        return False
    return row is not None


def _update_email_snapshot_priority(
    db_path: Path, email_id: int, new_priority: str
) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(emails)").fetchall()
            }
            migrations: list[str] = []
            if "priority" not in columns:
                migrations.append("ALTER TABLE emails ADD COLUMN priority TEXT;")
            if "priority_source" not in columns:
                migrations.append(
                    "ALTER TABLE emails ADD COLUMN priority_source TEXT DEFAULT 'auto';"
                )
            for statement in migrations:
                conn.execute(statement)
            if migrations:
                conn.commit()
                columns = {
                    str(row[1])
                    for row in conn.execute("PRAGMA table_info(emails)").fetchall()
                }
            if "priority" not in columns:
                logger.error(
                    "telegram_inbound_priority_snapshot_update_failed",
                    error="emails.priority column unavailable",
                )
                return False
            set_clauses = ["priority = ?"]
            params: list[object] = [new_priority]
            if "priority_source" in columns:
                set_clauses.append("priority_source = 'user_override'")
            params.append(email_id)
            cursor = conn.execute(
                f"""
                UPDATE emails
                SET {", ".join(set_clauses)}
                WHERE id = ?
                """,
                tuple(params),
            )
            conn.commit()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("telegram_inbound_priority_snapshot_update_failed", error=str(exc))
        return False
    return int(cursor.rowcount or 0) > 0


def _ensure_snooze_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_snooze (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_id INTEGER NOT NULL,
                deliver_at_utc TEXT NOT NULL,
                snoozed_at_utc TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                reminder_text TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                delivered_at TEXT,
                UNIQUE(email_id, deliver_at_utc)
            );
            """)
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(telegram_snooze)").fetchall()
        }
        if "snoozed_at_utc" not in columns:
            conn.execute("ALTER TABLE telegram_snooze ADD COLUMN snoozed_at_utc TEXT;")
        conn.commit()


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
                snoozed_at_utc,
                status,
                reminder_text,
                attempts,
                last_error,
                created_at,
                updated_at,
                delivered_at
            ) VALUES (?, ?, ?, 'pending', ?, 0, NULL, ?, ?, NULL)
            ON CONFLICT(email_id, deliver_at_utc) DO UPDATE SET
                status = 'pending',
                snoozed_at_utc = excluded.snoozed_at_utc,
                reminder_text = excluded.reminder_text,
                attempts = 0,
                last_error = NULL,
                updated_at = excluded.updated_at,
                delivered_at = NULL
            """,
            (
                email_id,
                deliver_at_utc.isoformat(),
                now,
                reminder_text.strip(),
                now,
                now,
            ),
        )
        conn.commit()


def _render_tier1_message(snapshot: dict[str, object]) -> str:
    priority = _normalize_priority_token(snapshot.get("priority")) or "\U0001f535"
    from_email = normalize_mojibake_text(
        str(
            snapshot.get("from_email")
            or "\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e"
        )
    )
    subject = normalize_mojibake_text(
        str(snapshot.get("subject") or "(\u0431\u0435\u0437 \u0442\u0435\u043c\u044b)")
    )
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
    base_text = tg_renderer.finalize_telegram_message(
        text=base_text,
        priority=priority,
        account_email=str(snapshot.get("account_email") or ""),
    )
    priority_source = str(snapshot.get("priority_source") or "").strip().lower()
    if priority_source == "user_override":
        base_text = f"{base_text}\n\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442: {priority} \u0432\u0440\u0443\u0447\u043d\u0443\u044e"
    return telegram_safe(normalize_mojibake_text(base_text))


def _render_decision_trace_details(snapshot: list[dict[str, object]]) -> str:
    if not snapshot:
        return "Нет данных explainability для этого решения (trace not available)"
    lines: list[str] = ["DecisionTraceV1"]
    for entry in snapshot:
        decision_kind = str(entry.get("decision_kind") or "")
        decision_label = str(entry.get("decision_label") or "")
        evidence = (
            entry.get("evidence") if isinstance(entry.get("evidence"), dict) else {}
        )
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
    if not raw:
        return None
    try:
        callback = decode_callback_data_contract(raw)
    except ValueError:
        callback = None
    if callback is not None:
        if callback.prefix == FEEDBACK_PREFIX:
            return "feedback", {
                "email_id": callback.msg_key,
                "feedback_action": callback.action,
            }
        if callback.prefix == PRIORITY_PREFIX:
            return "priority_inline", {
                "email_id": callback.msg_key,
                "priority_action": callback.action,
            }
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
        priority = _normalize_priority_token(priority_raw)
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
            priority = _normalize_priority_token(priority_raw)
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

    def get_updates(
        self, *, offset: int | None, limit: int = 20
    ) -> list[dict[str, object]]:
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
            response = self._requests.get(
                url, params=params, timeout=self.timeout_s + 5
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.error(
                "telegram_inbound_poll_failed",
                error_type=type(exc).__name__,
                error=_safe_log_text(exc),
            )
            return []
        if not payload or not payload.get("ok"):
            logger.error("telegram_inbound_poll_error", **_summarize_poll_payload(payload))
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
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS telegram_inbound_state (
                        state_key TEXT PRIMARY KEY,
                        last_update_id INTEGER,
                        updated_at TEXT
                    );
                    """)
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
    locale: str = "ru"
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
        set_inbound_locale(self.locale)
        data = _clean_text(callback.get("data"))
        message = callback.get("message")
        chat_id = ""
        callback_id = _clean_text(callback.get("id"))
        ack_text = _t("inbound.ok")
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
            self._answer_callback(callback_id, _t("inbound.bad_button"))
            return
        try:
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
                    ack_text = "Не нашёл письмо для изменения"
                    return
                logger.warning(
                    "telegram_inbound_callback_invalid",
                    callback_data=_safe_log_text(data),
                    chat_id=chat_id,
                    message_id=message_id,
                    from_user_id=from_user_id,
                )
                ack_text = _t("inbound.bad_button")
                return

            action, payload = parsed
            if action in {"priority", "prio_set"}:
                logger.info(
                    "tg_priority_callback_received",
                    callback_data=data,
                    chat_id=chat_id,
                    message_id=message_id,
                    from_user_id=from_user_id,
                )
                ack_text = self._apply_priority_edit(chat_id, message, payload)
                return
            if action == "priority_inline":
                logger.info(
                    "tg_priority_inline_callback_received",
                    callback_data=data,
                    chat_id=chat_id,
                    message_id=message_id,
                    from_user_id=from_user_id,
                )
                priority = _INLINE_PRIORITY_MAP.get(
                    str(payload.get("priority_action") or "").strip().lower()
                )
                if not priority:
                    ack_text = _t("inbound.bad_button")
                    return
                ack_text = self._apply_priority_edit(
                    chat_id,
                    message,
                    {
                        "email_id": str(payload.get("email_id") or ""),
                        "priority": priority,
                    },
                )
                return
            if action == "feedback":
                ack_text = self._handle_feedback_callback(chat_id, message, payload)
                return
            if action == "toggle":
                self._apply_toggle(chat_id, payload)
                ack_text = _t("inbound.ok")
                return
            if action == "help":
                self._reply(chat_id, self._priority_help_text())
                ack_text = _t("inbound.ok")
                return
            if action in {"details", "hide"}:
                self._toggle_decision_trace(
                    chat_id, message, payload, expanded=action == "details"
                )
                ack_text = _t("inbound.ok")
                return
            if action == "prio_menu":
                self._open_priority_menu(chat_id, message, payload)
                ack_text = _t("inbound.ok")
                return
            if action == "prio_back":
                self._close_priority_menu(chat_id, message, payload)
                ack_text = _t("inbound.ok")
                return
            if action == "snooze_menu":
                self._open_snooze_menu(chat_id, message, payload)
                ack_text = _t("inbound.ok")
                return
            if action == "snooze_back":
                self._close_snooze_menu(chat_id, message, payload)
                ack_text = _t("inbound.ok")
                return
            if action == "snooze_set":
                ack = self._set_snooze(chat_id, message, payload)
                ack_text = ack or _t("inbound.bad_button")
                return
            if action == "priority_ok":
                if self._record_priority_confirmation(payload):
                    ack_text = _t("inbound.ok")
                else:
                    ack_text = "Не нашёл письмо для изменения"
                return
            ack_text = _t("inbound.bad_button")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "telegram_inbound_callback_failed",
                callback_data=_safe_log_text(data),
                error=str(exc),
            )
            ack_text = _t("inbound.bad_button")
        finally:
            self._answer_callback(callback_id, ack_text)

    def handle_message(self, message: dict[str, object]) -> None:
        set_inbound_locale(self.locale)
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
            self._reply(chat_id, self._help_text())
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
        if command in {"/lang", "lang"}:
            self._handle_lang_command(message)
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

        self._reply(chat_id, self._help_text())

    def _reply(self, chat_id: str, text: str) -> None:
        safe_text = normalize_mojibake_text(text)
        try:
            self.send_reply(chat_id, safe_text)
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
            "text": normalize_mojibake_text(text)[:200],
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

    def _record_priority_feedback_signal(
        self,
        *,
        email_id: int,
        account_email: str,
        sender_email: str,
        old_priority: str,
        new_priority: str,
    ) -> str:
        normalized_old = _normalize_priority_token(old_priority)
        normalized_new = _normalize_priority_token(new_priority) or "🔵"
        if normalized_old and normalized_old == normalized_new:
            if self._record_priority_confirmation({"email_id": str(email_id)}):
                return "confirmation"
            return "missing"
        record_priority_correction(
            knowledge_db=self.knowledge_db,
            email_id=email_id,
            correction=normalized_new,
            entity_id=None,
            sender_email=sender_email or None,
            account_email=account_email or None,
            system_mode=system_health.mode,
            event_emitter=self.event_emitter,
            contract_event_emitter=self.contract_event_emitter,
            old_priority=normalized_old or None,
            engine="priority_v2_auto",
            source="telegram_inbound",
            surprise_mode=self.feature_flags.ENABLE_SURPRISE_BUDGET,
        )
        return "correction"

    def _handle_feedback_callback(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> str:
        email_id_raw = str(payload.get("email_id") or "").strip()
        if not email_id_raw.isdigit():
            return "Не нашёл письмо"
        email_id = int(email_id_raw)
        snapshot = _load_email_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return "Не нашёл письмо"
        action = str(payload.get("feedback_action") or "").strip().lower()
        if action == "snooze":
            self._open_snooze_menu(chat_id, message, {"email_id": email_id_raw})
            return _FEEDBACK_ACK_MAP["snooze"]
        decision = _FEEDBACK_DECISION_MAP.get(action)
        if not decision:
            return _t("inbound.bad_button")
        if _action_feedback_exists(
            self.knowledge_db.path,
            email_id=email_id,
            decision=decision,
        ):
            return "Уже отмечено"
        interpretation = _load_message_interpretation_snapshot(
            self.knowledge_db.path, email_id
        )
        proposed_action = {
            "type": "telegram_inline_feedback",
            "callback_action": action,
            "doc_kind": str(interpretation.get("doc_kind") or "").strip()
            if interpretation
            else "",
            "current_action": (
                str(interpretation.get("action") or "").strip() if interpretation else ""
            ),
            "source": "telegram_inline",
        }
        record_action_feedback(
            knowledge_db=self.knowledge_db,
            email_id=str(email_id),
            proposed_action=proposed_action,
            decision=decision,
            user_note="telegram_inline",
            system_mode=system_health.mode,
        )
        logger.info(
            "telegram_inline_feedback_recorded",
            email_id=email_id,
            chat_id=chat_id,
            feedback_action=action,
            decision=decision,
        )
        return _FEEDBACK_ACK_MAP.get(action, _t("inbound.ok"))

    def _apply_priority(
        self, chat_id: str, payload: dict[str, str], *, send_ack: bool = True
    ) -> None:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            self._reply(chat_id, _t("inbound.bad_email_id"))
            return
        email_id = int(email_id_raw)
        snapshot = _load_email_snapshot(self.knowledge_db.path, email_id)
        account_email = str(snapshot.get("account_email") or "") if snapshot else ""
        sender_email = str(snapshot.get("from_email") or "") if snapshot else ""
        old_priority = str(snapshot.get("priority") or "") if snapshot else ""
        self._record_priority_feedback_signal(
            email_id=email_id,
            account_email=account_email,
            sender_email=sender_email,
            old_priority=old_priority,
            new_priority=payload.get("priority") or "🔵",
        )
        if send_ack:
            priority = (
                _normalize_priority_token(payload.get("priority")) or "\U0001f535"
            )
            self._reply(
                chat_id,
                _t("inbound.priority_ack", priority=priority),
            )

    def _apply_priority_edit(
        self,
        chat_id: str,
        message: dict[str, object] | None,
        payload: dict[str, str],
    ) -> str:
        email_id_raw = payload.get("email_id")
        if not email_id_raw or not email_id_raw.isdigit():
            logger.warning(
                "tg_priority_callback_missing_email_id", callback_data=payload
            )
            return "\u041d\u0435 \u043d\u0430\u0448\u0451\u043b \u043f\u0438\u0441\u044c\u043c\u043e"
        if not message or not isinstance(message, dict):
            return "\u041d\u0435 \u043c\u043e\u0433\u0443 \u043e\u0442\u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c"
        message_id = message.get("message_id")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return "\u041d\u0435 \u043c\u043e\u0433\u0443 \u043e\u0442\u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c"
        email_id = int(email_id_raw)
        snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
        if not snapshot:
            return "\u041d\u0435 \u043d\u0430\u0448\u0451\u043b \u043f\u0438\u0441\u044c\u043c\u043e"
        account_email = str(snapshot.get("account_email") or "")
        sender_email = str(snapshot.get("from_email") or "")
        old_priority = _normalize_priority_token(snapshot.get("priority"))
        new_priority = (
            _normalize_priority_token(payload.get("priority")) or "\U0001f535"
        )
        feedback_kind = self._record_priority_feedback_signal(
            email_id=email_id,
            account_email=account_email,
            sender_email=sender_email,
            old_priority=old_priority or "",
            new_priority=new_priority,
        )
        if feedback_kind == "correction":
            logger.info(
                "tg_priority_feedback_saved",
                email_id=email_id,
                chat_id=chat_id,
                message_id=message_id_int,
                priority=new_priority,
            )
            if not _update_email_snapshot_priority(
                self.knowledge_db.path, email_id, new_priority
            ):
                logger.warning(
                    "tg_priority_snapshot_update_missing_email",
                    email_id=email_id,
                    chat_id=chat_id,
                    message_id=message_id_int,
                    priority=new_priority,
                )
                return "\u041d\u0435 \u043d\u0430\u0448\u0451\u043b \u043f\u0438\u0441\u044c\u043c\u043e"
            logger.info(
                "tg_priority_snapshot_updated",
                email_id=email_id,
                chat_id=chat_id,
                message_id=message_id_int,
                priority=new_priority,
            )
            snapshot = _load_email_render_snapshot(self.knowledge_db.path, email_id)
            if not snapshot:
                logger.warning(
                    "tg_priority_snapshot_reload_failed",
                    email_id=email_id,
                    chat_id=chat_id,
                    message_id=message_id_int,
                )
                return "\u041d\u0435 \u043d\u0430\u0448\u0451\u043b \u043f\u0438\u0441\u044c\u043c\u043e"
        else:
            logger.info(
                "tg_priority_confirmation_saved",
                email_id=email_id,
                chat_id=chat_id,
                message_id=message_id_int,
                priority=new_priority,
            )
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
        reply_markup = _build_default_reply_markup(
            db_path=self.knowledge_db.path,
            email_id=email_id,
            priority=new_priority,
            show_decision_trace=self.show_decision_trace,
            expanded=expanded,
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
            return "\u041d\u0435 \u043c\u043e\u0433\u0443 \u043e\u0442\u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c"
        return "\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442 \u043e\u0431\u043d\u043e\u0432\u043b\u0451\u043d"

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
        reply_markup = _build_default_reply_markup(
            db_path=self.knowledge_db.path,
            email_id=email_id,
            priority=str(snapshot.get("priority") or ""),
            show_decision_trace=self.show_decision_trace,
            expanded=expanded,
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
            deliver_local = datetime.combine(
                tomorrow, datetime.min.time(), tzinfo=now_local.tzinfo
            ).replace(
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
        snapshot = _load_email_snapshot(self.knowledge_db.path, email_id) or {}
        account_id = str(snapshot.get("account_email") or "").strip() or "unknown"
        snoozed_at_utc = datetime.now(timezone.utc)
        self.contract_event_emitter.emit(
            EventV1(
                event_type=EventType.SNOOZE_RECORDED,
                ts_utc=snoozed_at_utc.timestamp(),
                account_id=account_id,
                entity_id=None,
                email_id=email_id,
                payload={
                    "snooze_code": snooze_code,
                    "snoozed_at_utc": snoozed_at_utc.isoformat(),
                    "deliver_at_utc": deliver_local.astimezone(timezone.utc).isoformat(),
                },
            )
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

        if snooze_menu:
            reply_markup = build_email_actions_keyboard(
                email_id=email_id,
                expanded=expanded,
                snooze_menu=True,
                show_decision_trace=self.show_decision_trace,
            )
        else:
            reply_markup = _build_default_reply_markup(
                db_path=self.knowledge_db.path,
                email_id=email_id,
                priority=str(snapshot.get("priority") or ""),
                show_decision_trace=self.show_decision_trace,
                expanded=expanded,
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
        reply_markup = _build_default_reply_markup(
            db_path=self.knowledge_db.path,
            email_id=email_id,
            priority=str(snapshot.get("priority") or ""),
            show_decision_trace=self.show_decision_trace,
            expanded=expanded,
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
                "cooldown_active": "пауза после отключения",
                "insufficient_samples": "недостаточно данных",
                "correction_rate_spike": "слишком много исправлений",
                "analytics_failed": "ошибка аналитики",
            }
            reason = reason_map.get(gate_result.reason, "недостаточно данных")
            self._reply(chat_id, f"Пока нельзя: качество недостаточно ({reason}).")
            return
        self.runtime_flag_store.set_enable_auto_priority(True)
        self._reply(chat_id, _t("inbound.autopriority_on"))

    def _help_text(self) -> str:
        if _UI_LOCALE.startswith("en"):
            return "\n".join(
                [
                    _t("inbound.help.title"),
                    _t("inbound.help.status"),
                    _t("inbound.help.doctor"),
                    "/digest on — enable digests",
                    "/digest off — disable digests",
                    "/autopriority on — enable auto-priority",
                    "/autopriority off — disable auto-priority",
                    _t("inbound.help.commitments"),
                    _t("inbound.help.week"),
                    _t("inbound.help.stats"),
                    _t("inbound.help.support"),
                    _t("inbound.help.lang"),
                    _t("inbound.help.help"),
                ]
            )
        return "\n".join(
            [
                "\u041a\u043e\u043c\u0430\u043d\u0434\u044b:",
                "/status \u2014 \u043a\u0440\u0430\u0442\u043a\u0438\u0439 \u0441\u0442\u0430\u0442\u0443\u0441 \u0441\u0438\u0441\u0442\u0435\u043c\u044b",
                "/doctor \u2014 \u0434\u0438\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430 (\u043a\u0440\u0430\u0442\u043a\u043e)",
                "/digest on \u2014 \u0432\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0434\u0430\u0439\u0434\u0436\u0435\u0441\u0442\u044b",
                "/digest off \u2014 \u0432\u044b\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0434\u0430\u0439\u0434\u0436\u0435\u0441\u0442\u044b",
                "/autopriority on \u2014 \u0432\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442",
                "/autopriority off \u2014 \u0432\u044b\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442",
                "/commitments (/tasks) \u2014 \u043e\u0442\u043a\u0440\u044b\u0442\u044b\u0435 \u043e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432\u0430",
                "/week \u2014 \u043a\u0440\u0430\u0442\u043a\u0430\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u0437\u0430 7 \u0434\u043d\u0435\u0439",
                "/stats \u2014 \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u043e \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0438\u0437\u0430\u0446\u0438\u0438",
                "/support \u2014 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u0430\u0442\u044c \u043f\u0440\u043e\u0435\u043a\u0442",
                _t("inbound.help.lang"),
                "/help \u2014 \u044d\u0442\u0430 \u0441\u043f\u0440\u0430\u0432\u043a\u0430",
            ]
        )

    def _handle_lang_command(self, message: dict) -> None:
        chat = message.get("chat", {})
        if not isinstance(chat, dict):
            chat = {}
        chat_id = _clean_text(chat.get("id"))
        text = _clean_text(message.get("text", ""))
        parts = text.strip().split()
        if len(parts) < 2 or parts[1].lower() not in ("en", "ru"):
            self.send_reply(chat_id, "Usage: /lang en  or  /lang ru")
            return
        new_locale = parts[1].lower()
        self.override_store.set_value("ui_locale", new_locale)
        set_inbound_locale(new_locale)
        if new_locale == "en":
            self.send_reply(chat_id, "\u2713 Language set to English.")
        else:
            self.send_reply(chat_id, "\u2713 \u042f\u0437\u044b\u043a \u0438\u0437\u043c\u0435\u043d\u0451\u043d \u043d\u0430 \u0440\u0443\u0441\u0441\u043a\u0438\u0439.")

    def _handle_commitments(self, chat_id: str) -> None:
        account_emails = list(self._account_emails())
        if not account_emails:
            self._reply(
                chat_id,
                "\u2705 \u041d\u0435\u0442 \u043e\u0442\u043a\u0440\u044b\u0442\u044b\u0445 \u043e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432",
            )
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
            self._reply(
                chat_id,
                "\u2705 \u041d\u0435\u0442 \u043e\u0442\u043a\u0440\u044b\u0442\u044b\u0445 \u043e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432",
            )
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
            line = f"\u2022 {escape_tg_html(text)}"
            if deadline:
                line = f"{line} \u00b7 \u0434\u043e {escape_tg_html(deadline)}"
            deduped.append(line)
            if len(deduped) >= 7:
                break

        if not deduped:
            self._reply(
                chat_id,
                "\u2705 \u041d\u0435\u0442 \u043e\u0442\u043a\u0440\u044b\u0442\u044b\u0445 \u043e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432",
            )
            return
        self._reply(
            chat_id,
            "\n".join(
                [
                    "\U0001f4cb <b>\u041e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432\u0430:</b>",
                    *deduped,
                ]
            ),
        )

    def _week_text(self) -> str:
        account_emails = list(self._account_emails())
        if not account_emails:
            base = (
                "\U0001f4ca LetterBot.ru \u2014 \u043d\u0435\u0434\u0435\u043b\u044f\n"
                "\u041f\u0438\u0441\u0435\u043c: 0 \u00b7 \u0412\u0430\u0436\u043d\u044b\u0445: 0 \u00b7 \u041d\u0438\u0437\u043a\u0438\u0445: 0\n"
                "\u041a\u043e\u0440\u0440\u0435\u043a\u0446\u0438\u0439: 0 \u00b7 \u0422\u043e\u0447\u043d\u043e\u0441\u0442\u044c: \u043d/\u0434\n"
                "\u041e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432 \u043e\u0442\u043a\u0440\u044b\u0442\u043e: 0"
            )
            return "\n".join([base, self._stats_text(days=7, include_header=False)])
        primary = account_emails[0]
        summary = self.analytics.weekly_compact_summary(
            account_email=primary,
            account_emails=account_emails,
            days=7,
        )
        accuracy_pct = summary.get("accuracy_pct")
        accuracy_text = (
            f"{int(accuracy_pct)}%" if accuracy_pct is not None else "\u043d/\u0434"
        )
        base = (
            "\U0001f4ca LetterBot.ru \u2014 \u043d\u0435\u0434\u0435\u043b\u044f\n"
            f"\u041f\u0438\u0441\u0435\u043c: {int(summary.get('emails_total') or 0)} \u00b7 \u0412\u0430\u0436\u043d\u044b\u0445: {int(summary.get('important') or 0)} \u00b7 \u041d\u0438\u0437\u043a\u0438\u0445: {int(summary.get('low') or 0)}\n"
            f"\u041a\u043e\u0440\u0440\u0435\u043a\u0446\u0438\u0439: {int(summary.get('corrections') or 0)} \u00b7 \u0422\u043e\u0447\u043d\u043e\u0441\u0442\u044c: {accuracy_text}\n"
            f"\u041e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432 \u043e\u0442\u043a\u0440\u044b\u0442\u043e: {int(summary.get('open_commitments') or 0)}"
        )
        return "\n".join([base, self._stats_text(days=7, include_header=False)])

    def _stats_text(self, *, days: int = 7, include_header: bool = True) -> str:
        account_emails = list(self._account_emails())
        if not account_emails:
            header = (
                "\U0001f4c8 \u041a\u0430\u0447\u0435\u0441\u0442\u0432\u043e \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0438\u0437\u0430\u0446\u0438\u0438"
                if include_header
                else ""
            )
            trust = "\u041f\u043e\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445 \u043c\u0430\u043b\u043e \u2014 \u0434\u0435\u043b\u0430\u0435\u043c \u0432\u044b\u0432\u043e\u0434\u044b \u0432\u0440\u0443\u0447\u043d\u0443\u044e."
            lines = [
                line
                for line in [
                    header,
                    "\u041a\u043e\u0440\u0440\u0435\u043a\u0446\u0438\u0439: 0",
                    "Surprise rate: \u043d/\u0434",
                    "\u041f\u0435\u0440\u0435\u0445\u043e\u0434\u044b: \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445",
                    trust,
                ]
                if line
            ]
            return "\n".join(lines)

        primary = account_emails[0]
        accuracy: dict[str, object] = {}
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
        surprise_text = "\u043d/\u0434"
        if surprise_rate is not None:
            surprise_text = f"{float(surprise_rate) * 100:.0f}%"

        transitions_text = "\u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445"
        transitions = _top_priority_transitions(
            db_path=self.knowledge_db.path, days=days, limit=3
        )
        if transitions:
            transitions_text = ", ".join(
                f"{transition} \u00d7{count}" for transition, count in transitions
            )

        latency_text = "\u043d/\u0434"
        decisions_total = int(calibration_totals.get("decisions_total") or 0)
        corrected_total = int(calibration_totals.get("decisions_corrected") or 0)
        if decisions_total > 0:
            latency_text = f"{corrected_total}/{decisions_total} \u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u043f\u0435\u0440\u0435\u0441\u043c\u043e\u0442\u0440\u0435\u043d\u044b"

        trust_line = "\u041c\u043e\u0436\u043d\u043e \u0434\u043e\u0432\u0435\u0440\u044f\u0442\u044c \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0438\u0437\u0430\u0446\u0438\u0438: \u0434\u0430"
        if corrections < 3:
            trust_line = "\u041c\u043e\u0436\u043d\u043e \u0434\u043e\u0432\u0435\u0440\u044f\u0442\u044c \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0438\u0437\u0430\u0446\u0438\u0438: \u043e\u0441\u0442\u043e\u0440\u043e\u0436\u043d\u043e, \u043c\u0430\u043b\u043e \u0434\u0430\u043d\u043d\u044b\u0445"
        elif surprise_rate is not None and float(surprise_rate) > 0.35:
            trust_line = "\u041c\u043e\u0436\u043d\u043e \u0434\u043e\u0432\u0435\u0440\u044f\u0442\u044c \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0438\u0437\u0430\u0446\u0438\u0438: \u043f\u043e\u043a\u0430 \u043e\u0441\u0442\u043e\u0440\u043e\u0436\u043d\u043e"

        lines: list[str] = []
        if include_header:
            lines.append(
                "\U0001f4c8 \u041a\u0430\u0447\u0435\u0441\u0442\u0432\u043e \u0430\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0438\u0437\u0430\u0446\u0438\u0438"
            )
        lines.extend(
            [
                f"\u041a\u043e\u0440\u0440\u0435\u043a\u0446\u0438\u0439: {corrections}",
                f"Surprise rate: {surprise_text}",
                f"\u0421\u043a\u043e\u0440\u043e\u0441\u0442\u044c \u043a\u043e\u0440\u0440\u0435\u043a\u0446\u0438\u0439: {latency_text}",
                f"\u041f\u0435\u0440\u0435\u0445\u043e\u0434\u044b: {transitions_text}",
                trust_line,
            ]
        )
        return "\n".join(lines)

    def _priority_help_text(self) -> str:
        return _t("inbound.priority_help")

    def _support_text(self) -> str:
        support = self.support_settings or load_support_settings()
        if not support.enabled:
            return "\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u043f\u0440\u043e\u0435\u043a\u0442\u0430 \u0441\u0435\u0439\u0447\u0430\u0441 \u043d\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d\u0430."
        if not support.url or support.url == "CHANGE_ME":
            return "\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u0432\u043a\u043b\u044e\u0447\u0435\u043d\u0430, \u043d\u043e \u0441\u0441\u044b\u043b\u043a\u0430 \u0435\u0449\u0451 \u043d\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d\u0430."
        return "\n".join(
            [
                support.label
                or "\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u0430\u0442\u044c LetterBot.ru",
                support.text,
                support.url,
            ]
        )

    def _status_text(self, *, chat_id: str) -> str:
        mode_label = humanize_mode(system_health.mode.value, locale=_UI_LOCALE)
        sla = compute_notification_sla(analytics=self.analytics)
        digest_override = self.override_store.get_overrides().digest_enabled

        accounts = []
        for account_email in self._account_emails():
            daily = self.knowledge_db.get_last_digest_sent_at(
                account_email=account_email
            )
            weekly = self.knowledge_db.get_last_weekly_digest_sent_at(
                account_email=account_email
            )
            accounts.append(
                f"{account_email}: \u0434\u0435\u043d\u044c {_format_ts(daily)}, \u043d\u0435\u0434\u0435\u043b\u044f {_format_ts(weekly)}"
            )
        accounts_block = (
            "\n".join(accounts)
            if accounts
            else "\u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445"
        )

        runtime_flags, _ = self.runtime_flag_store.get_flags(force=True)
        auto_mode = (
            "\u0430\u0432\u0442\u043e"
            if self.feature_flags.ENABLE_AUTO_PRIORITY
            and runtime_flags.enable_auto_priority
            else "\u0442\u0435\u043d\u0435\u0432\u043e\u0439"
        )

        if digest_override is True:
            digest_flag = "\u0432\u043a\u043b\u044e\u0447\u0435\u043d\u044b"
        elif digest_override is False:
            digest_flag = "\u0432\u044b\u043a\u043b\u044e\u0447\u0435\u043d\u044b"
        else:
            digest_flag = (
                "\u0432\u043a\u043b\u044e\u0447\u0435\u043d\u044b"
                if self.feature_flags.ENABLE_DAILY_DIGEST
                else "\u0432\u044b\u043a\u043b\u044e\u0447\u0435\u043d\u044b"
            )

        preview = (
            "\u0432\u043a\u043b"
            if self.feature_flags.ENABLE_PREVIEW_ACTIONS
            else "\u0432\u044b\u043a\u043b"
        )
        anomalies = (
            "\u0432\u043a\u043b"
            if self.feature_flags.ENABLE_ANOMALY_ALERTS
            else "\u0432\u044b\u043a\u043b"
        )
        quality = (
            "\u0432\u043a\u043b"
            if self.feature_flags.ENABLE_QUALITY_METRICS
            else "\u0432\u044b\u043a\u043b"
        )

        llm_active = system_health.mode not in {
            OperationalMode.DEGRADED_NO_LLM,
            OperationalMode.EMERGENCY_READ_ONLY,
        }
        llm_state = (
            "\u0430\u043a\u0442\u0438\u0432\u0435\u043d"
            if llm_active
            else "\u0434\u0435\u0433\u0440\u0430\u0434\u0438\u0440\u043e\u0432\u0430\u043d"
        )
        delivery_mode = _status_llm_delivery_mode()

        status_lines = [
            "\u0421\u0442\u0430\u0442\u0443\u0441 \u0441\u0438\u0441\u0442\u0435\u043c\u044b",
            f"\u0420\u0435\u0436\u0438\u043c: {mode_label}",
            f"AI: {llm_state}",
            f"LLM delivery: {delivery_mode}",
            f"\u041e\u043f\u0435\u0440\u0430\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0439 (24\u0447): \u0434\u043e\u0441\u0442\u0430\u0432\u043a\u0430 {_format_percent(sla.delivery_rate_24h)}, \u043e\u0448\u0438\u0431\u043a\u0438 {_format_percent(sla.error_rate_24h)}",
            f"\u0414\u0430\u0439\u0434\u0436\u0435\u0441\u0442\u044b: {digest_flag}",
            f"\u0410\u0432\u0442\u043e\u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442: {auto_mode}",
            f"\u0424\u043b\u0430\u0433\u0438: \u043f\u0440\u0435\u0432\u044c\u044e={preview}, \u0430\u043d\u043e\u043c\u0430\u043b\u0438\u0438={anomalies}, \u043a\u0430\u0447\u0435\u0441\u0442\u0432\u043e={quality}",
            "\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0434\u0430\u0439\u0434\u0436\u0435\u0441\u0442\u043e\u0432:",
            accounts_block,
        ]
        insider_since = self.override_store.get_insider_since(chat_id=chat_id)
        if insider_since:
            status_lines.append(f"\u2b50 LetterBot.ru Insider since: {insider_since}")
        status_lines.append(f"Version: {get_version()}")
        return normalize_mojibake_text("\n".join(status_lines))

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
