from __future__ import annotations

import logging
import os
import platform
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from mailbot_v26.config.llm_queue import load_llm_queue_config
from mailbot_v26.config_loader import BotConfig
from mailbot_v26.llm import router as llm_router
from mailbot_v26.system_health import OperationalMode, SystemHealth
from mailbot_v26.ui.branding import append_watermark
from mailbot_v26.worker.telegram_sender import ping_telegram

logger = logging.getLogger(__name__)


class HealthStatus:
    OK = "OK"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class HealthCheckResult:
    component: str
    status: str
    details: str

    def as_dict(self) -> dict[str, str]:
        return {"component": self.component, "status": self.status, "details": self.details}


class StartupHealthChecker:
    def __init__(self, config_dir: Path, config: BotConfig) -> None:
        self._config_dir = config_dir
        self._config = config

    def run(self) -> list[dict[str, str]]:
        results = [
            self._safe_check(self._check_python),
            self._safe_check(self._check_os),
            self._safe_check(self._check_db),
            self._safe_check(self._check_telegram),
        ]
        results.extend(self._safe_check(self._check_gigachat, fallback=[]) or [])
        results.extend(self._safe_check(self._check_cloudflare, fallback=[]) or [])
        results.append(
            self._safe_check(
                self._check_llm_direct_path,
                fallback=HealthCheckResult(
                    "LLM Direct",
                    HealthStatus.FAILED,
                    "configured + capability probe failed",
                ),
            )
        )
        return [result.as_dict() for result in results]

    def evaluate_mode(self, results: Sequence[dict[str, str]]) -> OperationalMode:
        system_health = SystemHealth()
        components = {item["component"]: item for item in results}
        db_status = components.get("DB", {}).get("status")
        telegram_status = components.get("Telegram", {}).get("status")
        llm_ok = any(
            item.get("status") == HealthStatus.OK
            for name, item in components.items()
            if name in {"GigaChat", "Cloudflare"}
        )
        if db_status:
            system_health.update_component("CRM", db_status == HealthStatus.OK)
        if telegram_status:
            system_health.update_component("Telegram", telegram_status == HealthStatus.OK)
        system_health.update_component("LLM", llm_ok)
        return system_health.mode

    def _safe_check(
        self,
        fn,
        *,
        fallback: HealthCheckResult | list[HealthCheckResult] | None = None,
    ) -> HealthCheckResult | list[HealthCheckResult]:
        try:
            return fn()
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("startup_health_check_failed", extra={"component": fn.__name__}, exc_info=True)
            return (
                fallback
                if fallback is not None
                else HealthCheckResult(fn.__name__, HealthStatus.FAILED, str(exc))
            )

    def _check_python(self) -> HealthCheckResult:
        version = sys.version_info
        ok = version >= (3, 10)
        status = HealthStatus.OK if ok else HealthStatus.FAILED
        details = f"{version.major}.{version.minor}.{version.micro}"
        if not ok:
            details = f"{details} (requires >=3.10)"
        return HealthCheckResult("Python", status, details)

    def _check_os(self) -> HealthCheckResult:
        system = platform.system() or "Unknown"
        release = platform.release() or ""
        cwd = os.getcwd()
        details = f"{system} {release}".strip()
        if cwd:
            details = f"{details} (cwd={cwd})"
        return HealthCheckResult("OS", HealthStatus.OK, details)

    def _check_db(self) -> HealthCheckResult:
        db_path = self._config.storage.db_path
        try:
            with sqlite3.connect(f"file:{db_path}?mode=rwc", uri=True) as conn:
                conn.execute("BEGIN;")
                conn.execute("CREATE TEMP TABLE IF NOT EXISTS startup_health_probe (id INTEGER);")
                conn.execute("INSERT INTO startup_health_probe (id) VALUES (1);")
                conn.execute("ROLLBACK;")
        except sqlite3.Error as exc:
            return HealthCheckResult("DB", HealthStatus.FAILED, f"{db_path} ({exc})")
        return HealthCheckResult("DB", HealthStatus.OK, str(db_path))

    def _check_telegram(self) -> HealthCheckResult:
        token = self._config.keys.telegram_bot_token
        ok, details = ping_telegram(token)
        status = HealthStatus.OK if ok else HealthStatus.FAILED
        return HealthCheckResult("Telegram", status, details)

    def _check_gigachat(self) -> list[HealthCheckResult]:
        try:
            config = llm_router._load_llm_config(self._config_dir)
            router = llm_router.LLMRouter(config)
        except Exception as exc:
            return [HealthCheckResult("GigaChat", HealthStatus.FAILED, f"config error: {exc}")]
        provider = router._providers.get("gigachat")
        enabled = bool(config.gigachat_enabled or config.gigachat_api_key)
        if not enabled or not provider:
            return [HealthCheckResult("GigaChat", HealthStatus.DEGRADED, "disabled")]
        if not config.gigachat_api_key:
            return [HealthCheckResult("GigaChat", HealthStatus.FAILED, "missing api key")]
        ok = provider.healthcheck()
        status = HealthStatus.OK if ok else HealthStatus.FAILED
        details = "active" if ok else "healthcheck failed"
        return [HealthCheckResult("GigaChat", status, details)]

    def _check_cloudflare(self) -> list[HealthCheckResult]:
        try:
            config = llm_router._load_llm_config(self._config_dir)
            router = llm_router.LLMRouter(config)
        except Exception as exc:
            return [
                HealthCheckResult("Cloudflare", HealthStatus.FAILED, f"config error: {exc}")
            ]
        provider = router._providers.get("cloudflare")
        if not config.cloudflare_enabled or not provider:
            return [HealthCheckResult("Cloudflare", HealthStatus.DEGRADED, "disabled")]
        if not config.cloudflare_account_id or not config.cloudflare_api_key:
            return [HealthCheckResult("Cloudflare", HealthStatus.FAILED, "missing credentials")]
        ok = provider.healthcheck()
        status = HealthStatus.OK if ok else HealthStatus.FAILED
        details = "active" if ok else "healthcheck failed"
        return [HealthCheckResult("Cloudflare", status, details)]

    def _check_llm_direct_path(self) -> HealthCheckResult:
        llm_config = llm_router._load_llm_config(self._config_dir)
        router = llm_router.LLMRouter(llm_config)
        provider_name = llm_config.primary
        provider = router._providers.get(provider_name)
        if provider is None and llm_config.fallback:
            provider_name = llm_config.fallback
            provider = router._providers.get(provider_name)
        if provider is None:
            return HealthCheckResult("LLM Direct", HealthStatus.DEGRADED, "disabled")
        if not provider.healthcheck():
            return HealthCheckResult(
                "LLM Direct",
                HealthStatus.FAILED,
                "configured + capability probe failed",
            )
        return HealthCheckResult(
            "LLM Direct",
            HealthStatus.OK,
            "configured + capability probe passed",
        )


class LaunchReportBuilder:
    def __init__(
        self,
        version_label: str = "Letterbot Premium v26",
        *,
        config_dir: Path | None = None,
    ) -> None:
        self._version_label = version_label
        self._config_dir = config_dir or Path(__file__).resolve().parents[1] / "config"

    def build(
        self,
        results: Sequence[dict[str, str]],
        mode: OperationalMode,
        *,
        mail_accounts: Sequence[dict[str, str]] | None = None,
        mail_check_unavailable_reason: str | None = None,
    ) -> str:
        index = {item["component"]: item for item in results}
        mail_lines = self._format_mail_accounts(
            mail_accounts=mail_accounts,
            unavailable_reason=mail_check_unavailable_reason,
        )
        llm_delivery_mode = self._get_llm_delivery_mode(index)
        background_queue_mode = self._get_background_queue_mode()
        llm_direct_check = self._get_llm_direct_check(index)
        lines = [
            "---",
            f"{self._version_label} started",
            "",
            "System:",
            self._format_line("Python", index.get("Python")),
            self._format_line("OS", index.get("OS")),
            self._format_line("DB", index.get("DB")),
            "",
            "LLM:",
            self._format_line("GigaChat", index.get("GigaChat")),
            self._format_line("Cloudflare", index.get("Cloudflare")),
            f"- direct path check: {llm_direct_check}",
            f"- LLM delivery mode: {llm_delivery_mode}",
            f"- Immediate TG summaries: {self._get_immediate_summary_mode(llm_delivery_mode)}",
            f"- background queue: {background_queue_mode}",
            "",
            "Telegram:",
            self._format_line("Status", index.get("Telegram")),
            "",
            "Mail accounts:",
            *mail_lines,
            "",
            "Mode:",
            f"- Operational mode: {mode.value}",
            "---",
        ]
        return append_watermark("\n".join(lines), html=True)

    def _format_mail_accounts(
        self,
        *,
        mail_accounts: Sequence[dict[str, str]] | None,
        unavailable_reason: str | None,
    ) -> list[str]:
        if unavailable_reason:
            return [f"- check unavailable ({self._sanitize_details(unavailable_reason)})"]
        if not mail_accounts:
            return ["- none configured"]
        lines: list[str] = []
        for account in mail_accounts:
            account_id = account.get("account_id", "unknown")
            status = account.get("status", HealthStatus.FAILED)
            if status == HealthStatus.OK:
                lines.append(f"- {account_id}: OK")
                continue
            error = account.get("error") or "unknown error"
            lines.append(f"- {account_id}: FAILED ({self._sanitize_details(error)})")
        return lines

    def _format_line(self, label: str, entry: dict[str, str] | None) -> str:
        if not entry:
            return f"- {label}: FAILED (missing)"
        status = entry.get("status", HealthStatus.FAILED)
        details = entry.get("details", "")
        if details:
            return f"- {label}: {status} ({self._sanitize_details(details)})"
        return f"- {label}: {status}"

    def _sanitize_details(self, details: str) -> str:
        sanitized = " ".join((details or "").split())
        return sanitized[:180]

    def _get_llm_delivery_mode(self, index: dict[str, dict[str, str]]) -> str:
        llm_config = llm_router._load_llm_config(self._config_dir)
        has_direct_provider = (
            bool(llm_config.cloudflare_enabled)
            and bool(llm_config.cloudflare_account_id)
            and bool(llm_config.cloudflare_api_key)
        ) or (
            bool(llm_config.gigachat_enabled or llm_config.gigachat_api_key)
            and bool(llm_config.gigachat_api_key)
        )
        if not has_direct_provider:
            return "DISABLED"

        llm_direct_status = index.get("LLM Direct", {}).get("status")
        if llm_direct_status == HealthStatus.OK:
            return "DIRECT"
        return "HEURISTIC_FALLBACK"

    def _get_llm_direct_check(self, index: dict[str, dict[str, str]]) -> str:
        direct_entry = index.get("LLM Direct")
        if direct_entry:
            return direct_entry.get("details", "unknown")
        return "disabled"

    def _get_immediate_summary_mode(self, llm_delivery_mode: str) -> str:
        return "heuristic" if llm_delivery_mode != "DIRECT" else "direct"

    def _get_background_queue_mode(self) -> str:
        queue_config = load_llm_queue_config(self._config_dir)
        if queue_config.llm_request_queue_enabled and queue_config.max_concurrent_llm_calls == 1:
            return "enabled"
        return "disabled"


def dispatch_launch_report(bot_token: str, chat_id: str, report: str) -> bool:
    from mailbot_v26.pipeline.telegram_payload import TelegramPayload
    from mailbot_v26.telegram_utils import telegram_safe
    from mailbot_v26.worker.telegram_sender import send_telegram

    if not bot_token or not chat_id or not report:
        logger.error("launch_report_skipped", extra={"reason": "missing_token_chat_or_report"})
        return False
    payload = TelegramPayload(
        html_text=telegram_safe(report),
        priority="🔵",
        metadata={"bot_token": bot_token, "chat_id": chat_id},
    )
    try:
        return send_telegram(payload).delivered
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("launch_report_send_failed", extra={"error": str(exc)}, exc_info=True)
        return False


__all__ = [
    "HealthCheckResult",
    "HealthStatus",
    "LaunchReportBuilder",
    "StartupHealthChecker",
    "dispatch_launch_report",
]
