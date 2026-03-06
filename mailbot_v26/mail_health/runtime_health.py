from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple

from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.health.mail_accounts import _mask_login


@dataclass
class AccountRuntimeState:
    account_id: str
    consecutive_failures: int = 0
    last_error: str | None = None
    last_error_class: str | None = None
    failure_bucket: str | None = None
    next_retry_at_utc: datetime | None = None
    cooldown_until_utc: datetime | None = None
    cooldown_reason: str | None = None
    last_alert_sent_at_utc: datetime | None = None
    last_alert_fingerprint: str | None = None


@dataclass
class _AccountMeta:
    account_id: str
    login: str
    host: str
    port: int
    use_ssl: bool


class AccountRuntimeHealthManager:
    def __init__(self, state_path: Path, *, alert_cooldown_minutes: int = 60) -> None:
        self._state_path = state_path
        self._alert_cooldown = timedelta(minutes=alert_cooldown_minutes)
        self._states: Dict[str, AccountRuntimeState] = {}
        self._account_meta: Dict[str, _AccountMeta] = {}
        self._load_state()

    def register_account(self, account: AccountConfig) -> None:
        self._account_meta[account.account_id] = _AccountMeta(
            account_id=account.account_id,
            login=account.username or account.login,
            host=account.host,
            port=account.port,
            use_ssl=account.use_ssl,
        )

    def should_attempt(self, account_id: str, now_utc: datetime) -> bool:
        state = self._get_state(account_id)
        if state.next_retry_at_utc is None:
            return True
        return now_utc >= state.next_retry_at_utc

    def get_state(self, account_id: str) -> AccountRuntimeState:
        return self._get_state(account_id)

    def format_timestamp(self, value: datetime | None) -> str | None:
        return self._format_dt(value)

    def on_success(self, account_id: str, now_utc: datetime) -> None:
        state = self._get_state(account_id)
        updated = AccountRuntimeState(
            account_id=state.account_id,
            consecutive_failures=0,
            last_error=None,
            last_error_class=None,
            failure_bucket=None,
            next_retry_at_utc=None,
            cooldown_until_utc=None,
            cooldown_reason=None,
            last_alert_sent_at_utc=state.last_alert_sent_at_utc,
            last_alert_fingerprint=state.last_alert_fingerprint,
        )
        self._states[account_id] = updated
        self._save_state()

    def on_failure(
        self, account_id: str, exc: Exception, now_utc: datetime
    ) -> Tuple[bool, str]:
        state = self._get_state(account_id)
        message = self._normalize_error_message(exc)
        error_class = exc.__class__.__name__
        failure_bucket = self._classify_failure(error_class=error_class, message=message)
        if failure_bucket == state.failure_bucket:
            consecutive_failures = state.consecutive_failures + 1
        else:
            consecutive_failures = 1

        next_retry_at: datetime
        cooldown_until_utc: datetime | None = None
        cooldown_reason: str | None = None
        if failure_bucket == "imap_network_timeout" and consecutive_failures >= 3:
            cooldown_until_utc = now_utc + timedelta(hours=24)
            next_retry_at = cooldown_until_utc
            cooldown_reason = "repeated_handshake_connect_failures"
        else:
            backoff_minutes = self._calculate_backoff_minutes(consecutive_failures)
            next_retry_at = now_utc + timedelta(minutes=backoff_minutes)

        updated = AccountRuntimeState(
            account_id=account_id,
            consecutive_failures=consecutive_failures,
            last_error=message,
            last_error_class=error_class,
            failure_bucket=failure_bucket,
            next_retry_at_utc=next_retry_at,
            cooldown_until_utc=cooldown_until_utc,
            cooldown_reason=cooldown_reason,
            last_alert_sent_at_utc=state.last_alert_sent_at_utc,
            last_alert_fingerprint=state.last_alert_fingerprint,
        )
        fingerprint = self._build_fingerprint(
            account_id=account_id,
            error_class=error_class,
            message=message,
            failure_bucket=failure_bucket,
        )

        should_alert = self._should_alert(updated, fingerprint, now_utc)
        if should_alert:
            updated.last_alert_sent_at_utc = now_utc
            updated.last_alert_fingerprint = fingerprint

        self._states[account_id] = updated
        self._save_state()

        alert_text = self._build_alert_text(account_id, updated, now_utc)
        return should_alert, alert_text

    def _should_alert(
        self, state: AccountRuntimeState, fingerprint: str, now_utc: datetime
    ) -> bool:
        if (
            state.cooldown_until_utc is not None
            and now_utc < state.cooldown_until_utc
            and fingerprint == state.last_alert_fingerprint
        ):
            return False
        if fingerprint != state.last_alert_fingerprint:
            return True
        if state.last_alert_sent_at_utc is None:
            return True
        return now_utc - state.last_alert_sent_at_utc >= self._alert_cooldown

    @staticmethod
    def _normalize_error_message(exc: Exception) -> str:
        message = str(exc) or repr(exc)
        message = message if message else "<no error message>"
        return " ".join(message.split())

    @staticmethod
    def _classify_failure(*, error_class: str, message: str) -> str:
        lowered = f"{error_class} {message}".lower()
        if (
            "handshake" in lowered
            or "connect" in lowered
            or "socket" in lowered
            or "connection" in lowered
        ) and ("timeout" in lowered or "timed out" in lowered or "ssl" in lowered):
            return "imap_network_timeout"
        if "auth" in lowered or "password" in lowered or "login" in lowered:
            return "imap_auth"
        return "other"

    def _get_state(self, account_id: str) -> AccountRuntimeState:
        if account_id not in self._states:
            self._states[account_id] = AccountRuntimeState(account_id=account_id)
        return self._states[account_id]

    @staticmethod
    def _calculate_backoff_minutes(consecutive_failures: int) -> int:
        if consecutive_failures <= 1:
            return 1
        if consecutive_failures == 2:
            return 5
        if consecutive_failures == 3:
            return 15
        if consecutive_failures == 4:
            return 60
        return 360

    def _build_alert_text(
        self, account_id: str, state: AccountRuntimeState, now_utc: datetime
    ) -> str:
        meta = self._account_meta.get(account_id)
        login = meta.login if meta else account_id
        masked_login = _mask_login(login)
        host = meta.host if meta else "<unknown>"
        port = meta.port if meta else 0
        use_ssl = meta.use_ssl if meta else False
        error_class = state.last_error_class or "Error"
        error_msg = state.last_error or "<no error message>"
        retry_in = self._format_retry(state.next_retry_at_utc, now_utc)
        lines = [
            "\U0001F6A8 IMAP RUNTIME FAILURE",
            f"Account: {account_id}",
            f"Login: {masked_login}",
            f"Host: {host}:{port} ssl={use_ssl}",
            f"Error: {error_class}: {error_msg}",
            f"Next retry: {retry_in}",
        ]
        if state.cooldown_until_utc is not None:
            cooldown_text = (
                state.cooldown_until_utc.astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            lines.append(f"Cooldown: active until {cooldown_text} ({state.cooldown_reason or 'network'})")
        return "\n".join(lines)

    @staticmethod
    def _format_retry(next_retry_at: datetime | None, now_utc: datetime) -> str:
        if next_retry_at is None:
            return "now"
        remaining = max(next_retry_at - now_utc, timedelta())
        minutes = int(remaining.total_seconds() // 60)
        hours, minutes = divmod(minutes, 60)
        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes or not parts:
            parts.append(f"{minutes}m")
        retry_at_text = (
            next_retry_at.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        return f"in {' '.join(parts)} (at {retry_at_text})"

    def _build_fingerprint(
        self,
        account_id: str,
        error_class: str,
        message: str,
        failure_bucket: str,
    ) -> str:
        meta = self._account_meta.get(account_id)
        host = meta.host if meta else "<unknown>"
        port = meta.port if meta else 0
        if failure_bucket == "imap_network_timeout":
            return f"{failure_bucket}:{host}:{port}"
        return f"{error_class}:{message}:{host}:{port}"

    def _load_state(self) -> None:
        if not self._state_path.exists():
            return
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception:
            return
        for account_id, raw in payload.items():
            self._states[account_id] = AccountRuntimeState(
                account_id=account_id,
                consecutive_failures=raw.get("consecutive_failures", 0),
                last_error=raw.get("last_error"),
                last_error_class=raw.get("last_error_class"),
                failure_bucket=raw.get("failure_bucket"),
                next_retry_at_utc=self._parse_dt(raw.get("next_retry_at_utc")),
                cooldown_until_utc=self._parse_dt(raw.get("cooldown_until_utc")),
                cooldown_reason=raw.get("cooldown_reason"),
                last_alert_sent_at_utc=self._parse_dt(
                    raw.get("last_alert_sent_at_utc")
                ),
                last_alert_fingerprint=raw.get("last_alert_fingerprint"),
            )

    def _save_state(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {}
        for account_id, state in self._states.items():
            payload[account_id] = {
                **asdict(state),
                "next_retry_at_utc": self._format_dt(state.next_retry_at_utc),
                "cooldown_until_utc": self._format_dt(state.cooldown_until_utc),
                "last_alert_sent_at_utc": self._format_dt(
                    state.last_alert_sent_at_utc
                ),
            }
        tmp_path = self._state_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp_path.replace(self._state_path)

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _format_dt(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()


__all__ = ["AccountRuntimeHealthManager", "AccountRuntimeState"]
