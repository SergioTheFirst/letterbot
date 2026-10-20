"""LetterBot.ru v26 - Runtime orchestrator"""

from __future__ import annotations

import configparser
import logging
import argparse
import math
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from email import message_from_bytes
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Callable, List, Optional

from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    PipelineContext,
    _extract_attachment_text,
    _extract_attachments,
    _extract_body,
    configure_pipeline,
    parse_raw_email,
    remember_raw_email,
    stage_llm,
    stage_parse,
    stage_tg,
    store_inbound,
)
from mailbot_v26.deps import DependencyError, require_runtime_for
from mailbot_v26.dist_self_check import validate_dist_runtime
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    load_accounts_config,
    load_config as load_ini_config,
    load_general_config,
    load_ingest_config,
    load_keys_config,
    load_maintenance_config,
    load_storage_config,
    load_telegram_ui_config,
    validate_telegram_contract,
)
from mailbot_v26.config.paths import app_root, resolve_config_paths, runtime_log_path
from mailbot_v26.account_identity import logins_match, normalize_login
from mailbot_v26.config_yaml import (
    ConfigError as YamlConfigError,
    SCHEMA_NEWER_MESSAGE,
    build_bot_config,
    get_polling_intervals,
    load_config as load_yaml_config,
    validate_config as validate_yaml_config,
)
from mailbot_v26.health.mail_accounts import (
    _mask_login,
    run_startup_mail_account_healthcheck,
)
from mailbot_v26.imap_client import MAX_UID_FETCH_ATTEMPTS, ResilientIMAP
from mailbot_v26.integrity import verify_shipped_files
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline.processor import (
    InboundMessage,
    MessageProcessor,
    event_emitter,
)
from mailbot_v26.pipeline.delivery_guard import (
    DELIVERY_UNKNOWN,
    DUPLICATE_SUPPRESSED,
    EmailDeliveryGuard,
    delivery_unknown_error_class,
)
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.pipeline import processor as processor_module
from mailbot_v26.pipeline.digest_scheduler import (
    DigestStorage,
    configure_digest_config_dir,
    run_digest_tick,
)
from mailbot_v26.observability import configure_logging as configure_runtime_logging
from mailbot_v26.observability import get_logger
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.state_manager import StateManager
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.telegram import (
    InboundStateStore,
    TelegramInboundClient,
    TelegramInboundProcessor,
    run_inbound_polling,
)
from mailbot_v26.telegram.decision_trace_ui import build_email_actions_keyboard
from mailbot_v26.telegram.inbound import build_chat_account_scopes
from mailbot_v26.storage.self_check import run_self_check
from mailbot_v26.system.instance_lock import (
    InstanceLock,
    InstanceLockError,
    acquire_instance_lock,
    instance_lock_path,
)
from mailbot_v26.system.startup_health import (
    LaunchReportBuilder,
    StartupHealthChecker,
    dispatch_launch_report,
)
from mailbot_v26.text.mime_utils import decode_mime_header
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.worker.telegram_sender import send_telegram
from mailbot_v26.tools.backfill_events import maybe_backfill_events
from mailbot_v26.version import __version__

CURRENT_DIR = Path(__file__).resolve().parent
LOG_PATH = runtime_log_path()
# Backward-compatible start-module exports used by tests and tools.
_START_COMPAT_EXPORTS = (_extract_body, _extract_attachments, _extract_attachment_text)
RUN_STARTED_AT_UTC = datetime.now(timezone.utc)
REPO_ROOT = CURRENT_DIR.parent
MAX_PIPELINE_STAGE_ATTEMPTS = 3
MAX_TELEGRAM_STAGE_ATTEMPTS = 3
MAX_QUEUE_BACKOFF_SECONDS = 600


class TelegramRetryableError(RuntimeError):
    """Telegram did not take the message; retry no sooner than retry_after."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _queue_backoff_seconds(attempts: int, exc: BaseException | None = None) -> int:
    """Exponential queue backoff, stretched to Telegram's retry_after (429)."""
    backoff = min(MAX_QUEUE_BACKOFF_SECONDS, 10 * (2**attempts))
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(exc, TelegramRetryableError) and retry_after:
        backoff = max(backoff, int(math.ceil(retry_after)))
    return backoff


def _not_before_after(seconds: float) -> str:
    """Queue not_before: naive-UTC isoformat, the format claim_next compares (F11)."""
    moment = datetime.now(timezone.utc) + timedelta(seconds=math.ceil(seconds))
    return moment.replace(tzinfo=None).isoformat()


def _configure_logging() -> None:
    try:
        configure_runtime_logging(log_path=LOG_PATH, console_stream=sys.stdout)
    except OSError as exc:
        print(f"File logging unavailable: {exc}")
        configure_runtime_logging(console_stream=sys.stdout)


def _mask_startup_value(value: str | None) -> str:
    token = str(value or "").strip()
    if not token:
        return "<missing>"
    if len(token) <= 4:
        return "*" * len(token)
    return f"{token[:2]}...({len(token)})"


def _looks_like_placeholder(value: str | None) -> bool:
    token = str(value or "").strip()
    return not token or token == "CHANGE_ME"


def _is_bootstrap_example_account(section_name: str) -> bool:
    return section_name.strip().lower() == "example_account"


def _run_startup_preflight(config_dir: Path) -> tuple[bool, list[str], list[str]]:
    critical: list[str] = []
    warnings: list[str] = []
    settings_path = config_dir / "settings.ini"
    legacy_settings_path = config_dir / "config.ini"
    accounts_path = config_dir / "accounts.ini"

    if not settings_path.exists() and not legacy_settings_path.exists():
        critical.append(f"Missing settings.ini in {config_dir}")
    if not accounts_path.exists():
        critical.append(f"Missing accounts.ini in {config_dir}")
        return False, critical, warnings

    parser = configparser.ConfigParser()
    # utf-8-sig: Notepad's "UTF-8 with BOM" must parse like plain UTF-8.
    parser.read(accounts_path, encoding="utf-8-sig")
    imap_sections = [
        section_name
        for section_name in parser.sections()
        if section_name.lower() not in {"telegram", "cloudflare", "gigachat", "llm"}
    ]
    if not imap_sections:
        critical.append("accounts.ini has no IMAP account sections")
        return False, critical, warnings

    for section_name in imap_sections:
        if _is_bootstrap_example_account(section_name):
            critical.append(
                "[example_account] is still the bootstrap template: rename the section "
                "and replace example login/host values"
            )
            continue

        section = parser[section_name]
        missing = [
            key
            for key in ("login", "password", "host")
            if _looks_like_placeholder(section.get(key, ""))
        ]
        if missing:
            critical.append(
                f"[{section_name}] missing required fields: {', '.join(missing)}"
            )

    telegram_section = parser["telegram"] if parser.has_section("telegram") else None
    if telegram_section is None or _looks_like_placeholder(
        telegram_section.get("bot_token", "")
    ):
        warnings.append(
            "[telegram] bot_token is not configured (Telegram delivery may be disabled)"
        )
    return not critical, critical, warnings


def _print_startup_preflight_failure(
    config_dir: Path, critical: list[str], warnings: list[str]
) -> None:
    command_target = f'python -m mailbot_v26 init-config --config-dir "{config_dir.resolve()}"'
    ready_target = (
        "python -m mailbot_v26 config-ready "
        f'--config-dir "{config_dir.resolve()}" --verbose'
    )
    if bool(getattr(sys, "frozen", False)):
        executable_name = Path(sys.executable).name or "Letterbot.exe"
        command_target = (
            f'{executable_name} init-config --config-dir "{config_dir.resolve()}"'
        )
        ready_target = (
            f'{executable_name} config-ready --config-dir "{config_dir.resolve()}" '
            "--verbose"
        )
    print("[ERROR] Configuration is not ready for startup.")
    print(f"[ERROR] Config dir: {config_dir.resolve()}")
    for item in critical:
        print(f"[ERROR] {item}")
    for item in warnings:
        print(f"[WARN] {item}")
    print(f"[NEXT] Run: {command_target}")
    if bool(getattr(sys, "frozen", False)):
        run_bat_path = Path(sys.executable).resolve().parent / "run.bat"
        if run_bat_path.exists():
            print(f'[NEXT] Or run: "{run_bat_path}"')
    print("[NEXT] Fill settings.ini and accounts.ini placeholders, then run " f"{ready_target}")


def _ensure_runtime_dirs(*, db_path: Path, log_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)


def _build_startup_confirmation_lines(
    *,
    config: BotConfig,
    resolved_config_dir: Path,
    two_file_mode: bool,
) -> list[str]:
    lines = [
        f"[OK] Loaded {len(config.accounts)} accounts",
        "[CONFIG] summary: "
        f"config_dir={resolved_config_dir.resolve()} "
        f"two_file_mode={str(two_file_mode).lower()} "
        f"db_path={config.storage.db_path} "
        f"log_path={LOG_PATH}",
    ]
    for account in config.accounts:
        lines.append(
            "[CONFIG] account "
            f"[{account.account_id}] "
            f"login={_mask_startup_value(account.login)} "
            f"host={_mask_startup_value(account.host)} "
            f"chat_configured={str(bool(account.telegram_chat_id)).lower()}"
        )
    return lines


_configure_logging()
logger = logging.getLogger("mailbot")
digest_logger = get_logger("mailbot")


def _get_account_by_login(config: BotConfig, login: str) -> Optional["AccountConfig"]:
    for acc in config.accounts:
        if logins_match(acc.login, login):
            return acc
    return None


def _check_build_integrity() -> None:
    # The manifest sits next to Letterbot.exe (app_root), not under _internal.
    root = app_root()
    manifest_path = root / "manifest.sha256.json"
    if not manifest_path.exists():
        return
    try:
        # Shipped files only: logs/state/data created at runtime are not the build.
        tampered = verify_shipped_files(root, manifest_path)
    except Exception:
        logger.exception("integrity_manifest_failed")
        print("WARNING: build files modified (see log)")
        return
    if tampered:
        logger.warning("integrity_manifest_changed files=%s", tampered)
        print("WARNING: build files modified (see log)")


def _get_account_label(account: AccountConfig) -> str:
    label = account.name or account.login or account.account_id
    return label.strip()


def _prefix_account_text(text: str, account: AccountConfig) -> str:
    label = _get_account_label(account)
    if not label:
        return text
    return f"[{label}] {text}"


def _load_yaml_config_or_exit(config_path: Path) -> tuple[dict[str, object], BotConfig]:
    try:
        raw_config = load_yaml_config(config_path)
    except FileNotFoundError as exc:
        message = str(exc)
        logger.error("config_missing %s", message)
        print(f"[ERROR] {message}")
        sys.exit(1)
    except YamlConfigError as exc:
        message = str(exc)
        raw_detail = getattr(exc, "raw_detail", None)
        logger.error("config_invalid %s", message)
        if raw_detail:
            logger.debug("config_yaml_parse_raw %s", raw_detail)
        print(f"[ERROR] {message}")
        sys.exit(1)

    ok, error = validate_yaml_config(raw_config)
    if not ok:
        message = error or "Invalid config.yaml"
        logger.error("config_invalid %s", message)
        print(f"[ERROR] {message}")
        if message == SCHEMA_NEWER_MESSAGE:
            sys.exit(2)
        sys.exit(1)
    config = build_bot_config(raw_config, repo_root=REPO_ROOT)
    return raw_config, config


def load_config(
    config_dir: Path | None,
) -> tuple[Path | None, dict[str, object], BotConfig]:
    paths = resolve_config_paths(config_dir)
    raw_config: dict[str, object] = {}

    if paths.two_file_mode:
        config = _load_ini_config_or_defaults(paths.config_dir)
        return None, raw_config, config

    config_path = paths.yaml_path
    if config_path is not None:
        raw_config, config = _load_yaml_config_or_defaults(
            config_path, paths.config_dir
        )
        return config_path, raw_config, config

    logger.warning(
        "config.yaml missing; using deterministic defaults for YAML-only features"
    )
    print("[INFO] config.yaml not found. YAML-only gates use deterministic defaults.")
    config = _load_ini_config_or_defaults(paths.config_dir)
    return None, raw_config, config


_ORIGINAL_LOAD_CONFIG = load_config


def _load_ini_config_or_defaults(config_dir: Path) -> BotConfig:
    try:
        config = load_ini_config(config_dir)
    except Exception as exc:
        logger.warning("config_ini_load_failed %s", exc)
        print(
            "[WARN] Failed to load INI config; using deterministic defaults where possible."
        )
        config = BotConfig(
            general=load_general_config(config_dir),
            ingest=load_ingest_config(config_dir),
            maintenance=load_maintenance_config(config_dir),
            accounts=load_accounts_config(config_dir),
            keys=load_keys_config(config_dir),
            storage=load_storage_config(config_dir),
        )
    if (config_dir / "settings.ini").exists() and (
        config_dir / "accounts.ini"
    ).exists():
        print("Using new 2-file config mode")
    return config


def _load_yaml_config_or_defaults(
    config_path: Path, config_dir: Path
) -> tuple[dict[str, object], BotConfig]:
    try:
        raw_config = load_yaml_config(config_path)
    except (FileNotFoundError, YamlConfigError) as exc:
        logger.warning("config_yaml_invalid_or_missing %s", exc)
        raw_detail = getattr(exc, "raw_detail", None)
        if raw_detail:
            logger.debug("config_yaml_parse_raw %s", raw_detail)
        print(f"[INFO] {exc}. Falling back to INI configuration.")
        return {}, _load_ini_config_or_defaults(config_dir)

    ok, error = validate_yaml_config(raw_config)
    if not ok:
        message = error or "Invalid config.yaml"
        logger.warning("config_invalid %s", message)
        print(f"[INFO] {message}. Falling back to INI configuration.")
        if message == SCHEMA_NEWER_MESSAGE and bool(getattr(sys, "frozen", False)):
            print("[WARN] Обновление: распакуйте новый ZIP в новую папку.")
            print(
                "[WARN] Обновление: скопируйте старый config.yaml и запустите run.bat."
            )
        return raw_config, _load_ini_config_or_defaults(config_dir)
    config = build_bot_config(raw_config, repo_root=REPO_ROOT)
    return raw_config, config


def _build_system_payload(
    *,
    text: str,
    bot_token: str,
    chat_id: str,
    priority: str = "🔵",
) -> TelegramPayload:
    return TelegramPayload(
        html_text=telegram_safe(text),
        priority=priority,
        metadata={"bot_token": bot_token, "chat_id": chat_id},
    )


def _ui_text_for_locale(locale: str, *, en: str, ru: str) -> str:
    return en if str(locale or "").strip().casefold().startswith("en") else ru


def _telegram_delivery_failed_notice(*, locale: str) -> str:
    return _ui_text_for_locale(
        locale,
        en="Telegram delivery failed. Check the email client.",
        ru="Доставка в Telegram не удалась. Проверьте почту вручную.",
    )


def _telegram_delivery_failed_detail(
    *,
    locale: str,
    email_id: int,
    account_email: str,
    error_text: str,
) -> str:
    if str(locale or "").strip().casefold().startswith("en"):
        return "\n".join(
            [
                "🔴 TELEGRAM DELIVERY FAILED",
                f"Email ID: {email_id}",
                f"Account: {account_email}",
                f"Reason: {error_text}",
            ]
        )
    return "\n".join(
        [
            "🔴 СБОЙ ДОСТАВКИ В TELEGRAM",
            f"Письмо ID: {email_id}",
            f"Аккаунт: {account_email}",
            f"Причина: {error_text}",
        ]
    )


def _storage_has_account_history(storage: Storage | None, account_email: str) -> bool:
    if storage is None:
        return False
    probe = getattr(storage, "has_email_history", None)
    if not callable(probe):
        return False
    try:
        return bool(probe(account_email))
    except Exception:
        logger.exception("first_run_history_probe_failed account=%s", account_email)
        return False


def _is_first_run_account(
    *,
    state: StateManager,
    storage: Storage | None,
    account_email: str,
) -> bool:
    last_uid = state.get_last_uid(account_email)
    last_check = state.get_last_check_time(account_email)
    has_state_cursor = last_uid > 0 or last_check is not None
    has_db_history = _storage_has_account_history(storage, account_email)
    return (not has_state_cursor) and (not has_db_history)


def _build_first_run_bootstrap_note(hours: int, max_messages: int) -> str:
    return f"First run: showing messages from last {hours}h (up to {max_messages} messages)."


def _cleanup_pipeline_cache(email_id: int) -> None:
    PIPELINE_CACHE.pop(email_id, None)
    PIPELINE_INBOUND_CACHE.pop(email_id, None)
    PIPELINE_RAW_CACHE.pop(email_id, None)


def _coerce_received_at(header_value: str | None) -> datetime:
    if not header_value:
        return datetime.now(timezone.utc)
    try:
        parsed = parsedate_to_datetime(header_value)
    except Exception:
        return datetime.now(timezone.utc)
    if parsed is None:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_attachment_payloads(inbound: InboundMessage) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for attachment in inbound.attachments or []:
        payload: dict[str, object] = {
            "filename": attachment.filename,
            "content_type": attachment.content_type,
            "text": attachment.text,
            "size_bytes": attachment.size_bytes,
        }
        if attachment.content:
            payload["content"] = attachment.content
        if attachment.metadata:
            payload["metadata"] = attachment.metadata
        payloads.append(payload)
    return payloads


def _run_premium_processor(
    *,
    ctx: PipelineContext,
    inbound: InboundMessage,
    raw: bytes | None,
    config: BotConfig,
    delivery_guard: EmailDeliveryGuard | None = None,
) -> None:
    message_obj = message_from_bytes(raw) if raw else None
    from_header = decode_mime_header(message_obj.get("From", "")) if message_obj else ""
    from_name, from_email = parseaddr(from_header or inbound.sender)
    date_header = decode_mime_header(message_obj.get("Date", "")) if message_obj else ""
    received_at = _coerce_received_at(date_header)
    attachments = _build_attachment_payloads(inbound)
    rfc_message_id = inbound.rfc_message_id
    in_reply_to = inbound.in_reply_to
    references = inbound.references
    if message_obj:
        rfc_message_id = (
            rfc_message_id
            or decode_mime_header(message_obj.get("Message-ID", ""))
            or None
        )
        in_reply_to = (
            in_reply_to
            or decode_mime_header(message_obj.get("In-Reply-To", ""))
            or None
        )
        references = (
            references or decode_mime_header(message_obj.get("References", "")) or None
        )

    account = _get_account_by_login(config, ctx.account_email)
    if not account:
        raise RuntimeError(f"Missing account config for {ctx.account_email}")

    processor_module.process_message(
        account_email=ctx.account_email,
        message_id=ctx.email_id,
        from_email=from_email or inbound.sender or "",
        from_name=from_name or None,
        subject=inbound.subject,
        received_at=received_at,
        body_text=inbound.body or "",
        attachments=attachments,
        telegram_chat_id=account.telegram_chat_id,
        telegram_bot_token=config.keys.telegram_bot_token,
        rfc_message_id=rfc_message_id or None,
        in_reply_to=in_reply_to or None,
        references=references or None,
        delivery_guard=delivery_guard,
    )


def _fail_open_process(
    config: BotConfig,
    processor: MessageProcessor,
    ctx: Optional[PipelineContext],
    storage: Storage | None = None,
) -> None:
    if ctx is None:
        logger.error("Fail-open skipped: missing context")
        return
    if storage is None:
        # Never POST without a delivery reservation (one email -> one message).
        logger.error(
            "Fail-open skipped for email %s: no delivery log to reserve the send",
            ctx.email_id,
        )
        return

    inbound = PIPELINE_INBOUND_CACHE.get(ctx.email_id)
    if inbound is None:
        raw_email = PIPELINE_RAW_CACHE.get(ctx.email_id)
        if raw_email is not None:
            try:
                inbound = parse_raw_email(raw_email, config)
            except Exception as exc:
                logger.error(
                    "Fail-open parse failed for email %s: %s", ctx.email_id, exc
                )

    if inbound is None:
        logger.error("Fail-open skipped for email %s: no email content", ctx.email_id)
        return

    try:
        final_text = processor.process(ctx.account_email, inbound)
    except Exception:
        logger.exception("Fail-open processor failure for email %s", ctx.email_id)
        return

    if not (final_text and final_text.strip()):
        logger.error("Fail-open produced empty output for email %s", ctx.email_id)
        return

    account = _get_account_by_login(config, ctx.account_email)
    if not account:
        logger.error("Fail-open missing account for %s", ctx.account_email)
        return

    payload = _build_system_payload(
        text=final_text.strip(),
        bot_token=config.keys.telegram_bot_token,
        chat_id=account.telegram_chat_id,
    )
    guard = _new_email_delivery_guard(
        storage=storage, config=config, ctx=ctx, email_id=ctx.email_id
    )
    if guard.before_send() is None:
        guard.after_send(send_telegram(payload))
    logger.error(
        "Fail-open Telegram send status for email %s: %s",
        ctx.email_id,
        guard.outcome,
    )
    # No queue item is left to retry a fail-open send: retryable counts as failed.
    try:
        _settle_email_delivery(
            storage=storage,
            config=config,
            guard=guard,
            ctx=ctx,
            queue_id=None,
            attempts=int(getattr(ctx, "attempts", 0) or 0),
            emit_events=True,
            retryable_is_final=True,
        )
    except Exception:
        logger.exception(
            "telegram_delivery_settle_failed email_id=%s outcome=%s path=fail_open",
            ctx.email_id,
            guard.outcome,
        )


def _emit_contract_event(
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
    email_id: int | None,
    payload: dict[str, object],
    entity_id: str | None = None,
) -> None:
    try:
        processor_module.contract_event_emitter.emit(
            EventV1(
                event_type=event_type,
                ts_utc=ts_utc,
                account_id=account_id,
                entity_id=entity_id,
                email_id=email_id,
                payload=payload,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(
            "contract_event_emit_failed",
            event_type=event_type.value,
            error=str(exc),
        )


def _emit_imap_health_event(
    *,
    account_id: str,
    subtype: str,
    detail: str,
    ts_utc: float | None = None,
    email_id: int | None = None,
    message_uid: int | None = None,
    attempt_count: int | None = None,
    extra_payload: dict[str, object] | None = None,
) -> None:
    payload: dict[str, object] = {
        "subtype": str(subtype or "").strip() or "unknown",
        "detail": str(detail or "").strip(),
    }
    if message_uid is not None:
        payload["message_uid"] = int(message_uid)
    if attempt_count is not None:
        payload["attempt_count"] = int(attempt_count)
    if extra_payload:
        payload.update(extra_payload)
    _emit_contract_event(
        event_type=EventType.IMAP_HEALTH,
        ts_utc=ts_utc or datetime.now(timezone.utc).timestamp(),
        account_id=account_id,
        email_id=email_id,
        payload=payload,
        entity_id=f"imap_health:{payload['subtype']}",
    )


def _persist_inbound_and_enqueue_parse(
    *,
    storage: Storage,
    account_email: str,
    uid: int,
    message_id: str | None,
    from_email: str | None,
    from_name: str | None,
    subject: str | None,
    received_at: str | None,
    attachments_count: int,
    raw_email: bytes,
    inbound: InboundMessage,
    uidvalidity: int | None = None,
    dedupe_message_id: bool = False,
) -> tuple[int, bool]:
    normalized_account_email = normalize_login(account_email)
    # Email row and PARSE queue row in one transaction (F5). uidvalidity lets
    # a restart refetch the message by UID (WP2b). In a UIDVALIDITY resync a
    # settled Message-ID is a duplicate and an orphan is re-attached (WP2c).
    email_id, created = storage.persist_email_and_enqueue(
        account_email=normalized_account_email,
        uid=uid,
        message_id=message_id,
        from_email=from_email,
        from_name=from_name,
        subject=subject,
        received_at=received_at,
        attachments_count=attachments_count,
        uidvalidity=uidvalidity,
        dedupe_message_id=dedupe_message_id,
    )
    if not created:
        logger.info(
            "duplicate_ingest_skipped account_email=%s uid=%s email_id=%s",
            normalized_account_email,
            uid,
            email_id,
        )
        return email_id, False

    ctx = PipelineContext(
        email_id=email_id,
        account_email=normalized_account_email,
        uid=uid,
    )
    PIPELINE_CACHE[email_id] = ctx
    remember_raw_email(email_id, raw_email)
    store_inbound(email_id, inbound)
    return email_id, True


# D-P2-1: at most one aggregated dead-letter alert per account in this window.
DEAD_LETTER_ALERT_INTERVAL = timedelta(hours=24)
# Emails dead-lettered at CONTEXT (WP2b) per normalized login since that
# account's last poll; the poll folds them into its aggregated alert.
_CONTEXT_DEAD_LETTERS: dict[str, int] = {}


def _known_uid_probe(
    storage: Storage | None, account_email: str
) -> Callable[[int], bool] | None:
    """is_known_uid for fetch_new_messages(): True once the UID is stored."""
    if storage is None:
        return None
    normalized_account_email = normalize_login(account_email)
    return lambda uid: storage.find_email_id(normalized_account_email, uid) is not None


def _known_message_id_probe(
    storage: Storage | None, account_email: str
) -> Callable[[str], bool] | None:
    """is_known_message_id for fetch_new_messages(): True once an email with
    this Message-ID is settled for the account, under any UID (WP2c)."""
    if storage is None:
        return None
    normalized_account_email = normalize_login(account_email)
    return lambda message_id: storage.is_known_message_id(
        normalized_account_email, message_id
    )


def _retire_previous_uid_epoch(
    *, storage: Storage | None, imap: object, login: str
) -> int:
    """After a UIDVALIDITY change, detach stored rows of older epochs from
    their UIDs before the poll persists anything (WP2c, F6): the new epoch
    reuses UIDs. Returns the number of rows retired. A storage error
    propagates, so the poll fails and the cursor stays (retried next poll).
    """
    if storage is None or not getattr(imap, "last_uidvalidity_changed", False):
        return 0
    # getattr: test doubles of ResilientIMAP lack it (F10).
    uidvalidity = getattr(imap, "current_uidvalidity", None)
    if uidvalidity is None:
        logger.warning(
            "imap_uid_epoch_retire_skipped account=%s reason=uidvalidity_unknown",
            login,
        )
        return 0
    retired = storage.retire_uid_epoch(normalize_login(login), int(uidvalidity))
    logger.warning(
        "imap_uid_epoch_retired account=%s uidvalidity=%s rows=%s",
        login,
        uidvalidity,
        retired,
    )
    return retired


def _dead_letter_alert_text(
    *, locale: str, masked_login: str, count: int, context_count: int = 0
) -> str:
    en = [f"Mailbox {masked_login}: some new mail could not be loaded"]
    ru = [f"Почта {masked_login}: часть новых писем не удалось загрузить"]
    if count:
        en.append(f"Skipped after {MAX_UID_FETCH_ATTEMPTS} attempts: {count}")
        ru.append(f"Пропущено после {MAX_UID_FETCH_ATTEMPTS} попыток: {count}")
    if context_count:
        en.append(f"Not found in the mailbox after a restart: {context_count}")
        ru.append(f"Не найдено в ящике после перезапуска: {context_count}")
    en.append("Check your mail client. Details: Health page of the web panel.")
    ru.append("Проверьте почтовый клиент. Подробности: страница Health в веб-панели.")
    return _ui_text_for_locale(locale, en="\n".join(en), ru="\n".join(ru))


def _maybe_send_dead_letter_alert(
    *,
    state: StateManager,
    account: AccountConfig,
    login: str,
    dead_count: int,
    bot_token: str,
    now_utc: datetime,
    context_dead_count: int = 0,
) -> bool:
    """One aggregated alert per account per 24 h for dead-lettered mail.

    Count only, no subjects or senders (D-P2-1). The window starts when an
    alert is delivered, so an undelivered one is retried on the next dead letter.
    context_dead_count: emails lost after a restart (WP2b, stage CONTEXT).
    """
    if not account.telegram_chat_id:
        digest_logger.warning(
            "imap_dead_letter_alert_skipped",
            account_id=account.account_id,
            reason="no_chat_id",
            dead_letters=dead_count,
            context_dead_letters=context_dead_count,
        )
        return False
    last_sent = state.get_dead_letter_alert_at(login)
    if last_sent is not None and now_utc - last_sent < DEAD_LETTER_ALERT_INTERVAL:
        digest_logger.info(
            "imap_dead_letter_alert_suppressed",
            account_id=account.account_id,
            dead_letters=dead_count,
            context_dead_letters=context_dead_count,
            last_alert_at=last_sent.isoformat(),
        )
        return False
    text = _dead_letter_alert_text(
        locale=processor_module._resolve_outbound_ui_locale(),
        masked_login=_mask_login(account.username or account.login),
        count=dead_count,
        context_count=context_dead_count,
    )
    payload = _build_system_payload(
        text=text,
        bot_token=bot_token,
        chat_id=account.telegram_chat_id,
        priority="🔴",
    )
    result = send_telegram(payload)
    delivered = bool(getattr(result, "delivered", False))
    if delivered:
        state.set_dead_letter_alert_at(login, now_utc)
    digest_logger.warning(
        "imap_dead_letter_alert_sent",
        account_id=account.account_id,
        login=login,
        dead_letters=dead_count,
        context_dead_letters=context_dead_count,
        delivered=delivered,
    )
    return delivered


def _commit_imap_cursor(
    *,
    imap: object,
    persisted_uids: list[int],
    account: AccountConfig,
    login: str,
    state: StateManager,
    bot_token: str,
) -> None:
    """Move the IMAP cursor once the poll's messages are stored (WP2a)."""
    # getattr: test doubles of ResilientIMAP only implement fetch_new_messages.
    commit_cursor = getattr(imap, "commit_cursor", None)
    if not callable(commit_cursor):
        return
    commit = commit_cursor(persisted_uids)
    dead_uids = tuple(commit.dead_uids) if commit is not None else ()
    # Context dead letters since this account's last poll join its alert.
    context_dead = _CONTEXT_DEAD_LETTERS.pop(normalize_login(login), 0)
    if not dead_uids and not context_dead:
        return
    now_utc = datetime.now(timezone.utc)
    for uid in dead_uids:
        logger.error(
            "imap_uid_dead_lettered account=%s uid=%s attempts=%s",
            login,
            uid,
            MAX_UID_FETCH_ATTEMPTS,
        )
        _emit_imap_health_event(
            account_id=login,
            subtype="dead_letter",
            detail=f"FETCH dead-lettered after {MAX_UID_FETCH_ATTEMPTS} attempts",
            ts_utc=now_utc.timestamp(),
            message_uid=uid,
            attempt_count=MAX_UID_FETCH_ATTEMPTS,
            extra_payload={"stage": "FETCH"},
        )
    try:
        _maybe_send_dead_letter_alert(
            state=state,
            account=account,
            login=login,
            dead_count=len(dead_uids),
            bot_token=bot_token,
            now_utc=now_utc,
            context_dead_count=context_dead,
        )
    except Exception:
        # The poll itself succeeded; a failed alert must not trigger IMAP backoff.
        logger.exception("imap_dead_letter_alert_failed account=%s", login)


def _build_telegram_delivery_key(
    *,
    email_id: int,
    kind: str = "email",
    snooze_ts: str | None = None,
) -> str:
    base_key = f"email:{email_id}"
    if kind == "email":
        return base_key
    if kind == "snooze":
        if not snooze_ts:
            raise ValueError("snooze_ts is required for snooze delivery key")
        return f"snooze:{base_key}:{snooze_ts}"
    return f"{kind}:{base_key}"


def _new_email_delivery_guard(
    *,
    storage: Storage,
    config: BotConfig,
    ctx: PipelineContext,
    email_id: int,
) -> EmailDeliveryGuard:
    account = _get_account_by_login(config, ctx.account_email)
    return EmailDeliveryGuard(
        storage,
        email_id=email_id,
        account_id=ctx.account_email,
        chat_id=account.telegram_chat_id if account else None,
        delivery_key=_build_telegram_delivery_key(email_id=email_id, kind="email"),
    )


def _settle_write(action, *args, email_id: int, what: str) -> None:
    """Status write after a send: log a failure, never retry the send for it."""
    try:
        action(*args)
    except Exception:
        logger.exception(
            "telegram_delivery_settle_failed email_id=%s write=%s", email_id, what
        )


def _update_telegram_health(
    guard: EmailDeliveryGuard,
    ctx: PipelineContext,
    *,
    available: bool,
    reason: str | None = None,
) -> None:
    # Same component the premium processor updates, so a later legacy success
    # also clears a degraded mode set by an unconfirmed delivery.
    change = processor_module.system_health.update_component(
        "Telegram", available, reason=reason
    )
    processor_module._notify_system_mode_change(
        change=change,
        chat_id=guard.chat_id or "",
        account_email=ctx.account_email,
    )


def _record_delivery_unknown(
    *,
    storage: Storage,
    guard: EmailDeliveryGuard,
    ctx: PipelineContext,
    attempts: int,
    emit_events: bool,
) -> None:
    email_id = guard.email_id
    error_class = delivery_unknown_error_class(guard.error)
    _settle_write(
        storage.set_email_delivery_unknown,
        email_id,
        f"{DELIVERY_UNKNOWN}:{error_class}",
        email_id=email_id,
        what="set_email_delivery_unknown",
    )
    logger.error(
        "telegram_delivery_unknown email_id=%s attempts=%s error_class=%s",
        email_id,
        attempts,
        error_class,
    )
    if not emit_events:
        return  # the premium processor already reported it
    _update_telegram_health(guard, ctx, available=False, reason=DELIVERY_UNKNOWN)
    payload: dict[str, object] = {
        "attempts": attempts,
        "error": DELIVERY_UNKNOWN,
        "delivery_state": "unknown",
        "error_class": error_class,
    }
    event_emitter.emit(
        type="telegram_delivery_failed",
        timestamp=datetime.now(timezone.utc),
        email_id=email_id,
        payload=dict(payload),
    )
    _emit_contract_event(
        event_type=EventType.TELEGRAM_FAILED,
        ts_utc=datetime.now(timezone.utc).timestamp(),
        account_id=ctx.account_email,
        email_id=email_id,
        payload={**payload, "delivered": False},
    )


def _record_delivery_failed(
    *,
    storage: Storage,
    config: BotConfig,
    guard: EmailDeliveryGuard,
    ctx: PipelineContext,
    attempts: int,
) -> None:
    email_id = guard.email_id
    error = guard.error or "telegram delivery failed"
    account = _get_account_by_login(config, ctx.account_email)
    if account:
        locale = processor_module._resolve_outbound_ui_locale()
        notice = _telegram_delivery_failed_notice(locale=locale)
        payload = _build_system_payload(
            text=notice,
            bot_token=config.keys.telegram_bot_token,
            chat_id=account.telegram_chat_id,
            priority="🔴",
        )
        fallback_result = send_telegram(payload)
        if not fallback_result.delivered:
            logger.error(
                "telegram_delivery_failed_notice_failed email_id=%s attempts=%s locale=%s error=%s",
                email_id,
                attempts,
                locale,
                fallback_result.error or "",
            )
    _settle_write(
        storage.set_email_delivery_failed,
        email_id,
        error,
        email_id=email_id,
        what="set_email_delivery_failed",
    )
    error_class = "DeliveryError"
    logger.error(
        "telegram_delivery_failed email_id=%s attempts=%s error_class=%s error=%s",
        email_id,
        attempts,
        error_class,
        error,
    )
    event_emitter.emit(
        type="telegram_delivery_failed",
        timestamp=datetime.now(timezone.utc),
        email_id=email_id,
        payload={
            "attempts": attempts,
            "error": error,
            "error_class": error_class,
        },
    )


def _settle_email_delivery(
    *,
    storage: Storage,
    config: BotConfig,
    guard: EmailDeliveryGuard,
    ctx: PipelineContext,
    queue_id: int | None,
    attempts: int,
    emit_events: bool,
    retryable_is_final: bool = False,
) -> bool:
    """Finish the email by guard.outcome; False: nothing final, caller continues.

    emit_events=False for the premium processor, which reports its own
    delivered/unknown events. A failed mark_done propagates so the queue
    retries the bookkeeping; the guard keeps that retry from posting again.
    """
    email_id = guard.email_id
    outcome = guard.outcome
    if outcome == "retryable" and retryable_is_final:
        outcome = "failed"
    if outcome == "delivered":
        if queue_id is not None:
            storage.mark_done(queue_id)
        _cleanup_pipeline_cache(email_id)
        if emit_events:
            _update_telegram_health(guard, ctx, available=True)
            event_emitter.emit(
                type="telegram_delivery_succeeded",
                timestamp=datetime.now(timezone.utc),
                email_id=email_id,
                payload={"attempt": attempts},
            )
            _emit_contract_event(
                event_type=EventType.TELEGRAM_DELIVERED,
                ts_utc=datetime.now(timezone.utc).timestamp(),
                account_id=ctx.account_email,
                email_id=email_id,
                payload={"attempt": attempts},
            )
        return True
    if outcome == "duplicate":
        if queue_id is not None:
            storage.mark_done(queue_id)
        _cleanup_pipeline_cache(email_id)
        logger.info(
            "telegram_delivery_skipped_duplicate account_id=%s email_id=%s delivery_key=%s",
            ctx.account_email,
            email_id,
            guard.delivery_key,
        )
        event_emitter.emit(
            type="telegram_delivery_skipped_duplicate",
            timestamp=datetime.now(timezone.utc),
            email_id=email_id,
            payload={"delivery_key": guard.delivery_key, "kind": "email"},
        )
        return True
    if outcome == "unknown":
        _record_delivery_unknown(
            storage=storage,
            guard=guard,
            ctx=ctx,
            attempts=attempts,
            emit_events=emit_events,
        )
        if queue_id is not None:
            storage.mark_done(queue_id)
        _cleanup_pipeline_cache(email_id)
        return True
    if outcome == "failed":
        _record_delivery_failed(
            storage=storage,
            config=config,
            guard=guard,
            ctx=ctx,
            attempts=attempts,
        )
        if queue_id is not None:
            storage.mark_done(queue_id)
        _cleanup_pipeline_cache(email_id)
        return True
    return False


# A context refetch retries its IMAP session once: a queue pass must not
# block on a dead server (the queue item retries with backoff).
CONTEXT_REFETCH_IMAP_RETRIES = 1


class PipelineContextUnavailable(Exception):
    """The queued email cannot be refetched from IMAP (gone or not the same
    message): it is dead-lettered, never sent fail-open (no content)."""

    def __init__(self, reason: str, *, account_email: str, uid: int) -> None:
        super().__init__(f"context_unavailable:{reason}")
        self.reason = reason
        self.account_email = account_email
        self.uid = uid


def _rebuild_pipeline_context(
    *,
    storage: Storage,
    config: BotConfig,
    email_id: int,
    stage: str,
) -> PipelineContext | None:
    """Rebuild a pipeline context lost with the in-memory caches (restart).

    Refetches the message from IMAP (WP2b), parses it and fills the caches;
    a TG item runs the LLM stage inline first (no premium re-run). None when
    the email is not stored (the "no pipeline context" retry path). A
    network error propagates, so the item is retried; a message that is gone
    or does not match raises PipelineContextUnavailable.
    """
    locator = storage.get_email_locator(email_id)
    if locator is None:
        return None
    account_email = str(locator["account_email"])
    uid = int(locator["uid"])
    account = _get_account_by_login(config, account_email)
    if account is None:
        raise PipelineContextUnavailable(
            "account_not_configured", account_email=account_email, uid=uid
        )
    imap = ResilientIMAP(
        account,
        state=None,
        max_email_mb=config.general.max_email_mb,
        retries=CONTEXT_REFETCH_IMAP_RETRIES,
    )
    refetch = imap.fetch_message_by_uid(
        uid,
        expected_uidvalidity=locator["uidvalidity"],
        expected_message_id=locator["message_id"],
    )
    logger.info(
        "pipeline_context_refetch email_id=%s stage=%s uid=%s found_uid=%s reason=%s",
        email_id,
        stage,
        uid,
        refetch.uid,
        refetch.reason,
    )
    if refetch.raw is None:
        raise PipelineContextUnavailable(
            refetch.reason, account_email=account_email, uid=uid
        )
    inbound = parse_raw_email(refetch.raw, config)
    ctx = PipelineContext(email_id=email_id, account_email=account_email, uid=uid)
    remember_raw_email(email_id, refetch.raw)
    store_inbound(email_id, inbound)
    try:
        if stage in ("LLM", "TG"):
            stage_parse(ctx)
        if stage == "TG":
            stage_llm(ctx)
    except Exception:
        # A half-built context must not be reused by the retry.
        _cleanup_pipeline_cache(email_id)
        raise
    PIPELINE_CACHE[email_id] = ctx
    return ctx


def _dead_letter_unavailable_context(
    *,
    storage: Storage,
    queue_id: int,
    email_id: int,
    stage: str,
    attempts: int,
    lost: PipelineContextUnavailable,
) -> None:
    """WP2b: a queued email that cannot be refetched after a restart.

    No content means no fail-open send: the email turns ERROR, IMAP_HEALTH
    shows it (dead_letter, stage CONTEXT) and the account's next poll folds
    it into the aggregated alert (D-P2-1).
    """
    login = normalize_login(lost.account_email)
    storage.set_email_error(email_id, str(lost))
    storage.mark_done(queue_id)
    _cleanup_pipeline_cache(email_id)
    _CONTEXT_DEAD_LETTERS[login] = _CONTEXT_DEAD_LETTERS.get(login, 0) + 1
    logger.error(
        "pipeline_context_dead_lettered email_id=%s stage=%s uid=%s reason=%s",
        email_id,
        stage,
        lost.uid,
        lost.reason,
    )
    _emit_imap_health_event(
        account_id=login,
        subtype="dead_letter",
        detail=f"CONTEXT dead-lettered: {lost.reason}",
        email_id=email_id,
        message_uid=lost.uid,
        attempt_count=attempts,
        extra_payload={"stage": "CONTEXT", "queue_stage": stage, "reason": lost.reason},
    )


def _finish_reserved_queue_item(
    *,
    storage: Storage,
    queue_id: int,
    email_id: int,
    stage: str,
    attempts: int,
    ctx: PipelineContext | None,
) -> bool:
    """A19: True when the email's delivery is already reserved and the item is done.

    Runs right after claim_next, before any context work. A confirmed delivery
    finishes the item without sending; an unconfirmed reservation (crash between
    the reservation and the POST, or a failed release) becomes DELIVERY_UNKNOWN.
    Neither is dead-lettered nor gets a "delivery failed" notice. A failed
    status write propagates so the caller retries the bookkeeping.
    """
    delivery_key = _build_telegram_delivery_key(email_id=email_id, kind="email")
    try:
        state = storage.get_telegram_delivery_state(delivery_key, email_id)
    except Exception:
        # The delivery guard still fails closed before any POST.
        logger.exception(
            "telegram_delivery_claim_check_failed email_id=%s stage=%s",
            email_id,
            stage,
        )
        return False
    if state is None:
        return False
    if state == "confirmed":
        storage.mark_done(queue_id)
        _cleanup_pipeline_cache(email_id)
        logger.info(
            "telegram_delivery_skipped_duplicate email_id=%s stage=%s delivery_key=%s source=claim_check",
            email_id,
            stage,
            delivery_key,
        )
        event_emitter.emit(
            type="telegram_delivery_skipped_duplicate",
            timestamp=datetime.now(timezone.utc),
            email_id=email_id,
            payload={"delivery_key": delivery_key, "kind": "email", "stage": stage},
        )
        return True
    # Same class the delivery guard reports for an unconfirmed reservation.
    error_class = delivery_unknown_error_class(DUPLICATE_SUPPRESSED)
    storage.set_email_delivery_unknown(email_id, f"{DELIVERY_UNKNOWN}:{error_class}")
    storage.mark_done(queue_id)
    _cleanup_pipeline_cache(email_id)
    logger.error(
        "telegram_delivery_unknown email_id=%s stage=%s attempts=%s error_class=%s source=claim_check",
        email_id,
        stage,
        attempts,
        error_class,
    )
    # No Telegram health change: the reservation is from an earlier run.
    payload: dict[str, object] = {
        "attempts": attempts,
        "error": DELIVERY_UNKNOWN,
        "delivery_state": "unknown",
        "error_class": error_class,
        "stage": stage,
    }
    event_emitter.emit(
        type="telegram_delivery_failed",
        timestamp=datetime.now(timezone.utc),
        email_id=email_id,
        payload=dict(payload),
    )
    account_id = ctx.account_email if ctx is not None else None
    if not account_id:
        try:
            account_id = storage.get_email_account(email_id)
        except Exception:
            logger.exception(
                "telegram_delivery_account_lookup_failed email_id=%s", email_id
            )
    _emit_contract_event(
        event_type=EventType.TELEGRAM_FAILED,
        ts_utc=datetime.now(timezone.utc).timestamp(),
        account_id=account_id or "unknown",
        email_id=email_id,
        payload={**payload, "delivered": False},
    )
    return True


def _acquire_instance_lock_or_exit(db_path: Path) -> InstanceLock:
    """Single worker per database: a second instance would notify twice."""
    try:
        lock = acquire_instance_lock(db_path)
    except InstanceLockError as exc:
        logger.error("single_instance_lock_failed error=%s", exc)
        print(f"[ERROR] Cannot check whether Letterbot is already running: {exc}")
        sys.exit(2)
    if lock is None:
        lock_path = instance_lock_path(db_path)
        logger.error("single_instance_lock_held lock=%s", lock_path)
        print(
            f"[ERROR] Letterbot is already running (lock: {lock_path}). "
            "Close the other Letterbot window, then start again."
        )
        sys.exit(2)
    logger.info("single_instance_lock_acquired lock=%s", lock.path)
    return lock


def _process_queue(
    storage: Storage,
    config: BotConfig,
    processor: MessageProcessor,
    flags: FeatureFlags,
) -> None:
    _process_due_snoozes_guarded(storage=storage, config=config)
    max_tg_attempts = MAX_TELEGRAM_STAGE_ATTEMPTS
    max_pipeline_attempts = MAX_PIPELINE_STAGE_ATTEMPTS
    while True:
        item = storage.claim_next(["PARSE", "LLM", "TG"])
        if not item:
            break

        queue_id = item["queue_id"]
        email_id = item["email_id"]
        stage = item["stage"]
        attempts = item["attempts"]
        delivery_guard: EmailDeliveryGuard | None = None
        ctx = PIPELINE_CACHE.get(email_id)
        try:
            if _finish_reserved_queue_item(
                storage=storage,
                queue_id=queue_id,
                email_id=email_id,
                stage=stage,
                attempts=attempts,
                ctx=ctx,
            ):
                continue
        except Exception as settle_exc:
            # Telegram already has (or may have) the message: retry only the
            # bookkeeping, never dead-letter or notify for it.
            logger.exception(
                "telegram_delivery_settle_failed email_id=%s stage=%s source=claim_check",
                email_id,
                stage,
            )
            try:
                storage.mark_error(
                    queue_id, str(settle_exc), _queue_backoff_seconds(attempts)
                )
            except Exception:
                logger.exception("Failed to mark error for queue_id %s", queue_id)
            continue
        if ctx:
            ctx.attempts = attempts  # type: ignore[attr-defined]

        try:
            print(f"[QUEUE] Claimed {stage} for email_id={email_id} attempt={attempts}")
            if ctx is None:
                try:
                    ctx = _rebuild_pipeline_context(
                        storage=storage, config=config, email_id=email_id, stage=stage
                    )
                except PipelineContextUnavailable as lost:
                    _dead_letter_unavailable_context(
                        storage=storage,
                        queue_id=queue_id,
                        email_id=email_id,
                        stage=stage,
                        attempts=attempts,
                        lost=lost,
                    )
                    continue
                if ctx is not None:
                    ctx.attempts = attempts  # type: ignore[attr-defined]
            if ctx is None:
                raise RuntimeError(f"No pipeline context for email {email_id}")

            if stage == "PARSE":
                premium_pipeline_enabled = bool(flags.ENABLE_PREMIUM_PROCESSOR)
                if premium_pipeline_enabled:
                    inbound = PIPELINE_INBOUND_CACHE.get(email_id)
                    raw = PIPELINE_RAW_CACHE.get(email_id)
                    if inbound is not None:
                        delivery_guard = _new_email_delivery_guard(
                            storage=storage, config=config, ctx=ctx, email_id=email_id
                        )
                        try:
                            _run_premium_processor(
                                ctx=ctx,
                                inbound=inbound,
                                raw=raw,
                                config=config,
                                delivery_guard=delivery_guard,
                            )
                        except Exception as exc:
                            logger.exception(
                                "premium_processor_failed email_id=%s error=%s",
                                email_id,
                                exc,
                            )
                        # Settle by what reached Telegram, not by how premium ended.
                        if _settle_email_delivery(
                            storage=storage,
                            config=config,
                            guard=delivery_guard,
                            ctx=ctx,
                            queue_id=queue_id,
                            attempts=attempts,
                            emit_events=False,
                        ):
                            continue
                        # Nothing final was sent (no-send branch or retryable):
                        # the legacy chain notifies.
                        logger.warning(
                            "premium_processor_fallback email_id=%s outcome=%s error=%s",
                            email_id,
                            delivery_guard.outcome,
                            delivery_guard.error or "",
                        )
                llm_not_before: str | None = None
                retry_after = (
                    delivery_guard.retry_after if delivery_guard is not None else None
                )
                if retry_after:
                    # Telegram asked to wait (429): the legacy chain starts after it.
                    llm_not_before = _not_before_after(retry_after)
                    logger.warning(
                        "premium_fallback_delayed email_id=%s retry_after_seconds=%s not_before_utc=%s",
                        email_id,
                        retry_after,
                        llm_not_before,
                    )
                stage_parse(ctx)
                storage.mark_done(queue_id)
                storage.enqueue_stage(email_id, "LLM", not_before=llm_not_before)
            elif stage == "LLM":
                stage_llm(ctx)
                storage.mark_done(queue_id)
                storage.enqueue_stage(email_id, "TG")
            elif stage == "TG":
                delivery_guard = _new_email_delivery_guard(
                    storage=storage, config=config, ctx=ctx, email_id=email_id
                )
                if delivery_guard.before_send() is None:
                    try:
                        result = stage_tg(ctx)
                    except Exception:
                        # Raised before the POST (rendering): nothing was sent.
                        delivery_guard.abort()
                        raise
                    delivery_guard.after_send(result)
                if not _settle_email_delivery(
                    storage=storage,
                    config=config,
                    guard=delivery_guard,
                    ctx=ctx,
                    queue_id=queue_id,
                    attempts=attempts,
                    emit_events=True,
                ):
                    # Definitely not sent (or no reservation): TG backoff retries.
                    raise TelegramRetryableError(
                        delivery_guard.error or "telegram delivery failed",
                        retry_after=delivery_guard.retry_after,
                    )
            else:
                storage.mark_done(queue_id)
                _cleanup_pipeline_cache(email_id)
                logger.warning("Unknown stage %s for email %s", stage, email_id)
        except Exception as queue_exc:
            logger.exception("Queue handling error for email %s", email_id)
            backoff = _queue_backoff_seconds(attempts, queue_exc)
            if delivery_guard is not None and delivery_guard.is_settled:
                # Telegram already has (or may have) the message; only the
                # bookkeeping failed. Retry that later; the guard blocks a resend.
                logger.error(
                    "telegram_delivery_settle_failed email_id=%s stage=%s outcome=%s error=%s",
                    email_id,
                    stage,
                    delivery_guard.outcome,
                    queue_exc,
                )
                try:
                    storage.mark_error(queue_id, str(queue_exc), backoff)
                except Exception:
                    logger.exception("Failed to mark error for queue_id %s", queue_id)
                continue
            if stage == "TG":
                if attempts >= max_tg_attempts:
                    error_class = queue_exc.__class__.__name__
                    try:
                        storage.set_email_delivery_failed(email_id, str(queue_exc))
                        storage.mark_done(queue_id)
                    except Exception:
                        logger.exception(
                            "Failed to mark delivery failed for queue_id %s", queue_id
                        )
                    _cleanup_pipeline_cache(email_id)
                    account = (
                        _get_account_by_login(config, ctx.account_email)
                        if ctx
                        else None
                    )
                    if account:
                        locale = processor_module._resolve_outbound_ui_locale()
                        notice = _telegram_delivery_failed_detail(
                            locale=locale,
                            email_id=email_id,
                            account_email=ctx.account_email,
                            error_text=str(queue_exc),
                        )
                        payload = _build_system_payload(
                            text=notice,
                            bot_token=config.keys.telegram_bot_token,
                            chat_id=account.telegram_chat_id,
                            priority="🔴",
                        )
                        result = send_telegram(payload)
                        if not result.delivered:
                            logger.error(
                                "telegram_delivery_failed_notice_failed email_id=%s attempts=%s locale=%s error=%s",
                                email_id,
                                attempts,
                                locale,
                                result.error or "",
                            )
                    logger.error(
                        "telegram_delivery_failed email_id=%s attempts=%s error_class=%s error=%s",
                        email_id,
                        attempts,
                        error_class,
                        queue_exc,
                    )
                    event_emitter.emit(
                        type="telegram_delivery_failed",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={
                            "attempts": attempts,
                            "error": str(queue_exc),
                            "error_class": error_class,
                        },
                    )
                    if ctx:
                        _emit_contract_event(
                            event_type=EventType.TELEGRAM_FAILED,
                            ts_utc=datetime.now(timezone.utc).timestamp(),
                            account_id=ctx.account_email,
                            email_id=email_id,
                            payload={
                                "attempts": attempts,
                                "error": str(queue_exc),
                                "error_class": error_class,
                            },
                        )
                else:
                    retry_ts = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                    error_class = queue_exc.__class__.__name__
                    try:
                        storage.mark_error(queue_id, str(queue_exc), backoff)
                    except Exception:
                        logger.exception(
                            "Failed to mark error for queue_id %s", queue_id
                        )
                    logger.info(
                        "telegram_delivery_retry email_id=%s attempt=%s backoff_seconds=%s error_class=%s next_retry_at_utc=%s",
                        email_id,
                        attempts,
                        backoff,
                        error_class,
                        retry_ts.isoformat(),
                    )
                    event_emitter.emit(
                        type="telegram_delivery_retry",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={
                            "attempt": attempts,
                            "backoff_seconds": backoff,
                            "error": str(queue_exc),
                            "error_class": error_class,
                            "next_retry_at_utc": retry_ts.isoformat(),
                        },
                    )
                continue
            account_id = ctx.account_email if ctx else ""
            _emit_imap_health_event(
                account_id=account_id or "unknown",
                subtype="processing_failure",
                detail=f"{stage} failed: {queue_exc}",
                email_id=email_id,
                message_uid=getattr(ctx, "uid", None),
                attempt_count=attempts,
                extra_payload={"stage": stage, "error": str(queue_exc)},
            )
            if attempts >= max_pipeline_attempts:
                try:
                    storage.set_email_error(email_id, str(queue_exc))
                    storage.mark_done(queue_id)
                except Exception:
                    logger.exception(
                        "Failed to dead-letter queue_id %s for email %s",
                        queue_id,
                        email_id,
                    )
                _emit_imap_health_event(
                    account_id=account_id or "unknown",
                    subtype="dead_letter",
                    detail=f"{stage} dead-lettered after {attempts} attempts",
                    email_id=email_id,
                    message_uid=getattr(ctx, "uid", None),
                    attempt_count=attempts,
                    extra_payload={"stage": stage, "error": str(queue_exc)},
                )
                _fail_open_process(config, processor, ctx, storage=storage)
                _cleanup_pipeline_cache(email_id)
                continue
            try:
                storage.mark_error(queue_id, str(queue_exc), backoff)
            except Exception:
                logger.exception("Failed to mark error for queue_id %s", queue_id)
            logger.warning(
                "pipeline_stage_retry_scheduled email_id=%s stage=%s attempts=%s backoff_seconds=%s",
                email_id,
                stage,
                attempts,
                backoff,
            )
    _process_due_snoozes_guarded(storage=storage, config=config)


def _render_snooze_reminder_text(
    storage: Storage, *, email_id: int, reminder_text: str, locale: str
) -> str:
    from mailbot_v26.pipeline import tg_renderer

    existing = str(reminder_text or "").strip()

    def _stored_text_matches_locale(text: str) -> bool:
        is_english = str(locale or "").strip().startswith("en")
        normalized = str(text or "")
        if not normalized:
            return False
        if is_english:
            return not any(
                marker in normalized
                for marker in (
                    " от ",
                    "Аккаунт:",
                    "Ответить",
                    "Проверить",
                    "Действий не требуется",
                    "Приоритет:",
                )
            )
        return not any(
            marker in normalized
            for marker in (
                " from ",
                "Account:",
                "Reply",
                "Review",
                "No action needed",
                "Priority:",
            )
        )

    if existing and _stored_text_matches_locale(existing):
        return existing
    try:
        columns = {
            str(row[1]) for row in storage.conn.execute("PRAGMA table_info(emails)").fetchall()
        }
    except sqlite3.OperationalError:
        columns = set()
    select_fields = [
        field
        for field in ("priority", "from_email", "subject", "action_line", "account_email")
        if field in columns
    ]
    row_map: dict[str, object] = {}
    if select_fields:
        row = storage.conn.execute(
            f"""
            SELECT {", ".join(select_fields)}
            FROM emails
            WHERE id = ?
            """,
            (email_id,),
        ).fetchone()
        if row:
            row_map = dict(zip(select_fields, row))
    if not row_map:
        if existing:
            return existing
        fallback = "Email" if str(locale or "").strip().startswith("en") else "Письмо"
        return f"{fallback} #{email_id}"
    priority = str(row_map.get("priority") or "🔵")
    from_email = str(row_map.get("from_email") or "")
    subject = str(row_map.get("subject") or "")
    action_line = str(row_map.get("action_line") or "")
    account_email = str(row_map.get("account_email") or "")
    reminder = tg_renderer.render_telegram_message(
        priority=priority,
        from_email=from_email,
        subject=subject,
        action_line=action_line,
        summary=None,
        attachments=[],
        locale=locale,
    )
    return tg_renderer.finalize_telegram_message(
        text=reminder,
        priority=priority,
        account_email=account_email,
        locale=locale,
    )


def _build_snooze_return_text(
    storage: Storage,
    *,
    email_id: int,
    reminder_text: str,
    snoozed_at_utc: str = "",
    locale: str,
    now_utc: datetime | None = None,
    local_tz: timezone | None = None,
) -> str:
    """Prefix a due snooze reminder with Smart Return context.

    ``now_utc`` defaults to the current time and ``local_tz`` (the user's
    calendar for "yesterday") to the machine's local time zone.
    """

    def _parse_utc(raw_value: str) -> datetime | None:
        value = str(raw_value or "").strip()
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _render_delay(now_utc: datetime, snoozed_at: datetime | None) -> str:
        is_english = str(locale or "").strip().startswith("en")
        if snoozed_at is None:
            return ""
        delta_seconds = max(int((now_utc - snoozed_at).total_seconds()), 0)
        if delta_seconds < 60:
            return "just now" if is_english else "только что"
        minutes = delta_seconds // 60
        if minutes < 60:
            if is_english:
                unit = "minute" if minutes == 1 else "minutes"
                return f"{minutes} {unit} ago"
            if minutes % 10 == 1 and minutes % 100 != 11:
                suffix = "минуту"
            elif minutes % 10 in {2, 3, 4} and minutes % 100 not in {12, 13, 14}:
                suffix = "минуты"
            else:
                suffix = "минут"
            return f"{minutes} {suffix} назад"
        hours = delta_seconds // 3600
        # "Yesterday" is the user's local calendar day: UTC dates disagree with
        # it between local midnight and UTC midnight. Past 48 hours it never
        # applies, which also keeps absurd stored dates away from astimezone().
        if hours < 48:
            local_today = now_utc.astimezone(local_tz).date()
            local_snooze_day = snoozed_at.astimezone(local_tz).date()
            if (local_today - local_snooze_day).days == 1:
                return "yesterday" if is_english else "вчера"
        if hours < 24:
            if is_english:
                unit = "hour" if hours == 1 else "hours"
                return f"{hours} {unit} ago"
            if hours % 10 == 1 and hours % 100 != 11:
                suffix = "час"
            elif hours % 10 in {2, 3, 4} and hours % 100 not in {12, 13, 14}:
                suffix = "часа"
            else:
                suffix = "часов"
            return f"{hours} {suffix} назад"
        days = delta_seconds // 86400
        if is_english:
            unit = "day" if days == 1 else "days"
            return f"{days} {unit} ago"
        if days % 10 == 1 and days % 100 != 11:
            suffix = "день"
        elif days % 10 in {2, 3, 4} and days % 100 not in {12, 13, 14}:
            suffix = "дня"
        else:
            suffix = "дней"
        return f"{days} {suffix} назад"

    def _load_snooze_count() -> int:
        try:
            row = storage.conn.execute(
                """
                SELECT COUNT(*)
                FROM events_v1
                WHERE event_type = ?
                  AND email_id = ?
                """,
                (EventType.SNOOZE_RECORDED.value, email_id),
            ).fetchone()
            if row:
                return int(row[0] or 0)
        except sqlite3.OperationalError:
            pass
        row = storage.conn.execute(
            "SELECT COUNT(*) FROM telegram_snooze WHERE email_id = ?",
            (email_id,),
        ).fetchone()
        return int(row[0] or 0) if row else 0

    existing = _render_snooze_reminder_text(
        storage,
        email_id=email_id,
        reminder_text=reminder_text,
        locale=locale,
    )
    prefix_lines: list[str] = []
    delay_text = _render_delay(
        now_utc if now_utc is not None else datetime.now(timezone.utc),
        _parse_utc(snoozed_at_utc),
    )
    if delay_text:
        prefix_lines.append(
            "⏰ You snoozed this email " + delay_text
            if str(locale or "").strip().startswith("en")
            else f"⏰ Вы отложили это письмо {delay_text}"
        )
    snooze_count = _load_snooze_count()
    if snooze_count >= 3:
        if str(locale or "").strip().startswith("en"):
            times_label = "time" if snooze_count == 1 else "times"
            prefix_lines.append(
                f"⚠️ You have already snoozed this email {snooze_count} {times_label}"
            )
        else:
            if snooze_count % 10 == 1 and snooze_count % 100 != 11:
                times_label = "раз"
            elif snooze_count % 10 in {2, 3, 4} and snooze_count % 100 not in {12, 13, 14}:
                times_label = "раза"
            else:
                times_label = "раз"
            prefix_lines.append(
                f"⚠️ Вы откладывали это письмо уже {snooze_count} {times_label}"
            )
    if not prefix_lines:
        return existing
    return "\n\n".join(["\n".join(prefix_lines), existing])


def _process_due_snoozes_guarded(*, storage: Storage, config: BotConfig) -> None:
    """Run snooze delivery without letting one failure abort the queue."""
    try:
        _process_due_snoozes(storage=storage, config=config)
    except Exception:
        logger.exception("snooze_processing_failed")


def _snooze_account(
    storage: Storage, config: BotConfig, email_id: int
) -> AccountConfig | None:
    """The configured account that owns the email, if it has a chat (B16)."""
    account_email = storage.get_email_account(email_id)
    for account in config.accounts:
        if account.telegram_chat_id and logins_match(account.login, account_email):
            return account
    return None


def _snooze_retry_at(now: datetime, attempts: int) -> tuple[int, str]:
    next_attempts = attempts + 1
    backoff_seconds = min(3600, 60 * (2 ** min(next_attempts, 5)))
    next_dt = now.timestamp() + backoff_seconds
    return next_attempts, datetime.fromtimestamp(next_dt, tz=timezone.utc).isoformat()


def _process_due_snoozes(*, storage: Storage, config: BotConfig) -> None:
    now = datetime.now(timezone.utc)
    due_items = storage.list_due_snoozes(now_iso=now.isoformat(), limit=20)
    if not due_items:
        return
    if not config.accounts:
        return
    ui_locale = processor_module._resolve_outbound_ui_locale()

    for item in due_items:
        snooze_id = int(item["id"])
        email_id = int(item["email_id"])
        deliver_at = str(item["deliver_at_utc"])
        attempts = int(item.get("attempts") or 0)
        account = _snooze_account(storage, config, email_id)
        if account is None:
            # Never fall back to another account's chat: keep it pending.
            next_attempts, next_iso = _snooze_retry_at(now, attempts)
            logger.warning(
                "snooze_account_unresolved snooze_id=%s email_id=%s attempts=%s",
                snooze_id,
                email_id,
                next_attempts,
            )
            storage.reschedule_snooze_retry(
                snooze_id=snooze_id,
                next_deliver_at_utc=next_iso,
                attempts=next_attempts,
                error="snooze account not configured",
            )
            continue
        reminder_text = _build_snooze_return_text(
            storage,
            email_id=email_id,
            reminder_text=str(item.get("reminder_text") or ""),
            snoozed_at_utc=str(item.get("snoozed_at_utc") or ""),
            locale=ui_locale,
        )
        delivery_key = _build_telegram_delivery_key(
            email_id=email_id,
            kind="snooze",
            snooze_ts=deliver_at,
        )
        dedup_state = storage.reserve_telegram_delivery(
            delivery_key=delivery_key,
            email_id=email_id,
            account_id=account.login,
            chat_id=account.telegram_chat_id,
            kind="snooze",
        )
        if dedup_state == "duplicate":
            storage.mark_snooze_delivered(snooze_id=snooze_id)
            continue
        payload = TelegramPayload(
            html_text=telegram_safe(
                f"{'⏰ Reminder' if str(ui_locale).startswith('en') else '⏰ Напоминание'}\n\n{reminder_text}"
            ),
            priority="🔵",
            metadata={
                "bot_token": config.keys.telegram_bot_token,
                "chat_id": account.telegram_chat_id,
            },
            reply_markup=build_email_actions_keyboard(
                email_id=email_id,
                expanded=False,
                initial_prio=True,
                show_decision_trace=load_telegram_ui_config().show_decision_trace,
                locale=ui_locale,
            ),
        )
        result = send_telegram(payload)
        if result.delivered:
            storage.mark_snooze_delivered(snooze_id=snooze_id)
            if dedup_state == "reserved":
                storage.finalize_telegram_delivery(
                    delivery_key=delivery_key,
                    telegram_message_id=(
                        str(result.message_id)
                        if result.message_id is not None
                        else None
                    ),
                )
            continue
        if dedup_state == "reserved":
            storage.release_telegram_delivery(delivery_key=delivery_key)
        next_attempts, next_iso = _snooze_retry_at(now, attempts)
        storage.reschedule_snooze_retry(
            snooze_id=snooze_id,
            next_deliver_at_utc=next_iso,
            attempts=next_attempts,
            error=result.error or "snooze telegram delivery failed",
        )


def main(config_dir: Path | None = None, *, max_cycles: int | None = None) -> None:
    require_runtime_for("runtime")
    dist_ok, dist_error = validate_dist_runtime(
        frozen=bool(getattr(sys, "frozen", False)),
        executable_path=Path(sys.executable),
    )
    if not dist_ok:
        print(dist_error)
        sys.exit(2)

    storage: Storage | None = None
    instance_lock: InstanceLock | None = None
    runtime_health = AccountRuntimeHealthManager(
        CURRENT_DIR / "data" / "runtime_health.json",
        locale_resolver=lambda: processor_module._resolve_outbound_ui_locale(),
    )
    try:
        resolved_paths = resolve_config_paths(config_dir)
        resolved_config_dir = resolved_paths.config_dir
        preflight_warnings: list[str] = []
        if load_config is _ORIGINAL_LOAD_CONFIG:
            preflight_ready, preflight_errors, preflight_warnings = (
                _run_startup_preflight(resolved_config_dir)
            )
            if not preflight_ready:
                _print_startup_preflight_failure(
                    resolved_config_dir, preflight_errors, preflight_warnings
                )
                sys.exit(2)
        for warning in preflight_warnings:
            print(f"[WARN] {warning}")

        config_result = load_config(config_dir)
        if (
            isinstance(config_result, tuple)
            and len(config_result) == 3
            and (config_result[0] is None or isinstance(config_result[0], Path))
        ):
            config_path, raw_config, config = config_result
        else:
            config_path = None
            raw_config = {}
            config = config_result
        instance_lock = _acquire_instance_lock_or_exit(Path(config.storage.db_path))
        processor_module.configure_processor_config_dir(resolved_config_dir)
        try:
            from mailbot_v26.ui.i18n import get_locale as _get_locale
            import configparser as _cp2

            _p = _cp2.ConfigParser()
            _p.read(
                [
                    str(resolved_config_dir / "settings.ini"),
                    str(resolved_config_dir / "config.ini"),
                ],
                encoding="utf-8-sig",
            )
            processor_module.configure_processor_locale(_get_locale(_p))
        except Exception as exc:
            # Notifications fall back to the default locale; say why.
            logger.warning("processor_locale_config_failed error=%s", exc)
        try:
            processor_module.configure_processor_db_path(config.storage.db_path)
            configure_digest_config_dir(resolved_config_dir)
            flags = FeatureFlags(base_dir=resolved_config_dir)
        except OSError as exc:
            logger.exception("startup_storage_init_failed")
            print(f"[ERROR] Cannot open the database or data folder: {exc}")
            print(
                "[ERROR] Make sure the Letterbot folder is writable "
                "(do not install into Program Files) and restart."
            )
            sys.exit(2)

        print("\n" + "=" * 60)
        print(f"LETTERBOT.RU {__version__} - STARTING")
        print("=" * 60)
        print(f"Log file: {LOG_PATH}\n")

        logger.info("=== LetterBot.ru %s started ===", __version__)
        _check_build_integrity()
        try:
            processor_module.system_snapshotter.log_startup()
        except Exception as exc:  # pragma: no cover - optional observability
            logger.error("system_health_snapshot_failed error=%s", exc)

        logger.info("Configuration loaded: %d accounts", len(config.accounts))

        telegram_errors = validate_telegram_contract(
            config, config_dir=resolved_config_dir
        )
        if telegram_errors:
            logger.error(
                "telegram_startup_validation_failed errors=%s", telegram_errors
            )
            print(
                "[ERROR] Telegram configuration is invalid. Fix accounts.ini and restart."
            )
            for error in telegram_errors:
                print(f"[ERROR] {error}")
            sys.exit(2)

        for line in _build_startup_confirmation_lines(
            config=config,
            resolved_config_dir=resolved_config_dir,
            two_file_mode=resolved_paths.two_file_mode,
        ):
            print(line)

        polling_interval, reload_interval = get_polling_intervals(raw_config)
        last_reload_at = time.monotonic()

        mail_health = run_startup_mail_account_healthcheck(
            config,
            send_telegram,
            return_outcome=True,
        )
        if hasattr(mail_health, "accounts_to_poll"):
            accounts_to_poll = list(mail_health.accounts_to_poll)
            mail_results = list(getattr(mail_health, "results", []) or [])
            mail_unavailable_reason = str(
                getattr(mail_health, "unavailable_reason", "") or ""
            )
        else:
            # Backward compatibility for tests/mocks returning a plain account list.
            accounts_to_poll = list(mail_health or [])
            mail_results = []
            mail_unavailable_reason = ""
        for account in config.accounts:
            runtime_health.register_account(account)

        llm_any_ok = False
        try:
            health_checker = StartupHealthChecker(resolved_config_dir, config)
            results = health_checker.run()
            health_results = results if isinstance(results, list) else []
            llm_any_ok = any(
                str(item.get("status") or "").upper() == "OK"
                for item in health_results
                if isinstance(item, dict)
                and str(item.get("component") or "")
                in {"GigaChat", "Cloudflare", "LLM Direct"}
            )
            mode = health_checker.evaluate_mode(results)
            report = LaunchReportBuilder(
                version_label=f"LetterBot.ru {__version__}",
                config_dir=resolved_config_dir,
            ).build(
                results,
                mode,
                mail_accounts=[
                    {
                        "account_id": item.account_id,
                        "status": item.status,
                        "error": item.error or "",
                    }
                    for item in mail_results
                ],
                mail_check_unavailable_reason=mail_unavailable_reason,
            )
            launch_chat_id = config.general.admin_chat_id
            if not launch_chat_id and config.accounts:
                launch_chat_id = config.accounts[0].telegram_chat_id
            if launch_chat_id:
                ok = dispatch_launch_report(
                    config.keys.telegram_bot_token,
                    launch_chat_id,
                    report,
                )
                logger.info("Launch report send status: %s", "OK" if ok else "FAIL")
            else:
                logger.warning("Launch report skipped: missing admin chat id")
        except Exception:
            logger.exception("Startup health check failed")

        flags.ENABLE_PREMIUM_PROCESSOR = bool(llm_any_ok)
        logger.info(
            "premium_processor_startup_toggle enabled=%s llm_any_ok=%s",
            flags.ENABLE_PREMIUM_PROCESSOR,
            llm_any_ok,
        )

        try:
            _ensure_runtime_dirs(db_path=config.storage.db_path, log_path=LOG_PATH)
            storage = Storage(config.storage.db_path)
            logger.info("Storage initialized at %s", config.storage.db_path)
        except Exception as exc:
            logger.exception("Failed to initialize storage")
            print(f"[ERROR] Storage initialization error: {exc}")
            time.sleep(10)
            return

        try:
            run_self_check()
        except Exception:
            logger.exception("Self-check execution failure")

        try:
            maybe_backfill_events(processor_module.DB_PATH)
        except Exception:
            logger.exception("events_backfill_failed")

        state = StateManager(CURRENT_DIR / "state.json")
        processor = MessageProcessor(config=config, state=state)
        configure_pipeline(
            config,
            processor,
            enable_premium_processor=flags.ENABLE_PREMIUM_PROCESSOR,
        )
        digest_storage = DigestStorage(
            knowledge_db=processor_module.knowledge_db,
            analytics=processor_module.analytics,
            event_emitter=processor_module.event_emitter,
            contract_event_emitter=processor_module.contract_event_emitter,
        )
        inbound_state_store = InboundStateStore(processor_module.DB_PATH)

        def _build_inbound_stack(current_config: BotConfig):
            allowed_ids = {
                chat_id
                for chat_id in (
                    [current_config.general.admin_chat_id]
                    + [account.telegram_chat_id for account in current_config.accounts]
                )
                if chat_id
            }
            client = TelegramInboundClient(
                bot_token=current_config.keys.telegram_bot_token,
                timeout_s=5,
            )
            processor = TelegramInboundProcessor(
                knowledge_db=processor_module.knowledge_db,
                analytics=processor_module.analytics,
                event_emitter=processor_module.event_emitter,
                contract_event_emitter=processor_module.contract_event_emitter,
                runtime_flag_store=processor_module.runtime_flag_store,
                auto_priority_gate=processor_module.auto_priority_quality_gate,
                auto_priority_gate_config=processor_module.auto_priority_gate_config,
                override_store=RuntimeOverrideStore(processor_module.DB_PATH),
                send_reply=lambda chat_id, text: client.send_message(
                    chat_id=chat_id, text=text
                ),
                feature_flags=processor_module.feature_flags,
                allowed_chat_ids=frozenset(allowed_ids),
                bot_token=current_config.keys.telegram_bot_token,
                locale=processor_module._UI_LOCALE,
                show_decision_trace=load_telegram_ui_config().show_decision_trace,
                chat_account_emails=build_chat_account_scopes(
                    current_config.accounts,
                    admin_chat_id=current_config.general.admin_chat_id,
                ),
            )
            return client, processor, frozenset(allowed_ids)

        inbound_client, inbound_processor, allowed_chat_ids = _build_inbound_stack(
            config
        )
        print("[OK] Ready to work\n")

        # Start web UI in background thread when running as frozen EXE
        # In source mode run_stack.py handles this as a separate process
        try:
            import sys as _sys

            if getattr(_sys, "frozen", False):
                from mailbot_v26.config_loader import (
                    load_storage_config,
                    load_web_config,
                )

                _web_cfg = load_web_config(resolved_config_dir)
                _storage_cfg = load_storage_config(resolved_config_dir)
                from mailbot_v26.web_observability.app import start_web_in_thread

                start_web_in_thread(
                    config_dir=resolved_config_dir,
                    db_path=_storage_cfg.db_path,
                    host=_web_cfg.host or "127.0.0.1",
                    port=_web_cfg.port or 8787,
                )
                print(
                    f"[OK] Web UI starting at http://{_web_cfg.host or '127.0.0.1'}:{_web_cfg.port or 8787}/"
                )
        except Exception as _web_exc:
            print(f"[WARN] Web UI could not start: {_web_exc}")

        bootstrap_notice_sent = False
        imap_health_started_accounts: set[str] = set()
        cycle = 0
        try:
            while True:
                cycle += 1
                print(f"\n{'=' * 60}")
                print(f"CYCLE #{cycle} - {time.strftime('%H:%M:%S')}")
                print(f"{'=' * 60}")
                logger.info("Cycle #%d started", cycle)

                try:
                    now_mono = time.monotonic()
                    if now_mono - last_reload_at >= reload_interval:
                        last_reload_at = now_mono
                        if config_path is None:
                            pass
                        else:
                            try:
                                reloaded_raw = load_yaml_config(config_path)
                            except YamlConfigError as exc:
                                logger.error("config_reload_failed error=%s", exc)
                                reloaded_raw = None
                            if reloaded_raw is None:
                                pass
                            else:
                                ok, error = validate_yaml_config(reloaded_raw)
                                if ok:
                                    updated_config = build_bot_config(
                                        reloaded_raw,
                                        repo_root=REPO_ROOT,
                                    )
                                    config = updated_config
                                    raw_config = reloaded_raw
                                    polling_interval, reload_interval = (
                                        get_polling_intervals(raw_config)
                                    )
                                    processor.config = config
                                    configure_pipeline(config, processor)
                                    accounts_to_poll = list(config.accounts)
                                    for account in config.accounts:
                                        runtime_health.register_account(account)
                                    (
                                        inbound_client,
                                        inbound_processor,
                                        allowed_chat_ids,
                                    ) = _build_inbound_stack(config)
                                    logger.info(
                                        "config_reloaded accounts=%s",
                                        len(config.accounts),
                                    )
                                else:
                                    logger.error(
                                        "config_reload_invalid error=%s", error
                                    )

                    try:
                        run_inbound_polling(
                            client=inbound_client,
                            processor=inbound_processor,
                            state_store=inbound_state_store,
                        )
                    except Exception:
                        logger.exception("telegram_inbound_poll_failed")

                    for account in accounts_to_poll:
                        login = account.login or "no_login"
                        now_utc = datetime.now(timezone.utc)
                        state_snapshot = runtime_health.get_state(account.account_id)
                        if not runtime_health.should_attempt(
                            account.account_id, now_utc
                        ):
                            digest_logger.warning(
                                "imap_account_skipped_backoff",
                                account_id=account.account_id,
                                login=login,
                                host=account.host,
                                port=account.port,
                                use_ssl=account.use_ssl,
                                next_retry_at=runtime_health.format_timestamp(
                                    state_snapshot.next_retry_at_utc
                                ),
                                consecutive_failures=state_snapshot.consecutive_failures,
                                cooldown_until=runtime_health.format_timestamp(
                                    state_snapshot.cooldown_until_utc
                                ),
                                cooldown_reason=state_snapshot.cooldown_reason,
                            )
                            continue

                        print(f"\n[MAIL] Checking: {login}")
                        digest_logger.info(
                            "imap_account_attempt",
                            account_id=account.account_id,
                            login=login,
                            host=account.host,
                            port=account.port,
                            use_ssl=account.use_ssl,
                            consecutive_failures=state_snapshot.consecutive_failures,
                            cooldown_until=runtime_health.format_timestamp(
                                state_snapshot.cooldown_until_utc
                            ),
                            cooldown_reason=state_snapshot.cooldown_reason,
                        )

                        try:
                            state_last_uid = state.get_last_uid(login)
                            state_last_check = state.get_last_check_time(login)
                            has_db_history = _storage_has_account_history(
                                storage, login
                            )
                            state_file_exists = bool(
                                getattr(
                                    getattr(state, "state_file", None),
                                    "exists",
                                    lambda: False,
                                )()
                            )
                            first_run_detected = _is_first_run_account(
                                state=state,
                                storage=storage,
                                account_email=login,
                            )
                            bootstrap_enabled = bool(
                                first_run_detected
                                and (not config.ingest.allow_prestart_emails)
                                and config.ingest.first_run_bootstrap_hours > 0
                                and config.ingest.first_run_bootstrap_max_messages > 0
                            )
                            digest_logger.info(
                                "imap_ingest_policy",
                                account_id=account.account_id,
                                account_email=login,
                                login=login,
                                first_run_detected=first_run_detected,
                                bootstrap_enabled=bootstrap_enabled,
                                bootstrap_window_hours=config.ingest.first_run_bootstrap_hours,
                                bootstrap_max_messages=config.ingest.first_run_bootstrap_max_messages,
                                allow_prestart_emails=config.ingest.allow_prestart_emails,
                                state_file_exists=state_file_exists,
                                state_last_uid=state_last_uid,
                                state_last_check=(
                                    state_last_check.isoformat()
                                    if state_last_check
                                    else None
                                ),
                                db_history_detected=has_db_history,
                            )
                            imap = ResilientIMAP(
                                account,
                                state,
                                RUN_STARTED_AT_UTC,
                                allow_prestart_emails=config.ingest.allow_prestart_emails,
                                first_run_bootstrap=first_run_detected,
                                first_run_bootstrap_hours=config.ingest.first_run_bootstrap_hours,
                                first_run_bootstrap_max_messages=config.ingest.first_run_bootstrap_max_messages,
                                max_email_mb=config.general.max_email_mb,
                            )
                            new_messages = imap.fetch_new_messages(
                                is_known_uid=_known_uid_probe(storage, account.login),
                                is_known_message_id=_known_message_id_probe(
                                    storage, account.login
                                ),
                            )
                            retired_rows = _retire_previous_uid_epoch(
                                storage=storage, imap=imap, login=login
                            )
                            digest_logger.info(
                                "imap_resync_state",
                                account_id=account.account_id,
                                account_email=login,
                                first_run_detected=first_run_detected,
                                bootstrap_enabled=imap.last_bootstrap_active,
                                bootstrap_window_hours=config.ingest.first_run_bootstrap_hours,
                                bootstrap_max_messages=config.ingest.first_run_bootstrap_max_messages,
                                uidvalidity_changed=imap.last_uidvalidity_changed,
                                resync_reason=imap.last_resync_reason,
                            )
                            success_ts = datetime.now(timezone.utc)
                            runtime_health.on_success(
                                account.account_id, success_ts
                            )
                            if state_snapshot.consecutive_failures > 0:
                                _emit_imap_health_event(
                                    account_id=login,
                                    subtype="reconnect",
                                    detail="IMAP polling recovered after backoff",
                                    ts_utc=success_ts.timestamp(),
                                    attempt_count=state_snapshot.consecutive_failures,
                                )
                                imap_health_started_accounts.add(account.account_id)
                            elif account.account_id not in imap_health_started_accounts:
                                _emit_imap_health_event(
                                    account_id=login,
                                    subtype="startup",
                                    detail="Initial IMAP poll succeeded",
                                    ts_utc=success_ts.timestamp(),
                                )
                                imap_health_started_accounts.add(account.account_id)
                            _emit_imap_health_event(
                                account_id=login,
                                subtype="success",
                                detail="IMAP poll succeeded",
                                ts_utc=success_ts.timestamp(),
                            )
                            if imap.last_uidvalidity_changed:
                                _emit_imap_health_event(
                                    account_id=login,
                                    subtype="uidvalidity_change",
                                    detail="UIDVALIDITY changed; bounded resync applied",
                                    ts_utc=success_ts.timestamp(),
                                    extra_payload={
                                        "resync_reason": imap.last_resync_reason,
                                        "bootstrap_enabled": bool(
                                            imap.last_bootstrap_active
                                        ),
                                        "retired_rows": retired_rows,
                                    },
                                )
                            digest_logger.info(
                                "imap_account_success",
                                account_id=account.account_id,
                                login=login,
                                host=account.host,
                                port=account.port,
                                use_ssl=account.use_ssl,
                                messages=len(new_messages),
                            )

                            if not new_messages:
                                print("   -- no new messages")
                                _commit_imap_cursor(
                                    imap=imap,
                                    persisted_uids=[],
                                    account=account,
                                    login=login,
                                    state=state,
                                    bot_token=config.keys.telegram_bot_token,
                                )
                                continue

                            if (
                                not bootstrap_notice_sent
                                and first_run_detected
                                and (not config.ingest.allow_prestart_emails)
                                and imap.last_fetch_included_prestart
                                and account.telegram_chat_id
                            ):
                                note_payload = _build_system_payload(
                                    text=_build_first_run_bootstrap_note(
                                        config.ingest.first_run_bootstrap_hours,
                                        config.ingest.first_run_bootstrap_max_messages,
                                    ),
                                    bot_token=config.keys.telegram_bot_token,
                                    chat_id=account.telegram_chat_id,
                                )
                                note_result = send_telegram(note_payload)
                                bootstrap_notice_sent = True
                                digest_logger.info(
                                    "first_run_bootstrap_notice_sent",
                                    account_id=account.account_id,
                                    login=login,
                                    delivered=note_result.delivered,
                                    bootstrap_window_hours=config.ingest.first_run_bootstrap_hours,
                                    bootstrap_max_messages=config.ingest.first_run_bootstrap_max_messages,
                                )

                            print(f"   -- received {len(new_messages)} new messages")

                            persisted_uids: list[int] = []
                            for uid, raw in new_messages:
                                print(f"      в”њв”Ђ UID {uid}")
                                try:
                                    inbound = parse_raw_email(raw, config)
                                    subject = (
                                        inbound.subject[:60]
                                        if inbound.subject
                                        else "(no subject)"
                                    )
                                    print(f"      в”‚  Subject: {subject}")

                                    message_obj = message_from_bytes(raw)
                                    message_id = (
                                        message_obj.get("Message-ID")
                                        if message_obj
                                        else None
                                    )
                                    from_header = (
                                        decode_mime_header(message_obj.get("From", ""))
                                        if message_obj
                                        else ""
                                    )
                                    from_name, from_email = parseaddr(
                                        from_header or inbound.sender
                                    )
                                    received_at = (
                                        decode_mime_header(message_obj.get("Date", ""))
                                        if message_obj
                                        else None
                                    )
                                    attachments_count = len(inbound.attachments or [])

                                    if storage:
                                        email_id, enqueued = (
                                            _persist_inbound_and_enqueue_parse(
                                                storage=storage,
                                                account_email=account.login,
                                                uid=uid,
                                                message_id=message_id,
                                                from_email=from_email or None,
                                                from_name=from_name or None,
                                                subject=inbound.subject,
                                                received_at=received_at or None,
                                                attachments_count=attachments_count,
                                                raw_email=raw,
                                                inbound=inbound,
                                                # getattr: fakes lack it (F10)
                                                uidvalidity=getattr(
                                                    imap, "current_uidvalidity", None
                                                ),
                                                # the whole resync, not only
                                                # the poll that saw the change
                                                dedupe_message_id=bool(
                                                    getattr(
                                                        imap,
                                                        "last_message_id_resync",
                                                        imap.last_uidvalidity_changed,
                                                    )
                                                ),
                                            )
                                        )
                                        if enqueued:
                                            print(
                                                f"      в”‚  Enqueued PARSE for email_id={email_id}"
                                            )
                                        else:
                                            print(
                                                f"      в”‚  Duplicate ingest skipped for email_id={email_id}"
                                            )
                                    else:
                                        final_text = processor.process(login, inbound)
                                        if final_text and final_text.strip():
                                            final_text = _prefix_account_text(
                                                final_text,
                                                account,
                                            )
                                            payload = _build_system_payload(
                                                text=final_text.strip(),
                                                bot_token=config.keys.telegram_bot_token,
                                                chat_id=account.telegram_chat_id,
                                            )
                                            result = send_telegram(payload)
                                            ok = result.delivered
                                            status = (
                                                "[OK] sent" if ok else "[FAIL] failed"
                                            )
                                            print(f"      в”‚  Telegram: {status}")
                                            logger.info(
                                                "UID %s: Telegram %s",
                                                uid,
                                                "OK" if ok else "FAIL",
                                            )
                                        else:
                                            print("      в”‚  Result: empty")
                                    persisted_uids.append(uid)

                                except Exception as e:
                                    print(f"      в””в”Ђ [ERROR] {e}")
                                    logger.exception("Processing error for UID %s", uid)

                            # Cursor only past stored UIDs, then onto disk.
                            _commit_imap_cursor(
                                imap=imap,
                                persisted_uids=persisted_uids,
                                account=account,
                                login=login,
                                state=state,
                                bot_token=config.keys.telegram_bot_token,
                            )
                            state.save()

                        except Exception as e:
                            now_utc = datetime.now(timezone.utc)
                            should_alert, alert_text = runtime_health.on_failure(
                                account.account_id, e, now_utc
                            )
                            state_snapshot = runtime_health.get_state(
                                account.account_id
                            )
                            print(f"   в””в”Ђ [IMAP ERROR] {e}")
                            backoff_minutes = 0
                            if state_snapshot.next_retry_at_utc:
                                backoff_minutes = int(
                                    max(
                                        (
                                            state_snapshot.next_retry_at_utc - now_utc
                                        ).total_seconds()
                                        // 60,
                                        0,
                                    )
                                )
                            digest_logger.error(
                                "imap_account_failure",
                                account_id=account.account_id,
                                login=login,
                                host=account.host,
                                port=account.port,
                                use_ssl=account.use_ssl,
                                error_class=e.__class__.__name__,
                                error_message=str(e),
                                consecutive_failures=state_snapshot.consecutive_failures,
                                cooldown_until=runtime_health.format_timestamp(
                                    state_snapshot.cooldown_until_utc
                                ),
                                cooldown_reason=state_snapshot.cooldown_reason,
                                next_retry_at=runtime_health.format_timestamp(
                                    state_snapshot.next_retry_at_utc
                                ),
                                backoff_minutes=backoff_minutes,
                            )
                            digest_logger.warning(
                                "imap_account_backoff_set",
                                account_id=account.account_id,
                                login=login,
                                host=account.host,
                                port=account.port,
                                use_ssl=account.use_ssl,
                                consecutive_failures=state_snapshot.consecutive_failures,
                                cooldown_until=runtime_health.format_timestamp(
                                    state_snapshot.cooldown_until_utc
                                ),
                                cooldown_reason=state_snapshot.cooldown_reason,
                                next_retry_at=runtime_health.format_timestamp(
                                    state_snapshot.next_retry_at_utc
                                ),
                                backoff_minutes=backoff_minutes,
                            )
                            if should_alert:
                                payload = _build_system_payload(
                                    text=alert_text,
                                    bot_token=config.keys.telegram_bot_token,
                                    chat_id=account.telegram_chat_id,
                                    priority="🔴",
                                )
                                result = send_telegram(payload)
                                digest_logger.warning(
                                    "imap_account_alert_sent",
                                    account_id=account.account_id,
                                    login=login,
                                    delivered=result.delivered,
                                )

                    if storage:
                        try:
                            _process_queue(storage, config, processor, flags)
                        except Exception:
                            # Queued rows stay in the queue and are retried by
                            # its own attempt/backoff logic; no blanket fail-open.
                            logger.exception("Queue dispatcher failure")

                    run_digest_tick(
                        now=datetime.now(timezone.utc),
                        config=config,
                        storage=digest_storage,
                        telegram_sender=send_telegram,
                        logger=digest_logger,
                    )
                except Exception:
                    logger.exception("Cycle %d failed", cycle)

                try:
                    state.save()
                except Exception:
                    logger.exception("State save failed after cycle %d", cycle)

                delay = max(120, polling_interval)
                print(f"\n[WAIT] Sleeping {delay} seconds...")
                try:
                    time.sleep(delay)
                except KeyboardInterrupt:
                    print("\n\n[STOP] Stopped by user")
                    logger.info("Stopped by user")
                    break
                except Exception:
                    logger.exception("Sleep failed after cycle %d", cycle)

                if max_cycles is not None and cycle >= max_cycles:
                    break
        except KeyboardInterrupt:
            print("\n\n[STOP] Stopped by user")
            logger.info("Stopped by user")
    finally:
        if storage:
            storage.close()
        if instance_lock is not None:
            instance_lock.release()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m mailbot_v26.start")
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=None,
        help="Optional config directory (default: mailbot_v26/config).",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Run finite number of polling cycles (for diagnostics/tests).",
    )
    return parser


def main_cli(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        main(config_dir=args.config_dir, max_cycles=args.max_cycles)
    except DependencyError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        return code
    return 0


if __name__ == "__main__":
    sys.exit(main_cli())
