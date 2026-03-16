"""LetterBot.ru v26 - Runtime orchestrator"""

from __future__ import annotations

import configparser
import logging
import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone
from email import message_from_bytes
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import List, Optional

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
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.account_identity import logins_match, normalize_login
from mailbot_v26.config_yaml import (
    ConfigError as YamlConfigError,
    SCHEMA_NEWER_MESSAGE,
    build_bot_config,
    get_polling_intervals,
    load_config as load_yaml_config,
    validate_config as validate_yaml_config,
)
from mailbot_v26.health.mail_accounts import run_startup_mail_account_healthcheck
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.integrity import verify_manifest
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline.processor import (
    InboundMessage,
    MessageProcessor,
    event_emitter,
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
from mailbot_v26.storage.self_check import run_self_check
from mailbot_v26.system.startup_health import (
    LaunchReportBuilder,
    StartupHealthChecker,
    dispatch_launch_report,
)
from mailbot_v26.text.mime_utils import decode_mime_header
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.ui.branding import append_watermark
from mailbot_v26.worker.telegram_sender import send_telegram
from mailbot_v26.tools.backfill_events import maybe_backfill_events
from mailbot_v26.version import __version__

CURRENT_DIR = Path(__file__).resolve().parent
LOG_PATH = CURRENT_DIR / "mailbot.log"
# Backward-compatible start-module exports used by tests and tools.
_START_COMPAT_EXPORTS = (_extract_body, _extract_attachments, _extract_attachment_text)
RUN_STARTED_AT_UTC = datetime.now(timezone.utc)
REPO_ROOT = CURRENT_DIR.parent
MAX_PIPELINE_STAGE_ATTEMPTS = 3
MAX_TELEGRAM_STAGE_ATTEMPTS = 3


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
    parser.read(accounts_path, encoding="utf-8")
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
    print("[ERROR] Configuration is not ready for startup.")
    print(f"[ERROR] Config dir: {config_dir.resolve()}")
    for item in critical:
        print(f"[ERROR] {item}")
    for item in warnings:
        print(f"[WARN] {item}")
    print(
        "[NEXT] Run: "
        f'python -m mailbot_v26 init-config --config-dir "{config_dir.resolve()}"'
    )
    print(
        "[NEXT] Fill settings.ini and accounts.ini placeholders, then run "
        f'python -m mailbot_v26 config-ready --config-dir "{config_dir.resolve()}" --verbose'
    )


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
    manifest_path = REPO_ROOT / "manifest.sha256.json"
    if not manifest_path.exists():
        return
    try:
        ok, changed_files = verify_manifest(REPO_ROOT, manifest_path)
    except Exception:
        logger.exception("integrity_manifest_failed")
        print("WARNING: build files modified (see log)")
        return
    if not ok:
        logger.warning("integrity_manifest_changed files=%s", changed_files)
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
    text = append_watermark(text, html=True)
    return TelegramPayload(
        html_text=telegram_safe(text),
        priority=priority,
        metadata={"bot_token": bot_token, "chat_id": chat_id},
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
    )


def _fail_open_process(
    config: BotConfig, processor: MessageProcessor, ctx: Optional[PipelineContext]
) -> None:
    if ctx is None:
        logger.error("Fail-open skipped: missing context")
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
    result = send_telegram(payload)
    ok = result.delivered
    status = "OK" if ok else "FAIL"
    logger.error(
        "Fail-open Telegram send status for email %s: %s", ctx.email_id, status
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
) -> tuple[int, bool]:
    normalized_account_email = normalize_login(account_email)
    find_email_id = getattr(storage, "find_email_id", None)
    existing_email_id = (
        find_email_id(normalized_account_email, uid)
        if callable(find_email_id)
        else None
    )
    email_id = storage.upsert_email(
        account_email=normalized_account_email,
        uid=uid,
        message_id=message_id,
        from_email=from_email,
        from_name=from_name,
        subject=subject,
        received_at=received_at,
        attachments_count=attachments_count,
    )
    if existing_email_id is not None:
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
    storage.enqueue_stage(email_id, "PARSE")
    return email_id, True


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


def _process_queue(
    storage: Storage,
    config: BotConfig,
    processor: MessageProcessor,
    flags: FeatureFlags,
) -> None:
    _process_due_snoozes(storage=storage, config=config)
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
        ctx = PIPELINE_CACHE.get(email_id)
        if ctx:
            ctx.attempts = attempts  # type: ignore[attr-defined]

        try:
            print(f"[QUEUE] Claimed {stage} for email_id={email_id} attempt={attempts}")
            if ctx is None:
                raise RuntimeError(f"No pipeline context for email {email_id}")

            if stage == "PARSE":
                premium_pipeline_enabled = bool(flags.ENABLE_PREMIUM_PROCESSOR)
                if premium_pipeline_enabled:
                    inbound = PIPELINE_INBOUND_CACHE.get(email_id)
                    raw = PIPELINE_RAW_CACHE.get(email_id)
                    if inbound is not None:
                        try:
                            _run_premium_processor(
                                ctx=ctx,
                                inbound=inbound,
                                raw=raw,
                                config=config,
                            )
                        except Exception as exc:
                            logger.exception(
                                "premium_processor_failed email_id=%s error=%s",
                                email_id,
                                exc,
                            )
                        else:
                            storage.mark_done(queue_id)
                            _cleanup_pipeline_cache(email_id)
                            continue
                stage_parse(ctx)
                storage.mark_done(queue_id)
                storage.enqueue_stage(email_id, "LLM")
            elif stage == "LLM":
                stage_llm(ctx)
                storage.mark_done(queue_id)
                storage.enqueue_stage(email_id, "TG")
            elif stage == "TG":
                account = (
                    _get_account_by_login(config, ctx.account_email) if ctx else None
                )
                delivery_key = _build_telegram_delivery_key(
                    email_id=email_id, kind="email"
                )
                dedup_state = storage.reserve_telegram_delivery(
                    delivery_key=delivery_key,
                    email_id=email_id,
                    account_id=ctx.account_email if ctx else None,
                    chat_id=account.telegram_chat_id if account else None,
                    kind="email",
                )
                if dedup_state == "duplicate" or (
                    dedup_state == "unavailable"
                    and storage.is_telegram_delivered(email_id)
                ):
                    storage.mark_done(queue_id)
                    logger.info(
                        "telegram_delivery_skipped_duplicate account_id=%s email_id=%s delivery_key=%s",
                        ctx.account_email if ctx else "",
                        email_id,
                        delivery_key,
                    )
                    event_emitter.emit(
                        type="telegram_delivery_skipped_duplicate",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={"delivery_key": delivery_key, "kind": "email"},
                    )
                    continue
                if dedup_state == "unavailable":
                    logger.warning(
                        "telegram_delivery_dedup_unavailable email_id=%s account_id=%s delivery_key=%s",
                        email_id,
                        ctx.account_email if ctx else "",
                        delivery_key,
                    )
                try:
                    result = stage_tg(ctx)
                except Exception:
                    if dedup_state == "reserved":
                        storage.release_telegram_delivery(delivery_key=delivery_key)
                    raise
                if result.delivered:
                    storage.mark_telegram_delivered(email_id)
                    if dedup_state == "reserved":
                        storage.finalize_telegram_delivery(
                            delivery_key=delivery_key,
                            telegram_message_id=(
                                str(result.message_id)
                                if result.message_id is not None
                                else None
                            ),
                        )
                    storage.mark_done(queue_id)
                    event_emitter.emit(
                        type="telegram_delivery_succeeded",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={"attempt": attempts},
                    )
                    if ctx:
                        _emit_contract_event(
                            event_type=EventType.TELEGRAM_DELIVERED,
                            ts_utc=datetime.now(timezone.utc).timestamp(),
                            account_id=ctx.account_email,
                            email_id=email_id,
                            payload={"attempt": attempts},
                        )
                else:
                    error = result.error or "telegram delivery failed"
                    if dedup_state == "reserved":
                        storage.release_telegram_delivery(delivery_key=delivery_key)
                    if result.retryable:
                        raise RuntimeError(error)
                    if account:
                        notice = "Telegram delivery failed. Check email client."
                        payload = _build_system_payload(
                            text=notice,
                            bot_token=config.keys.telegram_bot_token,
                            chat_id=account.telegram_chat_id,
                            priority="🔴",
                        )
                        fallback_result = send_telegram(payload)
                        if not fallback_result.delivered:
                            logger.error(
                                "telegram_delivery_failed_notice_failed email_id=%s",
                                email_id,
                            )
                    storage.set_email_delivery_failed(email_id, error)
                    storage.mark_done(queue_id)
                    PIPELINE_INBOUND_CACHE.pop(email_id, None)
                    PIPELINE_CACHE.pop(email_id, None)
                    PIPELINE_RAW_CACHE.pop(email_id, None)
                    logger.error(
                        "telegram_delivery_failed email_id=%s attempts=%s error=%s",
                        email_id,
                        attempts,
                        error,
                    )
                    event_emitter.emit(
                        type="telegram_delivery_failed",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={"attempts": attempts, "error": error},
                    )
            else:
                storage.mark_done(queue_id)
                logger.warning("Unknown stage %s for email %s", stage, email_id)
        except Exception as queue_exc:
            logger.exception("Queue handling error for email %s", email_id)
            backoff = min(600, 10 * (2**attempts))
            if stage == "TG":
                if attempts >= max_tg_attempts:
                    try:
                        storage.set_email_delivery_failed(email_id, str(queue_exc))
                        storage.mark_done(queue_id)
                    except Exception:
                        logger.exception(
                            "Failed to mark delivery failed for queue_id %s", queue_id
                        )
                    account = (
                        _get_account_by_login(config, ctx.account_email)
                        if ctx
                        else None
                    )
                    if account:
                        notice = (
                            "\U0001f534 TELEGRAM DELIVERY FAILED\n"
                            f"Email ID: {email_id}\n"
                            f"Account: {ctx.account_email}\n"
                            f"Reason: {queue_exc}"
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
                                "telegram_delivery_failed_notice_failed email_id=%s",
                                email_id,
                            )
                    logger.error(
                        "telegram_delivery_failed email_id=%s attempts=%s error=%s",
                        email_id,
                        attempts,
                        queue_exc,
                    )
                    event_emitter.emit(
                        type="telegram_delivery_failed",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={"attempts": attempts, "error": str(queue_exc)},
                    )
                    if ctx:
                        _emit_contract_event(
                            event_type=EventType.TELEGRAM_FAILED,
                            ts_utc=datetime.now(timezone.utc).timestamp(),
                            account_id=ctx.account_email,
                            email_id=email_id,
                            payload={"attempts": attempts, "error": str(queue_exc)},
                        )
                else:
                    try:
                        storage.mark_error(queue_id, str(queue_exc), backoff)
                    except Exception:
                        logger.exception(
                            "Failed to mark error for queue_id %s", queue_id
                        )
                    logger.info(
                        "telegram_delivery_retry email_id=%s attempt=%s backoff_seconds=%s",
                        email_id,
                        attempts,
                        backoff,
                    )
                    event_emitter.emit(
                        type="telegram_delivery_retry",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={"attempt": attempts, "backoff_seconds": backoff},
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
                _fail_open_process(config, processor, ctx)
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
    _process_due_snoozes(storage=storage, config=config)


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
) -> str:
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
        if (
            now_utc.date() != snoozed_at.date()
            and (now_utc.date() - snoozed_at.date()).days == 1
        ):
            return "yesterday" if is_english else "вчера"
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
    delay_text = _render_delay(datetime.now(timezone.utc), _parse_utc(snoozed_at_utc))
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


def _process_due_snoozes(*, storage: Storage, config: BotConfig) -> None:
    now = datetime.now(timezone.utc)
    due_items = storage.list_due_snoozes(now_iso=now.isoformat(), limit=20)
    if not due_items:
        return
    account = config.accounts[0] if config.accounts else None
    if not account:
        return
    ui_locale = processor_module._resolve_outbound_ui_locale()

    for item in due_items:
        snooze_id = int(item["id"])
        email_id = int(item["email_id"])
        deliver_at = str(item["deliver_at_utc"])
        attempts = int(item.get("attempts") or 0)
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
                f"{'📌 Reminder' if str(ui_locale).startswith('en') else '📌 Напоминание'}\n\n{reminder_text}"
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
        next_attempts = attempts + 1
        backoff_seconds = min(3600, 60 * (2 ** min(next_attempts, 5)))
        next_dt = now.timestamp() + backoff_seconds
        next_iso = datetime.fromtimestamp(next_dt, tz=timezone.utc).isoformat()
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

    storage: Storage | None = None
    runtime_health = AccountRuntimeHealthManager(
        CURRENT_DIR / "data" / "runtime_health.json"
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
                encoding="utf-8",
            )
            processor_module.configure_processor_locale(_get_locale(_p))
        except Exception:
            pass
        processor_module.configure_processor_db_path(config.storage.db_path)
        configure_digest_config_dir(resolved_config_dir)
        flags = FeatureFlags(base_dir=resolved_config_dir)
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
            )
            return client, processor, frozenset(allowed_ids)

        inbound_client, inbound_processor, allowed_chat_ids = _build_inbound_stack(
            config
        )
        print("[OK] Ready to work\n")

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
                        logger.exception("telegram_inbound_cycle_failed")

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
                            new_messages = imap.fetch_new_messages()
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

                                except Exception as e:
                                    print(f"      в””в”Ђ [ERROR] {e}")
                                    logger.exception("Processing error for UID %s", uid)

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
                            logger.exception("Queue dispatcher failure")
                            for ctx in list(PIPELINE_CACHE.values()):
                                _fail_open_process(config, processor, ctx)

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
