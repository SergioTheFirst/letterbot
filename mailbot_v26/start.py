"""MailBot Premium v26 - Runtime orchestrator"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from email import message_from_bytes
from email.utils import parseaddr
from pathlib import Path
from typing import List, Optional

CURRENT_DIR = Path(__file__).resolve().parent

from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    PipelineContext,
    _extract_attachment_text,
    configure_pipeline,
    parse_raw_email,
    remember_raw_email,
    stage_llm,
    stage_parse,
    stage_tg,
    store_inbound,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    InvalidAccountIdError,
    load_config,
    load_general_config,
    load_keys_config,
)
from mailbot_v26.health.mail_accounts import run_startup_mail_account_healthcheck
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor, event_emitter
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.pipeline import processor as processor_module
from mailbot_v26.pipeline.digest_scheduler import DigestStorage, run_digest_tick
from mailbot_v26.observability import get_logger
from mailbot_v26.state_manager import StateManager
from mailbot_v26.storage.self_check import run_self_check
from mailbot_v26.system.startup_health import (
    LaunchReportBuilder,
    StartupHealthChecker,
    dispatch_launch_report,
)
from mailbot_v26.text.mime_utils import decode_mime_header
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.worker.telegram_sender import send_telegram

LOG_PATH = CURRENT_DIR / "mailbot.log"


def _configure_logging() -> None:
    handlers: List[logging.Handler] = []
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        handlers.append(file_handler)
    except OSError as exc:
        print(f"File logging unavailable: {exc}")

    handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
        force=True,
    )


_configure_logging()
logger = logging.getLogger("mailbot")
digest_logger = get_logger("mailbot")


def _get_account_by_login(config: BotConfig, login: str) -> Optional["AccountConfig"]:
    for acc in config.accounts:
        if acc.login == login:
            return acc
    return None


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
                logger.error("Fail-open parse failed for email %s: %s", ctx.email_id, exc)

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
    logger.error("Fail-open Telegram send status for email %s: %s", ctx.email_id, status)


def _process_queue(storage: Storage, config: BotConfig, processor: MessageProcessor) -> None:
    max_tg_attempts = 3
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
            print(
                f"[QUEUE] Claimed {stage} for email_id={email_id} attempt={attempts}"
            )
            if ctx is None:
                raise RuntimeError(f"No pipeline context for email {email_id}")

            if stage == "PARSE":
                stage_parse(ctx)
                storage.mark_done(queue_id)
                storage.enqueue_stage(email_id, "LLM")
            elif stage == "LLM":
                stage_llm(ctx)
                storage.mark_done(queue_id)
                storage.enqueue_stage(email_id, "TG")
            elif stage == "TG":
                result = stage_tg(ctx)
                if result.delivered:
                    storage.mark_done(queue_id)
                    event_emitter.emit(
                        type="telegram_delivery_succeeded",
                        timestamp=datetime.now(timezone.utc),
                        email_id=email_id,
                        payload={"attempt": attempts},
                    )
                else:
                    error = result.error or "telegram delivery failed"
                    if result.retryable:
                        raise RuntimeError(error)
                    account = _get_account_by_login(config, ctx.account_email) if ctx else None
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
            backoff = min(600, 10 * (2 ** attempts))
            if stage == "TG":
                if attempts >= max_tg_attempts:
                    try:
                        storage.set_email_delivery_failed(email_id, str(queue_exc))
                        storage.mark_done(queue_id)
                    except Exception:
                        logger.exception("Failed to mark delivery failed for queue_id %s", queue_id)
                    account = _get_account_by_login(config, ctx.account_email) if ctx else None
                    if account:
                        notice = (
                            "\U0001F534 TELEGRAM DELIVERY FAILED\n"
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
                else:
                    try:
                        storage.mark_error(queue_id, str(queue_exc), backoff)
                    except Exception:
                        logger.exception("Failed to mark error for queue_id %s", queue_id)
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
            try:
                storage.mark_error(queue_id, str(queue_exc), backoff)
            except Exception:
                logger.exception("Failed to mark error for queue_id %s", queue_id)
            _fail_open_process(config, processor, ctx)


def main(config_dir: Path | None = None, *, max_cycles: int | None = None) -> None:
    print("\n" + "=" * 60)
    print("MAILBOT PREMIUM v26 - STARTING")
    print("=" * 60)
    print(f"Log file: {LOG_PATH}\n")

    logger.info("=== MailBot v26 started ===")
    program_start = datetime.now()
    try:
        processor_module.system_snapshotter.log_startup()
    except Exception as exc:  # pragma: no cover - optional observability
        logger.error("system_health_snapshot_failed", error=str(exc))

    storage: Storage | None = None
    runtime_health = AccountRuntimeHealthManager(CURRENT_DIR / "data" / "runtime_health.json")
    try:
        try:
            base_config_dir = config_dir or CURRENT_DIR / "config"
            config = load_config(base_config_dir)
            logger.info("Configuration loaded: %d accounts", len(config.accounts))
            print(f"[OK] Loaded {len(config.accounts)} accounts")
        except InvalidAccountIdError as exc:
            logger.exception("Invalid account id in accounts.ini")
            print(f"[ERROR] Configuration error: {exc}")
            try:
                general = load_general_config(base_config_dir)
                keys = load_keys_config(base_config_dir)
                if general.admin_chat_id:
                    payload = _build_system_payload(
                        text=f"\U0001F6A8 INVALID ACCOUNT ID\n{exc}",
                        bot_token=keys.telegram_bot_token,
                        chat_id=general.admin_chat_id,
                    )
                    result = send_telegram(payload)
                    ok = result.delivered
                    if not ok:
                        logger.error("Failed to send invalid account id alert")
                else:
                    logger.error("Invalid account id alert skipped: missing admin chat id")
            except Exception:
                logger.exception("Failed to send invalid account id alert")
            time.sleep(10)
            return
        except Exception as exc:
            logger.exception("Failed to load configuration")
            print(f"[ERROR] Configuration error: {exc}")
            time.sleep(10)
            return

        accounts_to_poll = run_startup_mail_account_healthcheck(config, send_telegram)
        for account in config.accounts:
            runtime_health.register_account(account)

        try:
            health_checker = StartupHealthChecker(base_config_dir, config)
            results = health_checker.run()
            mode = health_checker.evaluate_mode(results)
            report = LaunchReportBuilder().build(results, mode)
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

        try:
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

        state = StateManager(CURRENT_DIR / "state.json")
        processor = MessageProcessor(config=config, state=state)
        configure_pipeline(config, processor)
        digest_storage = DigestStorage(
            knowledge_db=processor_module.knowledge_db,
            analytics=processor_module.analytics,
            event_emitter=processor_module.event_emitter,
        )
        print("[OK] Ready to work\n")

        cycle = 0
        try:
            while True:
                cycle += 1
                print(f"\n{'=' * 60}")
                print(f"CYCLE #{cycle} - {time.strftime('%H:%M:%S')}")
                print(f"{'=' * 60}")
                logger.info("Cycle #%d started", cycle)

                try:
                    for account in accounts_to_poll:
                        login = account.login or "no_login"
                        now_utc = datetime.now(timezone.utc)
                        state_snapshot = runtime_health.get_state(account.account_id)
                        if not runtime_health.should_attempt(account.account_id, now_utc):
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
                        )

                        try:
                            imap = ResilientIMAP(
                                account,
                                state,
                                program_start,
                                max_email_mb=config.general.max_email_mb,
                            )
                            new_messages = imap.fetch_new_messages()
                            runtime_health.on_success(
                                account.account_id, datetime.now(timezone.utc)
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
                                print("   └─ no new messages")
                                continue

                            print(f"   └─ received {len(new_messages)} new messages")

                            for uid, raw in new_messages:
                                print(f"      ├─ UID {uid}")
                                try:
                                    inbound = parse_raw_email(raw, config)
                                    subject = inbound.subject[:60] if inbound.subject else "(no subject)"
                                    print(f"      │  Subject: {subject}")

                                    message_obj = message_from_bytes(raw)
                                    message_id = message_obj.get("Message-ID") if message_obj else None
                                    from_header = decode_mime_header(message_obj.get("From", "")) if message_obj else ""
                                    from_name, from_email = parseaddr(from_header or inbound.sender)
                                    received_at = decode_mime_header(message_obj.get("Date", "")) if message_obj else None
                                    attachments_count = len(inbound.attachments or [])

                                    if storage:
                                        email_id = storage.upsert_email(
                                            account_email=account.login,
                                            uid=uid,
                                            message_id=message_id,
                                            from_email=from_email or None,
                                            from_name=from_name or None,
                                            subject=inbound.subject,
                                            received_at=received_at or None,
                                            attachments_count=attachments_count,
                                        )
                                        ctx = PipelineContext(
                                            email_id=email_id,
                                            account_email=account.login,
                                            uid=uid,
                                        )
                                        PIPELINE_CACHE[email_id] = ctx
                                        remember_raw_email(email_id, raw)
                                        store_inbound(email_id, inbound)
                                        storage.enqueue_stage(email_id, "PARSE")
                                        print(f"      │  Enqueued PARSE for email_id={email_id}")
                                    else:
                                        final_text = processor.process(login, inbound)
                                        if final_text and final_text.strip():
                                            payload = _build_system_payload(
                                                text=final_text.strip(),
                                                bot_token=config.keys.telegram_bot_token,
                                                chat_id=account.telegram_chat_id,
                                            )
                                            result = send_telegram(payload)
                                            ok = result.delivered
                                            status = "[OK] sent" if ok else "[FAIL] failed"
                                            print(f"      │  Telegram: {status}")
                                            logger.info(
                                                "UID %s: Telegram %s",
                                                uid,
                                                "OK" if ok else "FAIL",
                                            )
                                        else:
                                            print("      │  Result: empty")

                                except Exception as e:
                                    print(f"      └─ [ERROR] {e}")
                                    logger.exception("Processing error for UID %s", uid)

                            state.save()

                        except Exception as e:
                            now_utc = datetime.now(timezone.utc)
                            should_alert, alert_text = runtime_health.on_failure(
                                account.account_id, e, now_utc
                            )
                            state_snapshot = runtime_health.get_state(account.account_id)
                            print(f"   └─ [IMAP ERROR] {e}")
                            backoff_minutes = 0
                            if state_snapshot.next_retry_at_utc:
                                backoff_minutes = int(
                                    max(
                                        (state_snapshot.next_retry_at_utc - now_utc).total_seconds()
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
                            _process_queue(storage, config, processor)
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

                delay = max(120, config.general.check_interval)
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


if __name__ == "__main__":
    main()
