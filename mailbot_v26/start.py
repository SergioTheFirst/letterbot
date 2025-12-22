"""MailBot Premium v26 - Runtime orchestrator"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
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
from mailbot_v26.config_loader import AccountConfig, BotConfig, load_config
from mailbot_v26.health.mail_accounts import run_startup_mail_account_healthcheck
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor
from mailbot_v26.pipeline import processor as processor_module
from mailbot_v26.state_manager import StateManager
from mailbot_v26.storage.self_check import run_self_check
from mailbot_v26.system.startup_health import (
    LaunchReportBuilder,
    StartupHealthChecker,
    dispatch_launch_report,
)
from mailbot_v26.text.mime_utils import decode_mime_header
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


def _get_account_by_login(config: BotConfig, login: str) -> Optional["AccountConfig"]:
    for acc in config.accounts:
        if acc.login == login:
            return acc
    return None


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

    ok = send_telegram(
        config.keys.telegram_bot_token,
        account.telegram_chat_id,
        final_text.strip(),
    )
    status = "OK" if ok else "FAIL"
    logger.error("Fail-open Telegram send status for email %s: %s", ctx.email_id, status)


def _process_queue(storage: Storage, config: BotConfig, processor: MessageProcessor) -> None:
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
                stage_tg(ctx)
                storage.mark_done(queue_id)
            else:
                storage.mark_done(queue_id)
                logger.warning("Unknown stage %s for email %s", stage, email_id)
        except Exception as queue_exc:
            logger.exception("Queue handling error for email %s", email_id)
            backoff = min(600, 10 * (2 ** attempts))
            try:
                storage.mark_error(queue_id, str(queue_exc), backoff)
            except Exception:
                logger.exception("Failed to mark error for queue_id %s", queue_id)
            _fail_open_process(config, processor, ctx)


def main(config_dir: Path | None = None) -> None:
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
    try:
        try:
            base_config_dir = config_dir or CURRENT_DIR / "config"
            config = load_config(base_config_dir)
            logger.info("Configuration loaded: %d accounts", len(config.accounts))
            print(f"[OK] Loaded {len(config.accounts)} accounts")
        except Exception as exc:
            logger.exception("Failed to load configuration")
            print(f"[ERROR] Configuration error: {exc}")
            time.sleep(10)
            return

        accounts_to_poll = run_startup_mail_account_healthcheck(config, send_telegram)

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
        print("[OK] Ready to work\n")

        cycle = 0
        try:
            while True:
                cycle += 1
                print(f"\n{'=' * 60}")
                print(f"CYCLE #{cycle} - {time.strftime('%H:%M:%S')}")
                print(f"{'=' * 60}")
                logger.info("Cycle %d started", cycle)

                for account in accounts_to_poll:
                    login = account.login or "no_login"
                    print(f"\n[MAIL] Checking: {login}")

                    try:
                        imap = ResilientIMAP(account, state, program_start)
                        new_messages = imap.fetch_new_messages()

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
                                        ok = send_telegram(
                                            config.keys.telegram_bot_token,
                                            account.telegram_chat_id,
                                            final_text.strip(),
                                        )
                                        status = "[OK] sent" if ok else "[FAIL] failed"
                                        print(f"      │  Telegram: {status}")
                                        logger.info("UID %s: Telegram %s", uid, "OK" if ok else "FAIL")
                                    else:
                                        print(f"      │  Result: empty")

                            except Exception as e:
                                print(f"      └─ [ERROR] {e}")
                                logger.exception("Processing error for UID %s", uid)

                        state.save()

                    except Exception as e:
                        print(f"   └─ [IMAP ERROR] {e}")
                        logger.exception("IMAP error for %s", login)

                if storage:
                    try:
                        _process_queue(storage, config, processor)
                    except Exception:
                        logger.exception("Queue dispatcher failure")
                        for ctx in list(PIPELINE_CACHE.values()):
                            _fail_open_process(config, processor, ctx)

                state.save()
                delay = max(120, config.general.check_interval)
                print(f"\n[WAIT] Sleeping {delay} seconds...")
                time.sleep(delay)

        except KeyboardInterrupt:
            print("\n\n[STOP] Stopped by user")
            logger.info("Stopped by user")
        except Exception as e:
            print(f"\n\n[CRITICAL] {e}")
            logger.exception("Fatal error")
            time.sleep(10)
    finally:
        if storage:
            storage.close()


if __name__ == "__main__":
    main()
