from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from typing import Callable, Iterable, List, Optional

from imapclient import IMAPClient

from mailbot_v26.config_loader import AccountConfig, BotConfig
from mailbot_v26.observability import logger as observability_logger
from mailbot_v26.system_health import system_health


@dataclass
class MailAccountHealth:
    account_id: str
    host: str
    status: str
    error: str | None


def check_mail_accounts(accounts: Iterable[AccountConfig]) -> List[MailAccountHealth]:
    results: List[MailAccountHealth] = []
    observability = observability_logger.get_logger("mailbot")
    for account in accounts:
        client: Optional[IMAPClient] = None
        try:
            client = IMAPClient(account.host, port=account.port, ssl=account.use_ssl)
            client.login(account.login, account.password)
            client.select_folder("INBOX")
            results.append(
                MailAccountHealth(
                    account_id=account.account_id,
                    host=account.host,
                    status="OK",
                    error=None,
                )
            )
            observability.info(
                "account_login_ok",
                account_id=account.account_id,
            )
        except Exception as exc:
            logging.getLogger(__name__).error(
                "IMAP login failed for %s: %s",
                account.account_id,
                exc,
            )
            results.append(
                MailAccountHealth(
                    account_id=account.account_id,
                    host=account.host,
                    status="FAILED",
                    error=str(exc),
                )
            )
            observability.error(
                "account_login_failed",
                account_id=account.account_id,
                error=str(exc),
            )
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    logging.getLogger(__name__).warning(
                        "IMAP logout failed for %s", account.account_id
                    )
    return results


def filter_accounts_by_health(
    accounts: Iterable[AccountConfig],
    results: Iterable[MailAccountHealth],
) -> List[AccountConfig]:
    failed = {result.account_id for result in results if result.status != "OK"}
    return [account for account in accounts if account.account_id not in failed]


def format_account_failure_message(result: MailAccountHealth) -> str:
    reason = result.error or "unknown error"
    return "\n".join(
        [
            "\U0001F6A8 ACCOUNT LOGIN FAILED",
            f"Account: {result.account_id}",
            f"Host: {result.host}",
            f"Reason: {reason}",
        ]
    )


def run_startup_mail_account_healthcheck(
    config: BotConfig,
    send_telegram_func: Callable[[str, str, str], bool],
) -> List[AccountConfig]:
    logger = logging.getLogger("mailbot")
    observability = observability_logger.get_logger("mailbot")
    results = check_mail_accounts(config.accounts)
    observability.info(
        "mail_account_healthcheck",
        results=[asdict(result) for result in results],
    )

    failed = [result for result in results if result.status != "OK"]
    if not failed:
        system_health.update_component("Mail", True)
        return list(config.accounts)

    observability.warning(
        "mail_account_startup_blocked",
        failed=[asdict(result) for result in failed],
    )

    chat_id = config.general.admin_chat_id
    if not chat_id and config.accounts:
        chat_id = config.accounts[0].telegram_chat_id
    if not chat_id:
        logger.error("Mail account warning skipped: missing admin chat id")
    else:
        for result in failed:
            warning = format_account_failure_message(result)
            ok = send_telegram_func(
                config.keys.telegram_bot_token,
                chat_id,
                warning,
            )
            if not ok:
                logger.error(
                    "Mail account warning failed to send for %s", result.account_id
                )

    accounts_to_poll = filter_accounts_by_health(config.accounts, results)
    if not accounts_to_poll:
        logger.error(
            "\n" + "!" * 80 + "\n"
            "[MAIL-ACCOUNT-ERROR] Все IMAP аккаунты недоступны. EMERGENCY MODE."
            "\n" + "!" * 80
        )
        system_health.update_component("Mail", False, reason="All IMAP accounts failed")
    else:
        system_health.update_component("Mail", True)

    return accounts_to_poll


__all__ = [
    "MailAccountHealth",
    "check_mail_accounts",
    "filter_accounts_by_health",
    "format_account_failure_message",
    "run_startup_mail_account_healthcheck",
]
