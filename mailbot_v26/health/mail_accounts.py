from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from typing import Callable, Iterable, List, Optional

from imapclient import IMAPClient

from mailbot_v26.config_loader import AccountConfig, BotConfig
from mailbot_v26.observability import logger as observability_logger


@dataclass
class MailAccountHealth:
    account_email: str
    status: str
    error: str | None


def check_mail_accounts(accounts: Iterable[AccountConfig]) -> List[MailAccountHealth]:
    results: List[MailAccountHealth] = []
    for account in accounts:
        client: Optional[IMAPClient] = None
        try:
            client = IMAPClient(account.host, port=account.port, ssl=account.use_ssl)
            client.login(account.login, account.password)
            client.select_folder("INBOX")
            results.append(
                MailAccountHealth(
                    account_email=account.login,
                    status="OK",
                    error=None,
                )
            )
        except Exception as exc:
            results.append(
                MailAccountHealth(
                    account_email=account.login,
                    status="FAILED",
                    error=str(exc),
                )
            )
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    logging.getLogger(__name__).warning(
                        "IMAP logout failed for %s", account.login
                    )
    return results


def filter_accounts_by_health(
    accounts: Iterable[AccountConfig],
    results: Iterable[MailAccountHealth],
) -> List[AccountConfig]:
    failed = {result.account_email for result in results if result.status != "OK"}
    return [account for account in accounts if account.login not in failed]


def format_warning_message(results: Iterable[MailAccountHealth]) -> str:
    failed = [result for result in results if result.status != "OK"]
    lines = [
        "🔴 ВНИМАНИЕ: недоступны почтовые аккаунты!",
        "Бот НЕ будет обрабатывать письма для них.",
        "",
        "Проблемные аккаунты:",
    ]
    for result in failed:
        reason = result.error or "unknown error"
        lines.append(f"- {result.account_email}: {reason}")
    return "\n".join(lines)


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
        return list(config.accounts)

    observability.warning(
        "mail_account_startup_blocked",
        failed=[asdict(result) for result in failed],
    )

    warning = format_warning_message(results)
    logger.error(
        "\n" + "!" * 80 + "\n"
        "[MAIL-ACCOUNT-ERROR] Обнаружены недоступные почтовые аккаунты."
        "\n" + warning + "\n" + "!" * 80
    )

    chat_id = config.general.admin_chat_id
    if not chat_id and config.accounts:
        chat_id = config.accounts[0].telegram_chat_id
    if chat_id:
        send_telegram_func(config.keys.telegram_bot_token, chat_id, warning)
    else:
        logger.warning("Mail account warning skipped: missing admin chat id")

    return filter_accounts_by_health(config.accounts, results)


__all__ = [
    "MailAccountHealth",
    "check_mail_accounts",
    "filter_accounts_by_health",
    "format_warning_message",
    "run_startup_mail_account_healthcheck",
]
