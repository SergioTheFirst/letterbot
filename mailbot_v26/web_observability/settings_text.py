"""EN/RU copy for the Settings lane pages (/accounts, later the editor).

Kept apart from ui/i18n.py so the dashboard lane can change its own catalog
without merge conflicts. Every key exists in both languages with the same
{placeholders}; test_settings_accounts_page guards the parity. Fix-card
snippets use only CHANGE_ME placeholders, never a user's real values.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

SNIPPET_PLACEHOLDER = "CHANGE_ME"

_EN: dict[str, str] = {
    # Page shell
    "accounts.title": "Mail accounts",
    "accounts.read_only": (
        "Mailbox status and settings. Changes are saved to accounts.ini with a "
        "backup; restart Letterbot to apply them."
    ),
    "accounts.file_label": "Settings file",
    "accounts.updated": "Updated at {time}",
    "accounts.refresh": "Refresh",
    "accounts.unavailable": (
        "The mailbox status can't be shown right now. Details are in the "
        "Letterbot log."
    ),
    "accounts.http_warning": "Unencrypted connection: use only on a trusted network.",
    "accounts.mailboxes": "Mailboxes",
    "accounts.col.mailbox": "Mailbox",
    "accounts.col.login": "Login",
    "accounts.col.provider": "Provider",
    "accounts.col.server": "Server",
    "accounts.col.last_check": "Last check",
    "accounts.col.chat": "Telegram chat",
    "accounts.col.state": "Status",
    "accounts.blocking.title": "Letterbot will not start with this accounts.ini",
    "accounts.blocking.intro": (
        "Letterbot checks these settings at every start (for example after a "
        "restart). Fix:"
    ),
    "accounts.fix.title": "How to fix",
    "accounts.telegram.title": "Telegram",
    "accounts.telegram.token": "Bot token",
    "accounts.telegram.default_chat": "Default chat",
    "accounts.telegram.hint_token": (
        "Create a bot with @BotFather and put its token under [telegram] as "
        "bot_token."
    ),
    "accounts.telegram.hint_chat": (
        "Chat id: a number like 272250747, or -100… for groups. Put it under "
        "[telegram] as chat_id (used by every mailbox) or as telegram_chat_id in a "
        "mailbox section."
    ),
    "notice.no_config_dir": (
        "This web console was started without a settings folder, so it can't show "
        "mailboxes. Start it through Letterbot (letterbot.bat or Letterbot.exe)."
    ),
    "notice.db_unavailable": (
        "Letterbot's database can't be read right now, so the last checks are "
        "unknown."
    ),
    # Secrets and chats
    "secret.set": "Set",
    "secret.missing": "Missing",
    "chat.set_mailbox": "Set (this mailbox)",
    "chat.set_default": "Set (default)",
    "chat.missing": "Missing",
    "chat.invalid": "Invalid",
    "chat.default.set": "Set",
    "chat.default.missing": "Not set",
    "chat.default.invalid": "Invalid",
    # Mailbox states
    "state.ok": "OK",
    "state.delayed": "Delayed",
    "state.not_checking": "Not checking",
    "state.not_checked": "Not checked yet",
    "state.unknown": "Status unknown",
    "state.setup_incomplete": "Setup incomplete",
    "state.login_rejected": "Login rejected",
    "state.cant_reach": "Can't reach server",
    "state.paused": "Paused until {time}",
    "state.connection_problem": "Connection problem",
    "state.unsupported": "Not supported",
    "detail.delayed": "Last successful check: {age}.",
    "detail.not_checking": "Last successful check: {age}. Is Letterbot running?",
    "detail.not_checked": "Letterbot checks a mailbox a few minutes after it starts.",
    "detail.unknown": "Letterbot's database can't be read right now.",
    "detail.login_rejected.gmail": "Gmail needs an app password, not your usual one.",
    "detail.login_rejected.yandex": (
        "Yandex needs an app password, not your usual one."
    ),
    "detail.login_rejected.mailru": (
        "Mail.ru needs a password for an external application, not your usual one."
    ),
    "detail.login_rejected.other": "The server refused the login and password.",
    "detail.cant_reach": (
        "Letterbot can't connect to the server. Check host, port and use_ssl."
    ),
    "detail.paused": (
        "Paused after repeated connection timeouts; Letterbot tries again by itself."
    ),
    "detail.connection_problem": "The last check failed.",
    "detail.unsupported": (
        "Outlook.com and Microsoft 365 don't accept passwords over IMAP."
    ),
    "detail.next_try": "Next try at {time}.",
    # Ages
    "age.never": "never",
    "age.unknown": "unknown",
    "age.just_now": "just now",
    "age.minutes": "{n} min ago",
    "age.hours": "{n} h ago",
    "age.days": "{n} d ago",
    # Server and providers
    "server.ssl": "SSL",
    "server.no_ssl": "no SSL",
    "provider.gmail": "Gmail",
    "provider.yandex": "Yandex",
    "provider.mailru": "Mail.ru",
    "provider.outlook": "Outlook / Microsoft 365",
    "provider.other": "Other",
    # Summary and Home card
    "summary.ok": "{n} OK",
    "summary.attention.one": "{n} needs attention",
    "summary.attention.other": "{n} need attention",
    "summary.waiting.one": "{n} not checked yet",
    "summary.waiting.other": "{n} not checked yet",
    "summary.no_config_dir": "Settings folder unknown",
    "summary.missing": "accounts.ini not found",
    "summary.no_mailboxes": "No mailbox added yet",
    "summary.encoding": "accounts.ini is not UTF-8",
    "summary.unreadable": "accounts.ini can't be read",
    "home.label": "Mailboxes",
    "home.link": "Open mail accounts",
    "home.unavailable": "Status unavailable",
    "home.token_missing": "Telegram bot token missing",
    "home.startup_blocked": "Letterbot will not start with this accounts.ini",
    # Issues (section and key names only, never values)
    "issue.accounts_missing": "accounts.ini is missing in the settings folder",
    "issue.no_mailboxes": "accounts.ini has no mailbox section yet",
    "issue.encoding": "accounts.ini is not saved as UTF-8",
    "issue.unreadable": "accounts.ini has a syntax mistake",
    "issue.settings_missing": (
        "settings.ini is missing in the settings folder (copy settings.ini.example)"
    ),
    "issue.missing_field": "[{section}] {field} is empty or still CHANGE_ME",
    "issue.example_section": (
        "[{section}] is still the template: rename it (for example to "
        "[work_mail]) and fill in your values"
    ),
    "issue.percent_doubled": (
        "[{section}] {field} contains %%: Letterbot reads % as written, so %% "
        "means two % signs"
    ),
    "issue.percent_doubled_unnamed": (
        "[{section}] a value contains %%: Letterbot reads % as written, so %% "
        "means two % signs"
    ),
    "issue.chat_missing": (
        "[{section}] has no Telegram chat: set telegram_chat_id here or chat_id "
        "under [telegram]"
    ),
    "issue.chat_invalid": "[{section}] {field} is not a chat id",
    "issue.chat_format": "[{section}] {field} doesn't look like a chat id",
    "issue.token_missing": "[{section}] {field} is empty or still CHANGE_ME",
    "issue.bad_section_name": (
        "[{section}] is not a valid name (lowercase letters, digits and _ only): "
        "Letterbot skips it"
    ),
    "issue.bad_port": "[{section}] port must be a number from 1 to 65535 (usually 993)",
    "issue.bad_ssl": "[{section}] use_ssl must be true or false",
    "issue.ssl_mismatch": "[{section}] port 993 needs use_ssl = true",
    "issue.bad_host": (
        "[{section}] host doesn't look like a server name (for example "
        "imap.gmail.com)"
    ),
    # Settings editor (S2) checks of typed values
    "issue.bad_type": "[{section}] {field} has a value of the wrong kind",
    "issue.bad_characters": (
        "[{section}] {field}: line breaks and control characters are not allowed"
    ),
    "issue.too_long": "[{section}] {field} is too long",
    "issue.edge_spaces": "[{section}] {field} must not start or end with a space",
    "issue.required": "[{section}] {field} is required",
    "issue.bad_id": (
        "Mailbox name: lowercase Latin letters, digits and _ only, for example "
        "work_mail"
    ),
    "issue.unknown_account": "[{section}] is no longer in accounts.ini: reload the page",
    "issue.account_exists": "[{section}] already exists: choose another name",
    "issue.unknown_field": "[{section}] the form sent a field Letterbot doesn't know",
    "issue.bad_request": "The request was not understood: reload the page",
    "issue.outlook_unsupported": (
        "[{section}] Outlook.com / Microsoft 365 don't accept passwords for IMAP: "
        "forward the mail to a supported mailbox"
    ),
    "issue.plain_imap": "[{section}] without SSL the password travels unencrypted",
    "issue.token_format": (
        "[{section}] {field} doesn't look like a bot token from @BotFather"
    ),
    "issue.layout": (
        "accounts.ini could not be changed safely: edit it in a text editor"
    ),
    # Fix cards
    "card.copy": "Copy",
    "card.copied": "Copied",
    "card.copy_failed": "Copy failed: select the text and press Ctrl+C",
    "card.server": "Server",
    "card.server_line": "{host} · port {port} · SSL",
    "card.snippet_label": "Example for accounts.ini (replace CHANGE_ME)",
    "card.first_run.title": "Add your first mailbox",
    "card.first_run.step1": (
        "Open accounts.ini in the settings folder (create it if it is missing; "
        "accounts.ini.example is a template)."
    ),
    "card.first_run.step2": (
        "Add a section like the example below. If [example_account] is still "
        "there, rename it: lowercase letters, digits and _ only."
    ),
    "card.first_run.step3": (
        "Fill in login, password (an app password for Gmail, Yandex and Mail.ru), "
        "host, port = 993 and use_ssl = true."
    ),
    "card.first_run.step4": "Under [telegram], set bot_token and a chat id.",
    "card.first_run.step5": "Save the file as UTF-8 and restart Letterbot.",
    "card.encoding.title": "Save accounts.ini as UTF-8",
    "card.encoding.intro": (
        "accounts.ini is not saved as UTF-8, so Letterbot can't read any mailbox "
        "from it."
    ),
    "card.encoding.step1": "Open accounts.ini in Notepad.",
    "card.encoding.step2": "Choose File > Save As, set Encoding to UTF-8 and save.",
    "card.encoding.step3": "Restart Letterbot.",
    "card.unreadable.title": "accounts.ini can't be read",
    "card.unreadable.intro": "There is a mistake in the file.",
    "card.unreadable.intro_line": "There is a mistake on line {line}.",
    "card.unreadable.step1": (
        "Every setting must be under a [section] header and written as key = value."
    ),
    "card.unreadable.step2": (
        "Each [section] may appear only once, and each key only once inside a "
        "section."
    ),
    "card.percent.title": "A value contains %%",
    "card.percent.intro": (
        "Letterbot reads accounts.ini as written, % included. Older versions "
        "needed %% for each %."
    ),
    "card.percent.step1": (
        "If you wrote %% to get one %, write a single % again: 50%%off becomes "
        "50%off."
    ),
    "card.gmail.title": "Gmail",
    "card.gmail.step1": "Turn on 2-Step Verification for your Google account.",
    "card.gmail.step2": (
        "Create an app password at myaccount.google.com/apppasswords and use it as "
        "password: 16 letters, without spaces."
    ),
    "card.gmail.step3": "Google Workspace: the administrator must allow IMAP access.",
    "card.yandex.title": "Yandex Mail",
    "card.yandex.step1": (
        "In Yandex Mail settings, open Mail clients and allow access via IMAP with "
        "app passwords."
    ),
    "card.yandex.step2": (
        "Create an app password for Mail in Yandex ID (Security > App passwords) "
        "and use it as password."
    ),
    "card.mailru.title": "Mail.ru",
    "card.mailru.step1": (
        "In Mail.ru settings, open Security and create a password for an external "
        "application."
    ),
    "card.mailru.step2": "Use it as password: your usual password will not work.",
    "card.outlook.title": "Outlook.com / Microsoft 365: not supported",
    "card.outlook.intro": (
        "Since September 2024 Microsoft accepts only OAuth sign-in for IMAP, not "
        "passwords, so Letterbot can't read this mailbox."
    ),
    "card.outlook.step1": (
        "Set up forwarding of new mail to a Gmail, Yandex or Mail.ru mailbox."
    ),
    "card.outlook.step2": (
        "Add that mailbox to accounts.ini and remove the Outlook section."
    ),
    "card.generic.title": "Other mail servers",
    "card.generic.step1": (
        "Check the host name for typos (for example imap.yourcompany.com)."
    ),
    "card.generic.step2": "Port 993 needs use_ssl = true.",
    "card.generic.step3": (
        "Rename [example_account]: lowercase letters, digits and _ only, for "
        "example [work_mail]."
    ),
    "card.generic.step4": "Save accounts.ini as UTF-8.",
    "card.generic.step5": (
        "Telegram chat id: a number like 272250747, or -100… for groups."
    ),
    # Telegram test message (plain text, sent only on "Send test message")
    "telegram.test_message": (
        "Letterbot test message: notifications will arrive in this chat."
    ),
    # Settings editor (S2): page labels and settings.js messages
    "editor.title": "Change settings",
    "editor.add": "Add mailbox",
    "editor.telegram": "Telegram",
    "editor.history": "History",
    "editor.loading": "Loading settings…",
    "editor.load_failed": (
        "Settings can't be loaded right now. Details are in the Letterbot log."
    ),
    "editor.file_problem": (
        "accounts.ini can't be read (see above). Fix it in a text editor or "
        "restore a backup from History."
    ),
    "editor.list_empty": "No mailboxes yet: add the first one.",
    "editor.edit": "Edit",
    "editor.remove": "Remove",
    "editor.template_row": "template from the first start",
    "editor.invalid_row": (
        "a section with a name Letterbot can't use: rename it in accounts.ini"
    ),
    "editor.form_add": "New mailbox",
    "editor.form_edit": "Mailbox [{id}]",
    "editor.provider": "Mail service",
    "editor.provider.gmail": "Gmail",
    "editor.provider.yandex": "Yandex",
    "editor.provider.mailru": "Mail.ru",
    "editor.provider.outlook": "Outlook.com / Microsoft 365",
    "editor.provider.other": "Other server",
    "editor.outlook": (
        "Outlook.com and Microsoft 365 no longer accept passwords for IMAP. "
        "Forward new mail to a Gmail, Yandex or Mail.ru mailbox and add that one."
    ),
    "editor.name": "Name in Letterbot",
    "editor.name_hint": "Lowercase Latin letters, digits and _, for example work_mail",
    "editor.login": "Login (e-mail address)",
    "editor.login_hidden": (
        "The saved login is not shown: type it again to change it."
    ),
    "editor.password": "Password",
    "editor.password_hint": "For Gmail, Yandex and Mail.ru use an app password.",
    "editor.password_saved": "Password saved",
    "editor.replace": "Replace",
    "editor.host": "IMAP server",
    "editor.port": "Port",
    "editor.ssl": "Use SSL (port 993)",
    "editor.chat": "Telegram chat",
    "editor.chat_default": "Default chat",
    "editor.chat_custom": "Own chat id",
    "editor.chat_hint": "A number like 272250747, or -100… for groups",
    "editor.test": "Test connection",
    "editor.testing": "Checking…",
    "editor.save": "Save",
    "editor.saving": "Saving…",
    "editor.cancel": "Cancel",
    "editor.reload": "Reload",
    "editor.saved": (
        "Saved {time}. Backup: {name}. Restart Letterbot to apply the changes."
    ),
    "editor.saved_first": "Saved {time}. Restart Letterbot to apply the changes.",
    "editor.conflict": "accounts.ini changed on disk. Reload.",
    "editor.no_changes": "Nothing to save: no field changed.",
    "editor.rejected": "Not saved: fix the marked fields.",
    "editor.failed": (
        "Not saved: the file could not be written. Details are in the Letterbot "
        "log."
    ),
    "editor.unreadable": (
        "accounts.ini can't be read: restore a backup or fix the file first."
    ),
    "editor.remove_confirm": (
        "Remove [{id}]? Letterbot stops checking this mailbox; processed history "
        "stays."
    ),
    "editor.history_title": "Backups of accounts.ini",
    "editor.history_empty": "No backups yet: one is made before every save.",
    "editor.restore": "Restore",
    "editor.restore_confirm": (
        "Restore {name}? The current accounts.ini is backed up first."
    ),
    "editor.restored": "Restored {name} at {time}. Restart Letterbot to apply.",
    "editor.issues_title": "Check these:",
    "editor.request_failed": "The request failed. Reload the page.",
    "editor.test_warning": "The check failed, but you can still save.",
    "editor.result.OK": "Connected, inbox found",
    "editor.result.AUTH": (
        "Login rejected. Gmail needs an app password, not your usual one."
    ),
    "editor.result.DNS": "Server not found: check the server name for typos.",
    "editor.result.REFUSED": (
        "Can't reach the server: check the server, the port and the network."
    ),
    "editor.result.TLS": (
        "Secure connection failed: port 993 needs SSL, port 143 usually not."
    ),
    "editor.result.TIMEOUT": "The server did not answer within 10 seconds.",
    "editor.result.OTHER": (
        "The check failed. For privacy the server's answer is not shown."
    ),
    "editor.result.RATE_LIMIT": "Wait a few seconds before the next check.",
    "editor.result.BUSY": "Another check is still running. Try again in a moment.",
    "editor.result.PORT_NOT_ALLOWED": (
        "Only ports 143 and 993 can be tested before the mailbox is saved."
    ),
    "editor.result.HOST_NOT_ALLOWED": "This server address can't be tested.",
    "editor.result.PASSWORD_REQUIRED": (
        "Enter the password: the saved one is used only for the saved server."
    ),
    "editor.result.LOGIN_REQUIRED": "Enter the login.",
    "editor.result.INVALID": "Check the server, port and login fields.",
    "editor.tg_title": "Telegram bot",
    "editor.tg_token": "Bot token",
    "editor.tg_token_saved": "Token saved",
    "editor.tg_token_hint": "From @BotFather: digits, a colon, then letters",
    "editor.tg_chat": "Default chat id",
    "editor.tg_check": "Check bot",
    "editor.tg_send": "Send test message",
    "editor.tg.OK_getme": "Bot @{name} answers.",
    "editor.tg.OK_getme_anonymous": "The bot answers.",
    "editor.tg.OK_send": "Test message sent: check Telegram.",
    "editor.tg.AUTH": "Telegram rejected the bot token.",
    "editor.tg.CHAT": (
        "Telegram can't deliver to this chat: send /start to the bot first or "
        "check the chat id."
    ),
    "editor.tg.NETWORK": "Can't reach Telegram: check the internet connection.",
    "editor.tg.TLS": "The secure connection to Telegram failed.",
    "editor.tg.TIMEOUT": "Telegram did not answer within 10 seconds.",
    "editor.tg.OTHER": "Telegram did not accept the request.",
    "editor.tg.TOKEN_MISSING": "Enter the bot token first.",
    "editor.tg.TOKEN_FORMAT": "This doesn't look like a bot token.",
    "editor.tg.CHAT_MISSING": "Enter a chat id or save a default chat first.",
    "editor.web_title": "Dashboard password and network access",
    "editor.web_password_set": "Dashboard password: set",
    "editor.web_password_missing": (
        "Dashboard password: not set (the dashboard opens only on this computer)"
    ),
    "editor.web_new_password": "New dashboard password",
    "editor.web_current_password": "Current dashboard password",
    "editor.web_password_hint": "At least 10 characters.",
    "editor.web_save_password": "Save password",
    "editor.web_password_saved": "Password saved {time}.",
    "editor.web.LENGTH": "Use at least 10 characters.",
    "editor.web.WEAK": "This password is too easy to guess.",
    "editor.web.INVALID": (
        "Line breaks and spaces at the start or end are not allowed."
    ),
    "editor.web.CURRENT_PASSWORD_WRONG": "The current password is wrong.",
    "editor.web.ENV_OVERRIDE": (
        "The WEB_PASSWORD environment variable sets the password; change it there."
    ),
    "editor.web.LEGACY_CONFIG": (
        "This installation still uses config.ini: run migrate-config first."
    ),
    "editor.web.RATE_LIMIT": "Too many attempts. Try again later.",
    "editor.web.LOOPBACK_ONLY": (
        "Network access can be changed only on the Letterbot computer."
    ),
    "editor.web.LAUNCHER_UNSUPPORTED": (
        "This launcher takes its network settings from config.yaml (web_ui)."
    ),
    "editor.web.PASSWORD_REQUIRED": "Set a dashboard password first.",
    "editor.web_lan_on": "Network access: on (local network, password login)",
    "editor.web_lan_off": "Network access: off (this computer only)",
    "editor.web_enable_lan": "Allow from the local network",
    "editor.web_disable_lan": "This computer only",
    "editor.web_restart": "Restart Letterbot to apply.",
    "editor.web_lan_hint": (
        "Other devices sign in with the dashboard password. The connection is not "
        "encrypted: use it only on a trusted network."
    ),
}

_RU: dict[str, str] = {
    # Page shell
    "accounts.title": "Почтовые ящики",
    "accounts.read_only": (
        "Состояние и настройки ящиков. Изменения сохраняются в accounts.ini с "
        "резервной копией; чтобы они вступили в силу, перезапустите Letterbot."
    ),
    "accounts.file_label": "Файл настроек",
    "accounts.updated": "Обновлено в {time}",
    "accounts.refresh": "Обновить",
    "accounts.unavailable": (
        "Состояние ящиков сейчас не удаётся показать. Подробности в журнале "
        "Letterbot."
    ),
    "accounts.http_warning": (
        "Соединение не зашифровано: используйте только в доверенной сети."
    ),
    "accounts.mailboxes": "Ящики",
    "accounts.col.mailbox": "Ящик",
    "accounts.col.login": "Логин",
    "accounts.col.provider": "Почтовый сервис",
    "accounts.col.server": "Сервер",
    "accounts.col.last_check": "Последняя проверка",
    "accounts.col.chat": "Чат Telegram",
    "accounts.col.state": "Состояние",
    "accounts.blocking.title": "С этим accounts.ini Letterbot не запустится",
    "accounts.blocking.intro": (
        "Letterbot проверяет эти настройки при каждом запуске (например, после "
        "перезапуска). Исправьте:"
    ),
    "accounts.fix.title": "Как исправить",
    "accounts.telegram.title": "Telegram",
    "accounts.telegram.token": "Токен бота",
    "accounts.telegram.default_chat": "Чат по умолчанию",
    "accounts.telegram.hint_token": (
        "Создайте бота через @BotFather и укажите его токен в разделе [telegram] в "
        "bot_token."
    ),
    "accounts.telegram.hint_chat": (
        "ID чата: число вида 272250747 или -100… для групп. Укажите его в разделе "
        "[telegram] в chat_id (для всех ящиков) или в telegram_chat_id в разделе "
        "ящика."
    ),
    "notice.no_config_dir": (
        "Веб-консоль запущена без папки настроек, поэтому не может показать ящики. "
        "Запускайте её через Letterbot (letterbot.bat или Letterbot.exe)."
    ),
    "notice.db_unavailable": (
        "База данных Letterbot сейчас недоступна для чтения, поэтому время "
        "последних проверок неизвестно."
    ),
    # Secrets and chats
    "secret.set": "Задан",
    "secret.missing": "Не задан",
    "chat.set_mailbox": "Задан (для ящика)",
    "chat.set_default": "Задан (по умолчанию)",
    "chat.missing": "Не задан",
    "chat.invalid": "Ошибка",
    "chat.default.set": "Задан",
    "chat.default.missing": "Не задан",
    "chat.default.invalid": "Ошибка",
    # Mailbox states
    "state.ok": "Работает",
    "state.delayed": "С задержкой",
    "state.not_checking": "Не проверяется",
    "state.not_checked": "Ещё не проверялся",
    "state.unknown": "Состояние неизвестно",
    "state.setup_incomplete": "Настройка не завершена",
    "state.login_rejected": "Вход отклонён",
    "state.cant_reach": "Сервер недоступен",
    "state.paused": "Пауза до {time}",
    "state.connection_problem": "Ошибка подключения",
    "state.unsupported": "Не поддерживается",
    "detail.delayed": "Последняя успешная проверка: {age}.",
    "detail.not_checking": "Последняя успешная проверка: {age}. Letterbot запущен?",
    "detail.not_checked": (
        "Letterbot проверяет ящик через несколько минут после запуска."
    ),
    "detail.unknown": "База данных Letterbot сейчас недоступна для чтения.",
    "detail.login_rejected.gmail": "Для Gmail нужен пароль приложения, а не обычный.",
    "detail.login_rejected.yandex": (
        "Для Яндекса нужен пароль приложения, а не обычный."
    ),
    "detail.login_rejected.mailru": (
        "Для Mail.ru нужен пароль для внешнего приложения, а не обычный."
    ),
    "detail.login_rejected.other": "Сервер отклонил логин и пароль.",
    "detail.cant_reach": (
        "Letterbot не может подключиться к серверу. Проверьте host, port и use_ssl."
    ),
    "detail.paused": (
        "Пауза после повторяющихся тайм-аутов соединения; Letterbot попробует "
        "снова сам."
    ),
    "detail.connection_problem": "Последняя проверка завершилась ошибкой.",
    "detail.unsupported": "Outlook.com и Microsoft 365 не принимают пароль по IMAP.",
    "detail.next_try": "Следующая попытка в {time}.",
    # Ages
    "age.never": "никогда",
    "age.unknown": "неизвестно",
    "age.just_now": "только что",
    "age.minutes": "{n} мин назад",
    "age.hours": "{n} ч назад",
    "age.days": "{n} дн. назад",
    # Server and providers
    "server.ssl": "SSL",
    "server.no_ssl": "без SSL",
    "provider.gmail": "Gmail",
    "provider.yandex": "Яндекс",
    "provider.mailru": "Mail.ru",
    "provider.outlook": "Outlook / Microsoft 365",
    "provider.other": "Другой",
    # Summary and Home card
    "summary.ok": "{n} в порядке",
    "summary.attention.one": "{n} требует внимания",
    "summary.attention.other": "{n} требуют внимания",
    "summary.waiting.one": "{n} ещё не проверен",
    "summary.waiting.other": "{n} ещё не проверены",
    "summary.no_config_dir": "Папка настроек неизвестна",
    "summary.missing": "Файл accounts.ini не найден",
    "summary.no_mailboxes": "Ящики ещё не добавлены",
    "summary.encoding": "accounts.ini не в кодировке UTF-8",
    "summary.unreadable": "Не удаётся прочитать accounts.ini",
    "home.label": "Почтовые ящики",
    "home.link": "Открыть почтовые ящики",
    "home.unavailable": "Состояние недоступно",
    "home.token_missing": "Не задан токен бота Telegram",
    "home.startup_blocked": "С этим accounts.ini Letterbot не запустится",
    # Issues (section and key names only, never values)
    "issue.accounts_missing": "В папке настроек нет accounts.ini",
    "issue.no_mailboxes": "В accounts.ini ещё нет раздела с ящиком",
    "issue.encoding": "accounts.ini сохранён не в UTF-8",
    "issue.unreadable": "В accounts.ini синтаксическая ошибка",
    "issue.settings_missing": (
        "В папке настроек нет settings.ini (скопируйте settings.ini.example)"
    ),
    "issue.missing_field": "[{section}] {field}: пусто или всё ещё CHANGE_ME",
    "issue.example_section": (
        "[{section}] — всё ещё шаблон: переименуйте раздел (например, в "
        "[work_mail]) и впишите свои значения"
    ),
    "issue.percent_doubled": (
        "[{section}] {field} содержит %%: Letterbot читает % как написано, "
        "поэтому %% — это два знака %"
    ),
    "issue.percent_doubled_unnamed": (
        "[{section}] в одном из значений есть %%: Letterbot читает % как "
        "написано, поэтому %% — это два знака %"
    ),
    "issue.chat_missing": (
        "[{section}]: не задан чат Telegram — укажите telegram_chat_id здесь или "
        "chat_id в [telegram]"
    ),
    "issue.chat_invalid": "[{section}] {field}: это не ID чата",
    "issue.chat_format": "[{section}] {field}: не похоже на ID чата",
    "issue.token_missing": "[{section}] {field}: пусто или всё ещё CHANGE_ME",
    "issue.bad_section_name": (
        "[{section}] — недопустимое имя (только строчные латинские буквы, цифры и "
        "_): Letterbot пропускает этот раздел"
    ),
    "issue.bad_port": "[{section}] port должен быть числом от 1 до 65535 (обычно 993)",
    "issue.bad_ssl": "[{section}] use_ssl должен быть true или false",
    "issue.ssl_mismatch": "[{section}] для порта 993 нужно use_ssl = true",
    "issue.bad_host": (
        "[{section}] host не похож на имя сервера (например, imap.gmail.com)"
    ),
    # Settings editor (S2) checks of typed values
    "issue.bad_type": "[{section}] {field}: значение не того вида",
    "issue.bad_characters": (
        "[{section}] {field}: переносы строк и управляющие символы не допускаются"
    ),
    "issue.too_long": "[{section}] {field}: слишком длинное значение",
    "issue.edge_spaces": (
        "[{section}] {field} не должен начинаться или заканчиваться пробелом"
    ),
    "issue.required": "[{section}] {field}: обязательное поле",
    "issue.bad_id": (
        "Имя ящика: только строчные латинские буквы, цифры и _, например work_mail"
    ),
    "issue.unknown_account": "[{section}] больше нет в accounts.ini: обновите страницу",
    "issue.account_exists": "[{section}] уже есть: выберите другое имя",
    "issue.unknown_field": "[{section}] форма прислала неизвестное поле",
    "issue.bad_request": "Запрос не распознан: обновите страницу",
    "issue.outlook_unsupported": (
        "[{section}] Outlook.com / Microsoft 365 не принимают пароли для IMAP: "
        "настройте пересылку в поддерживаемый ящик"
    ),
    "issue.plain_imap": "[{section}] без SSL пароль передаётся без шифрования",
    "issue.token_format": "[{section}] {field} не похож на токен бота от @BotFather",
    "issue.layout": (
        "accounts.ini не удалось безопасно изменить: отредактируйте его в "
        "текстовом редакторе"
    ),
    # Fix cards
    "card.copy": "Копировать",
    "card.copied": "Скопировано",
    "card.copy_failed": "Не удалось скопировать: выделите текст и нажмите Ctrl+C",
    "card.server": "Сервер",
    "card.server_line": "{host} · порт {port} · SSL",
    "card.snippet_label": "Пример для accounts.ini (замените CHANGE_ME)",
    "card.first_run.title": "Добавьте первый ящик",
    "card.first_run.step1": (
        "Откройте accounts.ini в папке настроек (если файла нет, создайте его; "
        "шаблон — accounts.ini.example)."
    ),
    "card.first_run.step2": (
        "Добавьте раздел, как в примере ниже. Если в файле остался "
        "[example_account], переименуйте его: только строчные латинские буквы, "
        "цифры и _."
    ),
    "card.first_run.step3": (
        "Заполните login, password (для Gmail, Яндекса и Mail.ru — пароль "
        "приложения), host, port = 993 и use_ssl = true."
    ),
    "card.first_run.step4": "В разделе [telegram] укажите bot_token и ID чата.",
    "card.first_run.step5": "Сохраните файл в UTF-8 и перезапустите Letterbot.",
    "card.encoding.title": "Сохраните accounts.ini в UTF-8",
    "card.encoding.intro": (
        "accounts.ini сохранён не в UTF-8, поэтому Letterbot не может прочитать "
        "из него ни одного ящика."
    ),
    "card.encoding.step1": "Откройте accounts.ini в Блокноте.",
    "card.encoding.step2": (
        "Выберите «Файл > Сохранить как», укажите кодировку UTF-8 и сохраните."
    ),
    "card.encoding.step3": "Перезапустите Letterbot.",
    "card.unreadable.title": "Не удаётся прочитать accounts.ini",
    "card.unreadable.intro": "В файле есть ошибка.",
    "card.unreadable.intro_line": "Ошибка в строке {line}.",
    "card.unreadable.step1": (
        "Каждая настройка должна стоять под заголовком [раздел] и иметь вид "
        "ключ = значение."
    ),
    "card.unreadable.step2": (
        "Каждый [раздел] может встречаться только один раз, а каждый ключ — один "
        "раз внутри раздела."
    ),
    "card.percent.title": "Значение содержит %%",
    "card.percent.intro": (
        "Letterbot читает accounts.ini как написано, вместе со знаком %. Старым "
        "версиям нужно было %% вместо каждого %."
    ),
    "card.percent.step1": (
        "Если вы писали %% ради одного %, верните один %: 50%%off записывается "
        "как 50%off."
    ),
    "card.gmail.title": "Gmail",
    "card.gmail.step1": "Включите двухэтапную аутентификацию в аккаунте Google.",
    "card.gmail.step2": (
        "Создайте пароль приложения на myaccount.google.com/apppasswords и укажите "
        "его в password: 16 букв, без пробелов."
    ),
    "card.gmail.step3": (
        "Google Workspace: администратор должен разрешить доступ по IMAP."
    ),
    "card.yandex.title": "Яндекс Почта",
    "card.yandex.step1": (
        "В настройках Яндекс Почты откройте «Почтовые программы» и разрешите "
        "доступ по IMAP с паролями приложений."
    ),
    "card.yandex.step2": (
        "Создайте пароль приложения для почты в Яндекс ID (Безопасность > Пароли "
        "приложений) и укажите его в password."
    ),
    "card.mailru.title": "Mail.ru",
    "card.mailru.step1": (
        "В настройках Mail.ru откройте «Безопасность» и создайте пароль для "
        "внешнего приложения."
    ),
    "card.mailru.step2": "Укажите его в password: обычный пароль не подойдёт.",
    "card.outlook.title": "Outlook.com / Microsoft 365: не поддерживается",
    "card.outlook.intro": (
        "С сентября 2024 года Microsoft принимает для IMAP только вход через OAuth, "
        "а не пароль, поэтому Letterbot не может читать этот ящик."
    ),
    "card.outlook.step1": "Настройте пересылку новых писем в ящик Gmail, Яндекс или Mail.ru.",
    "card.outlook.step2": "Добавьте этот ящик в accounts.ini и удалите раздел Outlook.",
    "card.generic.title": "Другие почтовые серверы",
    "card.generic.step1": (
        "Проверьте имя сервера host на опечатки (например, imap.yourcompany.com)."
    ),
    "card.generic.step2": "Для порта 993 нужно use_ssl = true.",
    "card.generic.step3": (
        "Переименуйте [example_account]: только строчные латинские буквы, цифры и "
        "_, например [work_mail]."
    ),
    "card.generic.step4": "Сохраните accounts.ini в UTF-8.",
    "card.generic.step5": "ID чата Telegram: число вида 272250747 или -100… для групп.",
    # Telegram test message (plain text, sent only on "Send test message")
    "telegram.test_message": (
        "Тестовое сообщение Letterbot: уведомления будут приходить в этот чат."
    ),
    # Settings editor (S2): page labels and settings.js messages
    "editor.title": "Изменить настройки",
    "editor.add": "Добавить ящик",
    "editor.telegram": "Telegram",
    "editor.history": "История",
    "editor.loading": "Загружаем настройки…",
    "editor.load_failed": (
        "Сейчас не удаётся загрузить настройки. Подробности — в журнале Letterbot."
    ),
    "editor.file_problem": (
        "accounts.ini не читается (см. выше). Исправьте его в текстовом редакторе "
        "или восстановите копию из «Истории»."
    ),
    "editor.list_empty": "Ящиков пока нет: добавьте первый.",
    "editor.edit": "Изменить",
    "editor.remove": "Удалить",
    "editor.template_row": "шаблон первого запуска",
    "editor.invalid_row": (
        "раздел с именем, которое Letterbot не принимает: переименуйте его в "
        "accounts.ini"
    ),
    "editor.form_add": "Новый ящик",
    "editor.form_edit": "Ящик [{id}]",
    "editor.provider": "Почтовый сервис",
    "editor.provider.gmail": "Gmail",
    "editor.provider.yandex": "Яндекс",
    "editor.provider.mailru": "Mail.ru",
    "editor.provider.outlook": "Outlook.com / Microsoft 365",
    "editor.provider.other": "Другой сервер",
    "editor.outlook": (
        "Outlook.com и Microsoft 365 больше не принимают пароли для IMAP. "
        "Настройте пересылку новых писем в ящик Gmail, Яндекс или Mail.ru и "
        "добавьте его."
    ),
    "editor.name": "Имя в Letterbot",
    "editor.name_hint": "Строчные латинские буквы, цифры и _, например work_mail",
    "editor.login": "Логин (адрес почты)",
    "editor.login_hidden": (
        "Сохранённый логин не показан: введите его заново, чтобы изменить."
    ),
    "editor.password": "Пароль",
    "editor.password_hint": "Для Gmail, Яндекса и Mail.ru нужен пароль приложения.",
    "editor.password_saved": "Пароль сохранён",
    "editor.replace": "Заменить",
    "editor.host": "IMAP-сервер",
    "editor.port": "Порт",
    "editor.ssl": "Использовать SSL (порт 993)",
    "editor.chat": "Чат Telegram",
    "editor.chat_default": "Чат по умолчанию",
    "editor.chat_custom": "Свой ID чата",
    "editor.chat_hint": "Число вида 272250747 или -100… для групп",
    "editor.test": "Проверить подключение",
    "editor.testing": "Проверяем…",
    "editor.save": "Сохранить",
    "editor.saving": "Сохраняем…",
    "editor.cancel": "Отмена",
    "editor.reload": "Обновить",
    "editor.saved": (
        "Сохранено в {time}. Резервная копия: {name}. Перезапустите Letterbot, "
        "чтобы применить изменения."
    ),
    "editor.saved_first": (
        "Сохранено в {time}. Перезапустите Letterbot, чтобы применить изменения."
    ),
    "editor.conflict": "accounts.ini изменён на диске. Обновите страницу.",
    "editor.no_changes": "Нечего сохранять: поля не изменились.",
    "editor.rejected": "Не сохранено: исправьте отмеченные поля.",
    "editor.failed": (
        "Не сохранено: файл не удалось записать. Подробности — в журнале Letterbot."
    ),
    "editor.unreadable": (
        "accounts.ini не читается: сначала восстановите копию или исправьте файл."
    ),
    "editor.remove_confirm": (
        "Удалить [{id}]? Letterbot перестанет проверять этот ящик; обработанная "
        "история останется."
    ),
    "editor.history_title": "Резервные копии accounts.ini",
    "editor.history_empty": (
        "Резервных копий пока нет: копия делается перед каждым сохранением."
    ),
    "editor.restore": "Восстановить",
    "editor.restore_confirm": (
        "Восстановить {name}? Текущий accounts.ini сначала сохранится в резервную "
        "копию."
    ),
    "editor.restored": (
        "Восстановлено {name} в {time}. Перезапустите Letterbot, чтобы применить."
    ),
    "editor.issues_title": "Проверьте:",
    "editor.request_failed": "Запрос не прошёл. Обновите страницу.",
    "editor.test_warning": "Проверка не прошла, но сохранить всё равно можно.",
    "editor.result.OK": "Подключено, ящик найден",
    "editor.result.AUTH": (
        "Вход отклонён. Для Gmail нужен пароль приложения, а не обычный."
    ),
    "editor.result.DNS": "Сервер не найден: проверьте имя сервера на опечатки.",
    "editor.result.REFUSED": (
        "Не удаётся связаться с сервером: проверьте сервер, порт и сеть."
    ),
    "editor.result.TLS": (
        "Не удалось защищённое соединение: для порта 993 нужен SSL, для 143 "
        "обычно нет."
    ),
    "editor.result.TIMEOUT": "Сервер не ответил за 10 секунд.",
    "editor.result.OTHER": (
        "Проверка не удалась. Ответ сервера не показывается ради приватности."
    ),
    "editor.result.RATE_LIMIT": "Подождите несколько секунд перед следующей проверкой.",
    "editor.result.BUSY": "Ещё идёт другая проверка. Повторите чуть позже.",
    "editor.result.PORT_NOT_ALLOWED": (
        "До сохранения ящика можно проверить только порты 143 и 993."
    ),
    "editor.result.HOST_NOT_ALLOWED": "Этот адрес сервера проверить нельзя.",
    "editor.result.PASSWORD_REQUIRED": (
        "Введите пароль: сохранённый используется только для сохранённого сервера."
    ),
    "editor.result.LOGIN_REQUIRED": "Введите логин.",
    "editor.result.INVALID": "Проверьте поля сервера, порта и логина.",
    "editor.tg_title": "Бот Telegram",
    "editor.tg_token": "Токен бота",
    "editor.tg_token_saved": "Токен сохранён",
    "editor.tg_token_hint": "От @BotFather: цифры, двоеточие и буквы",
    "editor.tg_chat": "ID чата по умолчанию",
    "editor.tg_check": "Проверить бота",
    "editor.tg_send": "Отправить тестовое сообщение",
    "editor.tg.OK_getme": "Бот @{name} отвечает.",
    "editor.tg.OK_getme_anonymous": "Бот отвечает.",
    "editor.tg.OK_send": "Тестовое сообщение отправлено: проверьте Telegram.",
    "editor.tg.AUTH": "Telegram отклонил токен бота.",
    "editor.tg.CHAT": (
        "Telegram не может доставить в этот чат: сначала отправьте боту /start "
        "или проверьте ID чата."
    ),
    "editor.tg.NETWORK": "Нет связи с Telegram: проверьте подключение к интернету.",
    "editor.tg.TLS": "Не удалось защищённое соединение с Telegram.",
    "editor.tg.TIMEOUT": "Telegram не ответил за 10 секунд.",
    "editor.tg.OTHER": "Telegram не принял запрос.",
    "editor.tg.TOKEN_MISSING": "Сначала введите токен бота.",
    "editor.tg.TOKEN_FORMAT": "Это не похоже на токен бота.",
    "editor.tg.CHAT_MISSING": "Введите ID чата или сначала сохраните чат по умолчанию.",
    "editor.web_title": "Пароль панели и доступ по сети",
    "editor.web_password_set": "Пароль панели: задан",
    "editor.web_password_missing": (
        "Пароль панели: не задан (панель открывается только на этом компьютере)"
    ),
    "editor.web_new_password": "Новый пароль панели",
    "editor.web_current_password": "Текущий пароль панели",
    "editor.web_password_hint": "Не короче 10 символов.",
    "editor.web_save_password": "Сохранить пароль",
    "editor.web_password_saved": "Пароль сохранён в {time}.",
    "editor.web.LENGTH": "Используйте не меньше 10 символов.",
    "editor.web.WEAK": "Этот пароль слишком легко угадать.",
    "editor.web.INVALID": (
        "Переносы строк и пробелы в начале или конце не допускаются."
    ),
    "editor.web.CURRENT_PASSWORD_WRONG": "Текущий пароль неверный.",
    "editor.web.ENV_OVERRIDE": (
        "Пароль задаёт переменная окружения WEB_PASSWORD: меняйте его там."
    ),
    "editor.web.LEGACY_CONFIG": (
        "Эта установка ещё использует config.ini: сначала выполните migrate-config."
    ),
    "editor.web.RATE_LIMIT": "Слишком много попыток. Повторите позже.",
    "editor.web.LOOPBACK_ONLY": (
        "Доступ по сети меняется только на компьютере, где работает Letterbot."
    ),
    "editor.web.LAUNCHER_UNSUPPORTED": (
        "Этот способ запуска берёт настройки сети из config.yaml (web_ui)."
    ),
    "editor.web.PASSWORD_REQUIRED": "Сначала задайте пароль панели.",
    "editor.web_lan_on": "Доступ по сети: включён (локальная сеть, вход по паролю)",
    "editor.web_lan_off": "Доступ по сети: выключен (только этот компьютер)",
    "editor.web_enable_lan": "Разрешить из локальной сети",
    "editor.web_disable_lan": "Только этот компьютер",
    "editor.web_restart": "Перезапустите Letterbot, чтобы применить.",
    "editor.web_lan_hint": (
        "Другие устройства входят по паролю панели. Соединение не шифруется: "
        "используйте только в доверенной сети."
    ),
}

_CATALOGS: dict[str, dict[str, str]] = {"en": _EN, "ru": _RU}


def resolve_locale(value: object) -> str:
    """'ru' for any ru* locale, else 'en' (the project default)."""
    return "ru" if str(value or "").strip().lower().startswith("ru") else "en"


def catalogs() -> dict[str, dict[str, str]]:
    """Copies of both catalogs (for the parity test)."""
    return {locale: dict(catalog) for locale, catalog in _CATALOGS.items()}


def text(key: str, locale: str, **params: object) -> str:
    """Localized string; a missing key is logged and shown as the key itself."""
    template = _CATALOGS[resolve_locale(locale)].get(key)
    if template is None:
        logger.warning("settings_text_missing key=%s locale=%s", key, locale)
        return key
    return template.format(**params) if params else template


def editor_texts(locale: str) -> dict[str, str]:
    """The "editor." strings settings.js needs, unformatted ({id}, {time} ...)."""
    catalog = _CATALOGS[resolve_locale(locale)]
    return {key: value for key, value in catalog.items() if key.startswith("editor.")}


def plural_text(key: str, count: int, locale: str) -> str:
    """'{key}.one' or '{key}.other' with n=count (RU: 1, 21, 31... take .one)."""
    if resolve_locale(locale) == "ru":
        form = "one" if count % 10 == 1 and count % 100 != 11 else "other"
    else:
        form = "one" if count == 1 else "other"
    return text(f"{key}.{form}", locale, n=count)


@dataclass(frozen=True)
class _CardSpec:
    steps: int
    intro: bool = False
    host: str = ""
    snippet_section: str = ""
    snippet_telegram: bool = False


_CARD_SPECS: dict[str, _CardSpec] = {
    "first_run": _CardSpec(
        steps=5,
        host=SNIPPET_PLACEHOLDER,
        snippet_section="work_mail",
        snippet_telegram=True,
    ),
    "encoding": _CardSpec(steps=3, intro=True),
    "unreadable": _CardSpec(steps=2, intro=True),
    "percent": _CardSpec(steps=1, intro=True),
    "gmail": _CardSpec(steps=3, host="imap.gmail.com", snippet_section="gmail_mail"),
    "yandex": _CardSpec(steps=2, host="imap.yandex.ru", snippet_section="yandex_mail"),
    "mailru": _CardSpec(steps=2, host="imap.mail.ru", snippet_section="mailru_mail"),
    "outlook": _CardSpec(steps=2, intro=True),
    "generic": _CardSpec(
        steps=5, host=SNIPPET_PLACEHOLDER, snippet_section="work_mail"
    ),
}
CARD_IDS: tuple[str, ...] = tuple(_CARD_SPECS)


def _snippet(spec: _CardSpec) -> str:
    if not spec.snippet_section:
        return ""
    lines = [
        f"[{spec.snippet_section}]",
        f"login = {SNIPPET_PLACEHOLDER}",
        f"password = {SNIPPET_PLACEHOLDER}",
        f"host = {spec.host}",
        "port = 993",
        "use_ssl = true",
        f"telegram_chat_id = {SNIPPET_PLACEHOLDER}",
    ]
    if spec.snippet_telegram:
        lines += ["", "[telegram]", f"bot_token = {SNIPPET_PLACEHOLDER}"]
    return "\n".join(lines)


def fix_card(card_id: str, locale: str, *, line: int | None = None) -> dict[str, object]:
    """One fix card: title, optional intro and server line, steps, snippet."""
    spec = _CARD_SPECS[card_id]
    intro = ""
    if card_id == "unreadable" and line:
        intro = text("card.unreadable.intro_line", locale, line=line)
    elif spec.intro:
        intro = text(f"card.{card_id}.intro", locale)
    server = ""
    if spec.host and spec.host != SNIPPET_PLACEHOLDER:
        server = text("card.server_line", locale, host=spec.host, port=993)
    return {
        "id": card_id,
        "title": text(f"card.{card_id}.title", locale),
        "intro": intro,
        "server": server,
        "steps": [
            text(f"card.{card_id}.step{number}", locale)
            for number in range(1, spec.steps + 1)
        ],
        "snippet": _snippet(spec),
    }


__all__ = [
    "CARD_IDS",
    "SNIPPET_PLACEHOLDER",
    "catalogs",
    "fix_card",
    "plural_text",
    "resolve_locale",
    "text",
]
