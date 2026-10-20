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
        "Status only: this page reads accounts.ini and changes nothing. To change "
        "a mailbox, edit accounts.ini and restart Letterbot."
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
    "issue.percent_sign": "[{section}] {field} contains %: write %% instead of each %",
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
    "card.percent.title": "A value contains the % sign",
    "card.percent.intro": (
        "Letterbot reads accounts.ini with %-substitution, so a lone % in a value "
        "is an error."
    ),
    "card.percent.step1": (
        "Write %% for every % in the value: for example 50%off becomes 50%%off."
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
}

_RU: dict[str, str] = {
    # Page shell
    "accounts.title": "Почтовые ящики",
    "accounts.read_only": (
        "Только просмотр: страница читает accounts.ini и ничего не меняет. Чтобы "
        "изменить ящик, отредактируйте accounts.ini и перезапустите Letterbot."
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
    "issue.percent_sign": (
        "[{section}] {field} содержит %: пишите %% вместо каждого %"
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
    "card.percent.title": "Значение содержит знак %",
    "card.percent.intro": (
        "Letterbot читает accounts.ini с подстановкой через %, поэтому одиночный % "
        "в значении — ошибка."
    ),
    "card.percent.step1": (
        "Пишите %% вместо каждого % в значении: например, 50%off записывается как "
        "50%%off."
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
