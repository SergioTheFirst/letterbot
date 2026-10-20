"""Every Letterbot command-line entry point and its place in the dashboard.

Owner decision 2026-10-20: no command exists on the command line without an
entry on the dashboard's Commands page (/commands, commands_page.py). The page
and test_web_commands share this registry: the test fails when a subcommand of
``python -m mailbot_v26`` (or one of its options), or a module with an
``if __name__ == "__main__"`` block, has no entry here.

An entry either names its place in the dashboard (``href``: a page, or a result
panel on /commands for the commands that only read) or is "command line only"
with a ``reason`` quoting CONSTITUTION IV: the dashboard never writes the
database or any file other than accounts.ini/settings.ini, and never starts or
stops Letterbot. Command lines are built only from these constants and the
running app's own paths, never from request input; the page escapes them.

The EN/RU copy of the page lives here too (same rules as settings_text: every
key in both languages with the same {placeholders}).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from mailbot_v26.web_observability.settings_text import resolve_locale

logger = logging.getLogger(__name__)

CLI_MODULE = "mailbot_v26"
FROZEN_EXE = "Letterbot.exe"
GROUPS = ("everyday", "configuration", "data", "developer")
COLLAPSED_GROUPS = frozenset({"developer"})
REASONS = ("start_stop", "writes_data", "raw_mail", "developer")


@dataclass(frozen=True)
class CommandEntry:
    """One command. ``module`` runs as ``python -m <module>``; for CLI_MODULE
    ``subcommand`` is the argparse subcommand ("" = run Letterbot)."""

    key: str
    group: str
    module: str = CLI_MODULE
    subcommand: str = ""
    args: str = ""
    # Every option the entry covers (the parity test checks the CLI's own).
    options: tuple[str, ...] = ()
    # Flags that take this installation's config folder / database path.
    config_flag: str = ""
    db_flag: str = ""
    # Dashboard place and the copy key of its link text, or the reason.
    href: str = ""
    place: str = ""
    reason: str = ""


_SETTINGS_SETUP = "/accounts#settings-setup"

ENTRIES: tuple[CommandEntry, ...] = (
    # Everyday
    CommandEntry("run", "everyday", options=("--config-dir",), config_flag="--config-dir", reason="start_stop"),
    CommandEntry(
        "run-stack",
        "everyday",
        module="mailbot_v26.tools.run_stack",
        args="all",
        options=("--config-dir", "--db-path", "--bind", "--port", "--no-browser", "--dry-run"),
        config_flag="--config-dir",
        reason="start_stop",
    ),
    CommandEntry("doctor", "everyday", subcommand="doctor", options=("--strict", "--config-dir"), href="/doctor", place="doctor"),
    CommandEntry(
        "lan-url",
        "everyday",
        subcommand="doctor",
        args="--print-lan-url",
        options=("--print-lan-url",),
        href="/commands#lan-url",
        place="panel",
    ),
    CommandEntry("version", "everyday", subcommand="version", options=("--version",), href="/commands#version", place="panel"),
    # Configuration
    CommandEntry("init-config", "configuration", subcommand="init-config", options=("--config-dir",), href=_SETTINGS_SETUP, place="settings"),
    CommandEntry("migrate-config", "configuration", subcommand="migrate-config", options=("--config-dir",), href=_SETTINGS_SETUP, place="settings"),
    CommandEntry(
        "validate-config",
        "configuration",
        subcommand="validate-config",
        options=("--compat", "--strict", "--config-dir"),
        href="/commands#validate-config",
        place="panel",
    ),
    CommandEntry(
        "config-ready",
        "configuration",
        subcommand="config-ready",
        options=("--verbose", "--config-dir"),
        href="/commands#config-ready",
        place="panel",
    ),
    # Data and maintenance
    CommandEntry(
        "cleanup-status",
        "data",
        module="mailbot_v26.tools.cleanup",
        args="--status",
        options=("--status",),
        href="/commands#cleanup-status",
        place="panel",
    ),
    CommandEntry(
        "cleanup",
        "data",
        module="mailbot_v26.tools.cleanup",
        args="--run",
        options=("--run", "--vacuum", "--batch-size", "--vacuum-threshold-rows", "--db"),
        db_flag="--db",
        reason="writes_data",
    ),
    CommandEntry("backup", "data", subcommand="backup", reason="writes_data"),
    CommandEntry("restore", "data", subcommand="restore", args='"<backup.zip>"', reason="writes_data"),
    CommandEntry("export", "data", subcommand="export", args="--since 30d", options=("--since",), reason="writes_data"),
    CommandEntry(
        "ensure-indexes",
        "data",
        module="mailbot_v26.maintenance.ensure_indexes",
        options=("--db-path",),
        db_flag="--db-path",
        reason="writes_data",
    ),
    CommandEntry("backfill-events", "data", module="mailbot_v26.tools.backfill_events", reason="writes_data"),
    # Developer and release tools
    CommandEntry(
        "start",
        "developer",
        module="mailbot_v26.start",
        options=("--config-dir", "--max-cycles"),
        config_flag="--config-dir",
        reason="start_stop",
    ),
    CommandEntry(
        "web",
        "developer",
        module="mailbot_v26.web_observability.app",
        options=("--config", "--db", "--config-yaml", "--bind", "--port"),
        config_flag="--config",
        db_flag="--db",
        reason="start_stop",
    ),
    CommandEntry("diagnostics", "developer", module="mailbot_v26.diagnostics", reason="raw_mail"),
    CommandEntry(
        "template-promotion",
        "developer",
        module="mailbot_v26.domain.template_promotion",
        options=(
            "--db",
            "--min-corrections",
            "--min-consistency",
            "--min-distinct-senders-for-domain",
            "--report",
            "--json",
        ),
        db_flag="--db",
        reason="raw_mail",
    ),
    CommandEntry(
        "dry-run",
        "developer",
        module="mailbot_v26.tools.dry_run",
        args='--fixture "<message.eml>"',
        options=("--fixture", "--json"),
        reason="raw_mail",
    ),
    CommandEntry(
        "eval-golden-corpus",
        "developer",
        module="mailbot_v26.tools.eval_golden_corpus",
        args="--report",
        options=("--path", "--report"),
        reason="developer",
    ),
    CommandEntry(
        "perf-baseline",
        "developer",
        module="mailbot_v26.tools.perf_baseline",
        options=("--db", "--runs", "--out", "--corpus", "--work-dir"),
        db_flag="--db",
        reason="developer",
    ),
    CommandEntry("consistency-audit", "developer", module="mailbot_v26.consistency_audit", options=("--root",), reason="developer"),
    CommandEntry("audit-project", "developer", module="mailbot_v26.audit_project", reason="developer"),
    CommandEntry("validation-selftest", "developer", module="mailbot_v26.bot_core.validation", reason="developer"),
    CommandEntry("classifier-selftest", "developer", module="mailbot_v26.bot_core.classifier", reason="developer"),
    CommandEntry("verify-dist", "developer", module="mailbot_v26.tools.verify_dist", args='"<dist\\Letterbot>"', reason="developer"),
    CommandEntry(
        "source-bundle",
        "developer",
        module="mailbot_v26.tools.source_bundle",
        options=("--output", "--repo-root"),
        reason="developer",
    ),
    CommandEntry(
        "build-installer",
        "developer",
        module="mailbot_v26.tools.build_windows_installer",
        args='"<dist folder>" "<installer.exe>"',
        options=("--work-dir", "--install-dir"),
        reason="developer",
    ),
    CommandEntry("make-ini-compact", "developer", module="mailbot_v26.tools.make_ini_compact", reason="developer"),
    CommandEntry("capture-screenshot", "developer", module="mailbot_v26.tools.capture_web_screenshot", reason="developer"),
    CommandEntry("storage-smoke-test", "developer", module="mailbot_v26.tools.storage_smoke_test", reason="developer"),
)


def command_label(entry: CommandEntry, *, frozen: bool) -> str:
    """The command without this installation's paths."""
    if entry.module == CLI_MODULE:
        parts = [FROZEN_EXE if frozen else f"python -m {CLI_MODULE}"]
        if entry.subcommand:
            parts.append(entry.subcommand)
    else:
        parts = [f"python -m {entry.module}"]
    if entry.args:
        parts.append(entry.args)
    return " ".join(parts)


def command_line(
    entry: CommandEntry,
    *,
    frozen: bool,
    config_dir: str | None,
    db_path: str | None,
) -> str:
    """The exact command for this installation (its config folder and database)."""
    parts = [command_label(entry, frozen=frozen)]
    if entry.config_flag and config_dir:
        parts.append(f'{entry.config_flag} "{config_dir}"')
    if entry.db_flag and db_path:
        parts.append(f'{entry.db_flag} "{db_path}"')
    return " ".join(parts)


def entry_view(
    entry: CommandEntry,
    locale: str,
    *,
    frozen: bool,
    config_dir: str | None,
    db_path: str | None,
) -> dict[str, object]:
    """What the page shows for one entry (plain strings; the template escapes)."""
    view: dict[str, object] = {
        "key": entry.key,
        "module": entry.module,
        "subcommand": entry.subcommand,
        "label": command_label(entry, frozen=frozen),
        "purpose": text(f"purpose.{entry.key}", locale),
        "options": text("entry.options", locale, options=", ".join(entry.options))
        if entry.options
        else "",
        "href": entry.href,
    }
    if entry.href:
        view["place"] = text(f"place.{entry.place}", locale)
        return view
    view["command"] = command_line(entry, frozen=frozen, config_dir=config_dir, db_path=db_path)
    view["reason"] = text(f"reason.{entry.reason}", locale)
    # Letterbot.exe runs only `python -m mailbot_v26`; the other modules ship
    # with the source folder.
    view["source_note"] = (
        text("entry.source_only", locale) if frozen and entry.module != CLI_MODULE else ""
    )
    return view


# ------------------------------------------------------------------ copy

_EN: dict[str, str] = {
    "page.title": "Commands",
    "page.intro": (
        "Every Letterbot command-line command and where to do the same here. "
        "Commands that only read run on this page; commands that change data or "
        "start Letterbot stay on the command line, ready to copy."
    ),
    "page.no_config_dir": (
        "This dashboard was started without a configuration folder: commands are "
        "shown without --config-dir."
    ),
    "group.everyday": "Everyday",
    "group.configuration": "Configuration",
    "group.data": "Data and maintenance",
    "group.developer": "Developer and release tools",
    "group.developer_hint": "For working on the Letterbot source code.",
    "entry.in_dashboard": "In the dashboard",
    "entry.cli_only": "Command line only",
    "entry.options": "Options: {options}",
    "entry.source_only": (
        "Letterbot.exe does not include this tool: run it from the Letterbot "
        "source folder."
    ),
    "place.doctor": "Doctor page",
    "place.settings": "Settings page: Configuration files",
    "place.panel": "Result on this page",
    "copy.label": "Copy",
    "copy.done": "Copied",
    "copy.failed": "Copy failed: the text is selected, press Ctrl+C.",
    "checks.title": "Checks on this page",
    "checks.intro": (
        "These commands only read the settings and the database. They ran when "
        "this page opened."
    ),
    "checks.run_again": "Run again",
    "check.unavailable": "The check could not run ({error}). Details are in the Letterbot log.",
    "check.no_config_dir": "No configuration folder: this dashboard was started without one.",
    "validate.title": "Configuration check (validate-config)",
    "validate.ok": "No problems found.",
    "validate.warn": "Problems found: {count}",
    "compat.title": "config.yaml schema (validate-config --compat)",
    "compat.absent": "No config.yaml: this installation uses accounts.ini and settings.ini.",
    "compat.versions": "Supported schema version: {supported}. This file: {version}.",
    "compat.ok": "Status: OK",
    "compat.fail": "Status: FAIL. Run the command on the command line for the details.",
    "compat.unreadable": "config.yaml could not be read ({error}).",
    "ready.title": "Ready to start (config-ready)",
    "ready.ok": "Ready: Letterbot can start with these settings.",
    "ready.not_ready": "Not ready: fix the problems below.",
    "ready.critical": "Must fix",
    "ready.warnings": "Warnings",
    "cleanup.title": "Cleanup status (cleanup --status)",
    "cleanup.db": "Database: {path}",
    "cleanup.missing_db": "Database not found; nothing to clean.",
    "cleanup.total": "Records cleanup would delete: {count}",
    "cleanup.freelist": "Free pages in the database file: {count}",
    "cleanup.target": "{table} ({name}): older than {days} days: {count}",
    "cleanup.not_created": "{table} ({name}): table not created yet",
    "cleanup.protected": "Never deleted: {count} event types and the tables {tables}.",
    "version.title": "Version (version)",
    "version.text": "LetterBot.ru {version}",
    "lan.title": "Address for other devices (doctor --print-lan-url)",
    "lan.local_only": "This computer only: {url}",
    "lan.local_hint": (
        "Not reachable from the local network: allow network access on the "
        "Settings page."
    ),
    "lan.lan": "Open from another device: {url}",
    "lan.undetected": (
        "Open from another device: http://<PC IPv4>:{port}/ (the address could not "
        "be detected: run ipconfig and use the IPv4 address)."
    ),
    "lan.firewall": "The Windows firewall may block incoming connections.",
    "reason.start_stop": (
        "Starts Letterbot or a part of it. The dashboard never starts or stops "
        "Letterbot (CONSTITUTION IV)."
    ),
    "reason.writes_data": (
        "Writes the database or data files. The dashboard never writes the "
        "database or any file other than accounts.ini and settings.ini "
        "(CONSTITUTION IV)."
    ),
    "reason.raw_mail": (
        "Prints raw mail data. The dashboard shows only sanitized aggregates, "
        "never raw email text, subjects or senders (CONSTITUTION IV)."
    ),
    "reason.developer": (
        "A tool for the Letterbot source folder: it reads or builds source and "
        "release files outside this installation. The dashboard never writes "
        "files other than accounts.ini and settings.ini and never starts "
        "processes (CONSTITUTION IV)."
    ),
    "purpose.run": "Start Letterbot: check mail and send Telegram notifications.",
    "purpose.run-stack": (
        "Start the mail worker and the dashboard together (or one of them: "
        "worker, web, doctor)."
    ),
    "purpose.doctor": (
        "Check the installation: settings, database, mail and Telegram connections."
    ),
    "purpose.lan-url": "Show the address for opening the dashboard from another device.",
    "purpose.version": "Show the Letterbot version.",
    "purpose.init-config": (
        "Create missing configuration files (accounts.ini, settings.ini) from the "
        "templates."
    ),
    "purpose.migrate-config": (
        "Convert an older configuration (config.ini, config.yaml, keys.ini) to "
        "accounts.ini and settings.ini."
    ),
    "purpose.validate-config": (
        "Check accounts.ini and settings.ini for mistakes (--compat: the "
        "config.yaml schema version)."
    ),
    "purpose.config-ready": "Check that the settings are complete enough for Letterbot to start.",
    "purpose.cleanup-status": "Count the expired low-value records that cleanup would delete.",
    "purpose.cleanup": "Delete expired low-value records (without --run it only counts them).",
    "purpose.backup": "Create a backup archive of the Letterbot data.",
    "purpose.restore": "Restore the Letterbot data from a backup archive (.zip).",
    "purpose.export": "Export events, commitments and snapshots to files (--since: time window).",
    "purpose.ensure-indexes": "Create the maintenance indexes of the events table (safe to repeat).",
    "purpose.backfill-events": (
        "Rebuild missing events from the stored e-mails (database.sqlite in the "
        "current folder)."
    ),
    "purpose.start": "Run the mail worker directly (what starting Letterbot runs).",
    "purpose.web": "Start only the dashboard.",
    "purpose.diagnostics": (
        "Print the CRM diagnostics report: top senders, priorities, recent actions."
    ),
    "purpose.template-promotion": "Analyse which corrected templates could be promoted.",
    "purpose.dry-run": "Process one .eml file offline and print the result, without sending.",
    "purpose.eval-golden-corpus": "Score the pipeline on the golden test corpus.",
    "purpose.perf-baseline": (
        "Measure dashboard, SQL and pipeline speed on a temporary copy of the database."
    ),
    "purpose.consistency-audit": "Check the source code against the CONSTITUTION rules.",
    "purpose.audit-project": "Search the source code for forbidden strings.",
    "purpose.validation-selftest": "Self-test of the text cleaning applied after the LLM.",
    "purpose.classifier-selftest": "Self-test of the attachment classifier.",
    "purpose.verify-dist": "Check a built dist\\Letterbot folder against the release contract.",
    "purpose.source-bundle": "Build a clean source archive without runtime files.",
    "purpose.build-installer": "Build the single-file Windows installer.",
    "purpose.make-ini-compact": "Write the compact INI template file.",
    "purpose.capture-screenshot": "Capture dashboard screenshots (needs Playwright).",
    "purpose.storage-smoke-test": "Run the storage smoke test on a separate test database.",
}

_RU: dict[str, str] = {
    "page.title": "Команды",
    "page.intro": (
        "Все команды Letterbot для командной строки и где сделать то же самое здесь. "
        "Команды, которые только читают, выполняются на этой странице; команды, "
        "которые меняют данные или запускают Letterbot, остаются в командной строке, "
        "их можно скопировать."
    ),
    "page.no_config_dir": (
        "Панель запущена без папки настроек: команды показаны без --config-dir."
    ),
    "group.everyday": "Каждый день",
    "group.configuration": "Настройки",
    "group.data": "Данные и обслуживание",
    "group.developer": "Инструменты разработки и релиза",
    "group.developer_hint": "Для работы с исходным кодом Letterbot.",
    "entry.in_dashboard": "В панели",
    "entry.cli_only": "Только командная строка",
    "entry.options": "Параметры: {options}",
    "entry.source_only": (
        "В Letterbot.exe этого инструмента нет: запускайте его из папки с "
        "исходниками Letterbot."
    ),
    "place.doctor": "Страница «Доктор»",
    "place.settings": "Страница настроек: файлы настроек",
    "place.panel": "Результат на этой странице",
    "copy.label": "Копировать",
    "copy.done": "Скопировано",
    "copy.failed": "Не удалось скопировать: текст выделен, нажмите Ctrl+C.",
    "checks.title": "Проверки на этой странице",
    "checks.intro": (
        "Эти команды только читают настройки и базу данных. Они выполнились при "
        "открытии страницы."
    ),
    "checks.run_again": "Выполнить снова",
    "check.unavailable": "Проверка не выполнилась ({error}). Подробности — в журнале Letterbot.",
    "check.no_config_dir": "Нет папки настроек: панель запущена без неё.",
    "validate.title": "Проверка настроек (validate-config)",
    "validate.ok": "Проблем не найдено.",
    "validate.warn": "Найдено проблем: {count}",
    "compat.title": "Схема config.yaml (validate-config --compat)",
    "compat.absent": "config.yaml нет: эта установка использует accounts.ini и settings.ini.",
    "compat.versions": "Поддерживаемая версия схемы: {supported}. Этот файл: {version}.",
    "compat.ok": "Статус: OK",
    "compat.fail": "Статус: ошибка. Подробности покажет команда в командной строке.",
    "compat.unreadable": "Не удалось прочитать config.yaml ({error}).",
    "ready.title": "Готовность к запуску (config-ready)",
    "ready.ok": "Готово: Letterbot может запуститься с этими настройками.",
    "ready.not_ready": "Не готово: исправьте проблемы ниже.",
    "ready.critical": "Нужно исправить",
    "ready.warnings": "Предупреждения",
    "cleanup.title": "Состояние очистки (cleanup --status)",
    "cleanup.db": "База данных: {path}",
    "cleanup.missing_db": "База данных не найдена; очищать нечего.",
    "cleanup.total": "Записей удалит очистка: {count}",
    "cleanup.freelist": "Свободных страниц в файле базы: {count}",
    "cleanup.target": "{table} ({name}): старше {days} дн.: {count}",
    "cleanup.not_created": "{table} ({name}): таблица ещё не создана",
    "cleanup.protected": "Никогда не удаляются: {count} типов событий и таблицы {tables}.",
    "version.title": "Версия (version)",
    "version.text": "LetterBot.ru {version}",
    "lan.title": "Адрес для других устройств (doctor --print-lan-url)",
    "lan.local_only": "Только этот компьютер: {url}",
    "lan.local_hint": (
        "Из локальной сети недоступно: включите доступ по сети на странице настроек."
    ),
    "lan.lan": "Открыть с другого устройства: {url}",
    "lan.undetected": (
        "Открыть с другого устройства: http://<IPv4 компьютера>:{port}/ (адрес не "
        "определился: выполните ipconfig и возьмите IPv4-адрес)."
    ),
    "lan.firewall": "Брандмауэр Windows может блокировать входящие подключения.",
    "reason.start_stop": (
        "Запускает Letterbot или его часть. Панель никогда не запускает и не "
        "останавливает Letterbot (CONSTITUTION IV)."
    ),
    "reason.writes_data": (
        "Записывает базу данных или файлы данных. Панель никогда не пишет в базу "
        "данных и в файлы, кроме accounts.ini и settings.ini (CONSTITUTION IV)."
    ),
    "reason.raw_mail": (
        "Выводит сырые данные писем. Панель показывает только обезличенные сводки "
        "и никогда — сырой текст писем, темы или отправителей (CONSTITUTION IV)."
    ),
    "reason.developer": (
        "Инструмент для папки с исходниками Letterbot: читает или собирает "
        "исходные и релизные файлы вне этой установки. Панель никогда не пишет "
        "файлы, кроме accounts.ini и settings.ini, и не запускает процессы "
        "(CONSTITUTION IV)."
    ),
    "purpose.run": "Запустить Letterbot: проверять почту и отправлять уведомления в Telegram.",
    "purpose.run-stack": (
        "Запустить обработку почты и панель вместе (или одно из них: worker, web, "
        "doctor)."
    ),
    "purpose.doctor": (
        "Проверить установку: настройки, базу данных, подключение к почте и Telegram."
    ),
    "purpose.lan-url": "Показать адрес, по которому панель открывается с другого устройства.",
    "purpose.version": "Показать версию Letterbot.",
    "purpose.init-config": (
        "Создать недостающие файлы настроек (accounts.ini, settings.ini) из шаблонов."
    ),
    "purpose.migrate-config": (
        "Преобразовать старые настройки (config.ini, config.yaml, keys.ini) в "
        "accounts.ini и settings.ini."
    ),
    "purpose.validate-config": (
        "Проверить accounts.ini и settings.ini на ошибки (--compat: версия схемы "
        "config.yaml)."
    ),
    "purpose.config-ready": "Проверить, что настроек достаточно для запуска Letterbot.",
    "purpose.cleanup-status": "Посчитать устаревшие второстепенные записи, которые удалит очистка.",
    "purpose.cleanup": "Удалить устаревшие второстепенные записи (без --run только подсчёт).",
    "purpose.backup": "Создать архив резервной копии данных Letterbot.",
    "purpose.restore": "Восстановить данные Letterbot из архива резервной копии (.zip).",
    "purpose.export": "Выгрузить события, обязательства и снимки в файлы (--since: период).",
    "purpose.ensure-indexes": "Создать служебные индексы таблицы событий (можно повторять).",
    "purpose.backfill-events": (
        "Восстановить недостающие события по сохранённым письмам (database.sqlite "
        "в текущей папке)."
    ),
    "purpose.start": "Запустить обработку почты напрямую (то, что запускает старт Letterbot).",
    "purpose.web": "Запустить только панель.",
    "purpose.diagnostics": (
        "Вывести отчёт CRM-диагностики: главные отправители, приоритеты, последние "
        "действия."
    ),
    "purpose.template-promotion": "Проанализировать, какие исправленные шаблоны можно закрепить.",
    "purpose.dry-run": "Обработать один файл .eml без сети и показать результат, ничего не отправляя.",
    "purpose.eval-golden-corpus": "Оценить конвейер на эталонном наборе писем.",
    "purpose.perf-baseline": "Измерить скорость панели, SQL и конвейера на временной копии базы.",
    "purpose.consistency-audit": "Проверить исходный код на соответствие правилам CONSTITUTION.",
    "purpose.audit-project": "Найти в исходном коде запрещённые строки.",
    "purpose.validation-selftest": "Самопроверка очистки текста после LLM.",
    "purpose.classifier-selftest": "Самопроверка классификатора вложений.",
    "purpose.verify-dist": "Проверить собранную папку dist\\Letterbot по контракту релиза.",
    "purpose.source-bundle": "Собрать чистый архив исходников без рабочих файлов.",
    "purpose.build-installer": "Собрать установщик Windows одним файлом.",
    "purpose.make-ini-compact": "Записать компактный шаблон INI.",
    "purpose.capture-screenshot": "Снять скриншоты панели (нужен Playwright).",
    "purpose.storage-smoke-test": "Быстрая проверка хранилища на отдельной тестовой базе.",
}

_CATALOGS: dict[str, dict[str, str]] = {"en": _EN, "ru": _RU}


def catalogs() -> dict[str, dict[str, str]]:
    """Copies of both catalogs (for the parity test)."""
    return {locale: dict(catalog) for locale, catalog in _CATALOGS.items()}


def text(key: str, locale: str, **params: object) -> str:
    """Localized string; a missing key is logged and shown as the key itself."""
    template = _CATALOGS[resolve_locale(locale)].get(key)
    if template is None:
        logger.warning("commands_text_missing key=%s locale=%s", key, locale)
        return key
    return template.format(**params) if params else template


__all__ = [
    "CLI_MODULE",
    "COLLAPSED_GROUPS",
    "ENTRIES",
    "GROUPS",
    "REASONS",
    "CommandEntry",
    "catalogs",
    "command_label",
    "command_line",
    "entry_view",
    "text",
]
