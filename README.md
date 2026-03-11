### What is Letterbot

Letterbot is a self-hosted email triage assistant. It connects to your IMAP mailbox, analyzes incoming emails, and sends concise notifications to Telegram — only for messages that actually require your attention.

Runs on your machine. No cloud. All data stored locally in SQLite.

### Who it's for

- **Freelancers & sole traders** — don't want to miss an invoice, contract, or urgent reply
- **Privacy-conscious developers** — don't want work email processed in someone else's cloud
- **Small businesses** — multiple mailboxes, no budget for corporate email SaaS
- **Old hardware owners** — great "second life" scenario for a spare laptop or home PC

### Example Telegram notification

```
🎖 Letterbot
🟡 Invoice from Romashka LLC
Suggested action: pay the invoice
87,500 ₽ · due Apr 15 · attachment: invoice.xlsx

[🔴 Urgent]  [🟡 Important]  [🔵 Low]
[⏰ Snooze 2h]  [📅 Tomorrow]
```

### Features

| Feature | Description |
|---|---|
| **Telegram notifications** | One email → one message. Summary, priority, suggested action |
| **Multi-Inbox Routing** | N mailboxes → N separate Telegram chats |
| **Weekly digest** | Summary of emails, invoices, contracts, and silent-partner risks |
| **Snooze with memory** | Snooze an email — it returns with context of where you left off |
| **Manual correction** | Adjust priority in Telegram — the system learns from your feedback |
| **AI as enhancement** | Core logic works without any external LLM. AI is an optional layer |
| **Local web UI** | Cockpit, archive, health, events — observability dashboard on your machine |
| **Open source** | Code is open. No black boxes |

### Installation

**Requirements:**
- Python 3.10+
- Windows 10/11, Linux, or macOS
- IMAP access to your mailbox
- Telegram Bot Token (create via [@BotFather](https://t.me/BotFather))

**Steps:**

```bash
# 1. Clone the repository
git clone https://github.com/SergioTheFirst/letterbot.git
cd letterbot

# 2. Install dependencies
python -m pip install -r requirements.txt

# 3. Generate config files
python -m mailbot_v26 init-config --config-dir mailbot_v26/config
```

Fill in the generated files:
- `mailbot_v26/config/settings.ini` — general settings (Telegram, LLM, operational modes)
- `mailbot_v26/config/accounts.ini` — IMAP parameters for your mailboxes

### Running

```bash
# Windows
letterbot.bat

# Or directly
python -m mailbot_v26 --config-dir mailbot_v26/config
```

### Health Check

```bash
python -m mailbot_v26 doctor
```

Shows status of IMAP connections, Telegram bot, LLM provider, and database.

### Tests

```bash
# Syntax check
python -m compileall mailbot_v26 -q

# Run test suite
python -m pytest -q --tb=short

# Golden corpus evaluation
python -m mailbot_v26.tools.eval_golden_corpus
```

### Privacy

Letterbot runs **locally**. In the default mode:

- emails are never sent to any Letterbot server (there isn't one)
- all data is stored in SQLite on your machine
- IMAP connection is read-only — your mailbox is never modified
- external LLMs (GigaChat, OpenAI, etc.) are used **only if you explicitly enable them** in config

### Support the project

Letterbot is free and will stay free. If it helps you, consider a voluntary donation:

- [Boosty](https://boosty.to/personalbot/donate?qr=true)
- [CloudTips](https://pay.cloudtips.ru/p/00d77c6a)

---

## License

Letterbot is distributed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

This means:
- you can use and modify the code freely
- if you run a modified version as a network service — you must publish the source code

See the [LICENSE](LICENSE) file for details.

---

## Links

| | |
|---|---|
| 🌐 Website | [letterbot.ru](https://letterbot.ru) |
| 💬 Telegram community | [t.me/+1xHH6NwJONVlZTA6](https://t.me/+1xHH6NwJONVlZTA6) |
| 📧 Email | [master@letterbot.ru](mailto:master@letterbot.ru) |
| ☕ Donate | [Boosty](https://boosty.to/personalbot/donate?qr=true) · [CloudTips](https://pay.cloudtips.ru/p/00d77c6a) |

---

# Letterbot

> **[RU]** Локальный AI-ассистент для почты с Telegram-уведомлениями  
> **[EN]** Self-hosted AI email triage assistant with Telegram notifications

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Platform: Windows · Linux · macOS](https://img.shields.io/badge/Platform-Windows%20·%20Linux%20·%20macOS-lightgrey.svg)]()
[![GitHub Stars](https://img.shields.io/github/stars/SergioTheFirst/letterbot?style=flat)](https://github.com/SergioTheFirst/letterbot)

---

## 🇷🇺 Русский

### Что это такое

Letterbot читает вашу почту и пересылает **только важные письма** в Telegram.

Работает на вашем компьютере. Почта не уходит в облако. Данные хранятся локально в SQLite. Поддерживает несколько почтовых ящиков — каждый можно направить в свой Telegram-чат.

### Для кого

- **Фрилансеры и ИП** — не хотят пропустить счёт, договор или срочный ответ
- **Технари** — важна приватность, не хотят отдавать письма в чужое облако
- **Малый бизнес** — несколько ящиков, нет бюджета на корпоративный SaaS
- **Старый компьютер** — хороший сценарий «второй жизни» для ноутбука или домашнего ПК

### Как это выглядит в Telegram

```
🎖 Letterbot
🟡 Счёт от ООО «Ромашка»
Требуется действие: оплатить счёт
87 500 ₽ · срок до 15.04 · вложение: invoice.xlsx

[🔴 Срочно]  [🟡 Важно]  [🔵 Низкий]
[⏰ Snooze 2ч]  [📅 Завтра]
```

### Основные возможности

| Функция | Описание |
|---|---|
| **Telegram-уведомления** | Одно письмо — одно сообщение. Суть, приоритет, suggested action |
| **Multi-Inbox Routing** | N почтовых ящиков → N разных Telegram-чатов |
| **Weekly digest** | Сводка за неделю: счета, договоры, риски молчания контрагентов |
| **Snooze с памятью** | Отложить письмо — вернётся с контекстом: что было, где остановились |
| **Ручная коррекция** | Поправили приоритет в Telegram — система обучается на вашем выборе |
| **AI как усиление** | Базовая логика работает без внешних LLM. AI — опциональный слой |
| **Локальный web UI** | Cockpit, архив, health, events — приборная панель на вашей машине |
| **Open source** | Код открыт. Никаких чёрных ящиков |

### Установка

**Требования:**
- Python 3.10+
- Windows 10/11, Linux или macOS
- IMAP-доступ к почтовому ящику
- Telegram Bot Token (создаётся через [@BotFather](https://t.me/BotFather))

**Шаги:**

```bash
# 1. Клонировать репозиторий
git clone https://github.com/SergioTheFirst/letterbot.git
cd letterbot

# 2. Установить зависимости
python -m pip install -r requirements.txt

# 3. Создать конфигурацию
python -m mailbot_v26 init-config --config-dir mailbot_v26/config
```

Заполните созданные файлы:
- `mailbot_v26/config/settings.ini` — общие настройки (Telegram, LLM, режимы работы)
- `mailbot_v26/config/accounts.ini` — IMAP-параметры ваших ящиков

### Запуск

```bash
# Windows
letterbot.bat

# Или напрямую
python -m mailbot_v26 --config-dir mailbot_v26/config
```

### Диагностика

```bash
# Проверить здоровье системы
python -m mailbot_v26 doctor
```

Команда `doctor` покажет статус IMAP-соединений, Telegram-бота, LLM-провайдера и базы данных.

### Тесты

```bash
# Проверить синтаксис
python -m compileall mailbot_v26 -q

# Запустить тест-сьют
python -m pytest -q --tb=short

# Запустить golden corpus eval
python -m mailbot_v26.tools.eval_golden_corpus
```

### Приватность

Letterbot работает **локально**. В базовом режиме:

- письма не отправляются на сервер проекта
- данные хранятся в SQLite на вашей машине
- IMAP-подключение — только чтение, ящик не изменяется
- внешние LLM (GigaChat, OpenAI и др.) используются **только если вы явно их включите** в конфиге

### Поддержать проект

Letterbot бесплатный и будет таким. Поддержать разработку можно добровольным донатом:

- [Boosty](https://boosty.to/personalbot/donate?qr=true)
- [CloudTips](https://pay.cloudtips.ru/p/00d77c6a)

---


*© 2026 Letterbot.ru — читает почту, пересылает важное.*