# LetterBot

> **[EN]** A local AI email assistant that turns your inbox into one clear Telegram signal per email — no cloud, no subscription, no data leaving your machine.
>
> **[RU]** Локальный AI-ассистент для почты: одно письмо → одно уведомление в Telegram. Без облака, без подписки, данные не покидают ваш компьютер.

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Platform: Windows](https://img.shields.io/badge/Platform-Windows%2010%2F11-lightgrey.svg)]()
[![Tests](https://img.shields.io/badge/Tests-1541%20passed-brightgreen.svg)]()
[![Version](https://img.shields.io/badge/Version-28.0.0-orange.svg)](https://github.com/SergioTheFirst/letterbot/releases)

---

## 🇬🇧 English

### What is LetterBot?

LetterBot is a **self-hosted email triage assistant** that connects to your mailboxes via IMAP, classifies incoming mail, and sends only the relevant messages to Telegram — with priority, a plain-English summary, and a suggested next action.

Think of it as a **smart filter between your inbox and your attention**.

### Why LetterBot?

Modern email is not a communication tool — it is an unstructured stream of interruptions. Your brain performs hundreds of micro-decisions per day just to triage the inbox. Psychologists call this *decision fatigue*. By evening, your capacity for good decisions has measurably declined.

LetterBot removes the triage step from your plate:

- You stop checking email every 15 minutes
- Important messages surface to you with context — not just a subject line
- You respond intentionally instead of reactively
- All of it runs on your own machine. No data leaves your control

### Who it is for

| If you are... | LetterBot solves... |
|---|---|
| A freelancer or sole trader | Missing invoices, contracts, or urgent replies buried in noise |
| A developer or sysadmin | Routing multiple mailboxes to separate Telegram chats, keeping everything local |
| A small business owner | Keeping correspondence off foreign cloud servers |
| Anyone drowning in email | Replacing compulsive inbox-checking with intentional responses |

### How it looks in Telegram

```
🟡💰 Invoice — Romashka LLC
Pay the invoice
87,500 ₽ · due Apr 15 · 📎 invoice.xlsx

[🔴 Urgent] [🟡 Important] [🔵 Low]
[⏰ Snooze 2h] [📅 Tomorrow]
```

One message. Everything you need to decide. No noise.

### Features

| Feature | Description |
|---|---|
| **Telegram notifications** | One email → one message. Priority, summary, suggested action |
| **Deterministic core** | Works fully without any AI. Classification is rule-based and reproducible |
| **AI as enhancement** | GigaChat (Sber) or Cloudflare Workers AI improve summary quality when enabled |
| **Multi-inbox routing** | N mailboxes → N separate Telegram chats |
| **Priority learning** | Tap a priority button in Telegram — the system learns from your correction |
| **Snooze with memory** | Snooze an email — it returns at the right time with full context |
| **Daily & weekly digest** | Structured overview: invoices, contracts, commitments, anomalies |
| **Local web dashboard** | Cockpit, archive, health, events — read-only observability on your machine |
| **Degraded mode** | If AI is unavailable, the system keeps working and tells you exactly why |
| **Open source** | Code is public, auditable, AGPL-3.0 |

### Architecture

```
IMAP mailbox
     │
     ▼
  PARSE  →  classify type, extract facts, detect amounts/deadlines
     │
     ▼
  SCORE  →  priority engine (deterministic rules + optional AI)
     │
     ▼
 DELIVER →  single Telegram message  ←──  user corrections feed back
     │
     ▼
  STORE  →  local SQLite (events, priorities, sender profiles)
     │
     ▼
 WEB UI  →  read-only dashboard at http://127.0.0.1:8787
```

Everything runs locally. No relay servers.

### Quick Start (Windows — no Python required)

**1. Download and extract**

Download [`Letterbot.zip`](https://github.com/SergioTheFirst/letterbot/releases/latest) and extract to any folder, for example `C:\letterbot`.

**2. Create your Telegram bot**

- Open Telegram → find **@BotFather** → send `/newbot`
- Copy the **bot token** (looks like `1234567890:AAF-xxxxxxxxxxxx`)
- Send `/start` to your new bot
- Open `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` in a browser
- Find the numeric `id` value — this is your **chat_id**

**3. Configure**

Open `mailbot_v26\config\accounts.ini` in Notepad and fill in:

```ini
[my_inbox]
login    = you@example.com
password = YOUR_APP_PASSWORD
host     = imap.gmail.com
port     = 993
use_ssl  = true
telegram_chat_id = 123456789

[telegram]
bot_token = 1234567890:AAF-xxxxxxxxxxxx
chat_id   = 123456789
```

**4. Run**

Double-click `run.bat`. Wait for `[OK] Ready to work`.

Send yourself a test email. A Telegram notification should arrive within 2 minutes.

**5. Open the web dashboard**

Navigate to `http://127.0.0.1:8787` in your browser.

> **SmartScreen warning:** Click "More info" → "Run anyway". This is normal for unsigned executables. The source code is fully public and auditable.

### IMAP settings for common providers

| Provider | host | port | Notes |
|---|---|---|---|
| Gmail | `imap.gmail.com` | 993 | Requires app password + 2FA enabled |
| Yandex Mail | `imap.yandex.ru` | 993 | Create app password in account security |
| Mail.ru | `imap.mail.ru` | 993 | Works for @bk.ru, @list.ru, @internet.ru |
| Outlook / Microsoft 365 | `outlook.office365.com` | 993 | App password if 2FA is on |

### Bot commands

| Command | Description |
|---|---|
| `/help` | List all commands |
| `/status` | System status and delivery metrics |
| `/doctor` | Run diagnostics |
| `/digest on\|off` | Enable or disable daily digest |
| `/stats` | Auto-priority quality report |
| `/week` | 7-day summary |
| `/lang en\|ru` | Switch interface language |

### Source mode (for developers)

```bash
git clone https://github.com/SergioTheFirst/letterbot.git
cd letterbot
letterbot.bat          # creates .venv, installs deps, starts worker + web
```

Config files are created automatically on first run.

```bash
# Health check
python -m mailbot_v26 doctor --config-dir mailbot_v26/config

# Validate config
python -m mailbot_v26 validate-config --config-dir mailbot_v26/config

# Run tests
python -m pytest -q --tb=short

# Golden corpus evaluation
python -m mailbot_v26.tools.eval_golden_corpus
```

### Privacy

LetterBot runs **entirely locally**:

- Emails are never sent to any LetterBot server (there isn't one)
- All data is stored in SQLite on your machine
- IMAP connection is **read-only** — your mailbox is never modified
- External AI (GigaChat, Cloudflare) is used **only if you explicitly enable it**
- Zero telemetry

### Roadmap

| Version | Focus |
|---|---|
| v28 (current) | Behavioral patterns, EN/RU locale, digest quality |
| v29 | Attention economics — measure and reduce cognitive cost per email |
| v30 | Predictive intelligence — anticipate what needs attention before it arrives |

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

Found a bug? [Open an issue](https://github.com/SergioTheFirst/letterbot/issues).
Have an idea? Start a [discussion](https://github.com/SergioTheFirst/letterbot/discussions).

Good first issues are labeled [`good first issue`](https://github.com/SergioTheFirst/letterbot/issues?q=label%3A%22good+first+issue%22).

### Support the project

LetterBot is free and will stay free.

- [Boosty](https://boosty.to/personalbot/donate?qr=true)
- [CloudTips](https://pay.cloudtips.ru/p/00d77c6a)

### Links

| | |
|---|---|
| 🌐 Website | [letterbot.ru](https://letterbot.ru) |
| 💬 Telegram community | [t.me/+1xHH6NwJONVlZTA6](https://t.me/+1xHH6NwJONVlZTA6) |
| 📧 Email | [master@letterbot.ru](mailto:master@letterbot.ru) |
| 📦 Releases | [Latest release](https://github.com/SergioTheFirst/letterbot/releases/latest) |

### License

[AGPL-3.0](LICENSE) — free to use and modify. If you run a modified version as a network service, you must publish the source code.

---

## 🇷🇺 Русский

### Что такое LetterBot?

LetterBot — **локальный ассистент для работы с почтой**, который подключается к вашим ящикам через IMAP, классифицирует входящие письма и отправляет в Telegram только то, что реально требует внимания — с приоритетом, кратким содержанием и рекомендованным действием.

Думайте о нём как об **умном фильтре между вашим почтовым ящиком и вашим вниманием**.

### Почему LetterBot?

Современная почта — это не инструмент общения, а неструктурированный поток прерываний. Каждый день ваш мозг выполняет сотни мини-решений только для того, чтобы разобрать входящие. Психологи называют это *усталостью от решений* — к вечеру способность принимать качественные решения заметно снижается.

LetterBot снимает с вас этап сортировки:

- Вы перестаёте проверять почту каждые 15 минут
- Важные письма сами находят вас — с контекстом, а не просто темой
- Вы реагируете обдуманно, а не рефлекторно
- Всё работает на вашей машине. Данные не выходят за её пределы

### Для кого

| Если вы... | LetterBot решает... |
|---|---|
| Фрилансер или ИП | Пропущенные счета, договоры или срочные ответы, утонувшие в спаме |
| Разработчик или сисадмин | Маршрутизация нескольких ящиков в разные Telegram-чаты, всё локально |
| Малый бизнес | Переписка не уходит в чужое облако |
| Любой, кто тонет в письмах | Замена навязчивой проверки почты на осознанные ответы |

### Как это выглядит в Telegram

```
🟡💰 Счёт — ООО «Купаж»
Оплатить счёт
87 500 ₽ · срок до 15.04 · 📎 invoice.xlsx

[🔴 Срочно] [🟡 Важно] [🔵 Низкий]
[⏰ Отложить 2ч] [📅 Завтра]
```

Одно сообщение. Всё что нужно для решения. Без шума.

### Возможности

| Функция | Описание |
|---|---|
| **Telegram-уведомления** | Одно письмо → одно сообщение. Приоритет, суть, рекомендованное действие |
| **Детерминированное ядро** | Работает полностью без AI. Классификация по правилам, воспроизводима |
| **AI как усиление** | GigaChat (Сбер) или Cloudflare Workers AI улучшают качество сводок |
| **Multi-inbox роутинг** | N ящиков → N разных Telegram-чатов |
| **Обучение на коррекциях** | Нажали кнопку приоритета в Telegram — система учится на вашем выборе |
| **Snooze с памятью** | Отложили письмо — вернётся в нужный момент с полным контекстом |
| **Дайджест** | Дневная и недельная сводка: счета, договоры, обязательства, аномалии |
| **Локальная веб-панель** | Кокпит, архив, health, события — наблюдаемость на вашей машине |
| **Degraded mode** | Если AI недоступен — система продолжает работать и честно сообщает почему |
| **Открытый код** | Код публичен, проверяем, лицензия AGPL-3.0 |

### Архитектура

```
IMAP-ящик
     │
     ▼
  PARSE  →  классификация типа, извлечение фактов, суммы/дедлайны
     │
     ▼
  SCORE  →  движок приоритетов (детерминированные правила + опц. AI)
     │
     ▼
DELIVER →  одно Telegram-сообщение  ←──  коррекции пользователя
     │
     ▼
  STORE  →  локальный SQLite (события, приоритеты, профили отправителей)
     │
     ▼
 WEB UI  →  панель только для чтения http://127.0.0.1:8787
```

Всё работает локально. Никаких промежуточных серверов.

### Быстрый старт (Windows — Python не нужен)

**1. Скачать и распаковать**

Скачайте [`Letterbot.zip`](https://github.com/SergioTheFirst/letterbot/releases/latest) и распакуйте в любую папку, например `C:\letterbot`.

**2. Создать Telegram-бота**

- Откройте Telegram → найдите **@BotFather** → отправьте `/newbot`
- Скопируйте **токен бота** (вида `1234567890:AAF-xxxxxxxxxxxx`)
- Напишите своему боту `/start`
- Откройте в браузере `https://api.telegram.org/bot<ТОКЕН>/getUpdates`
- Найдите числовое значение `id` — это ваш **chat_id**

**3. Настроить**

Откройте `mailbot_v26\config\accounts.ini` в Блокноте и заполните:

```ini
[my_inbox]
login    = you@example.com
password = ПАРОЛЬ_ПРИЛОЖЕНИЯ
host     = imap.yandex.ru
port     = 993
use_ssl  = true
telegram_chat_id = 123456789

[telegram]
bot_token = 1234567890:AAF-xxxxxxxxxxxx
chat_id   = 123456789
```

**4. Запустить**

Дважды кликните `run.bat`. Дождитесь `[OK] Ready to work`.

Отправьте себе тестовое письмо. Уведомление в Telegram должно прийти в течение 2 минут.

**5. Открыть веб-панель**

Перейдите на `http://127.0.0.1:8787` в браузере.

> **Предупреждение SmartScreen:** нажмите «Подробнее» → «Выполнить в любом случае». Это стандартное поведение для неподписанных exe-файлов. Исходный код полностью открыт и доступен для проверки.

### IMAP-настройки популярных провайдеров

| Провайдер | host | port | Примечание |
|---|---|---|---|
| Яндекс.Почта | `imap.yandex.ru` | 993 | Создать пароль приложения в безопасности аккаунта |
| Mail.ru | `imap.mail.ru` | 993 | Работает для @bk.ru, @list.ru, @internet.ru |
| Gmail | `imap.gmail.com` | 993 | Нужен пароль приложения + включена 2FA |
| Outlook / Microsoft 365 | `outlook.office365.com` | 993 | Пароль приложения при включённой 2FA |

### Команды бота

| Команда | Описание |
|---|---|
| `/help` | Список всех команд |
| `/status` | Статус системы и метрики доставки |
| `/doctor` | Диагностика |
| `/digest on\|off` | Включить или выключить дайджест |
| `/stats` | Отчёт о качестве автоприоритета |
| `/week` | Сводка за 7 дней |
| `/lang en\|ru` | Переключить язык интерфейса |

### Режим разработчика (из исходников)

```bash
git clone https://github.com/SergioTheFirst/letterbot.git
cd letterbot
letterbot.bat          # создаёт .venv, устанавливает зависимости, запускает worker + web
```

Конфиги создаются автоматически при первом запуске.

```bash
# Диагностика
python -m mailbot_v26 doctor --config-dir mailbot_v26/config

# Валидация конфига
python -m mailbot_v26 validate-config --config-dir mailbot_v26/config

# Тесты
python -m pytest -q --tb=short

# Golden corpus
python -m mailbot_v26.tools.eval_golden_corpus
```

### Приватность

LetterBot работает **полностью локально**:

- Письма не отправляются ни на какой сервер проекта (его не существует)
- Все данные хранятся в SQLite на вашей машине
- IMAP-соединение **только на чтение** — ящик не изменяется
- Внешний AI (GigaChat, Cloudflare) используется **только если вы явно включили его**
- Никакой телеметрии

### Roadmap

| Версия | Фокус |
|---|---|
| v28 (текущая) | Поведенческие паттерны, EN/RU локаль, качество дайджеста |
| v29 | Attention economics — измерять и снижать когнитивную стоимость каждого письма |
| v30 | Предиктивный интеллект — предвидеть что потребует внимания до того как оно придёт |

### Участие в разработке

Инструкция для контрибьюторов: [CONTRIBUTING.md](CONTRIBUTING.md)

Нашли баг? [Создайте issue](https://github.com/SergioTheFirst/letterbot/issues).
Есть идея? Откройте [обсуждение](https://github.com/SergioTheFirst/letterbot/discussions).

Задачи для новичков помечены меткой [`good first issue`](https://github.com/SergioTheFirst/letterbot/issues?q=label%3A%22good+first+issue%22).

### Поддержать проект

LetterBot бесплатный и останется таким.

- [Boosty](https://boosty.to/personalbot/donate?qr=true)
- [CloudTips](https://pay.cloudtips.ru/p/00d77c6a)

### Ссылки

| | |
|---|---|
| 🌐 Сайт | [letterbot.ru](https://letterbot.ru) |
| 💬 Telegram-сообщество | [t.me/+1xHH6NwJONVlZTA6](https://t.me/+1xHH6NwJONVlZTA6) |
| 📧 Email | [master@letterbot.ru](mailto:master@letterbot.ru) |
| 📦 Релизы | [Последний релиз](https://github.com/SergioTheFirst/letterbot/releases/latest) |

### Лицензия

[AGPL-3.0](LICENSE) — свободное использование и модификация. Если вы запускаете изменённую версию как сетевой сервис — вы обязаны опубликовать исходный код.

---

*© 2026 LetterBot.ru — почту в Telegram. Локально, бесплатно, навсегда.*
