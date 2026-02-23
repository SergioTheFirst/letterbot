# README.md - Letterbot Premium v26 (Celeron Hell Edition)

*Новая архитектура для экстремально низких ресурсов, максимального качества и полной отказоустойчивости.*

---

#  Описание проекта

**Letterbot v26** - это интеллектуальный почтовый агент с Telegram-уведомлениями, созданный для работы на *ультраслабом железе* (Windows, Celeron, 3GB RAM), но с качеством корпоративного уровня.

Бот обрабатывает входящие письма, извлекает факты, классифицирует вложения, формирует краткие (<=200 символов) информативные сводки и отправляет их в Telegram.

Ключевые свойства системы:

* **Качество важнее скорости:** 30-180 секунд на письмо допустимы
* **Сверхнадёжность:** защита от OOM, перегрева CPU, зависаний OCR/LLM
* **Многослойная архитектура:** IMAP -> Extract -> Classify -> Summarize -> Validate -> Format
* **Zero Hallucinations philosophy:** данные всегда извлекаются только из текста
* **LLM Multi-Stage:** классификация -> извлечение -> самопроверка -> валидация
* **Lazy loading моделей:** ML и OCR загружаются только по необходимости
* **Adaptive degradation:** при дефиците RAM включается Hell-Mode (минимальная локальная обработка, Cloudflare Only)

---

# Установка/Запуск

1. Установите зависимости: `pip install -r requirements.txt`.
2. Заполните реальные конфиги в `mailbot_v26/config/config.ini`, `mailbot_v26/config/accounts.ini`, `mailbot_v26/config/keys.ini`.
3. Рекомендуемый запуск на Windows: `run_mailbot.bat`.
4. Ручные команды:
   - Проверка: `python -m mailbot_v26.doctor`
   - Запуск: `python -m mailbot_v26.start`

# Support

Letterbot остаётся бесплатным и без рекламного спама.
Поддержать разработку можно через карту/СБП/ЮMoney — реквизиты задаются в `config.yaml` (`support.methods`) и доступны в Web UI на странице `/support` после логина.
Пользовательский переключатель поддержки: `support.enabled` (имеет приоритет над legacy `features.donate_enabled`).
В Telegram это опционально: редкий P.S. в дайджесте включается через `support.telegram` (например, раз в 30 дней).

---

#  Архитектура Premium v26

```
mailbot_v26/
│
├── start.py                 # Оркестратор (главный цикл)
│
├── bot_core/                # Ядро: логика, pipeline, управление состояниями
│   ├── imap_client.py       # Hybrid UID + SINCE поиск, защита от дублей
│   ├── extractor_engine.py  # PDF->text, DOCX->text, OCR, Hell-safe режимы
│   ├── classifier.py        # Keyword + Lazy ML классификация типов документов
│   ├── llm_engine.py        # Cloudflare LLM + многоэтапная проверка
│   ├── validator.py         # Cross-check дат/сумм + Jaccard ROUGE Lite
│   ├── formatter.py         # Финальное сжатие и форматирование
│   ├── registry.py          # Типы документов и валидаторы
│   ├── context_manager.py   # Анализ цепочек, повторные письма
│   ├── hell_wrapper.py      # OOM guard, RAM monitor, retry/backoff
│   └── state_manager.py     # Хранение last_uid, last_date, heartbeat, threads
│
├── worker/
│   └── processor.py         # Конвейер обработки: извлечение->LLM->валидация->Telegram
│
├── web/
│   ├── web_main.py          # Легковесная панель Bottle / FastAPI
│   └── templates/           # HTML для мониторинга статуса
│
├── prompts/                 # PROMPTS v3.0 (Anti-Hallucination Edition)
│   ├── body.txt             # Суть письма <=180 символов
│   ├── actions.txt          # Действия (разделитель |)
│   ├── invoice.txt          # Инвойсы
│   ├── contract.txt         # Договоры
│   ├── price_offer.txt      # КП
│   ├── delivery_act.txt     # Акты
│   ├── scanned.txt          # Сканированные документы
│   ├── claim.txt            # Претензии / рекламации
│   ├── bank_statement.txt   # Выписки
│   ├── hr_doc.txt           # Кадровые документы
│   └── shrink.txt           # Сжатие чрезмерно длинных ответов
│
├── tools/
│   ├── pdftotext.exe        # Poppler - легчайший PDF extractor
│   ├── catdoc.exe           # DOC extractor
│   ├── xlhtml.exe           # XLS extractor
│   └── tesseract/           # OCR (light configuration)
│
├── accounts.ini             # IMAP-аккаунты
├── config.ini               # Настройки бота
├── requirements.txt         # Минимальные зависимости
└── state/
    ├── state.json           # UID, heartbeat, thread history
    └── logs/                # Ошибки, quarantine, LLM latency stats
```

---

#  Основная философия Premium v26

### 1. **Качество > Скорость**

Лучше 2 минуты на письмо, чем неправильная сводка.

### 2. **Zero Hallucinations**

Все данные должны быть *проверяемыми* в исходном тексте.

### 3. **Silence on absence**

Если факта нет -> никаких "нет данных", пустота -> это правильное поведение.

### 4. **Layered Pipeline**

Каждый слой уменьшает хаос:

1. Extract ->
2. Classify ->
3. LLM Extract ->
4. LLM Verify ->
5. Deep Validation ->
6. Format (<=200 chars)

### 5. **Lazy ML + Lazy LLM**

Модели загружаются только при необходимости.

### 6. **Hell-Mode**

Защита от слабого железа:

* при RAM<500MB -> OCR отключается
* при CPU>80% -> heavy jobs пропускаются
* Cloudflare Only Mode (локальная ML отключена)
* fallback: regex facts

### 7. **Self-Verification**

LLM проверяет свои же факты вторым запросом («извлечено -> проверь -> удали ложное»).

---

#  Обработка письма: полный pipeline

```
IMAP fetch (Hybrid UID + SINCE)
    ↓
Extractor Engine
    PDF->text, DOCX->text, XLS->text, OCR если нужно
    ↓
Classifier
    Keyword -> 70%
    Lazy ML -> 30% (включается при неопределенности)
    ↓
LLM Stage 1 (Extract facts)
    ↓
LLM Stage 2 (Verify facts)
    ↓
Deep Validation
    Regex cross-check
    Jaccard similarity
    Negation detection
    ↓
Formatting Engine
    Короткая (<=200 символов) сводка, только факты
    ↓
Telegram Sender
```

---

#  Типы документов (Document Registry)

Каждый документ имеет:

* keywords
* prompt template
* validator (регулярка или функция)
* fallback форматирование

Примеры:

```
invoice -> invoice.txt + validator: сумма + дата
contract -> contract.txt + validator: дата + предмет
...
```

---

# 🤖 PROMPTS v3.0 - Anti-Hallucination Edition

Все промпты содержат:

```
ОСНОВЫВАЙСЯ ТОЛЬКО НА ТЕКСТЕ.
НЕ ФАНТАЗИРУЙ.
ФАКТЫ КОТОРЫХ НЕТ - УДАЛЯЙ.
ЕСЛИ ФАКТ НЕ НАЙДЕН, ВЕРНИ NONE ДЛЯ ЭТОГО ФАКТА.
ОТВЕТ ТОЛЬКО ОДНОЙ СТРОКОЙ. РАЗДЕЛИТЕЛЬ: |
```

Далее Python валидатор удаляет сегменты с "NONE".

Это обеспечивает:

* дисциплину формата
* отсутствие воды
* отсутствие вступлений
* возможность строгой валидации

---

# 🧪 Валидация качества

Валидация проходит 4 уровня:

1. **NONE Filtering**
2. **Cross-ref дат и сумм по оригиналу**
3. **Jaccard similarity >= 0.5**
4. **Negation detection**

Если summary не проходит валидацию -> отправляется fallback.

---

# ⚠️ Fallback Mode (если LLM недоступна)

Бот не молчит без информации.

Формируется:

```
Тема: ...
Суммы: ...
Даты: ...
Действия: ...
```

Только факты, без LLM.

---

# 🔧 Требования к окружению

### Минимальные:

* Windows 10
* Python 3.10+
* RAM: 3GB
* CPU: Celeron

### Основные зависимости:

```
imapclient
pymupdf
mammoth
openpyxl
pytesseract
natasha
requests
psutil
jinja2
```

---

# 🚀 Как запускать

```
python -m mailbot_v26.start
```

Веб-панель мониторинга:

```
http://localhost:8080
```

---

# 📦 Roadmap

### Phase 1 (v26.1)

* Полностью рабочий pipeline (LLM+валидация)
* OCR light
* Context Manager
* Hell-wrapper

### Phase 2 (v26.2)

* Feedback learning
* Sender-based templates
* Smart prioritization

### Phase 3 (v26.3)

* Analytics dashboard
* Multi-account parallelism (в рамках Hell-mode ограничений)
