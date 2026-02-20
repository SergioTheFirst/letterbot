# Letterbot Premium — Windows Quickstart

## 1) Установка и запуск одной командой
1. Скачайте или клонируйте репозиторий.
2. Откройте папку проекта.
3. Запустите `install_and_run.bat`.

Скрипт создаст `.venv`, установит зависимости и запустит `python -m mailbot_v26`.
Скрипт использует только `.venv\Scripts\python -m pip ...`, без глобального pip.

Опционально для прокси/офлайн-среды:
1. На машине с интернетом: `.venv\Scripts\python -m pip download -r requirements.txt -d wheelhouse`
2. На целевой машине: `.venv\Scripts\python -m pip install --no-index --find-links wheelhouse -r requirements.txt`

## 2) Где лежат конфиги
Все конфиги находятся в `mailbot_v26/config/`:
- `accounts.ini` — IMAP аккаунты и Telegram chat_id.
- `config.ini` — общие настройки, storage, LLM flags.
- `keys.ini` — ключи Telegram и Cloudflare.

## 3) Включение/выключение GigaChat
Откройте `mailbot_v26/config/config.ini` и измените:
```
[gigachat]
enabled = true  # включить
enabled = false # выключить
```

## 4) Диагностика: doctor mode
Если бот не работает, сначала запускайте:
```
python -m mailbot_v26 doctor
```
Команда выводит отчёт в консоль и отправляет его в Telegram одним сообщением.

## 5) Полезные команды
- Запуск бота: `run_mailbot.bat`
- Запуск тестов: `run_tests.bat`
