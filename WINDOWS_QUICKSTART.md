# Letterbot Premium — Windows Quickstart

## 1) Установка и запуск одной командой
1. Скачайте или клонируйте репозиторий.
2. Откройте папку проекта.
3. Запустите `install_and_run.bat`.

Скрипт создаст `.venv`, установит зависимости и запустит `python -m mailbot_v26`.

## 2) Где лежат конфиги
Все конфиги находятся в `mailbot_v26/config/`:
- `settings.ini` — общие настройки (web/storage/feature flags).
- `accounts.ini` — IMAP аккаунты и Telegram chat_id.

## 3) Диагностика: doctor mode
Если бот не работает, сначала запускайте:
```
python -m mailbot_v26 doctor --config-dir mailbot_v26/config
```

## 4) Полезные команды
- Запуск бота: `run_mailbot.bat`
- Проверка готовности конфига: `python -m mailbot_v26 config-ready --config-dir mailbot_v26/config --verbose`
- Валидация: `python -m mailbot_v26 validate-config --config-dir mailbot_v26/config`
- Запуск тестов: `run_tests.bat`
