# Delivery Policy (v1.5)

## Источник времени
- Локальное время машины: now().astimezone().
- Workday/weekend определяется по локальной дате.

## Quiet window (DND)
- Конфиг: [delivery_policy].night_hours или quiet_hours.
- Формат: start-end (например, 21-7).
- В quiet window некритичные уведомления не отправляются.

## Матрица решений (упрощённая)

| Time-of-day | Workday/Weekend | DND | Attention debt | Решение по умолчанию |
|---|---|---|---|---|
| Day | Workday | off | low | IMMEDIATE/BATCH по value |
| Day | Weekend | off | low | BATCH_TODAY |
| Night | any | on | low | DEFER_TO_MORNING |
| Any | any | any | high | BATCH_TODAY |
| Any | any | any | critical risk | IMMEDIATE |

## Пороговые значения
- immediate_value_threshold
- batch_value_threshold
- critical_risk_threshold
- max_immediate_per_hour

## Reason codes
- quiet_hours
- critical_risk
- high_value
- weekend_batch
- attention_debt
- low_signal
- default_batch
- defer_to_morning

## Telegram UX (per-email)
- Одно письмо → одно Telegram сообщение.
- Progressive disclosure только через spoiler внутри того же сообщения (без кнопок).
- Эмодзи ограничены закрытым списком (🔴 🟡 🔵 ⚡ 💬 ⏸️ 📎).
