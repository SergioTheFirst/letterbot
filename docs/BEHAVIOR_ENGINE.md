# Behavior Engine

## Delivery decisions
- Единственный режим доставки: IMMEDIATE.
- Решения детерминированы и логируются в `DELIVERY_POLICY_APPLIED`.
- Reason codes используются только для аналитики и не меняют факт немедленной отправки.

## Signals (analytics-only)
- `critical_risk`, `high_value`, `low_signal`, `attention_gate` фиксируются как причины.
- Attention debt метрики допускаются только как сигнал; доставка не подавляется.

## Tier-1 UX
- Один email → одно сообщение в Telegram без батчей или отложенных окон.
- Обогащение возможно только через edit-in-place в рамках SLA.
