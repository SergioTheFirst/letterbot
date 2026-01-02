# Behavior Engine / Attention Decision Engine

## Цель
Детерминированно решать режим доставки без LLM и фиксировать решение в events_v1.

## Оси ситуаций
- Время: workday/weekend, quiet window.
- Срочность: critical risk, commitments, deadlines.
- Ценность: приоритет, тип письма, сигнал качества.
- Нагрузка: attention debt, лимит мгновенных уведомлений.

## 12 паттернов (минимальный набор)
1. Critical-night: ночное время + критический риск → IMMEDIATE.
2. Critical-day: дневное время + критический риск → IMMEDIATE.
3. Commitments-night: commitments/deadline ночью → IMMEDIATE.
4. Commitments-day: commitments/deadline днём → IMMEDIATE.
5. High-value-day: высокая ценность днём → IMMEDIATE.
6. High-value-night: высокая ценность ночью → DEFER_TO_MORNING.
7. Medium-value-day: средняя ценность днём → BATCH_TODAY.
8. Medium-value-night: средняя ценность ночью → DEFER_TO_MORNING.
9. Low-value-day: низкая ценность днём → BATCH_TODAY.
10. Low-value-night: низкая ценность ночью → SILENT_LOG.
11. Debt-capped: внимание превышено → BATCH_TODAY.
12. Signal-weak: не хватает фактов → SILENT_LOG.

## Инварианты
- Fast First соблюдается для критичных писем.
- Не увеличивать частоту уведомлений (только снижать/батчить/деферить).
- Никаких предположений без фактов; если данных нет — нейтральные значения.
- Нет LLM в принятии решений.

## Scoring (v1.5)
- Value: маппинг приоритета (🔴/🟠/🟡/🔵) + penalties/boosts.
- Risk: commitments + deadlines + severity.
- Debt: число IMMEDIATE за окно vs лимит.

## Режимы доставки
- IMMEDIATE: отправка сейчас.
- BATCH_TODAY: включить в ближайший дайджест.
- DEFER_TO_MORNING: отложить до утра (quiet hours).
- SILENT_LOG: только событие в events_v1.

## Упаковка (packaging directives)
- Facts всегда показываются.
- Uncertainty блок — только если низкая уверенность.
- Consequences — только с evidence из истории/events.
- Если данных нет — блоки не показывать.
