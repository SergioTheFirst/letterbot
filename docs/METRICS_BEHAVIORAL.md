# Behavioral Metrics (events_v1)

## Источник
Все метрики строятся из events_v1.

## Метрики
- Surprise Rate (proxy): доля SURPRISE_DETECTED от PRIORITY_CORRECTION_RECORDED.
- Time-to-Action (proxy): delay между EMAIL_RECEIVED и PRIORITY_CORRECTION_RECORDED.
- Compression: доля BATCH/DEFER/SILENT от всех DELIVERY_POLICY_APPLIED.
- Deferral reasons: топ reason_codes из DELIVERY_POLICY_APPLIED.
- Attention debt trend: ATTENTION_DEBT_UPDATED (bucketed).

## Ограничения
- Без чтения сырого текста писем.
- Без изменения Telegram payload schema.
- Метрики должны быть воспроизводимы при backfill.
