$path='mailbot_v26/pipeline/processor.py'
$enc=[System.Text.UTF8Encoding]::new($false)
$text=[System.IO.File]::ReadAllText($path,$enc)
$text=[System.Text.RegularExpressions.Regex]::Replace($text,'telegram_text = f"\{telegram_text\}\\n.*?\{escape_tg_html\(_sanitize_preview_line\(context.preview_hint\)\)\}"','telegram_text = f"{telegram_text}\n💡 {escape_tg_html(_sanitize_preview_line(context.preview_hint))}"')
$text=[System.Text.RegularExpressions.Regex]::Replace($text,'_PREVIEW_DECISION_LINE\s*=\s*"[^"]*"','_PREVIEW_DECISION_LINE = "[Принять] [Отклонить]"')
$text=[System.Text.RegularExpressions.Regex]::Replace($text,'_PREVIEW_PRIORITY_LINE\s*=\s*"[^"]*"','_PREVIEW_PRIORITY_LINE = "[Сделать Высокий] [Сделать Средний] [Сделать Низкий]"')
$text=[System.Text.RegularExpressions.Regex]::Replace($text,'priority\s*==\s*"[^"]*"\s*\n\s*or _deadline_within_days','priority == "🔴"`n            or _deadline_within_days')
[System.IO.File]::WriteAllText($path,$text,$enc)
