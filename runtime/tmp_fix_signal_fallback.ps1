$path='mailbot_v26/pipeline/processor.py'
$enc=[System.Text.UTF8Encoding]::new($false)
$text=[System.IO.File]::ReadAllText($path,$enc)
$pattern='def _build_signal_fallback\(subject: str, from_email: str\) -> str:[\s\S]*?def _notify_system_mode_change\('
$replacement=@"
def _build_signal_fallback(subject: str, from_email: str) -> str:
    safe_subject = subject or "(без темы)"
    safe_sender = from_email or "неизвестно"
    return (
        "Тело письма недоступно (низкое качество извлечения).\n"
        f"Тема: {safe_subject}\n"
        f"От: {safe_sender}"
    )


def _notify_system_mode_change(
"@
$new=[System.Text.RegularExpressions.Regex]::Replace($text,$pattern,$replacement,[System.Text.RegularExpressions.RegexOptions]::Singleline)
if($new -eq $text){ throw 'replace_signal_fallback_failed' }
$new=$new.Replace('hints.append(f"🔃 {explanation}")','hints.append(f"🔁 {explanation}")')
[System.IO.File]::WriteAllText($path,$new,$enc)
