$path='mailbot_v26/pipeline/processor.py'
$enc=[System.Text.UTF8Encoding]::new($false)
$text=[System.IO.File]::ReadAllText($path,$enc)
$pattern='high_impact = \([\s\S]*?\)\s*\n\s*low_confidence'
$replacement=@"
high_impact = (
            priority == "🔴"
            or _deadline_within_days(commitments, received_at=received_at, days=3)
            or _is_urgent_action(action_text)
        )
        low_confidence
"@
$new=[System.Text.RegularExpressions.Regex]::Replace($text,$pattern,$replacement,[System.Text.RegularExpressions.RegexOptions]::Singleline)
if($new -eq $text){ throw 'replace_high_impact_failed' }
[System.IO.File]::WriteAllText($path,$new,$enc)
