$path='mailbot_v26/pipeline/processor.py'
$enc=[System.Text.UTF8Encoding]::new($false)
$text=[System.IO.File]::ReadAllText($path,$enc)
$pattern='def _build_signal_hints\(insights: list\[Insight\]\) -> list\[str\]:[\s\S]*?def _extract_narrative_insight\('
$replacement=@"
def _build_signal_hints(insights: list[Insight]) -> list[str]:
    hints: list[str] = []
    seen_types: set[str] = set()
    for insight in insights:
        insight_type = str(insight.type or "").strip().lower()
        explanation = _sanitize_preview_line(insight.explanation)
        normalized_expl = explanation.lower()
        if (
            "silence" in insight_type
            or "молч" in insight_type
            or "silence" in normalized_expl
            or "молчит" in normalized_expl
        ) and "silence" not in seen_types:
            if explanation:
                hints.append(f"⚠ {explanation}")
                seen_types.add("silence")
                continue
        if (
            "deadlock" in insight_type
            or "без ответа" in insight_type
            or "deadlock" in normalized_expl
            or "без ответа" in normalized_expl
        ) and "deadlock" not in seen_types:
            if explanation:
                hints.append(f"🔃 {explanation}")
                seen_types.add("deadlock")
                continue
    return hints


def _extract_narrative_insight(
"@
$new=[System.Text.RegularExpressions.Regex]::Replace($text,$pattern,$replacement,[System.Text.RegularExpressions.RegexOptions]::Singleline)
if($new -eq $text){ throw 'replace_signal_hints_failed' }
[System.IO.File]::WriteAllText($path,$new,$enc)
