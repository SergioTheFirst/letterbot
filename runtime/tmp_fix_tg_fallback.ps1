$path='mailbot_v26/pipeline/processor.py'
$enc=[System.Text.UTF8Encoding]::new($false)
$text=[System.IO.File]::ReadAllText($path,$enc)
$pattern='def _build_tg_fallback\([\s\S]*?def _build_tg_short_template\(\*, priority: str, subject: str, from_email: str\) -> str:'
$replacement=@"
def _build_tg_fallback(
    *,
    priority: str = "🔵",
    subject: str,
    from_email: str,
    attachments: list[dict[str, Any]] | None = None,
    attachment_summary: str | None = None,
) -> str:
    rendered = tg_renderer.build_tg_fallback(
        priority=priority,
        subject=subject,
        from_email=from_email,
        attachments=attachments or [],
    )
    if attachment_summary:
        rendered = f"{rendered}\n{escape_tg_html(attachment_summary)}"
    return append_watermark(rendered, html=True)


def _build_tg_short_template(*, priority: str, subject: str, from_email: str) -> str:
"@
$new=[System.Text.RegularExpressions.Regex]::Replace($text,$pattern,$replacement,[System.Text.RegularExpressions.RegexOptions]::Singleline)
if($new -eq $text){ throw 'replace_tg_fallback_failed' }
[System.IO.File]::WriteAllText($path,$new,$enc)
