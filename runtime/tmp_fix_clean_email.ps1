$path = 'mailbot_v26/text/clean_email.py'
$enc = [System.Text.UTF8Encoding]::new($false)
$text = [System.IO.File]::ReadAllText($path, $enc)
$pattern = 'elif zone == "quoted":[\s\S]*?if zone == "forwarded":'
$replacement = @"
elif zone == "quoted":
            if _is_segment_forward_start(line):
                zone = "forwarded"
            elif _is_segment_signature_start(line):
                zone = "signature"
            elif _is_disclaimer_start(line):
                zone = "disclaimer"
        elif zone == "signature":
            if _is_segment_forward_start(line):
                zone = "forwarded"
            elif _is_disclaimer_start(line):
                zone = "disclaimer"

        if zone == "forwarded":
"@
$new = [System.Text.RegularExpressions.Regex]::Replace($text, $pattern, $replacement, [System.Text.RegularExpressions.RegexOptions]::Singleline)
if ($new -eq $text) { throw 'quoted_block_replace_failed' }
[System.IO.File]::WriteAllText($path, $new, $enc)
