from mailbot_v26.text.sanitize import sanitize_text


def test_sanitize_removes_binary_markers():
    raw = """Line 1
IHDR garbage
Useful line
IDAT other
PK header
Content_Types xml
"""
    cleaned = sanitize_text(raw)
    assert "IHDR" not in cleaned
    assert "IDAT" not in cleaned
    assert "PK" not in cleaned
    assert "Content_Types" not in cleaned
    assert "Useful line" in cleaned


def test_sanitize_truncates_and_collapses():
    raw = "Line 1\n\n\nLine 2\n" + ("Long text " * 200)
    cleaned = sanitize_text(raw, max_len=100)
    assert "\n\n\n" not in cleaned
    assert cleaned.endswith("...")


def test_sanitize_filters_base64_like_runs():
    raw = "Header\n" + ("Q" * 60)
    cleaned = sanitize_text(raw)
    assert "Q" * 30 not in cleaned


def test_sanitize_handles_null_bytes():
    raw = "Good\x00Text\nAnother"
    cleaned = sanitize_text(raw)
    assert "\x00" not in cleaned
    assert "Good" in cleaned
