from mailbot_v26.quality.semantic_validator import (
    detect_templates,
    validate_overlap,
)


def test_overlap_positive():
    summary = "Документ касается темы поставок оборудования и оплаты счета"
    original = (
        "Письмо касается темы поставок оборудования, оплаты счета и сроков поставки."
    )
    passed, ratio = validate_overlap(summary, original)
    assert passed is True
    assert ratio > 0.5


def test_overlap_negative():
    summary = "Короткое сообщение про отпуск и отдых"
    original = "В письме обсуждаются счета и договор на поставку"
    passed, ratio = validate_overlap(summary, original)
    assert passed is False
    assert ratio == 0


def test_detect_templates():
    text = (
        "Данный документ касается важной темы. Также файл очевидно дополняет описание"
    )
    matches = detect_templates(text)
    assert any("касается" in pattern for pattern in matches)
    assert any("файл" in pattern for pattern in matches)
