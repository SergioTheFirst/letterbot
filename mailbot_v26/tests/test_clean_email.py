from mailbot_v26.text.clean_email import clean_email_body, segment_email_body


def test_clean_email_removes_forward_headers():
    body = (
        "Главный текст письма\n"
        "Дополнительная строка\n"
        "From: someone@example.com\n"
        "Sent: Monday\n"
        "Subject: test\n"
        "To: person\n"
        "Rest of quoted"
    )
    cleaned = clean_email_body(body)
    assert "Главный текст письма" in cleaned
    assert "Дополнительная строка" in cleaned
    assert "From:" not in cleaned
    assert "Sent:" not in cleaned
    assert "Subject:" not in cleaned
    assert "To:" not in cleaned


def test_clean_email_removes_signature_block():
    body = (
        "Основной текст\n"
        "С уважением,\n"
        "Имя\n"
        "Телефон"
    )
    cleaned = clean_email_body(body)
    assert cleaned == "Основной текст"


def test_clean_email_cuts_russian_external_disclaimer_tail():
    body = (
        "Полезный текст до дисклеймера\n"
        "ВНЕШНЯЯ ПОЧТА: Не переходите по ссылкам\n"
        "Шум, который не должен попасть в превью"
    )

    cleaned = clean_email_body(body)

    assert cleaned == "Полезный текст до дисклеймера"


def test_clean_email_cuts_english_external_disclaimer_tail():
    body = (
        "Useful content\n"
        "External email: Use caution when opening attachments\n"
        "Noisy footer"
    )

    cleaned = clean_email_body(body)

    assert cleaned == "Useful content"


def test_clean_email_keeps_text_without_disclaimer():
    body = "Обычный текст письма\nБез лишних предупреждений"

    cleaned = clean_email_body(body)

    assert cleaned == body


def test_clean_email_does_not_cut_mid_sentence_external_words():
    body = "Это обычный текст, где фраза external email встречается в середине предложения."

    cleaned = clean_email_body(body)

    assert cleaned == body


def test_clean_email_cuts_inline_disclaimer_after_subject_prefix():
    body = "RE: Оплата счета ВНЕШНЯЯ ПОЧТА: Если отправитель неизвестен\nхвост"

    cleaned = clean_email_body(body)

    assert cleaned == "RE: Оплата счета"


def test_segment_email_body_splits_sections():
    body = (
        "Короткий статус по задаче\n"
        "Best regards\n"
        "Finance Team\n"
        "----Original Message----\n"
        "From: old@example.com\n"
        "Итого к оплате 999 999 руб."
    )

    segmented = segment_email_body(body)

    assert segmented["main_body"] == "Короткий статус по задаче"
    assert segmented["signature"].startswith("Best regards")
    assert segmented["forwarded_thread"].startswith("----Original Message----")
