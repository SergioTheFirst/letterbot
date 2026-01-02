from __future__ import annotations

from mailbot_v26.behavior.threading import (
    compute_thread_key,
    extract_message_ids,
    normalize_subject,
)


def test_normalize_subject_strips_prefixes() -> None:
    assert normalize_subject("Re: Fwd:   Hello   world ") == "hello world"
    assert normalize_subject("Ответ: Пересл: AW: ТЕМА ") == "тема"
    assert normalize_subject("") == ""


def test_extract_message_ids_parses_brackets() -> None:
    header = "<id1@example.com> <id2@example.com>"
    assert extract_message_ids(header) == ["id1@example.com", "id2@example.com"]


def test_extract_message_ids_tolerates_malformed() -> None:
    assert extract_message_ids("id@example.com") == ["id@example.com"]
    assert extract_message_ids(" ") == []


def test_compute_thread_key_is_stable_and_separated() -> None:
    key_one = compute_thread_key(
        account_email="account@example.com",
        rfc_message_id="<msg@example.com>",
        in_reply_to=None,
        references="<root@example.com>",
        subject="Subject",
        from_email="sender@example.com",
    )
    key_two = compute_thread_key(
        account_email="other@example.com",
        rfc_message_id="<msg@example.com>",
        in_reply_to=None,
        references="<root@example.com>",
        subject="Subject",
        from_email="sender@example.com",
    )
    assert key_one != key_two
    assert len(key_one) == 16
