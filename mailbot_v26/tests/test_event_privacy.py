"""WP3a step 1: pure privacy helpers for event payloads and fingerprints."""

from __future__ import annotations

import hashlib
import hmac
import itertools
import json
import re

import pytest

from mailbot_v26.events.contract import (
    EventType,
    EventV1,
    fingerprint,
    fingerprint_fields,
)
from mailbot_v26.events.privacy import (
    TEXT_MARKERS,
    normalize_reminder_subject,
    redact_payload,
    scrub_email_received,
    scrub_legacy_payload,
    scrub_payload,
    subject_hash,
    subject_words,
    summary_words,
    text_markers,
)
from mailbot_v26.priority import priority_engine_v2
from mailbot_v26.priority.priority_engine_v2 import PriorityEngineV2

KEY = bytes(range(32))
OTHER_KEY = bytes(range(1, 33))
# A made-up token in the shape of a real one (bot id : secret).
FAKE_TOKEN = "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawZ"
_MISSING = object()

# Fingerprints computed by the code before WP3a; they must never change, or
# exact re-emits of existing events would stop deduplicating.
_GOLDEN_FINGERPRINTS = [
    (
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=1700000000.5,
            account_id="account@example.com",
            entity_id=None,
            email_id=42,
            payload={
                "from_email": "a@example.com",
                "subject": "Счёт №42",
                "attachments_count": 1,
            },
        ),
        "6a7ac16639551cd4c49b3bc58817eedcdce9e05a26e6c30777fd4c31b8d6bf12",
    ),
    (
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=1700000000.5,
            account_id="account@example.com",
            entity_id=None,
            email_id=42,
            payload={"from_email": "a@example.com"},
            payload_json='{"explicit": true}',
        ),
        "426e2866255b8eacb5497988e41768b2f670ab7003f64e7b3122068fec93ad21",
    ),
    (
        EventV1(
            event_type=EventType.TELEGRAM_FAILED,
            ts_utc=1700000123.0,
            account_id="acc",
            entity_id="entity-1",
            email_id=None,
            payload={"error": "boom", "nested": {"b": [1, 2], "a": None}},
            schema_version=2,
        ),
        "e8acf9c2e8ce320e84de20878262b89cebb5827c8a57ed415193699ecbf2d287",
    ),
]


# --- fingerprints --------------------------------------------------------------


@pytest.mark.parametrize(("event", "expected"), _GOLDEN_FINGERPRINTS)
def test_existing_fingerprints_are_unchanged(event: EventV1, expected: str) -> None:
    assert fingerprint(event) == expected


@pytest.mark.parametrize(("event", "expected"), _GOLDEN_FINGERPRINTS)
def test_fingerprint_fields_reproduces_event_fingerprints(
    event: EventV1, expected: str
) -> None:
    payload = event.payload_json if event.payload_json is not None else event.payload
    for event_type in (event.event_type, event.event_type.value):
        assert (
            fingerprint_fields(
                event_type=event_type,
                ts_utc=event.ts_utc,
                account_id=event.account_id,
                entity_id=event.entity_id,
                email_id=event.email_id,
                payload=payload,
                schema_version=event.schema_version,
            )
            == expected
        )


def test_fingerprint_fields_accepts_stored_event_types_outside_the_enum() -> None:
    common = dict(
        ts_utc=1.5, account_id="acc", entity_id=None, email_id=None, schema_version=1
    )
    first = fingerprint_fields(event_type="legacy_custom", payload={"a": 1}, **common)
    second = fingerprint_fields(event_type="legacy_custom", payload={"a": 2}, **common)

    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert first != second


# --- reminder subject normalization ------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Re: Invoice 42", "invoice 42"),
        ("  FWD:   Invoice    42  ", "invoice 42"),
        ("fw:Invoice", "invoice"),
        ("Re: Re: Invoice", "re: invoice"),
        ("Reminder: Invoice", "reminder: invoice"),
        ("НАПОМИНАНИЕ  об\tоплате", "напоминание об оплате"),
        ("Re:", ""),
        ("   ", ""),
        ("", ""),
    ],
)
def test_normalize_reminder_subject(raw: str, expected: str) -> None:
    assert normalize_reminder_subject(raw) == expected
    assert PriorityEngineV2._normalize_subject(raw) == expected


def test_priority_engine_delegates_subject_normalization(monkeypatch) -> None:
    # One normalizer for raw comparisons and keyed hashes keeps them identical.
    monkeypatch.setattr(
        priority_engine_v2, "normalize_reminder_subject", lambda subject: "delegated"
    )

    assert PriorityEngineV2._normalize_subject("Re: anything") == "delegated"


# --- keyed subject hash ------------------------------------------------------


def test_subject_hash_is_a_truncated_hmac_of_the_normalized_subject() -> None:
    expected = hmac.new(KEY, "invoice 42".encode("utf-8"), hashlib.sha256).hexdigest()[:32]

    assert subject_hash("Re:  INVOICE   42 ", KEY) == expected
    assert subject_hash("invoice 42", KEY) == expected
    assert re.fullmatch(r"[0-9a-f]{32}", expected)


def test_subject_hash_separates_subjects_and_keys() -> None:
    assert subject_hash("Invoice 42", KEY) != subject_hash("Invoice 43", KEY)
    assert subject_hash("Invoice 42", KEY) != subject_hash("Invoice 42", OTHER_KEY)
    assert subject_hash("Fwd: Re: Invoice 42", KEY) != subject_hash("Invoice 42", KEY)


@pytest.mark.parametrize("blank", [None, "", "   ", "Re:", "fwd:  ", 0])
def test_subject_hash_of_a_blank_subject_is_empty(blank: object) -> None:
    assert subject_hash(blank, KEY) == ""


def test_subject_hash_handles_unencodable_text_and_rejects_empty_key() -> None:
    assert re.fullmatch(r"[0-9a-f]{32}", subject_hash("broken \udcff subject", KEY))
    with pytest.raises(ValueError):
        subject_hash("Invoice", b"")


# --- derived facts -----------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("  \t ", None),
        (0, None),
        ("!!!", 0),
        ("Hello world", 2),
        ("Счёт №123 оплачен_до пятницы", 4),
        (42, 1),
    ],
)
def test_word_counts_skip_blank_text(value: object, expected: int | None) -> None:
    assert subject_words(value) == expected
    assert summary_words(value) == expected


def test_text_markers_are_a_sorted_subset() -> None:
    assert TEXT_MARKERS == ("dispute", "fast reply", "invoice", "paid")
    assert text_markers("PAID: Invoice", "Thanks for the Fast Reply") == [
        "fast reply",
        "invoice",
        "paid",
    ]
    assert text_markers("fast  reply, disputed") == ["dispute"]
    assert text_markers(None, "", "nothing here") == []
    assert text_markers() == []


def test_text_markers_follow_the_serialized_text() -> None:
    # The trust score scans the JSON text: "\x0c" serializes as "\f", so the
    # stored text of "\x0cast reply" did contain "fast reply".
    assert text_markers("\x0cast reply") == ["fast reply"]
    assert text_markers("\x1dispute") == ["dispute"]
    assert text_markers("İnvoice") == []


# --- scrubbing ---------------------------------------------------------------


def _live_email_received(subject: object) -> dict[str, object]:
    return {
        "from_email": "billing@vendor.example",
        "subject": subject,
        "attachments_count": 1,
        "body_chars": 120,
        "word_count": 20,
        "occurred_at_utc": 1700000000.0,
        "thread_key": "thread-1",
        "engine": "priority_v2",
    }


def test_scrub_email_received_live_payload() -> None:
    raw = _live_email_received("Invoice 42 is PAID")
    scrubbed = scrub_email_received(raw)

    assert scrubbed == {
        "from_email": "billing@vendor.example",
        "attachments_count": 1,
        "body_chars": 120,
        "word_count": 20,
        "occurred_at_utc": 1700000000.0,
        "thread_key": "thread-1",
        "engine": "priority_v2",
        "subject_words": 4,
        "text_markers": ["invoice", "paid"],
    }
    assert raw["subject"] == "Invoice 42 is PAID"  # input untouched


def test_scrub_email_received_backfill_payload() -> None:
    raw = {
        "from_email": "billing@vendor.example",
        "subject": "Invoice 7",
        "body_summary": "It was paid yesterday",
        "attachments_count": 0,
    }

    assert scrub_email_received(raw) == {
        "from_email": "billing@vendor.example",
        "attachments_count": 0,
        "subject_words": 2,
        "summary_words": 4,
        "text_markers": ["invoice", "paid"],
    }


def test_scrub_email_received_omits_counts_for_blank_text() -> None:
    raw = {"from_email": "a@vendor.example", "subject": "  ", "body_summary": None}

    assert scrub_email_received(raw) == {"from_email": "a@vendor.example", "text_markers": []}


@pytest.mark.parametrize(
    "payload",
    [
        _live_email_received("Dispute about invoice 9"),
        {"from_email": "a@vendor.example", "subject": "", "body_summary": "fast reply"},
        {"from_email": "a@vendor.example", "attachments_count": 0},
    ],
)
def test_scrub_email_received_is_idempotent(payload: dict[str, object]) -> None:
    once = scrub_email_received(payload)

    assert scrub_email_received(once) == once
    assert scrub_payload(EventType.EMAIL_RECEIVED, once) == once


def test_scrubbed_email_received_never_contains_the_raw_text() -> None:
    raw = {
        "from_email": "a@vendor.example",
        "subject": "SUBJECT-CANARY-31337 invoice",
        "body_summary": "BODY-CANARY-27182 paid",
    }
    text = json.dumps(scrub_email_received(raw), ensure_ascii=False)

    assert "CANARY" not in text
    assert '"subject"' not in text
    assert '"body_summary"' not in text


def _weekly_words_raw(payload: dict[str, object]) -> int | None:
    """weekly_attention_entities on a raw payload (current reader)."""
    summary = str(payload.get("body_summary") or "").strip()
    subject = str(payload.get("subject") or "").strip()
    text = summary or subject
    if not text:
        return None
    return len(re.findall(r"\b\w+\b", text))


def _weekly_words_derived(payload: dict[str, object]) -> int | None:
    """The rule readers apply to derived payloads (WP3a R3)."""
    if "summary_words" in payload:
        return int(payload["summary_words"])
    if "subject_words" in payload:
        return int(payload["subject_words"])
    return None


_TEXT_VALUES = [_MISSING, None, "", "   ", "!!!", 0, 42, "Hello world", "Счёт №7 оплачен_до", "\tTabbed\nline "]


@pytest.mark.parametrize(
    ("subject", "summary"), list(itertools.product(_TEXT_VALUES, _TEXT_VALUES))
)
def test_derived_word_counts_match_weekly_attention_on_raw_payloads(
    subject: object, summary: object
) -> None:
    raw: dict[str, object] = {"from_email": "a@vendor.example"}
    if subject is not _MISSING:
        raw["subject"] = subject
    if summary is not _MISSING:
        raw["body_summary"] = summary

    assert _weekly_words_derived(scrub_email_received(raw)) == _weekly_words_raw(raw)


_TRICKY_TEXT = [
    _MISSING,
    None,
    "",
    "Invoice PAID",
    "fast  reply",
    "Thanks for the Fast reply!",
    "Dispute: wrong amount",
    "\x0cast reply",
    "\x1dispute",
    'quoted "paid" value',
    "back\\slash invoice",
    "Счёт invoice №5",
    "İnvoice",
    "no markers here",
]


@pytest.mark.parametrize("from_email", ["a@vendor.example", "paid-invoices@billing.example"])
@pytest.mark.parametrize("summary", [_MISSING, None, "was paid", "\x0fast reply"])
@pytest.mark.parametrize("subject", _TRICKY_TEXT)
def test_scrubbed_email_received_keeps_trust_score_substring_results(
    subject: object, summary: object, from_email: str
) -> None:
    raw: dict[str, object] = {
        "from_email": from_email,
        "attachments_count": 0,
        "thread_key": "thread-1",
    }
    if subject is not _MISSING:
        raw["subject"] = subject
    if summary is not _MISSING:
        raw["body_summary"] = summary
    raw_text = json.dumps(raw, ensure_ascii=False).lower()
    scrubbed_text = json.dumps(scrub_email_received(raw), ensure_ascii=False).lower()

    for marker in TEXT_MARKERS:
        assert (marker in raw_text) == (marker in scrubbed_text), marker
    assert f'"from_email": "{from_email}"' in scrubbed_text


def test_scrub_payload_drops_raw_text_from_other_event_types() -> None:
    raw = {
        "priority": "🟡",
        "confidence": 55,
        "sender": "a@vendor.example",
        "subject": "Contract renewal",
        "engine": "priority_v2",
    }

    assert scrub_payload(EventType.PRIORITY_DECISION_RECORDED, raw) == {
        "priority": "🟡",
        "confidence": 55,
        "sender": "a@vendor.example",
        "engine": "priority_v2",
    }
    assert scrub_payload("priority_decision_recorded", raw) == scrub_payload(
        EventType.PRIORITY_DECISION_RECORDED, raw
    )


def test_scrub_payload_masks_tokens_and_drops_nested_raw_text() -> None:
    raw = {
        "error": f"HTTPSConnectionPool: /bot{FAKE_TOKEN}/sendMessage failed",
        "attempts": [{"detail": f"token {FAKE_TOKEN}", "subject": "nested subject"}],
        "context": {"body_summary": "nested body", "summary": "nested summary", "count": 3},
        "retryable": True,
        "status": None,
    }
    scrubbed = scrub_payload(EventType.TELEGRAM_FAILED, raw)
    text = json.dumps(scrubbed, ensure_ascii=False)

    assert FAKE_TOKEN.split(":", 1)[1] not in text
    assert "bot<redacted>" in scrubbed["error"]
    assert scrubbed["attempts"] == [{"detail": "token <redacted-token>"}]
    assert scrubbed["context"] == {"summary_chars": 14, "count": 3}
    assert scrubbed["retryable"] is True and scrubbed["status"] is None
    assert scrub_payload(EventType.TELEGRAM_FAILED, scrubbed) == scrubbed
    assert raw["attempts"][0]["subject"] == "nested subject"  # input untouched


def test_redact_payload_masks_strings_only() -> None:
    assert redact_payload({"a": FAKE_TOKEN, "b": 5, "c": [FAKE_TOKEN, None]}) == {
        "a": "<redacted-token>",
        "b": 5,
        "c": ["<redacted-token>", None],
    }


def test_scrub_legacy_email_received_keeps_a_keyed_subject_hash() -> None:
    raw = {
        "account_email": "owner@example.com",
        "from_email": "billing@vendor.example",
        "subject": "Re: Reminder: Invoice 42",
        "mail_type": "PAYMENT_REMINDER",
        "mail_type_reasons": ["mt.base=PAYMENT_REMINDER"],
    }
    scrubbed = scrub_legacy_payload("email_received", raw, subject_key=KEY)

    assert scrubbed == {
        "account_email": "owner@example.com",
        "from_email": "billing@vendor.example",
        "mail_type": "PAYMENT_REMINDER",
        "mail_type_reasons": ["mt.base=PAYMENT_REMINDER"],
        "subject_hash": subject_hash("Reminder: Invoice 42", KEY),
    }
    assert scrub_legacy_payload("email_received", scrubbed, subject_key=KEY) == scrubbed
    assert scrub_legacy_payload("email_received", scrubbed, subject_key=None) == scrubbed


def test_scrub_legacy_email_received_without_key_stores_no_hash() -> None:
    raw = {"from_email": "a@vendor.example", "subject": "Invoice", "mail_type": "INVOICE"}

    assert scrub_legacy_payload("email_received", raw, subject_key=None) == {
        "from_email": "a@vendor.example",
        "mail_type": "INVOICE",
    }


def test_scrub_legacy_payload_other_types() -> None:
    assert scrub_legacy_payload(
        "document_identity_seen",
        {"document_id": "INV-7", "subject": "Invoice INV-7", "issuer_key": "k"},
        subject_key=KEY,
    ) == {"document_id": "INV-7", "issuer_key": "k"}
    assert scrub_legacy_payload(
        "telegram_empty_summary", {"summary": "Body text here"}, subject_key=KEY
    ) == {"summary_chars": 14}
    assert scrub_legacy_payload(
        "telegram_empty_summary", {"summary": None}, subject_key=KEY
    ) == {"summary_chars": 0}
    assert scrub_legacy_payload(
        "telegram_delivery_failed", {"error": f"/bot{FAKE_TOKEN}/x"}, subject_key=None
    ) == {"error": "/bot<redacted>/x"}
