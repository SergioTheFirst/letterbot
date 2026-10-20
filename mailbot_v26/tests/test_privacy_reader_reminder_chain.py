"""WP3a step 3 (R1): reminder chains on keyed subject hashes.

Legacy ``email_received`` events lose the raw subject: newer and scrubbed rows
carry ``subject_hash`` (HMAC of the normalized subject; the key lives next to
the database, see mailbot_v26.storage.privacy_key). PriorityEngineV2 must build
the same reminder chains from them as from raw rows, which
test_privacy_reader_characterization.py pins. Without the key such rows never
match by subject: cross-sender matching is off for them and that degraded
state is logged once. Raw rows need no key.
"""

from __future__ import annotations

import itertools
import json
import logging
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from mailbot_v26.events.privacy import scrub_legacy_payload
from mailbot_v26.priority.priority_engine_v2 import (
    PriorityEngineV2,
    PriorityV2Config,
    VipSenderMatcher,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.privacy_key import subject_key_path
from mailbot_v26.tests.test_privacy_reader_characterization import (
    _NOW,
    _OTHER_SENDER,
    _SENDER,
    _SUBJECT,
    _SUBJECT_KEY_HEX,
    ACCOUNT,
    _insert_baseline_chain,
    _insert_legacy_email_received,
    _new_db,
    _reminder_chain_detail,
)

KEY = bytes.fromhex(_SUBJECT_KEY_HEX)
_MISSING = object()
_KEY_UNAVAILABLE = "subject_hash_key_unavailable"


class _LegacyWindowAnalytics(KnowledgeAnalytics):
    """Serves a fixed legacy email_received window; no database needed.

    The engine still finds the subject-hash key next to ``path``.
    """

    def __init__(self, path: Path, rows: list[dict[str, object]]) -> None:
        super().__init__(path)
        self._rows = rows

    def recent_email_events(
        self, *, days: int, now_dt: datetime | None = None
    ) -> list[dict[str, object]]:
        return list(self._rows)


def _write_key(db_path: Path) -> None:
    subject_key_path(db_path).write_text(_SUBJECT_KEY_HEX + "\n", encoding="ascii")


def _hashed(payload: dict[str, object]) -> dict[str, object]:
    return scrub_legacy_payload("email_received", payload, subject_key=KEY)


def _raw(payload: dict[str, object]) -> dict[str, object]:
    return dict(payload)


def _legacy_rows(
    payloads: list[dict[str, object]],
    transform: Callable[[dict[str, object]], dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            "type": "email_received",
            "timestamp": (_NOW - timedelta(hours=hours_ago)).isoformat(),
            "payload": json.dumps(transform(payload), ensure_ascii=False),
        }
        for hours_ago, payload in enumerate(payloads, start=1)
    ]


def _engine(db_path: Path, rows: list[dict[str, object]]) -> PriorityEngineV2:
    return PriorityEngineV2(
        _LegacyWindowAnalytics(db_path, rows),
        config=PriorityV2Config(),
        vip_senders=VipSenderMatcher(()),
    )


def _chain(engine: PriorityEngineV2, *, subject: str, from_email: str = _SENDER) -> str | None:
    result = engine.compute(
        subject=subject,
        body_text="",
        from_email=from_email,
        mail_type="UNKNOWN",
        received_at=_NOW,
        commitments=[],
    )
    details = [item.detail for item in result.breakdown if item.signal == "reminder_chain"]
    assert len(details) <= 1
    return details[0] if details else None


def _baseline_payloads() -> list[dict[str, object]]:
    """Two same-sender reminders: each extra matching event raises the chain to 3."""
    return [
        {"from_email": _SENDER, "subject": f"Unrelated {index}", "mail_type": "PAYMENT_REMINDER"}
        for index in (1, 2)
    ]


def _candidate(subject: object) -> dict[str, object]:
    payload: dict[str, object] = {"from_email": _OTHER_SENDER, "mail_type": "PAYMENT_REMINDER"}
    if subject is not _MISSING:
        payload["subject"] = subject
    return payload


def _key_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno >= logging.WARNING and _KEY_UNAVAILABLE in record.getMessage()
    ]


def _scrub_legacy_in_place(db_path: Path, *, subject_key: bytes | None) -> None:
    """Rewrite legacy event payloads the way WP3a writers store them."""
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute("SELECT rowid, type, payload FROM events").fetchall()
        for row_id, event_type, payload in rows:
            scrubbed = scrub_legacy_payload(
                event_type, json.loads(payload), subject_key=subject_key
            )
            conn.execute(
                "UPDATE events SET payload = ? WHERE rowid = ?",
                (json.dumps(scrubbed, ensure_ascii=False), row_id),
            )
        conn.commit()
        stored = [str(text) for (text,) in conn.execute("SELECT payload FROM events")]
    assert stored
    assert not any('"subject"' in text for text in stored)


_ROW_SUBJECTS: list[object] = [
    _MISSING,
    None,
    "",
    "   ",
    "Re:",
    _SUBJECT,
    "RE:   reminder:   INVOICE 42 ",
    "fw:Reminder:  Invoice 42",
    "Fwd: Re: Reminder: Invoice 42",
    "Re: Re: Reminder: Invoice 42",
    "re: reminder: invoice 42",
    "Another subject",
    "Re: НАПОМИНАНИЕ  об оплате №7",
    "напоминание об оплате №7",
]
_CURRENT_SUBJECTS = [
    _SUBJECT,
    # Normalization strips one prefix only, so it must run once on each side.
    "Re: Re: Reminder: Invoice 42",
    "re: reminder: invoice 42",
    "Напоминание об оплате №7",
    "Re:",
    "   ",
]


def test_r1_hashed_rows_give_the_raw_chain_with_the_key(tmp_path: Path) -> None:
    db_path = tmp_path / "keyed.sqlite"
    _write_key(db_path)
    outcomes: set[str | None] = set()
    for current, row_subject in itertools.product(_CURRENT_SUBJECTS, _ROW_SUBJECTS):
        payloads = [*_baseline_payloads(), _candidate(row_subject)]
        raw = _chain(_engine(db_path, _legacy_rows(payloads, _raw)), subject=current)
        hashed = _chain(_engine(db_path, _legacy_rows(payloads, _hashed)), subject=current)
        assert hashed == raw, (current, row_subject)
        outcomes.add(raw)
    # The matrix holds cross-sender matches and misses, not only one kind.
    assert outcomes == {"2", "3"}


def test_r1_partly_scrubbed_window_counts_like_the_raw_one(tmp_path: Path) -> None:
    db_path = tmp_path / "keyed.sqlite"
    _write_key(db_path)
    payloads = [*_baseline_payloads(), *(_candidate(subject) for subject in _ROW_SUBJECTS)]
    mixed_rows = [
        row_raw if index % 2 else row_hashed
        for index, (row_raw, row_hashed) in enumerate(
            zip(_legacy_rows(payloads, _raw), _legacy_rows(payloads, _hashed))
        )
    ]
    for current in _CURRENT_SUBJECTS:
        raw = _chain(_engine(db_path, _legacy_rows(payloads, _raw)), subject=current)
        assert _chain(_engine(db_path, mixed_rows), subject=current) == raw, current


def test_r1_without_the_key_hashed_rows_match_only_the_same_sender(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "unkeyed.sqlite"
    rows = _legacy_rows(
        [*_baseline_payloads(), _candidate("Re: " + _SUBJECT)], _hashed
    )
    engine = _engine(db_path, rows)

    with caplog.at_level(logging.WARNING):
        first = _chain(engine, subject=_SUBJECT)
        second = _chain(engine, subject=_SUBJECT)
        # A blank current subject needs no comparison: every reminder counts.
        blank = _chain(engine, subject="  ")

    assert (first, second, blank) == ("2", "2", "3")
    assert len(_key_warnings(caplog)) == 1  # degraded state, logged once
    assert not subject_key_path(db_path).exists()  # readers never create the key

    _write_key(db_path)
    assert _chain(_engine(db_path, rows), subject=_SUBJECT) == "3"


def test_r1_raw_rows_need_no_key_and_log_nothing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "unkeyed.sqlite"
    rows = _legacy_rows([*_baseline_payloads(), _candidate("Re: " + _SUBJECT)], _raw)

    with caplog.at_level(logging.WARNING):
        assert _chain(_engine(db_path, rows), subject=_SUBJECT) == "3"

    assert _key_warnings(caplog) == []


def test_r1_scrubbed_database_keeps_reminder_chains(tmp_path: Path) -> None:
    db_path = _new_db(tmp_path, with_subject_key=True)
    with closing(sqlite3.connect(db_path)) as conn:
        _insert_baseline_chain(conn)
        for days_ago, subject in ((3, "RE:   reminder:   INVOICE 42 "), (4, "Another subject")):
            _insert_legacy_email_received(
                conn,
                ts=_NOW - timedelta(days=days_ago),
                payload={
                    "account_email": ACCOUNT,
                    "from_email": _OTHER_SENDER,
                    "subject": subject,
                    "mail_type": "PAYMENT_REMINDER",
                },
            )
        conn.commit()

    before = _reminder_chain_detail(db_path, subject=_SUBJECT, from_email=_SENDER)
    _scrub_legacy_in_place(db_path, subject_key=KEY)
    after = _reminder_chain_detail(db_path, subject=_SUBJECT, from_email=_SENDER)

    assert before == after == "3"
