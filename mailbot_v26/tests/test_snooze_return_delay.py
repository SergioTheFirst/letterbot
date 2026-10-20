"""Smart Return delay text on due snooze reminders ("You snoozed this email ...").

"yesterday" is a calendar notion of the user's local day, so it must not be
derived from UTC dates: under UTC+3 the two calendars disagree between local
midnight (21:00 UTC) and UTC midnight. A snooze made less than an hour ago is
always reported in minutes, whichever midnight lies in between.

The clock and the local time zone are injected, so these tests do not depend
on the machine's time zone or on the time of day they run.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.start import _build_snooze_return_text

UTC_PLUS_3 = timezone(timedelta(hours=3))
EN_REMINDER = "\U0001f535 from sender@example.com:\n<b>Subject</b>"
RU_REMINDER = "\U0001f535 от sender@example.com:\n<b>Тема</b>"


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def _delay_line(storage: Storage, *, now: str, snoozed_at: str, locale: str) -> str:
    text = _build_snooze_return_text(
        storage,
        email_id=1,
        reminder_text=EN_REMINDER if locale == "en" else RU_REMINDER,
        snoozed_at_utc=_utc(snoozed_at).isoformat(),
        locale=locale,
        now_utc=_utc(now),
        local_tz=UTC_PLUS_3,
    )
    return text.split("\n", 1)[0]


@pytest.fixture()
def storage(tmp_path: Path) -> Storage:
    return Storage(tmp_path / "snooze.sqlite")


@pytest.mark.parametrize(
    ("now", "snoozed_at", "expected_en", "expected_ru"),
    [
        pytest.param(
            # 03:10 local; snoozed at 02:50 local, but 23:50 UTC the day before.
            "2026-10-20T00:10:00",
            "2026-10-19T23:50:00",
            "20 minutes ago",
            "20 минут назад",
            id="20-min-across-utc-midnight",
        ),
        pytest.param(
            # 00:10 local; snoozed at 23:50 local the day before (same UTC day).
            "2026-10-19T21:10:00",
            "2026-10-19T20:50:00",
            "20 minutes ago",
            "20 минут назад",
            id="20-min-across-local-midnight",
        ),
        pytest.param(
            # 01:30 local; snoozed at 23:30 local the day before, same UTC day.
            "2026-10-19T22:30:00",
            "2026-10-19T20:30:00",
            "yesterday",
            "вчера",
            id="local-yesterday-between-local-and-utc-midnight",
        ),
        pytest.param(
            # 04:00 local; snoozed at 00:30 local today, 21:30 UTC the day before.
            "2026-10-20T01:00:00",
            "2026-10-19T21:30:00",
            "3 hours ago",
            "3 часа назад",
            id="same-local-day-across-utc-midnight",
        ),
        pytest.param(
            # 20:00 local; snoozed at 10:00 local the same day.
            "2026-10-20T17:00:00",
            "2026-10-20T07:00:00",
            "10 hours ago",
            "10 часов назад",
            id="same-day-hours",
        ),
        pytest.param(
            # 10:00 local; snoozed at 09:00 local two days earlier.
            "2026-10-22T07:00:00",
            "2026-10-20T06:00:00",
            "2 days ago",
            "2 дня назад",
            id="two-days",
        ),
    ],
)
def test_snooze_delay_uses_local_calendar_and_elapsed_minutes(
    storage: Storage, now: str, snoozed_at: str, expected_en: str, expected_ru: str
) -> None:
    assert _delay_line(storage, now=now, snoozed_at=snoozed_at, locale="en") == (
        "⏰ You snoozed this email " + expected_en
    )
    assert _delay_line(storage, now=now, snoozed_at=snoozed_at, locale="ru") == (
        "⏰ Вы отложили это письмо " + expected_ru
    )


def test_snooze_delay_just_now_across_utc_midnight(storage: Storage) -> None:
    line = _delay_line(
        storage, now="2026-10-20T00:00:20", snoozed_at="2026-10-19T23:59:50", locale="en"
    )
    assert line == "⏰ You snoozed this email just now"


def test_snooze_delay_with_absurd_stored_date_still_renders(storage: Storage) -> None:
    # The machine-local conversion (local_tz=None) raises OSError on Windows
    # for such dates; the reminder must still go out.
    text = _build_snooze_return_text(
        storage,
        email_id=1,
        reminder_text=EN_REMINDER,
        snoozed_at_utc="0001-01-02T00:00:00+00:00",
        locale="en",
        now_utc=_utc("2026-10-20T00:10:00"),
    )
    first_line = text.split("\n", 1)[0]
    assert first_line.startswith("⏰ You snoozed this email ")
    assert first_line.endswith(" days ago")
