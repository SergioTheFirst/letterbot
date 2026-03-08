from datetime import date

from mailbot_v26.insights import commitment_tracker


class FixedDate(date):
    @classmethod
    def today(cls) -> "FixedDate":
        return cls(2025, 1, 10)


def test_detect_commitments_with_deadline(monkeypatch) -> None:
    monkeypatch.setattr(commitment_tracker, "date", FixedDate)
    text = "Вышлю отчет до 25.12.2025."

    commitments = commitment_tracker.detect_commitments(text)

    assert len(commitments) == 1
    commitment = commitments[0]
    assert commitment.deadline_iso == "2025-12-25"
    assert commitment.confidence == 0.9
    assert commitment.commitment_text == "Вышлю отчет до 25.12.2025"


def test_extract_deadline_relative_and_weekday(monkeypatch) -> None:
    monkeypatch.setattr(commitment_tracker, "date", FixedDate)

    assert commitment_tracker.extract_deadline_ru("Созвонимся завтра") == "2025-01-11"
    assert (
        commitment_tracker.extract_deadline_ru("В понедельник созвонимся")
        == "2025-01-13"
    )
    assert commitment_tracker.extract_deadline_ru("до 25.12.2025") == "2025-12-25"
