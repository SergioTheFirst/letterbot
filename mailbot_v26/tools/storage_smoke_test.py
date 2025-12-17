from __future__ import annotations

from pathlib import Path

from mailbot_v26.bot_core.storage import Storage


def main() -> None:
    db_path = Path(__file__).resolve().parents[1] / "data" / "test_mailbot.sqlite"
    if db_path.exists():
        db_path.unlink()

    storage = Storage(db_path)
    try:
        first_id = storage.upsert_email(
            account_email="user@example.com",
            uid=1,
            message_id="msg-1",
            from_email="sender@example.com",
            from_name="Sender",
            subject="Test",
            received_at="2024-01-01T00:00:00",
            attachments_count=2,
        )
        second_id = storage.upsert_email(
            account_email="user@example.com",
            uid=1,
            message_id="msg-1",
            from_email="sender@example.com",
            from_name="Sender",
            subject="Test",
            received_at="2024-01-01T00:00:00",
            attachments_count=2,
        )

        assert first_id == second_id, "Upsert must not duplicate emails"

        storage.enqueue_stage(first_id, "PARSE")
        storage.enqueue_stage(first_id, "PARSE")

        item = storage.claim_next(["PARSE"])
        assert item is not None, "Queue item should be claimed"
        assert item["email_id"] == first_id, "Claimed email id mismatch"

        storage.mark_done(item["queue_id"])
        nothing_left = storage.claim_next(["PARSE"])
        assert nothing_left is None, "Queue should be empty after mark_done"

        print("OK")
    finally:
        storage.close()
        if db_path.exists():
            db_path.unlink()


if __name__ == "__main__":
    main()
