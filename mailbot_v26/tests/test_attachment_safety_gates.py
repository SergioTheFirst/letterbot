from __future__ import annotations

from email.message import EmailMessage
from pathlib import Path
from unittest.mock import patch

from mailbot_v26.bot_core import pipeline as core_pipeline
from mailbot_v26.config_loader import BotConfig, GeneralConfig, KeysConfig, StorageConfig
from mailbot_v26.constants import MAX_CHARS_PER_ATTACHMENT, MAX_TOTAL_MAIL_BYTES
from mailbot_v26.pipeline import tg_renderer


def _build_config(tmp_path: Path) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=10,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
            admin_chat_id="",
        ),
        accounts=[],
        keys=KeysConfig(
            telegram_bot_token="token",
            cf_account_id="cf",
            cf_api_token="api",
        ),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def test_gate_oversize_attachment(tmp_path: Path) -> None:
    message = EmailMessage()
    message["Subject"] = "Oversize"
    message["From"] = "sender@example.com"
    message.set_content("Body")
    message.add_attachment(
        b"x",
        maintype="application",
        subtype="pdf",
        filename="huge.pdf",
    )
    attachment_part = message.get_payload()[-1]
    attachment_part["Content-Length"] = str(50 * 1024 * 1024)

    inbound = core_pipeline.parse_raw_email(message.as_bytes(), _build_config(tmp_path))

    assert inbound.attachments
    attachment = inbound.attachments[0]
    assert attachment.metadata["skipped_reason"] == "too_large"
    assert attachment.size_bytes == 50 * 1024 * 1024
    block = tg_renderer.format_attachments_block(
        [
            {
                "filename": attachment.filename,
                "size_bytes": attachment.size_bytes,
                "skipped_reason": attachment.metadata.get("skipped_reason"),
            }
        ]
    )
    assert "too large" in block
    assert "extraction disabled" in block


def test_gate_total_limit(tmp_path: Path) -> None:
    message = EmailMessage()
    message["Subject"] = "Total limit"
    message["From"] = "sender@example.com"
    message.set_content("Body")

    for idx in range(5):
        message.add_attachment(
            f"data-{idx}".encode(),
            maintype="application",
            subtype="pdf",
            filename=f"file-{idx}.pdf",
        )
        message.get_payload()[-1]["Content-Length"] = str(6 * 1024 * 1024)

    inbound = core_pipeline.parse_raw_email(message.as_bytes(), _build_config(tmp_path))

    assert len(inbound.attachments) == 5
    skipped = [att.metadata.get("skipped_reason") for att in inbound.attachments]
    assert skipped[:3] == [None, None, None]
    assert skipped[3:] == ["total_limit", "total_limit"]


def test_gate_extraction_truncation() -> None:
    with patch.object(
        core_pipeline,
        "extract_pdf_text",
        return_value=("Safe text " * 10_000),
    ):
        attachment = core_pipeline.Attachment(
            filename="sample.pdf",
            content=b"data",
            content_type="application/pdf",
            text="",
            size_bytes=100,
        )
        extracted = core_pipeline._extract_attachment_text(
            attachment,
            max_chars=MAX_CHARS_PER_ATTACHMENT,
            max_zip_uncompressed_bytes=MAX_TOTAL_MAIL_BYTES,
        )
    assert len(extracted) == MAX_CHARS_PER_ATTACHMENT


def test_binary_leak_hard_suppression() -> None:
    attachments = [
        {
            "filename": "leak.bin",
            "text": "data=b'\\x00\\x01\\x02'",
        },
        {
            "filename": "base64.txt",
            "text": "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=" * 3,
        },
    ]
    block = tg_renderer.format_attachments_block(attachments)

    assert "data=b'" not in block
    assert "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=" not in block
    assert "leak.bin" in block
    assert "base64.txt" in block
