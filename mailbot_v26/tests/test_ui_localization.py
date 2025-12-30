from datetime import datetime

from mailbot_v26.pipeline import processor
from mailbot_v26.ui.i18n import humanize_mail_type, humanize_reason_codes


def test_humanize_mail_type_known_and_unknown() -> None:
    assert humanize_mail_type("INVOICE_FINAL", locale="ru") == "Счёт — финальный"
    assert humanize_mail_type("CUSTOM_UNKNOWN", locale="ru") == "CUSTOM_UNKNOWN"


def test_priority_explain_lines_hide_internal_codes() -> None:
    lines = processor._build_priority_explain_lines(  # type: ignore[attr-defined]
        mail_type="INVOICE_FINAL",
        mail_type_reasons=["mt.invoice.final.keyword=финальн"],
        priority_v2_result=None,
        commitments=[],
        received_at=datetime(2024, 1, 1),
    )

    combined = " ".join(lines).lower()
    assert "invoice" not in combined
    assert any("сч" in line.lower() for line in lines)
    assert humanize_reason_codes(["mt.invoice.final.keyword=финальн"], locale="ru")[0].startswith(
        "финальный"
    )
