from types import SimpleNamespace

from mailbot_v26.ui import branding, i18n
from mailbot_v26.ui.branding import WATERMARK_LINE


def test_priority_ack_no_noisy_phrase() -> None:
    ack = i18n.t("inbound.priority_ack", priority="🔴")
    assert "Учту в качестве." not in ack


def test_append_watermark_respects_toggle(monkeypatch) -> None:
    monkeypatch.setattr(
        branding,
        "load_branding_config",
        lambda: SimpleNamespace(show_watermark=False),
    )
    branding.reset_branding_cache()

    assert branding.append_watermark("hello") == "hello"

    monkeypatch.setattr(
        branding,
        "load_branding_config",
        lambda: SimpleNamespace(show_watermark=True),
    )
    branding.reset_branding_cache()

    assert branding.append_watermark("hello") == f"hello\n{WATERMARK_LINE}"
