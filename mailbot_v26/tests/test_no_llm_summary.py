from mailbot_v26.pipeline import processor


def test_build_no_llm_summary_uses_body_text_when_present() -> None:
    summary = processor._build_no_llm_summary(
        "Согласуйте акт до пятницы. Дедлайн 15 марта.",
        attachments=[],
        commitments_present=False,
    )

    assert summary
    assert "Согласуйте акт" in summary
