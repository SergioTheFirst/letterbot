from mailbot_v26.llm.summarizer import LLMSummarizer


def test_summarizer_fallback_without_llm():
    text = "abc" * 300
    summarizer = LLMSummarizer(None)
    result = summarizer.summarize_email(text)
    assert result.startswith("abc")
    assert len(result) <= 603


def test_summarizer_handles_llm_failure():
    def failing_call(prompt: str) -> str:
        raise RuntimeError("llm unavailable")

    summarizer = LLMSummarizer(failing_call)
    result = summarizer.summarize_email("Important notice about payment")
    assert "Important" in result
    assert "..." in result or len(result) <= 600


def test_summarizer_attachment_fallback():
    summarizer = LLMSummarizer(None)
    text = "data" * 200
    result = summarizer.summarize_attachment(text, kind="PDF")
    assert result.startswith("data")
    assert len(result) <= 603
