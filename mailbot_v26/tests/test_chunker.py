from mailbot_v26.llm.chunker import chunk_text


def test_chunker_respects_overlap():
    text = "".join(str(i) for i in range(1000))
    chunks = chunk_text(text, size=100, overlap=10)
    assert len(chunks) > 5
    for idx in range(1, len(chunks)):
        assert chunks[idx-1][-10:] == chunks[idx][:10]


def test_chunker_handles_small_text():
    assert chunk_text("short", size=100, overlap=20) == ["short"]
