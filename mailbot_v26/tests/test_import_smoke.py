def test_import_smoke() -> None:
    import mailbot_v26
    from mailbot_v26.pipeline import processor
    from mailbot_v26.pipeline import stage_llm

    assert mailbot_v26 is not None
    assert processor is not None
    assert stage_llm is not None
