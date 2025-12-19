import importlib


def test_pipeline_import_sanity() -> None:
    module = importlib.import_module("mailbot_v26.pipeline.processor")
    assert module is not None
