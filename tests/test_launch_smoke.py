from importlib import import_module


def test_import_entrypoints():
    assert import_module("mailbot_v26")
    assert import_module("mailbot_v26.__main__")
    start = import_module("mailbot_v26.start")
    assert hasattr(start, "main")
