import importlib


def test_import_mailbot_v26_package():
    assert importlib.import_module("mailbot_v26")


def test_import_mailbot_v26_main():
    assert importlib.import_module("mailbot_v26.__main__")


def test_import_mailbot_v26_start():
    assert importlib.import_module("mailbot_v26.start")
