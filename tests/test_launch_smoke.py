from importlib import import_module


def test_mailbot_launch_imports():
    modules = [
        "mailbot_v26.start",
        "mailbot_v26.imap_client",
        "mailbot_v26.pipeline.processor",
        "mailbot_v26.state_manager",
    ]
    for module_name in modules:
        import_module(module_name)
