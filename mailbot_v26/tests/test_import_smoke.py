def test_import_smoke() -> None:
    import mailbot_v26
    from mailbot_v26.pipeline import processor
    from mailbot_v26.pipeline import stage_llm

    assert mailbot_v26 is not None
    assert processor is not None
    assert stage_llm is not None


def test_health_monitor_import() -> None:
    """Regression: health_monitor had bare 'from state_manager import' (no package prefix)."""
    import importlib

    mod = importlib.import_module("mailbot_v26.health_monitor")
    assert hasattr(mod, "MailHealthMonitor") or mod is not None  # module loads clean


def test_intelligence_package_import() -> None:
    """Regression: intelligence/__init__.py had bare 'from intelligence.priority_engine import'."""
    from mailbot_v26.intelligence import PriorityEngine

    assert PriorityEngine is not None
