from pathlib import Path

from mailbot_v26.features.flags import FeatureFlags


def write_config(tmpdir: Path, content: str) -> Path:
    path = tmpdir / "config.ini"
    path.write_text(content, encoding="utf-8")
    return path


def test_flags_default_to_false_when_missing(tmp_path: Path) -> None:
    flags = FeatureFlags(tmp_path)
    assert not flags.ENABLE_AUTO_PRIORITY
    assert not flags.ENABLE_TASK_SUGGESTIONS
    assert not flags.ENABLE_TG_EDITING
    assert not flags.ENABLE_SHADOW_PERSISTENCE
    assert not flags.ENABLE_CRM_DIAGNOSTICS
    assert flags.AUTO_PRIORITY_CONFIDENCE_THRESHOLD == 0.6


def test_flags_loaded_from_config(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """[features]
enable_auto_priority = true
enable_task_suggestions = false
enable_tg_editing = 1
enable_shadow_persistence = yes
enable_crm_diagnostics = on
auto_priority_confidence_threshold = 0.8
""",
    )
    flags = FeatureFlags(tmp_path)
    assert flags.ENABLE_AUTO_PRIORITY is True
    assert flags.ENABLE_TASK_SUGGESTIONS is False
    assert flags.ENABLE_TG_EDITING is True
    assert flags.ENABLE_SHADOW_PERSISTENCE is True
    assert flags.ENABLE_CRM_DIAGNOSTICS is True
    assert flags.AUTO_PRIORITY_CONFIDENCE_THRESHOLD == 0.8


def test_invalid_flag_values_fallback_to_false(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """[features]
enable_auto_priority = notabool
""",
    )
    flags = FeatureFlags(tmp_path)
    assert flags.ENABLE_AUTO_PRIORITY is False
