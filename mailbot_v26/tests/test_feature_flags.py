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
    assert not flags.ENABLE_ANOMALY_ALERTS
    assert not flags.ENABLE_PREMIUM_PROCESSOR
    assert not flags.ENABLE_PREMIUM_CLARITY_V1
    assert not flags.ENABLE_UNCERTAINTY_QUEUE
    assert flags.AUTO_PRIORITY_CONFIDENCE_THRESHOLD == 0.6
    assert flags.ENABLE_PRIORITY_V2 is True


def test_flags_loaded_from_config(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """[features]
enable_auto_priority = true
enable_task_suggestions = false
enable_tg_editing = 1
enable_shadow_persistence = yes
enable_crm_diagnostics = on
enable_anomaly_alerts = true
enable_priority_v2 = false
enable_premium_processor = true
enable_premium_clarity_v1 = true
enable_uncertainty_queue = true
auto_priority_confidence_threshold = 0.8
""",
    )
    flags = FeatureFlags(tmp_path)
    assert flags.ENABLE_AUTO_PRIORITY is True
    assert flags.ENABLE_TASK_SUGGESTIONS is False
    assert flags.ENABLE_TG_EDITING is True
    assert flags.ENABLE_SHADOW_PERSISTENCE is True
    assert flags.ENABLE_CRM_DIAGNOSTICS is True
    assert flags.ENABLE_ANOMALY_ALERTS is True
    assert flags.ENABLE_PRIORITY_V2 is False
    assert flags.ENABLE_PREMIUM_PROCESSOR is True
    assert flags.ENABLE_PREMIUM_CLARITY_V1 is True
    assert flags.ENABLE_UNCERTAINTY_QUEUE is True
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


def test_support_alias_overrides_donate_enabled(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """[features]
donate_enabled = false
support = true
""",
    )
    flags = FeatureFlags(tmp_path)
    assert flags.DONATE_ENABLED is True


def test_support_section_enabled_overrides_features_flags(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """[features]
donate_enabled = false
support = false

[support]
enabled = true
""",
    )
    flags = FeatureFlags(tmp_path)
    assert flags.DONATE_ENABLED is True


def test_support_section_falls_back_to_legacy_donate_flag(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """[features]
donate_enabled = true
""",
    )
    flags = FeatureFlags(tmp_path)
    assert flags.DONATE_ENABLED is True


def test_digest_flags_loaded_from_settings_aliases_in_two_file_mode(tmp_path: Path) -> None:
    (tmp_path / "accounts.ini").write_text("[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8")
    (tmp_path / "settings.ini").write_text(
        """[features]
daily_digest_enabled = true
weekly_digest_enabled = true
""",
        encoding="utf-8",
    )

    flags = FeatureFlags(tmp_path)

    assert flags.ENABLE_DAILY_DIGEST is True
    assert flags.ENABLE_WEEKLY_DIGEST is True


def test_premium_processor_defaults_to_true_in_two_file_mode(tmp_path: Path) -> None:
    (tmp_path / "accounts.ini").write_text("[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8")
    (tmp_path / "settings.ini").write_text("[features]\nenable_daily_digest = true\n", encoding="utf-8")

    flags = FeatureFlags(tmp_path)

    assert flags.ENABLE_PREMIUM_PROCESSOR is True
