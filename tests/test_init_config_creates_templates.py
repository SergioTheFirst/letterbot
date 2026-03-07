from pathlib import Path

from mailbot_v26.tools.config_bootstrap import init_config


def test_init_config_creates_templates(tmp_path: Path) -> None:
    result = init_config(tmp_path)

    assert (tmp_path / "settings.ini").exists()
    assert (tmp_path / "accounts.ini").exists()
    assert result["created"]
    assert not (tmp_path / "settings.ini.example").exists()

    accounts_content = (tmp_path / "accounts.ini").read_text(encoding="utf-8")
    assert "account_id rules" in accounts_content


def test_init_config_writes_examples_if_existing(tmp_path: Path) -> None:
    existing = tmp_path / "settings.ini"
    existing.write_text("existing", encoding="utf-8")

    init_config(tmp_path)

    assert existing.read_text(encoding="utf-8") == "existing"
    assert (tmp_path / "settings.ini.example").exists()
