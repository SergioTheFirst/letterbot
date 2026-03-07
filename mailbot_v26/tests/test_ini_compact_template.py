from __future__ import annotations

from pathlib import Path

from mailbot_v26.tools.make_ini_compact import make_ini_compact


def test_compact_template_order_and_support_position(tmp_path: Path) -> None:
    source = tmp_path / "config.ini.example"
    source.write_text("[general]\ncheck_interval = 120\n", encoding="utf-8")
    target = tmp_path / "config.ini.compact.example"

    written = make_ini_compact(source, target)

    assert written.exists()
    lines = written.read_text(encoding="utf-8").splitlines()
    idx_general = lines.index("[general]")
    idx_storage = lines.index("[storage]")
    idx_advanced = lines.index("; Advanced / Rare / Debug")
    idx_features = lines.index("[features]")
    idx_support = lines.index("support = false")

    assert idx_general < idx_storage < idx_advanced < idx_features
    assert idx_support > idx_features
    assert idx_support >= len(lines) - 5


def test_repo_compact_template_exists() -> None:
    compact_path = Path("mailbot_v26/config/config.ini.compact.example")
    assert compact_path.exists()
    content = compact_path.read_text(encoding="utf-8")
    assert "[general]" in content
    assert "support = false" in content
