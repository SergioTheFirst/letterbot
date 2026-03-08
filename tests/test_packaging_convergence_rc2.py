from __future__ import annotations

from pathlib import Path

from mailbot_v26.tools.windows_version_resource import render_windows_version_info
from mailbot_v26.version import get_version


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_pyinstaller_spec_uses_main_entrypoint_and_2file_datas() -> None:
    spec = _read("pyinstaller.spec")
    assert "mailbot_v26/__main__.py" in spec
    assert "Analysis(" in spec
    assert 'name="Letterbot"' in spec
    assert "settings.ini.example" in spec
    assert "accounts.ini.example" in spec
    assert "config.ini" not in spec
    assert "keys.ini" not in spec


def test_build_and_verify_scripts_use_letterbot_contract() -> None:
    build = _read("build_windows_onefolder.bat")
    verify_bat = _read("verify_dist.bat")
    verify_py = _read("mailbot_v26/tools/verify_dist.py")
    run_dist = _read("run_dist.bat")

    assert "dist\\Letterbot" in build
    assert "Letterbot.exe" in run_dist
    assert "settings.ini.example" in build
    assert "accounts.ini.example" in build
    assert "config.example.yaml" not in build
    assert "dist\\Letterbot" in verify_bat
    assert "dist/Letterbot" in verify_py
    assert "MailBot.exe" not in verify_py


def test_windows_version_resource_file_synced_with_version() -> None:
    version = get_version()
    info_path = Path("build/windows_version_info.txt")
    assert info_path.exists()
    text = info_path.read_text(encoding="utf-8")
    assert f"FileVersion', '{version}'" in text
    assert f"ProductVersion', '{version}'" in text
    assert text == render_windows_version_info(version)


def test_windows_docs_do_not_use_legacy_new_install_flow() -> None:
    docs = [
        "README_QUICKSTART_WINDOWS.md",
        "WINDOWS_QUICKSTART.md",
        "docs/WINDOWS_QUICKSTART.md",
        "docs/PRODUCTION_GATES.md",
        "docs/RELEASE_ARTIFACT_CONTRACT.md",
        "docs/RELEASE_CHECKLIST_WINDOWS.md",
        "docs/SMARTSCREEN.md",
        "docs/SMOKE_TESTS_WINDOWS.md",
    ]
    forbidden = (
        "copy config.example.yaml to config.yaml",
        "config.example.yaml",
        "dist/MailBot",
        "MailBot.exe",
    )
    for path in docs:
        text = _read(path)
        lowered = text.lower()
        for token in forbidden:
            assert (
                token.lower() not in lowered
            ), f"{path} contains legacy token: {token}"

    troubleshooting = _read("docs/TROUBLESHOOTING_WINDOWS.md")
    assert "legacy" in troubleshooting.lower()
