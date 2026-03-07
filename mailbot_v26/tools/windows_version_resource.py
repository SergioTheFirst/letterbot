from __future__ import annotations

from pathlib import Path

from mailbot_v26.version import get_version


def _numeric_version(version: str) -> str:
    core = version.split("-", 1)[0]
    parts = core.split(".")
    while len(parts) < 4:
        parts.append("0")
    return ".".join(parts[:4])


def render_windows_version_info(version: str | None = None) -> str:
    app_version = version or get_version()
    numeric = _numeric_version(app_version)
    escaped = app_version.replace("\\", "\\\\").replace("'", "\\'")
    return (
        "# UTF-8\n"
        "VSVersionInfo(\n"
        f"  ffi=FixedFileInfo(filevers=({numeric.replace('.', ', ')}), prodvers=({numeric.replace('.', ', ')}), "
        "mask=0x3f, flags=0x0, OS=0x40004, fileType=0x1, subtype=0x0, date=(0, 0)),\n"
        "  kids=[\n"
        "    StringFileInfo([\n"
        "      StringTable('040904B0', [\n"
        "        StringStruct('CompanyName', 'Letterbot'),\n"
        "        StringStruct('FileDescription', 'Letterbot'),\n"
        "        StringStruct('FileVersion', '"
        + escaped
        + "'),\n"
        "        StringStruct('InternalName', 'Letterbot'),\n"
        "        StringStruct('OriginalFilename', 'Letterbot.exe'),\n"
        "        StringStruct('ProductName', 'Letterbot'),\n"
        "        StringStruct('ProductVersion', '"
        + escaped
        + "')\n"
        "      ])\n"
        "    ]),\n"
        "    VarFileInfo([VarStruct('Translation', [1033, 1200])])\n"
        "  ]\n"
        ")\n"
    )


def ensure_windows_version_info(path: Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_windows_version_info(), encoding="utf-8")
    return target


__all__ = ["ensure_windows_version_info", "render_windows_version_info"]
