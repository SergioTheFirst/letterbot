# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from mailbot_v26.tools.windows_version_resource import ensure_windows_version_info

block_cipher = None

datas = [
    (str(Path("mailbot_v26/web_observability/templates")), "mailbot_v26/web_observability/templates"),
    (str(Path("mailbot_v26/web_observability/static")), "mailbot_v26/web_observability/static"),
    (str(Path("mailbot_v26/config/settings.ini.example")), "mailbot_v26/config"),
    (str(Path("mailbot_v26/config/accounts.ini.example")), "mailbot_v26/config"),
]

version_info_path = ensure_windows_version_info(Path("build/windows_version_info.txt"))

a = Analysis(
    ["mailbot_v26/__main__.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=[
        "flask",
        "flask.json",
        "flask.logging",
        "flask.sessions",
        "werkzeug",
        "werkzeug.serving",
        "werkzeug.routing",
        "werkzeug.exceptions",
        "jinja2",
        "jinja2.ext",
        "dotenv",
        "langdetect",
        "waitress",
        "nltk",
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Letterbot",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    version=str(version_info_path),
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name="Letterbot",
)
