# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

block_cipher = None

datas = [
    (str(Path("mailbot_v26/web_observability/templates")), "mailbot_v26/web_observability/templates"),
    (str(Path("mailbot_v26/web_observability/static")), "mailbot_v26/web_observability/static"),
    (str(Path("mailbot_v26/config/config.ini")), "mailbot_v26/config"),
    (str(Path("mailbot_v26/config/keys.ini")), "mailbot_v26/config"),
    (str(Path("mailbot_v26/config/accounts.ini")), "mailbot_v26/config"),
]

a = Analysis(
    ["mailbot_v26/start.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=[],
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
    name="MailBot",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name="MailBot",
)
