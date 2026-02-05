---
name: windows-release-onefolder
description: "Windows one-folder release build with PyInstaller spec, dist layout, and tamper-evident manifest generation/verification. Use when changing build scripts, packaging, or integrity checks."
---

1) Build one-folder first and keep pyinstaller.spec as the canonical build definition.
2) Produce dist/MailBot layout: exe, config.example.yaml, quickstart, run_dist.bat, manifest.
3) Exclude user config.yaml from the manifest.
4) Startup warning is fail-open: warn but do not block execution.
5) Require unit tests for manifest functions and run `python -m compileall mailbot_v26` + `pytest -q`.
