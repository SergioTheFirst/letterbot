# Security Policy

## Reporting a vulnerability

- Do not open a public GitHub issue for a suspected security problem.
- Report the issue privately to the maintainer using the contact channel documented in the repository profile or release notes.
- Include a minimal reproduction, affected version, and whether credentials or private data may have been exposed.

## What is in scope

- Credential leaks or unsafe handling of tokens, passwords, API keys, or IMAP secrets
- Remote code execution, privilege escalation, or unsafe file-write behavior
- Authentication or authorization bypass in the web cockpit
- Dangerous data-loss paths in cleanup, retention, or release tooling

## What is not a security issue

- General setup questions or support requests
- Feature requests
- Performance issues without a confidentiality, integrity, or availability impact
- Local placeholder/example credentials such as `CHANGE_ME`

## Local secrets on disk

- `accounts.ini`, `settings.ini` and `keys.ini` hold passwords and tokens in plain text. They are git-ignored and never go into the source bundle or the installer.
- Each save on the Settings page first keeps the previous file next to it as `<name>.ini.bak-YYYYMMDD-HHMMSS` (the newest 10 per file); `python -m mailbot_v26 migrate-config` keeps `<name>.ini.bak`. These copies hold the same secrets: old and new mailbox passwords, the bot token and earlier dashboard passwords. They are git-ignored (`*.ini.bak`, `*.ini.bak-*`, and `.*.ini.*.tmp` for a write interrupted by a crash) and excluded from the source bundle and the installer.
- After rotating a leaked secret, delete the backups that still contain it.

## Secret exposure response

- Rotate or revoke any real leaked credential outside this PR.
- Do not rely on masking alone if a real secret was committed.
- Git history cleanup can be handled separately after rotation/revocation.
