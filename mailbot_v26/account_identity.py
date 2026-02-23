from __future__ import annotations


def normalize_login(value: str | None) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    if "@" not in cleaned and ("\\" in cleaned or "/" in cleaned):
        cleaned = cleaned.replace("/", "\\")
    return cleaned.casefold()


def logins_match(left: str | None, right: str | None) -> bool:
    return normalize_login(left) == normalize_login(right)
