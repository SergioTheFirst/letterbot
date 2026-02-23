from __future__ import annotations

from mailbot_v26.account_identity import logins_match, normalize_login


def test_logins_match_windows_domain_username_case_insensitive() -> None:
    assert logins_match(r"HQ\MedvedevSS", r"hq\medvedevss")


def test_logins_match_normalizes_slash_direction_for_windows_login() -> None:
    assert logins_match("HQ/MedvedevSS", r"hq\medvedevss")


def test_normalize_login_preserves_email_shape_but_casefolds() -> None:
    assert normalize_login("  User@Example.COM ") == "user@example.com"
