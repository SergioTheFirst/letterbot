"""S2: config/ini_editor.py, the Settings page's line-preserving INI writer.

Only the edited lines change: comments, key order, unknown keys, other
sections, the UTF-8 BOM and each line's own line ending stay byte for byte.
Values are written as typed ("%" included). Writes are atomic and every write
first keeps a timestamped backup (newest 10 kept); restore backs up the
current file before copying the chosen backup back.
"""

from __future__ import annotations

import codecs
import os
from datetime import datetime
from pathlib import Path

import pytest

from mailbot_v26.config import ini_editor
from mailbot_v26.config.ini_editor import (
    AddSection,
    IniEditError,
    RemoveKey,
    RemoveSection,
    SetValue,
    apply_edits,
    file_etag,
    list_backups,
    restore_backup,
    write_with_backup,
)
from mailbot_v26.config.ini_utils import new_user_ini_parser

NOW = datetime(2026, 10, 20, 14, 30, 5)
ORIGINAL = (
    "; my mailboxes\r\n"
    "[work_mail]\r\n"
    "; Windows login: HQ\\owner\r\n"
    "login = worker@gmail.com\r\n"
    "password = old-secret\r\n"
    "host = imap.gmail.com\r\n"
    "custom_note = keep me\r\n"
    "\r\n"
    "; the bot\r\n"
    "[telegram]\r\n"
    "bot_token = 123:abc\r\n"
    "\r\n"
    "[cloudflare]\r\n"
    "account_id = cf\r\n"
)


def _parse(raw: bytes) -> dict[str, dict[str, str]]:
    parser = new_user_ini_parser()
    parser.read_string(raw.decode("utf-8-sig"))
    return {section: dict(parser.items(section, raw=True)) for section in parser.sections()}


def test_set_value_changes_only_that_line_and_keeps_bom_and_crlf() -> None:
    raw = codecs.BOM_UTF8 + ORIGINAL.encode("utf-8")

    result = apply_edits(raw, [SetValue("work_mail", "password", "new 50%off%(x)s")])

    expected = ORIGINAL.replace("password = old-secret", "password = new 50%off%(x)s")
    assert result == codecs.BOM_UTF8 + expected.encode("utf-8")
    assert _parse(result)["work_mail"]["password"] == "new 50%off%(x)s"


def test_set_value_keeps_the_key_spelling_and_delimiter_style() -> None:
    raw = b"[a]\nPassword=old\nhost:   h1\nempty =\n"

    result = apply_edits(
        raw,
        [SetValue("a", "password", "p2"), SetValue("a", "host", "h2"), SetValue("a", "empty", "v")],
    )

    assert result == b"[a]\nPassword=p2\nhost:   h2\nempty = v\n"


def test_set_value_replaces_a_multi_line_value_with_its_continuations() -> None:
    raw = b"[a]\nnote = first\n  second\n\n  third\nnext = 1\n"

    result = apply_edits(raw, [SetValue("a", "note", "one line")])

    assert result == b"[a]\nnote = one line\nnext = 1\n"


def test_new_key_goes_after_the_section_last_key_before_trailing_comments() -> None:
    raw = ORIGINAL.encode("utf-8")

    result = apply_edits(raw, [SetValue("work_mail", "telegram_chat_id", "272250747")])

    text = result.decode("utf-8")
    assert "custom_note = keep me\r\ntelegram_chat_id = 272250747\r\n\r\n; the bot\r\n" in text
    assert _parse(result)["work_mail"]["telegram_chat_id"] == "272250747"


def test_add_section_appends_with_the_file_newline_style() -> None:
    raw = b"[telegram]\r\nbot_token = t"  # no final newline

    result = apply_edits(
        raw,
        [AddSection("gmail_mail", (("login", "a@gmail.com"), ("port", "993")))],
    )

    assert result == (
        b"[telegram]\r\nbot_token = t\r\n\r\n[gmail_mail]\r\nlogin = a@gmail.com\r\nport = 993\r\n"
    )


def test_remove_section_keeps_the_next_section_comment_and_no_double_blank() -> None:
    raw = ORIGINAL.encode("utf-8")

    result = apply_edits(raw, [RemoveSection("work_mail")])

    assert result.decode("utf-8") == (
        "; my mailboxes\r\n"
        "\r\n"
        "; the bot\r\n"
        "[telegram]\r\n"
        "bot_token = 123:abc\r\n"
        "\r\n"
        "[cloudflare]\r\n"
        "account_id = cf\r\n"
    )


def test_remove_last_section_and_remove_key() -> None:
    raw = ORIGINAL.encode("utf-8")

    result = apply_edits(
        raw, [RemoveSection("cloudflare"), RemoveKey("work_mail", "custom_note")]
    )

    parsed = _parse(result)
    assert "cloudflare" not in parsed
    assert "custom_note" not in parsed["work_mail"]
    assert result.decode("utf-8").endswith("[telegram]\r\nbot_token = 123:abc\r\n")


def test_mixed_line_endings_stay_per_line() -> None:
    raw = b"[a]\r\nx = 1\ny = 2\r\n"

    assert apply_edits(raw, [SetValue("a", "y", "3")]) == b"[a]\r\nx = 1\ny = 3\r\n"


@pytest.mark.parametrize(
    "value",
    ["two\nlines", "carriage\rreturn", "nul\x00byte", " leading", "trailing ", "bell\x07"],
    ids=["newline", "cr", "nul", "leading_space", "trailing_space", "control"],
)
def test_values_that_cannot_round_trip_are_refused(value: str) -> None:
    with pytest.raises(IniEditError) as excinfo:
        apply_edits(ORIGINAL.encode("utf-8"), [SetValue("work_mail", "password", value)])

    assert excinfo.value.code == "bad_value"
    assert value.strip() not in str(excinfo.value)


@pytest.mark.parametrize(
    "edit",
    [
        SetValue("work_mail", "Password", "x"),
        SetValue("work mail", "password", "x"),
        SetValue("work_mail]\n[x", "password", "x"),
        AddSection("telegram", (("a", "b"),)),
    ],
    ids=["uppercase_key", "space_in_section", "header_injection", "existing_section"],
)
def test_bad_names_and_existing_sections_are_refused(edit: object) -> None:
    with pytest.raises(IniEditError):
        apply_edits(ORIGINAL.encode("utf-8"), [edit])


def test_unreadable_input_is_not_edited() -> None:
    with pytest.raises(UnicodeDecodeError):
        apply_edits("[a]\nx = пароль\n".encode("cp1251"), [SetValue("a", "x", "y")])


def test_etag_changes_with_the_bytes() -> None:
    assert file_etag(None) == "missing"
    assert file_etag(b"a") != file_etag(b"b")
    assert len(file_etag(b"a")) == 16


def test_write_with_backup_keeps_the_previous_bytes(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"old")

    name = write_with_backup(path, b"new", now=NOW)

    assert name == "accounts.ini.bak-20261020-143005"
    assert path.read_bytes() == b"new"
    assert (tmp_path / name).read_bytes() == b"old"
    assert write_with_backup(path, b"newer", now=NOW) == "accounts.ini.bak-20261020-143005-1"
    assert list_backups(path) == [
        "accounts.ini.bak-20261020-143005-1",
        "accounts.ini.bak-20261020-143005",
    ]


def test_write_without_a_previous_file_makes_no_backup(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"

    assert write_with_backup(path, b"first", now=NOW) is None
    assert path.read_bytes() == b"first"
    assert list_backups(path) == []


def test_backups_rotate_to_the_newest_ten(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"v0")
    for minute in range(12):
        write_with_backup(path, f"v{minute + 1}".encode(), now=NOW.replace(minute=minute))

    names = list_backups(path)
    assert len(names) == 10
    assert names[0] == "accounts.ini.bak-20261020-141105"
    assert names[-1] == "accounts.ini.bak-20261020-140205"
    assert (tmp_path / names[0]).read_bytes() == b"v11"


def test_list_backups_ignores_other_names(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"
    for name in (
        "accounts.ini.bak",
        "accounts.ini.bak-2026",
        "settings.ini.bak-20261020-143005",
        "accounts.ini.bak-20261020-143005.txt",
    ):
        (tmp_path / name).write_bytes(b"x")

    assert list_backups(path) == []


def test_backup_and_temp_names_are_marked_as_secret_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # .gitignore, the source bundle and the installer exclude these names:
    # they hold the same passwords and tokens as the live file.
    temp_names: list[str] = []
    real_mkstemp = ini_editor.tempfile.mkstemp

    def recording_mkstemp(*args: object, **kwargs: object):
        handle, name = real_mkstemp(*args, **kwargs)
        temp_names.append(Path(name).name)
        return handle, name

    monkeypatch.setattr(ini_editor.tempfile, "mkstemp", recording_mkstemp)
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"old")
    backups = [write_with_backup(path, b"new", now=NOW), write_with_backup(path, b"newer", now=NOW)]
    settings = tmp_path / "settings.ini"
    settings.write_bytes(b"old")
    backups.append(write_with_backup(settings, b"new", now=NOW))

    assert len(temp_names) == 6
    for name in [*backups, *temp_names, "keys.ini.bak", "config.ini.bak"]:
        assert ini_editor.is_secret_copy_name(name), name
    for name in ("accounts.ini", "settings.ini", "accounts.ini.example", "settings.ini.example", "notes.tmp"):
        assert not ini_editor.is_secret_copy_name(name), name


def test_failed_replace_keeps_the_original_and_leaves_no_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"original")

    def broken_replace(src: object, dst: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(ini_editor.os, "replace", broken_replace)
    with pytest.raises(OSError):
        write_with_backup(path, b"new", now=NOW)

    assert path.read_bytes() == b"original"
    leftovers = [item.name for item in tmp_path.iterdir() if item.suffix == ".tmp"]
    assert leftovers == []


def test_restore_backs_up_the_current_file_first(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"[a]\nx = 1\n")
    first = write_with_backup(path, b"[a]\nx = 2\n", now=NOW)

    current_backup = restore_backup(path, first, now=NOW.replace(minute=40))

    assert path.read_bytes() == b"[a]\nx = 1\n"
    assert current_backup == "accounts.ini.bak-20261020-144005"
    assert (tmp_path / current_backup).read_bytes() == b"[a]\nx = 2\n"


@pytest.mark.parametrize(
    "name",
    ["../accounts.ini.bak-20261020-143005", "accounts.ini", "keys.ini", "accounts.ini.bak-20261020-999999x"],
)
def test_restore_refuses_names_that_are_not_listed_backups(tmp_path: Path, name: str) -> None:
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"[a]\nx = 1\n")

    with pytest.raises(IniEditError) as excinfo:
        restore_backup(path, name, now=NOW)

    assert excinfo.value.code == "backup_not_found"
    assert path.read_bytes() == b"[a]\nx = 1\n"


def test_restore_refuses_an_unreadable_backup(tmp_path: Path) -> None:
    path = tmp_path / "accounts.ini"
    path.write_bytes(b"[a]\nx = 1\n")
    (tmp_path / "accounts.ini.bak-20261020-143005").write_bytes(b"no section header\n")

    with pytest.raises(IniEditError) as excinfo:
        restore_backup(path, "accounts.ini.bak-20261020-143005", now=NOW)

    assert excinfo.value.code == "backup_unreadable"
    assert path.read_bytes() == b"[a]\nx = 1\n"
    assert sorted(os.listdir(tmp_path)) == ["accounts.ini", "accounts.ini.bak-20261020-143005"]
